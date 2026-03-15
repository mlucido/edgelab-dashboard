"""
Tests for the EdgeLab execution engine.

Imports from src.execution.autonomous_trader and src.execution.circuit_breaker.
autonomous_trader may be absent (built by parallel agent) — those tests are
skipped with pytest.mark.skipif when the import fails.
circuit_breaker is present and tested fully.
"""
from __future__ import annotations

import json
import sys
import os
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------------------------------------------------------------------------
# Optional import — autonomous_trader may not exist yet
# ---------------------------------------------------------------------------

try:
    from src.execution import autonomous_trader as _at_module
    HAS_AUTONOMOUS_TRADER = True
except ImportError:
    HAS_AUTONOMOUS_TRADER = False

_skip_trader = pytest.mark.skipif(
    not HAS_AUTONOMOUS_TRADER,
    reason="src.execution.autonomous_trader not yet built",
)

# Circuit breaker is always present
from src.execution.circuit_breaker import (
    DAILY_LOSS_LIMIT,
    WIN_RATE_MIN,
    WIN_RATE_LOOKBACK,
    CONSECUTIVE_LOSS_LIMIT,
    STRATEGY_PAUSE_SECONDS,
    API_ERROR_WINDOW,
    check_all,
    check_strategy_allowed,
    record_trade_outcome,
    record_api_error,
    reset_all,
    reset_strategy,
    get_status,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_redis_mock(store: dict | None = None) -> AsyncMock:
    """
    Build a minimal async Redis mock backed by an in-memory dict.
    Supports: get, set, delete.
    """
    db: dict[str, str] = {} if store is None else dict(store)

    async def _get(key: str) -> str | None:
        return db.get(key)

    async def _set(key: str, value: str) -> None:
        db[key] = str(value)

    async def _delete(*keys: str) -> None:
        for k in keys:
            db.pop(k, None)

    async def _aclose() -> None:
        pass

    mock = AsyncMock()
    mock.get = AsyncMock(side_effect=_get)
    mock.set = AsyncMock(side_effect=_set)
    mock.delete = AsyncMock(side_effect=_delete)
    mock.aclose = AsyncMock(side_effect=_aclose)
    # Attach the dict so tests can inspect state
    mock._db = db
    return mock


# ---------------------------------------------------------------------------
# Circuit breaker — basic state tests
# ---------------------------------------------------------------------------

class TestCircuitBreakerGlobalPause:
    @pytest.mark.asyncio
    async def test_global_pause_blocks_trading(self):
        r = _make_redis_mock({"edgelab:cb:global_pause": "1",
                               "edgelab:cb:global_pause_reason": "Manual stop"})
        can_trade, reason = await check_all(redis=r)
        assert not can_trade
        assert "Manual stop" in reason

    @pytest.mark.asyncio
    async def test_no_pause_allows_trading(self):
        r = _make_redis_mock()
        can_trade, reason = await check_all(redis=r)
        assert can_trade
        assert reason == "ok"

    @pytest.mark.asyncio
    async def test_reset_all_clears_pause(self):
        r = _make_redis_mock({"edgelab:cb:global_pause": "1"})
        await reset_all(pause=False, redis=r)
        # After reset the key should be gone
        val = await r.get("edgelab:cb:global_pause")
        assert val is None

    @pytest.mark.asyncio
    async def test_reset_all_sets_pause(self):
        r = _make_redis_mock()
        await reset_all(pause=True, redis=r)
        val = await r.get("edgelab:cb:global_pause")
        assert val == "1"


class TestCircuitBreakerDailyPnL:
    @pytest.mark.asyncio
    async def test_daily_loss_limit_fires(self):
        r = _make_redis_mock({"edgelab:cb:daily_pnl": str(DAILY_LOSS_LIMIT - 1)})
        can_trade, reason = await check_all(redis=r)
        assert not can_trade
        assert "daily" in reason.lower() or "p&l" in reason.lower() or "pnl" in reason.lower()

    @pytest.mark.asyncio
    async def test_daily_pnl_ok(self):
        r = _make_redis_mock({"edgelab:cb:daily_pnl": "-50.00"})
        can_trade, _ = await check_all(redis=r)
        assert can_trade

    @pytest.mark.asyncio
    async def test_daily_pnl_exactly_at_limit(self):
        # Exactly at limit is still allowed (< not <=)
        r = _make_redis_mock({"edgelab:cb:daily_pnl": str(DAILY_LOSS_LIMIT)})
        can_trade, _ = await check_all(redis=r)
        assert can_trade


class TestCircuitBreakerWinRate:
    @pytest.mark.asyncio
    async def test_low_win_rate_triggers(self):
        # 8 wins / 20 = 40% — below 45% threshold
        outcomes = [1] * 8 + [0] * 12
        r = _make_redis_mock({"edgelab:cb:trade_outcomes": json.dumps(outcomes)})
        can_trade, reason = await check_all(redis=r)
        assert not can_trade
        assert "win rate" in reason.lower() or "calibration" in reason.lower()

    @pytest.mark.asyncio
    async def test_adequate_win_rate_ok(self):
        # 10 wins / 20 = 50% — above 45% threshold
        outcomes = [1] * 10 + [0] * 10
        r = _make_redis_mock({"edgelab:cb:trade_outcomes": json.dumps(outcomes)})
        can_trade, _ = await check_all(redis=r)
        assert can_trade

    @pytest.mark.asyncio
    async def test_fewer_than_lookback_skips_check(self):
        # Only 5 trades — not enough to evaluate win rate
        outcomes = [0] * 5
        r = _make_redis_mock({"edgelab:cb:trade_outcomes": json.dumps(outcomes)})
        can_trade, _ = await check_all(redis=r)
        assert can_trade


class TestCircuitBreakerApiErrors:
    @pytest.mark.asyncio
    async def test_api_error_spike_triggers(self):
        now = time.time()
        # 10 errors within the last 10 minutes
        errors = [now - i * 30 for i in range(10)]
        r = _make_redis_mock({"edgelab:cb:api_errors": json.dumps(errors)})
        can_trade, reason = await check_all(redis=r)
        assert not can_trade
        assert "api error" in reason.lower()

    @pytest.mark.asyncio
    async def test_old_api_errors_ignored(self):
        now = time.time()
        # 10 errors but all older than the window
        errors = [now - API_ERROR_WINDOW - i * 60 for i in range(10)]
        r = _make_redis_mock({"edgelab:cb:api_errors": json.dumps(errors)})
        can_trade, _ = await check_all(redis=r)
        assert can_trade

    @pytest.mark.asyncio
    async def test_few_api_errors_ok(self):
        now = time.time()
        errors = [now - 30, now - 60]
        r = _make_redis_mock({"edgelab:cb:api_errors": json.dumps(errors)})
        can_trade, _ = await check_all(redis=r)
        assert can_trade


# ---------------------------------------------------------------------------
# Strategy-level pause
# ---------------------------------------------------------------------------

class TestStrategyPause:
    @pytest.mark.asyncio
    async def test_paused_strategy_blocked(self):
        paused = {"momentum": time.time() + 3600}
        r = _make_redis_mock({"edgelab:cb:strategy_paused": json.dumps(paused)})
        allowed, reason = await check_strategy_allowed("momentum", redis=r)
        assert not allowed
        assert "momentum" in reason

    @pytest.mark.asyncio
    async def test_expired_pause_allowed(self):
        paused = {"momentum": time.time() - 1}  # already expired
        r = _make_redis_mock({"edgelab:cb:strategy_paused": json.dumps(paused)})
        allowed, _ = await check_strategy_allowed("momentum", redis=r)
        assert allowed

    @pytest.mark.asyncio
    async def test_different_strategy_not_blocked(self):
        paused = {"momentum": time.time() + 3600}
        r = _make_redis_mock({"edgelab:cb:strategy_paused": json.dumps(paused)})
        allowed, _ = await check_strategy_allowed("threshold", redis=r)
        assert allowed

    @pytest.mark.asyncio
    async def test_consecutive_losses_auto_pause(self):
        r = _make_redis_mock()
        strategy = "threshold"
        # Record CONSECUTIVE_LOSS_LIMIT losses in a row
        for _ in range(CONSECUTIVE_LOSS_LIMIT):
            await record_trade_outcome(win=False, strategy=strategy, pnl=-10.0, redis=r)
        # Strategy should now be paused
        allowed, reason = await check_strategy_allowed(strategy, redis=r)
        assert not allowed
        assert strategy in reason

    @pytest.mark.asyncio
    async def test_win_resets_consecutive_losses(self):
        r = _make_redis_mock()
        strategy = "arb"
        # 2 losses then a win — should NOT trigger pause
        await record_trade_outcome(win=False, strategy=strategy, pnl=-5.0, redis=r)
        await record_trade_outcome(win=False, strategy=strategy, pnl=-5.0, redis=r)
        await record_trade_outcome(win=True, strategy=strategy, pnl=10.0, redis=r)
        # Should be allowed
        allowed, _ = await check_strategy_allowed(strategy, redis=r)
        assert allowed

    @pytest.mark.asyncio
    async def test_reset_strategy_unpauses(self):
        paused = {"momentum": time.time() + 3600}
        r = _make_redis_mock({"edgelab:cb:strategy_paused": json.dumps(paused)})
        await reset_strategy("momentum", redis=r)
        allowed, _ = await check_strategy_allowed("momentum", redis=r)
        assert allowed


# ---------------------------------------------------------------------------
# Record API error
# ---------------------------------------------------------------------------

class TestRecordApiError:
    @pytest.mark.asyncio
    async def test_records_timestamp(self):
        r = _make_redis_mock()
        before = time.time()
        await record_api_error(redis=r)
        raw = await r.get("edgelab:cb:api_errors")
        errors = json.loads(raw)
        assert len(errors) == 1
        assert errors[0] >= before

    @pytest.mark.asyncio
    async def test_accumulates_multiple_errors(self):
        r = _make_redis_mock()
        for _ in range(5):
            await record_api_error(redis=r)
        raw = await r.get("edgelab:cb:api_errors")
        errors = json.loads(raw)
        assert len(errors) == 5


# ---------------------------------------------------------------------------
# get_status
# ---------------------------------------------------------------------------

class TestGetStatus:
    @pytest.mark.asyncio
    async def test_status_keys_present(self):
        r = _make_redis_mock()
        status = await get_status(redis=r)
        required_keys = {
            "can_trade", "global_pause", "daily_pnl",
            "win_rate", "paused_strategies", "api_errors_last_10min",
        }
        assert required_keys.issubset(status.keys())

    @pytest.mark.asyncio
    async def test_status_can_trade_true_when_clean(self):
        r = _make_redis_mock()
        status = await get_status(redis=r)
        assert status["can_trade"] is True
        assert status["global_pause"] is False

    @pytest.mark.asyncio
    async def test_status_reflects_pause(self):
        r = _make_redis_mock({"edgelab:cb:global_pause": "1",
                               "edgelab:cb:global_pause_reason": "Test halt"})
        status = await get_status(redis=r)
        assert status["can_trade"] is False
        assert status["global_pause"] is True


# ---------------------------------------------------------------------------
# Integration: full pipeline with mocked Redis
# ---------------------------------------------------------------------------

class TestCircuitBreakerIntegration:
    @pytest.mark.asyncio
    async def test_full_trade_cycle(self):
        """Record wins and losses, verify circuit breaker state reflects them.

        Constraints:
        - Max 10 trades/hour rate limit
        - Max 20% drawdown from peak
        8 wins * $5 = $40 peak. 1 loss * $5 = -$5 → $35, drawdown=12.5% → OK.
        9 total trades < 10/hr rate limit.
        """
        r = _make_redis_mock()

        for i in range(8):
            await record_trade_outcome(win=True, strategy="momentum", pnl=5.0, redis=r)
        for i in range(1):
            await record_trade_outcome(win=False, strategy="momentum", pnl=-5.0, redis=r)

        can_trade, _ = await check_all(redis=r)
        assert can_trade

    @pytest.mark.asyncio
    async def test_daily_pnl_accumulates(self):
        r = _make_redis_mock()
        await record_trade_outcome(win=True, strategy="arb", pnl=50.0, redis=r)
        await record_trade_outcome(win=True, strategy="arb", pnl=30.0, redis=r)
        await record_trade_outcome(win=False, strategy="arb", pnl=-10.0, redis=r)

        raw = await r.get("edgelab:cb:daily_pnl")
        total = float(raw)
        assert abs(total - 70.0) < 0.01

    @pytest.mark.asyncio
    async def test_losing_streak_triggers_strategy_pause_and_resets(self):
        r = _make_redis_mock()
        strategy = "threshold"

        for _ in range(CONSECUTIVE_LOSS_LIMIT):
            await record_trade_outcome(win=False, strategy=strategy, pnl=-5.0, redis=r)

        # Strategy paused
        allowed, _ = await check_strategy_allowed(strategy, redis=r)
        assert not allowed

        # Manual reset restores it
        await reset_strategy(strategy, redis=r)
        allowed, _ = await check_strategy_allowed(strategy, redis=r)
        assert allowed


# ---------------------------------------------------------------------------
# autonomous_trader — import-guarded tests
# ---------------------------------------------------------------------------

@_skip_trader
class TestAutonomousTrader:
    """
    Tests for src.execution.autonomous_trader.
    Skipped when the module is not yet present.
    """

    def test_module_importable(self):
        assert HAS_AUTONOMOUS_TRADER

    def test_trade_quality_filter_pass(self):
        """High-edge opportunity should pass the quality filter."""
        from src.execution.autonomous_trader import check_trade_quality  # type: ignore
        opp = {
            "strategy": "threshold",
            "market_id": "TEST-001",
            "current_price": 0.90,
            "implied_true_prob": 0.95,
            "days_to_resolution": 7,
            "liquidity": 5000,
        }
        passes, reason = check_trade_quality(opp)
        assert passes, f"Expected pass, got reason: {reason}"

    def test_trade_quality_filter_fail_negative_ev(self):
        """Opportunity with negative EV should be rejected."""
        from src.execution.autonomous_trader import check_trade_quality  # type: ignore
        opp = {
            "strategy": "threshold",
            "market_id": "TEST-002",
            "current_price": 0.97,
            "implied_true_prob": 0.90,
            "days_to_resolution": 7,
            "liquidity": 5000,
        }
        passes, reason = check_trade_quality(opp)
        assert not passes, f"Expected fail, but got: passes=True reason={reason}"

    def test_calculate_position_size_returns_valid(self):
        """Position sizing should return a valid dollar size."""
        from src.execution.autonomous_trader import calculate_position_size  # type: ignore
        opp = {
            "strategy": "threshold",
            "market_id": "TEST-003",
            "current_price": 0.90,
            "implied_true_prob": 0.95,
            "days_to_resolution": 7,
            "liquidity": 5000,
        }
        size, reason = calculate_position_size(opp, capital=10000)
        assert isinstance(size, float)
        assert size >= 0
        assert isinstance(reason, str)

    def test_check_portfolio_exposure_respects_limits(self):
        """Portfolio exposure check should block when fully deployed."""
        from src.execution.autonomous_trader import check_portfolio_exposure  # type: ignore
        opp = {
            "strategy": "threshold",
            "market_id": "TEST-004",
            "current_price": 0.85,
            "implied_true_prob": 0.92,
            "days_to_resolution": 7,
            "liquidity": 5000,
            "category": "Politics",
        }
        # Capital fully deployed — no room for more
        portfolio = {
            "deployed_capital": 9999.0,
            "category_exposure": {},
            "market_exposure": {},
            "open_position_count": 5,
        }
        passes, reason = check_portfolio_exposure(opp, 50.0, portfolio, capital=10000)
        assert not passes, f"Expected block, got: {reason}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
