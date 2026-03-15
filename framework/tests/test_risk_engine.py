"""
Tests for framework/risk_engine.py
-----------------------------------
Run with: python3 -m pytest framework/tests/test_risk_engine.py -v
"""

from __future__ import annotations

import asyncio
import json
import uuid

import pytest

from framework.risk_engine import (
    BankrollManager,
    KellyCalculator,
    RiskEngine,
    get_redis,
)

# ─── pytest-asyncio config ────────────────────────────────────────────────────

pytestmark = pytest.mark.asyncio

TEST_BANKROLL = 10_000.0
KEY_PREFIX = "edgelab:risk:"


# ─── Helpers ──────────────────────────────────────────────────────────────────

async def _flush_keys():
    r = await get_redis()
    keys = await r.keys(f"{KEY_PREFIX}*")
    if keys:
        await r.delete(*keys)


async def _make_engine() -> RiskEngine:
    e = RiskEngine()
    await e.set_daily_bankroll(TEST_BANKROLL)
    return e


async def _make_mgr() -> BankrollManager:
    engine = await _make_engine()
    return BankrollManager(engine)


# ─── KellyCalculator tests ────────────────────────────────────────────────────

class TestKellyCalculator:

    def setup_method(self):
        self.kelly = KellyCalculator()

    def test_never_exceeds_20pct_bankroll(self):
        """Even with edge=1.0, bet size must not exceed 20% of bankroll."""
        bet = self.kelly.fractional_kelly(edge=1.0, odds=2.0, bankroll=10_000.0)
        assert bet <= 10_000.0 * 0.20, f"Bet ${bet} exceeds 20% cap"

    def test_minimum_bet_is_one_dollar(self):
        """With near-zero edge, bet must still be at least $1."""
        bet = self.kelly.fractional_kelly(edge=0.0001, odds=1.0, bankroll=10_000.0)
        assert bet >= 1.0, f"Bet ${bet} is below $1 floor"

    def test_zero_edge_returns_minimum(self):
        """Zero edge → minimum $1 bet."""
        bet = self.kelly.fractional_kelly(edge=0.0, odds=1.0, bankroll=10_000.0)
        assert bet == 1.0

    def test_negative_edge_returns_minimum(self):
        """Negative edge → no bet, return $1 floor."""
        bet = self.kelly.fractional_kelly(edge=-0.5, odds=1.0, bankroll=10_000.0)
        assert bet == 1.0

    def test_normal_edge_within_bounds(self):
        """Moderate edge produces a bet within [1, 20% bankroll]."""
        bet = self.kelly.fractional_kelly(edge=0.1, odds=1.0, bankroll=10_000.0, fraction=0.25)
        assert 1.0 <= bet <= 2_000.0

    def test_large_bankroll_capped(self):
        """Large bankroll with high edge is still capped at 20%."""
        bet = self.kelly.fractional_kelly(edge=0.9, odds=3.0, bankroll=1_000_000.0)
        assert bet <= 1_000_000.0 * 0.20

    def test_small_bankroll_floored(self):
        """Very small bankroll still returns at least $1."""
        bet = self.kelly.fractional_kelly(edge=0.05, odds=1.0, bankroll=5.0)
        assert bet >= 1.0


# ─── Circuit breaker tests ────────────────────────────────────────────────────

class TestCircuitBreaker:

    async def test_circuit_breaker_triggers(self):
        """Circuit breaker fires when daily_pnl < -15% of bankroll."""
        await _flush_keys()
        engine = await _make_engine()
        r = await get_redis()
        await r.set(f"{KEY_PREFIX}daily_pnl", str(-1_600.0))
        allowed, reason = await engine.check_trade_allowed("test_strategy", 100.0)
        assert not allowed
        assert "Circuit breaker" in reason

    async def test_circuit_breaker_does_not_trigger_with_small_loss(self):
        """Small loss (-5%) does not trigger circuit breaker."""
        await _flush_keys()
        engine = await _make_engine()
        r = await get_redis()
        await r.set(f"{KEY_PREFIX}daily_pnl", str(-500.0))
        allowed, _ = await engine.check_trade_allowed("test_strategy", 100.0)
        assert allowed

    async def test_open_position_cap(self):
        """8 open positions prevents further trades."""
        await _flush_keys()
        engine = await _make_engine()
        r = await get_redis()
        positions = [
            {
                "trade_id": str(uuid.uuid4()),
                "strategy": "strat_a",
                "market_id": f"mkt_{i}",
                "size": 100.0,
                "entry": 0.5,
                "opened_at": "2026-01-01T00:00:00+00:00",
            }
            for i in range(8)
        ]
        await r.set(f"{KEY_PREFIX}open_positions", json.dumps(positions))
        allowed, reason = await engine.check_trade_allowed("strat_b", 50.0)
        assert not allowed
        assert "cap" in reason.lower() or "8" in reason

    async def test_strategy_allocation_cap(self):
        """Strategy with >25% bankroll already open is blocked."""
        await _flush_keys()
        engine = await _make_engine()
        r = await get_redis()
        positions = [
            {
                "trade_id": str(uuid.uuid4()),
                "strategy": "strat_x",
                "market_id": "mkt_0",
                "size": 2_500.0,
                "entry": 0.5,
                "opened_at": "2026-01-01T00:00:00+00:00",
            }
        ]
        await r.set(f"{KEY_PREFIX}open_positions", json.dumps(positions))
        allowed, reason = await engine.check_trade_allowed("strat_x", 1.0)
        assert not allowed
        assert "25%" in reason or "allocation" in reason.lower()

    async def test_strategy_allocation_under_cap_allowed(self):
        """Strategy with <25% bankroll allocated is allowed."""
        await _flush_keys()
        engine = await _make_engine()
        r = await get_redis()
        positions = [
            {
                "trade_id": str(uuid.uuid4()),
                "strategy": "strat_x",
                "market_id": "mkt_0",
                "size": 1_000.0,
                "entry": 0.5,
                "opened_at": "2026-01-01T00:00:00+00:00",
            }
        ]
        await r.set(f"{KEY_PREFIX}open_positions", json.dumps(positions))
        allowed, _ = await engine.check_trade_allowed("strat_x", 500.0)
        assert allowed


# ─── BankrollManager allocation caps/floors ───────────────────────────────────

class TestBankrollManager:

    async def test_allocation_floor(self):
        """Each active established strategy gets at least 5%."""
        await _flush_keys()
        mgr = await _make_mgr()
        perfs = {
            "strat_a": {"sharpe": 0.0, "trade_count": 50, "active": True},
            "strat_b": {"sharpe": 0.0, "trade_count": 50, "active": True},
            "strat_c": {"sharpe": 0.0, "trade_count": 50, "active": True},
        }
        allocs = await mgr.rebalance(perfs)
        for name, amt in allocs.items():
            assert amt >= TEST_BANKROLL * 0.05 - 0.01, (
                f"{name} allocation ${amt} below 5% floor"
            )

    async def test_allocation_cap(self):
        """No strategy should receive more than 30% of bankroll."""
        await _flush_keys()
        mgr = await _make_mgr()
        perfs = {
            "dominant": {"sharpe": 999.0, "trade_count": 100, "active": True},
            "weak_a": {"sharpe": 0.01, "trade_count": 50, "active": True},
            "weak_b": {"sharpe": 0.01, "trade_count": 50, "active": True},
        }
        allocs = await mgr.rebalance(perfs)
        for name, amt in allocs.items():
            assert amt <= TEST_BANKROLL * 0.30 + 0.01, (
                f"{name} allocation ${amt} exceeds 30% cap"
            )

    async def test_new_strategy_gets_8pct(self):
        """New strategies (<10 trades) receive exactly 8% each."""
        await _flush_keys()
        mgr = await _make_mgr()
        perfs = {
            "new_strat": {"sharpe": 0.5, "trade_count": 3, "active": True},
        }
        allocs = await mgr.rebalance(perfs)
        assert "new_strat" in allocs
        expected = TEST_BANKROLL * 0.08
        assert abs(allocs["new_strat"] - expected) < 0.01, (
            f"New strategy got ${allocs['new_strat']}, expected ${expected}"
        )

    async def test_get_allocation_returns_default_when_unset(self):
        """get_allocation returns a sensible default when no rebalance has run."""
        await _flush_keys()
        mgr = await _make_mgr()
        alloc = await mgr.get_allocation("unknown_strategy")
        assert alloc >= 0.0

    async def test_allocations_persist_in_redis(self):
        """Allocations written by rebalance() are retrievable via get_allocation()."""
        await _flush_keys()
        mgr = await _make_mgr()
        perfs = {
            "strat_z": {"sharpe": 1.0, "trade_count": 20, "active": True},
        }
        allocs = await mgr.rebalance(perfs)
        retrieved = await mgr.get_allocation("strat_z")
        assert abs(retrieved - allocs["strat_z"]) < 0.01
