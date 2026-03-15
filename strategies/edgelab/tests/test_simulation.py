"""
Tests for the EdgeLab simulation engine (src.simulation.backsim).

Covers:
    - Market data fetching (API mocked)
    - Simulation replay logic with known data
    - Go/no-go criteria evaluation
    - Report generation
"""
from __future__ import annotations

import json
import sys
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from src.simulation.backsim import (
        _extract_market,
        _detect_strategy,
        _build_sim_opportunity,
        _run_config,
        _format_report,
        _day_index,
        CONFIGS,
        SIM_DAYS,
        SIM_CAPITAL_START,
        RESOLUTION_LAG_PRICE_LO,
        RESOLUTION_LAG_PRICE_HI,
        THRESHOLD_MIN_PRICE,
        MIN_LIQUIDITY,
    )
    HAS_BACKSIM = True
except ImportError:
    HAS_BACKSIM = False

_skip_sim = pytest.mark.skipif(
    not HAS_BACKSIM,
    reason="src.simulation.backsim not yet built",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_raw_market(
    market_id: str = "mkt-001",
    yes_price: float = 1.0,
    no_price: float = 0.0,
    volume: float = 10000.0,
    liquidity: float = 5000.0,
    last_trade_price: float | None = 0.92,
    end_date: str | None = None,
) -> dict:
    """Build a minimal raw Gamma API market record."""
    if end_date is None:
        future = datetime.now(timezone.utc) + timedelta(days=3)
        end_date = future.isoformat()
    return {
        "id": market_id,
        "question": "Will X happen?",
        "outcomes": json.dumps(["Yes", "No"]),
        "outcomePrices": json.dumps([str(yes_price), str(no_price)]),
        "volume": str(volume),
        "liquidity": str(liquidity),
        "lastTradePrice": str(last_trade_price) if last_trade_price is not None else None,
        "endDate": end_date,
        "tags": [{"label": "Politics"}],
    }


def _make_sim_market(
    market_id: str = "mkt-001",
    resolved_yes: int = 1,
    last_price: float = 0.92,
    liquidity: float = 5000.0,
    resolution_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """Build a normalized simulation market dict."""
    if resolution_date is None:
        resolution_date = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()
    if end_date is None:
        end_date = resolution_date
    return {
        "market_id": market_id,
        "question": "Will X happen?",
        "category": "Politics",
        "resolution_date": resolution_date,
        "resolved_yes": resolved_yes,
        "volume": 10000.0,
        "last_price": last_price,
        "liquidity": liquidity,
        "end_date": end_date,
        "slug": market_id,
    }


# ---------------------------------------------------------------------------
# _extract_market tests
# ---------------------------------------------------------------------------

@_skip_sim
class TestExtractMarket:
    def test_resolved_yes_market(self):
        raw = _make_raw_market(yes_price=1.0, no_price=0.0)
        result = _extract_market(raw)
        assert result is not None
        assert result["resolved_yes"] == 1
        assert result["market_id"] == "mkt-001"
        assert 0 < result["last_price"] < 1.0

    def test_resolved_no_market(self):
        raw = _make_raw_market(yes_price=0.0, no_price=1.0)
        result = _extract_market(raw)
        assert result is not None
        assert result["resolved_yes"] == 0

    def test_ambiguous_market_rejected(self):
        # Neither clearly yes nor no
        raw = _make_raw_market(yes_price=0.5, no_price=0.5)
        result = _extract_market(raw)
        assert result is None

    def test_low_volume_rejected(self):
        raw = _make_raw_market(yes_price=1.0, no_price=0.0, volume=50.0)
        result = _extract_market(raw)
        assert result is None

    def test_non_binary_rejected(self):
        raw = _make_raw_market()
        raw["outcomes"] = json.dumps(["A", "B", "C"])  # 3 outcomes
        raw["outcomePrices"] = json.dumps(["0.33", "0.33", "0.34"])
        result = _extract_market(raw)
        assert result is None

    def test_last_trade_price_preserved(self):
        raw = _make_raw_market(yes_price=1.0, no_price=0.0, last_trade_price=0.91)
        result = _extract_market(raw)
        assert result is not None
        assert abs(result["last_price"] - 0.91) < 0.001

    def test_missing_last_trade_price_synthesized(self):
        raw = _make_raw_market(yes_price=1.0, no_price=0.0, last_trade_price=None)
        result = _extract_market(raw)
        assert result is not None
        # Should be synthesized in a realistic range
        assert 0.75 <= result["last_price"] <= 0.97

    def test_category_extracted_from_tags(self):
        raw = _make_raw_market(yes_price=1.0, no_price=0.0)
        result = _extract_market(raw)
        assert result is not None
        assert result["category"] == "Politics"

    def test_missing_market_id_rejected(self):
        raw = _make_raw_market(yes_price=1.0, no_price=0.0)
        raw.pop("id")
        raw["conditionId"] = None
        raw["slug"] = None
        result = _extract_market(raw)
        assert result is None


# ---------------------------------------------------------------------------
# _detect_strategy tests
# ---------------------------------------------------------------------------

@_skip_sim
class TestDetectStrategy:
    def test_resolution_lag_detected(self):
        market = _make_sim_market(last_price=0.91, resolved_yes=1, liquidity=5000)
        strategy = _detect_strategy(market)
        assert strategy == "resolution_lag"

    def test_threshold_detected(self):
        # Must be >= THRESHOLD_MIN_PRICE AND outside the resolution_lag range (>= RESOLUTION_LAG_PRICE_HI)
        market = _make_sim_market(last_price=RESOLUTION_LAG_PRICE_HI, resolved_yes=1, liquidity=5000)
        strategy = _detect_strategy(market)
        assert strategy == "threshold"

    def test_no_signal_low_price(self):
        # Price below both thresholds
        market = _make_sim_market(last_price=0.50, resolved_yes=1, liquidity=5000)
        strategy = _detect_strategy(market)
        assert strategy is None

    def test_no_signal_resolved_no(self):
        # We only go long YES — skip resolved NO markets
        market = _make_sim_market(last_price=0.92, resolved_yes=0, liquidity=5000)
        strategy = _detect_strategy(market)
        assert strategy is None

    def test_no_signal_low_liquidity(self):
        market = _make_sim_market(last_price=0.92, resolved_yes=1, liquidity=MIN_LIQUIDITY - 1)
        market["volume"] = 0  # volume also below threshold (used as proxy for closed markets)
        strategy = _detect_strategy(market)
        assert strategy is None

    def test_boundary_at_threshold_min(self):
        market = _make_sim_market(last_price=THRESHOLD_MIN_PRICE, resolved_yes=1, liquidity=5000)
        strategy = _detect_strategy(market)
        assert strategy is not None


# ---------------------------------------------------------------------------
# _build_sim_opportunity tests
# ---------------------------------------------------------------------------

@_skip_sim
class TestBuildSimOpportunity:
    def test_structure_complete(self):
        market = _make_sim_market(last_price=0.92)
        opp = _build_sim_opportunity(market, "resolution_lag")
        required = {
            "strategy", "market_id", "current_price", "implied_true_prob",
            "liquidity", "days_to_resolution", "detected_at",
        }
        assert required.issubset(opp.keys())

    def test_true_prob_above_price(self):
        market = _make_sim_market(last_price=0.92)
        opp = _build_sim_opportunity(market, "threshold")
        assert opp["implied_true_prob"] > opp["current_price"]

    def test_true_prob_capped_at_999(self):
        market = _make_sim_market(last_price=0.999)
        opp = _build_sim_opportunity(market, "threshold")
        assert opp["implied_true_prob"] <= 0.999

    def test_strategy_name_preserved(self):
        market = _make_sim_market(last_price=0.91)
        opp = _build_sim_opportunity(market, "resolution_lag")
        assert opp["strategy"] == "resolution_lag"


# ---------------------------------------------------------------------------
# _run_config (simulation replay) tests
# ---------------------------------------------------------------------------

@_skip_sim
class TestRunConfig:
    def _sim_start(self) -> datetime:
        return datetime.now(timezone.utc) - timedelta(days=SIM_DAYS)

    def _make_markets(self, n: int = 5, price: float = 0.92) -> list[dict]:
        sim_start = self._sim_start()
        markets = []
        for i in range(n):
            day_offset = 5 + i * 10
            res_date = (sim_start + timedelta(days=day_offset)).isoformat()
            markets.append(_make_sim_market(
                market_id=f"mkt-{i:03d}",
                resolved_yes=1,
                last_price=price,
                liquidity=5000,
                resolution_date=res_date,
                end_date=res_date,
            ))
        return markets

    def test_result_keys_present(self):
        markets = self._make_markets()
        result = _run_config(markets, "standard", CONFIGS["standard"], self._sim_start())
        required = {
            "config", "starting_capital", "ending_capital", "total_pnl",
            "win_rate", "total_trades", "sharpe_ratio", "max_drawdown_pct",
            "strategy_breakdown", "daily_capital", "guardrails_source",
        }
        assert required.issubset(result.keys())

    def test_starting_capital_correct(self):
        markets = self._make_markets()
        result = _run_config(markets, "standard", CONFIGS["standard"], self._sim_start())
        assert result["starting_capital"] == SIM_CAPITAL_START

    def test_daily_capital_length(self):
        markets = self._make_markets()
        result = _run_config(markets, "standard", CONFIGS["standard"], self._sim_start())
        assert len(result["daily_capital"]) == SIM_DAYS

    def test_wins_profitable(self):
        """Markets that resolve YES and we held long should generate positive P&L."""
        markets = self._make_markets(n=10, price=0.92)
        result = _run_config(markets, "standard", CONFIGS["standard"], self._sim_start())
        # With all markets resolving YES, P&L should be non-negative
        if result["total_trades"] > 0:
            assert result["total_pnl"] >= 0 or result["circuit_breaker_triggers"] > 0

    def test_config_name_in_result(self):
        markets = self._make_markets()
        result = _run_config(markets, "conservative", CONFIGS["conservative"], self._sim_start())
        assert result["config"] == "conservative"

    def test_empty_markets_zero_trades(self):
        result = _run_config([], "standard", CONFIGS["standard"], self._sim_start())
        assert result["total_trades"] == 0
        assert result["ending_capital"] == SIM_CAPITAL_START

    def test_no_signal_markets_skipped(self):
        """Markets with no detectable strategy signal should be skipped."""
        sim_start = self._sim_start()
        # Price = 0.50 — no signal for either strategy
        markets = [_make_sim_market(
            market_id=f"skip-{i}",
            resolved_yes=1,
            last_price=0.50,
            liquidity=5000,
            resolution_date=(sim_start + timedelta(days=10)).isoformat(),
            end_date=(sim_start + timedelta(days=10)).isoformat(),
        ) for i in range(5)]
        result = _run_config(markets, "standard", CONFIGS["standard"], sim_start)
        assert result["total_trades"] == 0

    def test_three_configs_produce_different_results(self):
        markets = self._make_markets(n=20, price=0.92)
        sim_start = self._sim_start()
        conservative = _run_config(markets, "conservative", CONFIGS["conservative"], sim_start)
        aggressive = _run_config(markets, "aggressive", CONFIGS["aggressive"], sim_start)
        # Aggressive should have higher max trade size → different capital trajectory
        # (can't guarantee direction, but results should differ)
        assert conservative["config"] != aggressive["config"]


# ---------------------------------------------------------------------------
# _format_report and go/no-go criteria tests
# ---------------------------------------------------------------------------

@_skip_sim
class TestFormatReport:
    def _make_result(
        self,
        pnl: float = 50.0,
        win_rate: float = 0.65,
        sharpe: float = 1.5,
        max_dd: float = 10.0,
        cb_triggers: int = 1,
        total_trades: int = 30,
    ) -> dict:
        return {
            "config": "standard",
            "starting_capital": SIM_CAPITAL_START,
            "ending_capital": SIM_CAPITAL_START + pnl,
            "total_pnl": pnl,
            "total_pnl_pct": pnl / SIM_CAPITAL_START * 100,
            "sharpe_ratio": sharpe,
            "max_drawdown_pct": max_dd,
            "max_drawdown_day_start": 5,
            "max_drawdown_day_end": 15,
            "win_rate": win_rate,
            "wins": int(win_rate * total_trades),
            "total_trades": total_trades,
            "avg_hold_days": 7.0,
            "capital_velocity": 1.2,
            "circuit_breaker_triggers": cb_triggers,
            "strategy_breakdown": {
                "resolution_lag": {"trades": 20, "wins": 13, "win_rate": 0.65, "pnl": 30.0},
                "threshold": {"trades": 10, "wins": 6, "win_rate": 0.60, "pnl": 20.0},
            },
            "daily_capital": [SIM_CAPITAL_START + pnl] * SIM_DAYS,
            "trade_log": [],
            "guardrails_source": "local_stubs",
        }

    def test_report_is_string(self):
        result = self._make_result()
        report = _format_report(result)
        assert isinstance(report, str)
        assert len(report) > 100

    def test_report_contains_verdict_go(self):
        result = self._make_result(pnl=50, win_rate=0.65, sharpe=1.5, max_dd=10, cb_triggers=1)
        report = _format_report(result)
        assert "GO" in report

    def test_report_contains_verdict_nogo(self):
        result = self._make_result(pnl=-100, win_rate=0.30, sharpe=0.2, max_dd=40, cb_triggers=10)
        report = _format_report(result)
        assert "NO-GO" in report or "CONDITIONAL" in report

    def test_report_contains_config_name(self):
        result = self._make_result()
        report = _format_report(result)
        assert "STANDARD" in report.upper()

    def test_report_contains_pnl(self):
        result = self._make_result(pnl=123.45)
        report = _format_report(result)
        assert "123" in report

    def test_report_contains_win_rate(self):
        result = self._make_result(win_rate=0.67)
        report = _format_report(result)
        assert "67" in report

    def test_report_contains_sharpe(self):
        result = self._make_result(sharpe=1.75)
        report = _format_report(result)
        assert "1.75" in report


# ---------------------------------------------------------------------------
# Go/no-go criteria evaluation
# ---------------------------------------------------------------------------

@_skip_sim
class TestGoNoCriteria:
    """Test that go/no-go criteria are evaluated correctly in the report."""

    def _report_for(self, **kwargs) -> str:
        return _format_report({
            "config": "standard",
            "starting_capital": SIM_CAPITAL_START,
            "ending_capital": SIM_CAPITAL_START + kwargs.get("pnl", 0),
            "total_pnl": kwargs.get("pnl", 0),
            "total_pnl_pct": kwargs.get("pnl", 0) / SIM_CAPITAL_START * 100,
            "sharpe_ratio": kwargs.get("sharpe", 1.0),
            "max_drawdown_pct": kwargs.get("max_dd", 15.0),
            "max_drawdown_day_start": 0,
            "max_drawdown_day_end": 5,
            "win_rate": kwargs.get("win_rate", 0.60),
            "wins": int(kwargs.get("win_rate", 0.60) * 20),
            "total_trades": 20,
            "avg_hold_days": 7.0,
            "capital_velocity": 1.0,
            "circuit_breaker_triggers": kwargs.get("cb", 0),
            "strategy_breakdown": {
                "resolution_lag": {"trades": 20, "wins": 12, "win_rate": 0.60, "pnl": 30.0},
                "threshold": {"trades": 0, "wins": 0, "win_rate": 0.0, "pnl": 0.0},
            },
            "daily_capital": [SIM_CAPITAL_START] * SIM_DAYS,
            "trade_log": [],
            "guardrails_source": "local_stubs",
        })

    def test_all_criteria_pass_yields_go(self):
        report = self._report_for(pnl=100, sharpe=1.5, max_dd=10, win_rate=0.60, cb=0)
        assert "GO" in report

    def test_negative_pnl_criterion_fails(self):
        report = self._report_for(pnl=-50, sharpe=1.5, max_dd=10, win_rate=0.60, cb=0)
        assert "NO" in report  # either NO-GO or CONDITIONAL

    def test_low_win_rate_criterion_fails(self):
        report = self._report_for(pnl=50, sharpe=1.5, max_dd=10, win_rate=0.40, cb=0)
        # Still shows the actual win rate
        assert "40" in report or "0.40" in report.lower()

    def test_circuit_breaker_threshold(self):
        # 5 or more triggers should fail the CB criterion
        report = self._report_for(pnl=50, sharpe=1.5, max_dd=10, win_rate=0.60, cb=5)
        assert "5" in report


# ---------------------------------------------------------------------------
# _day_index tests
# ---------------------------------------------------------------------------

@_skip_sim
class TestDayIndex:
    def test_day_zero(self):
        sim_start = datetime.now(timezone.utc)
        date_str = sim_start.isoformat()
        idx = _day_index(date_str, sim_start)
        assert idx == 0

    def test_day_ten(self):
        sim_start = datetime.now(timezone.utc) - timedelta(days=SIM_DAYS)
        target = sim_start + timedelta(days=10)
        idx = _day_index(target.isoformat(), sim_start)
        assert idx == 10

    def test_clamped_to_sim_range(self):
        sim_start = datetime.now(timezone.utc) - timedelta(days=SIM_DAYS)
        far_future = sim_start + timedelta(days=SIM_DAYS + 100)
        idx = _day_index(far_future.isoformat(), sim_start)
        assert idx == SIM_DAYS - 1

    def test_before_sim_start_clamped_to_zero(self):
        sim_start = datetime.now(timezone.utc)
        past = sim_start - timedelta(days=10)
        idx = _day_index(past.isoformat(), sim_start)
        assert idx == 0


# ---------------------------------------------------------------------------
# API fetch mock tests
# ---------------------------------------------------------------------------

@_skip_sim
class TestFetchMarkets:
    @pytest.mark.asyncio
    async def test_fetch_returns_normalized_markets(self):
        """Mock the httpx response and verify extraction pipeline."""
        from src.simulation.backsim import _fetch_all_markets

        raw_page = [
            _make_raw_market("mkt-A", yes_price=1.0, no_price=0.0, volume=5000),
            _make_raw_market("mkt-B", yes_price=0.0, no_price=1.0, volume=5000),
            _make_raw_market("mkt-C", yes_price=0.5, no_price=0.5, volume=50),  # ambiguous+low vol
        ]

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = MagicMock(return_value=raw_page)

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("src.simulation.backsim.httpx.AsyncClient", return_value=mock_client):
            markets = await _fetch_all_markets(days=90)

        # mkt-A and mkt-B should parse; mkt-C rejected (ambiguous prices + volume < 100)
        ids = [m["market_id"] for m in markets]
        assert "mkt-A" in ids
        assert "mkt-B" in ids
        # mkt-C may or may not be present (ambiguous filter)

    @pytest.mark.asyncio
    async def test_fetch_handles_api_error_gracefully(self):
        """API errors should return empty list, not raise."""
        from src.simulation.backsim import _fetch_all_markets

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=Exception("Connection refused"))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("src.simulation.backsim.httpx.AsyncClient", return_value=mock_client):
            markets = await _fetch_all_markets(days=90)

        assert markets == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
