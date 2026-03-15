"""Unit tests for EdgeLab strategy components."""

import json
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.risk.sizer import size_position, calculate_ev, annualized_return


class TestEVCalculator:
    def test_positive_ev(self):
        # Price 0.90, true prob 0.95, fee 2%
        ev = calculate_ev(0.90, 0.95, fee=0.02)
        assert ev > 0, f"Expected positive EV, got {ev}"

    def test_negative_ev(self):
        # Price 0.90, true prob 0.85 — overpaying
        ev = calculate_ev(0.90, 0.85, fee=0.02)
        assert ev < 0, f"Expected negative EV, got {ev}"

    def test_fair_price_negative_with_fees(self):
        # Price equals true prob — should be negative after fees
        ev = calculate_ev(0.90, 0.90, fee=0.02)
        assert ev < 0, f"Expected negative EV at fair price with fees, got {ev}"

    def test_zero_fee(self):
        ev = calculate_ev(0.90, 0.90, fee=0.0)
        assert abs(ev) < 0.001, f"Expected ~zero EV at fair price no fees, got {ev}"

    def test_extreme_prices(self):
        # At 99% price, even 99.9% true prob loses to fees
        ev_high = calculate_ev(0.99, 0.999, fee=0.02)
        assert ev_high < 0  # fee dominates at extreme prices
        ev_low = calculate_ev(0.50, 0.55, fee=0.02)
        assert ev_low > 0


class TestKellySizer:
    def test_basic_sizing(self):
        opp = {
            "current_price": 0.90,
            "implied_true_prob": 0.95,
        }
        result = size_position(opp, capital=10000)
        assert "recommended_size" in result
        assert result["recommended_size"] > 0
        assert result["recommended_size"] <= 500  # max 5% of 10K

    def test_negative_edge_no_bet(self):
        opp = {
            "current_price": 0.95,
            "implied_true_prob": 0.90,
        }
        result = size_position(opp, capital=10000)
        assert result["recommended_size"] == 0

    def test_max_position_cap(self):
        opp = {
            "current_price": 0.50,
            "implied_true_prob": 0.90,
        }
        result = size_position(opp, capital=10000)
        # Should be capped at 5% of capital = $500
        assert result["recommended_size"] <= 500

    def test_quarter_kelly(self):
        opp = {
            "current_price": 0.85,
            "implied_true_prob": 0.95,
        }
        result = size_position(opp, capital=10000)
        assert result["kelly_quarter"] < result["kelly_fraction"]


class TestAnnualizedReturn:
    def test_short_duration_high_annualized(self):
        ann = annualized_return(0.05, 7)  # 5% in 7 days
        assert ann > 1.0, f"Expected >100% annualized, got {ann}"

    def test_full_year(self):
        ann = annualized_return(0.10, 365)
        assert abs(ann - 0.10) < 0.01

    def test_zero_return(self):
        ann = annualized_return(0.0, 30)
        assert ann == 0.0

    def test_negative_return(self):
        ann = annualized_return(-0.05, 30)
        assert ann < 0


class TestArbDetector:
    def test_spread_calculation(self):
        """Test basic spread math."""
        poly_yes = 0.92
        kalshi_no = 0.12  # implies kalshi_yes = 0.88
        kalshi_yes = 1 - kalshi_no
        spread = poly_yes - kalshi_yes
        # Buy cheap side (kalshi at 0.88), sell expensive side concept
        locked_return = abs(spread) - 2 * 0.02
        assert locked_return == pytest.approx(0.0, abs=0.01)

    def test_profitable_arb(self):
        """When spread is large enough to cover fees."""
        poly_yes = 0.95
        kalshi_yes = 0.88
        spread = poly_yes - kalshi_yes  # 0.07
        locked_return = spread - 2 * 0.02  # 0.03
        assert locked_return > 0.015


class TestCalibrationLookup:
    def test_calibration_buckets(self):
        """Verify calibration table structure."""
        from src.calibration.loader import DEFAULT_CALIBRATION
        for pct in range(90, 100):
            assert pct in DEFAULT_CALIBRATION
            assert 0 < DEFAULT_CALIBRATION[pct] <= 1.0

    def test_calibration_monotonic(self):
        """Higher priced probability should have higher true probability."""
        from src.calibration.loader import DEFAULT_CALIBRATION
        prev = 0
        for pct in sorted(DEFAULT_CALIBRATION.keys()):
            assert DEFAULT_CALIBRATION[pct] >= prev
            prev = DEFAULT_CALIBRATION[pct]


class TestWhaleTracker:
    """Tests for whale tracker confidence scoring and sizing."""

    def test_confidence_score_high_wr(self):
        from src.strategies.whale_tracker import _confidence_score
        whale = {"win_rate": 0.80, "avg_bet_size": 1000}
        score = _confidence_score(whale, 1000)
        assert 40 < score <= 100

    def test_confidence_score_low_wr(self):
        from src.strategies.whale_tracker import _confidence_score
        whale = {"win_rate": 0.50, "avg_bet_size": 1000}
        score = _confidence_score(whale, 500)
        assert score < 40

    def test_confidence_increases_with_bet_size(self):
        from src.strategies.whale_tracker import _confidence_score
        whale = {"win_rate": 0.75, "avg_bet_size": 1000}
        score_small = _confidence_score(whale, 500)
        score_large = _confidence_score(whale, 3000)
        assert score_large > score_small

    def test_compute_size_range(self):
        from src.strategies.whale_tracker import _compute_our_size
        # At moderate confidence, should be between 1-2% of whale bet
        size = _compute_our_size(2000, 60)
        assert 20 <= size <= 40  # 1-2% of $2000

    def test_compute_size_low_confidence(self):
        from src.strategies.whale_tracker import _compute_our_size
        size = _compute_our_size(2000, 30)
        # Low confidence: closer to 1%
        assert size <= 25

    def test_compute_size_high_confidence(self):
        from src.strategies.whale_tracker import _compute_our_size
        size = _compute_our_size(2000, 95)
        # High confidence: closer to 2%
        assert size >= 35

    def test_load_empty_registry(self):
        from src.strategies.whale_tracker import _load_registry
        # Non-existent path should return empty list
        result = _load_registry("/nonexistent/path.json")
        assert result == []


class TestWhaleTradeQuality:
    """Tests for whale strategy in autonomous trader guardrails."""

    def test_whale_quality_pass(self):
        from src.execution.autonomous_trader import check_trade_quality
        opp = {
            "strategy": "whale",
            "current_price": 0.85,
            "confidence_score": 60,
            "liquidity": 5000,
            "days_to_resolution": 7,
        }
        passes, reason = check_trade_quality(opp)
        assert passes, reason

    def test_whale_quality_low_confidence(self):
        from src.execution.autonomous_trader import check_trade_quality
        opp = {
            "strategy": "whale",
            "current_price": 0.85,
            "confidence_score": 20,
            "liquidity": 5000,
            "days_to_resolution": 7,
        }
        passes, reason = check_trade_quality(opp)
        assert not passes
        assert "confidence" in reason.lower()

    def test_whale_quality_low_liquidity(self):
        from src.execution.autonomous_trader import check_trade_quality
        opp = {
            "strategy": "whale",
            "current_price": 0.85,
            "confidence_score": 60,
            "liquidity": 10,
            "days_to_resolution": 7,
        }
        passes, reason = check_trade_quality(opp)
        assert not passes
        assert "liquidity" in reason.lower()

    def test_whale_sizing(self):
        from src.execution.autonomous_trader import calculate_position_size
        opp = {
            "strategy": "whale",
            "suggested_size": 25.0,
        }
        size, reason = calculate_position_size(opp)
        assert 10 <= size <= 50
        assert "whale" in reason.lower()

    def test_whale_sizing_no_suggestion(self):
        from src.execution.autonomous_trader import calculate_position_size
        opp = {
            "strategy": "whale",
            "suggested_size": 0,
        }
        size, reason = calculate_position_size(opp)
        assert size == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
