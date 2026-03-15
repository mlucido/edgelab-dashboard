"""
Tests for:
  - src/execution/capital_allocator.py
  - src/strategies/threshold_config.py
  - sim_50k.py --optimized argparse flag
"""
from __future__ import annotations

import importlib
import os
import sys
from datetime import datetime, timezone, timedelta
from unittest import mock

import pytest

# ---------------------------------------------------------------------------
# capital_allocator tests
# ---------------------------------------------------------------------------

class TestCapitalAllocatorDefaults:
    """Tests using the default env (no overrides)."""

    def setup_method(self):
        # Ensure module is loaded fresh with defaults
        for key in ("ALLOC_RESOLUTION_LAG", "ALLOC_THRESHOLD", "ALLOC_ARB"):
            os.environ.pop(key, None)
        if "src.execution.capital_allocator" in sys.modules:
            del sys.modules["src.execution.capital_allocator"]
        from src.execution import capital_allocator
        self.ca = capital_allocator

    def test_allocation_weights_sum_to_one(self):
        total = (
            self.ca.ALLOC_RESOLUTION_LAG
            + self.ca.ALLOC_THRESHOLD
            + self.ca.ALLOC_ARB
        )
        assert abs(total - 1.0) <= 0.01

    def test_get_allocation_resolution_lag(self):
        result = self.ca.get_allocation("resolution_lag", 50_000)
        assert result == pytest.approx(30_000.0)

    def test_get_allocation_threshold(self):
        result = self.ca.get_allocation("threshold", 50_000)
        assert result == pytest.approx(12_500.0)

    def test_get_allocation_arb(self):
        result = self.ca.get_allocation("arb", 50_000)
        assert result == pytest.approx(7_500.0)

    def test_get_all_allocations_keys(self):
        allocs = self.ca.get_all_allocations(100_000)
        assert set(allocs.keys()) == {"resolution_lag", "threshold", "arb"}

    def test_get_all_allocations_values(self):
        allocs = self.ca.get_all_allocations(100_000)
        assert allocs["resolution_lag"] == pytest.approx(60_000.0)
        assert allocs["threshold"]      == pytest.approx(25_000.0)
        assert allocs["arb"]            == pytest.approx(15_000.0)

    def test_get_all_allocations_sum(self):
        allocs = self.ca.get_all_allocations(100_000)
        assert sum(allocs.values()) == pytest.approx(100_000.0)

    def test_get_position_size_capped_at_allocation(self):
        # With kelly_fraction=1.0 (100% Kelly), quarter-Kelly = 0.25 * 1.0 * 50k = $12,500
        # resolution_lag allocation = 60% of 50k = $30,000 — Kelly wins here
        size = self.ca.get_position_size("resolution_lag", 50_000, kelly_fraction=1.0)
        assert size == pytest.approx(12_500.0)

    def test_get_position_size_zero_kelly(self):
        size = self.ca.get_position_size("threshold", 50_000, kelly_fraction=0.0)
        assert size == 0.0

    def test_get_position_size_unknown_strategy_raises(self):
        with pytest.raises(KeyError):
            self.ca.get_position_size("unknown_strat", 50_000, 0.5)


class TestCapitalAllocatorValidation:
    """Tests for weight validation at module load time."""

    def _reload_with_env(self, lag, thr, arb):
        env = {
            "ALLOC_RESOLUTION_LAG": str(lag),
            "ALLOC_THRESHOLD":      str(thr),
            "ALLOC_ARB":            str(arb),
        }
        with mock.patch.dict(os.environ, env):
            if "src.execution.capital_allocator" in sys.modules:
                del sys.modules["src.execution.capital_allocator"]
            import importlib
            import src.execution.capital_allocator as ca
            return ca

    def test_valid_custom_weights_load_ok(self):
        ca = self._reload_with_env(0.50, 0.30, 0.20)
        assert ca.ALLOC_RESOLUTION_LAG == pytest.approx(0.50)

    def test_invalid_weights_raise_value_error(self):
        with pytest.raises(ValueError, match="sum to 1.0"):
            self._reload_with_env(0.50, 0.30, 0.30)  # sums to 1.10

    def teardown_method(self):
        # Restore defaults for other tests
        for key in ("ALLOC_RESOLUTION_LAG", "ALLOC_THRESHOLD", "ALLOC_ARB"):
            os.environ.pop(key, None)
        if "src.execution.capital_allocator" in sys.modules:
            del sys.modules["src.execution.capital_allocator"]


# ---------------------------------------------------------------------------
# threshold_config tests
# ---------------------------------------------------------------------------

class TestThresholdConfigFilters:
    """Tests for apply_filters() in threshold_config."""

    def setup_method(self):
        from src.strategies import threshold_config
        self.tc = threshold_config

    def _future_date(self, days: float) -> datetime:
        return datetime.now(timezone.utc) + timedelta(days=days)

    def _past_date(self, days: float) -> datetime:
        return datetime.now(timezone.utc) - timedelta(days=days)

    def test_passes_with_good_volume_and_near_resolution(self):
        opp = {"volume": 15_000, "end_date": self._future_date(3)}
        assert self.tc.apply_filters(opp) is True

    def test_fails_low_volume(self):
        opp = {"volume": 20, "end_date": self._future_date(2)}
        assert self.tc.apply_filters(opp) is False

    def test_fails_volume_exactly_at_minimum(self):
        # 50 is the threshold; anything strictly less should fail
        opp = {"volume": 49, "end_date": self._future_date(2)}
        assert self.tc.apply_filters(opp) is False

    def test_passes_volume_exactly_at_minimum(self):
        opp = {"volume": 50, "end_date": self._future_date(2)}
        assert self.tc.apply_filters(opp) is True

    def test_fails_too_many_days_to_resolution(self):
        opp = {"volume": 20_000, "end_date": self._future_date(35)}
        assert self.tc.apply_filters(opp) is False

    def test_passes_at_exactly_max_days(self):
        opp = {"volume": 20_000, "end_date": self._future_date(30)}
        assert self.tc.apply_filters(opp) is True

    def test_missing_volume_defaults_to_pass(self):
        opp = {"end_date": self._future_date(2)}
        assert self.tc.apply_filters(opp) is True

    def test_missing_end_date_defaults_to_pass(self):
        opp = {"volume": 20_000}
        assert self.tc.apply_filters(opp) is True

    def test_empty_opportunity_passes(self):
        assert self.tc.apply_filters({}) is True

    def test_iso_string_end_date(self):
        future = self._future_date(3).isoformat()
        opp = {"volume": 20_000, "end_date": future}
        assert self.tc.apply_filters(opp) is True

    def test_iso_string_end_date_too_far(self):
        future = self._future_date(35).isoformat()
        opp = {"volume": 20_000, "end_date": future}
        assert self.tc.apply_filters(opp) is False

    def test_past_end_date_passes_filter(self):
        # Already resolved markets have negative days_remaining — passes
        opp = {"volume": 20_000, "end_date": self._past_date(1)}
        assert self.tc.apply_filters(opp) is True


# ---------------------------------------------------------------------------
# sim_50k.py argparse tests
# ---------------------------------------------------------------------------

class TestSimArgparse:
    """Tests for --optimized flag parsing in sim_50k.py."""

    def _get_parse_args(self):
        """Import _parse_args from sim_50k.py, stubbing out unavailable deps."""
        # Stub aiohttp so the import succeeds in test environments without it
        fake_aiohttp = mock.MagicMock()
        modules_to_stub = {
            "aiohttp": fake_aiohttp,
        }
        with mock.patch.dict(sys.modules, modules_to_stub):
            spec = importlib.util.spec_from_file_location(
                "sim_50k",
                "/Users/mattlucido/Dropbox/EdgeLab/strategies/edgelab/sim_50k.py",
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module._parse_args

    def test_no_flag_optimized_is_false(self):
        parse_args = self._get_parse_args()
        with mock.patch("sys.argv", ["sim_50k.py"]):
            args = parse_args()
        assert args.optimized is False

    def test_optimized_flag_is_true(self):
        parse_args = self._get_parse_args()
        with mock.patch("sys.argv", ["sim_50k.py", "--optimized"]):
            args = parse_args()
        assert args.optimized is True

    def test_unknown_flag_exits(self):
        parse_args = self._get_parse_args()
        with mock.patch("sys.argv", ["sim_50k.py", "--unknown"]):
            with pytest.raises(SystemExit):
                parse_args()
