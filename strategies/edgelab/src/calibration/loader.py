"""
Shared calibration loader for EdgeLab strategies.

Reads from data/calibration_historical.json (range-key bucket format produced
by backtester.py) and exposes a normalised {int_pct: float_true_prob} dict plus
a convenience lookup function used by threshold_scanner and resolution_lag.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Resolved path: src/calibration/loader.py -> project root -> data/
_CALIBRATION_PATH = Path(__file__).parent.parent.parent / "data" / "calibration_historical.json"

# Fallback table (market_prob_pct -> calibrated_true_prob)
# Full range: mid-range markets (30-89%) have a modest edge assumption
# (true_prob slightly above market_prob) reflecting typical prediction market
# inefficiencies. High-confidence range (90-99%) uses historical calibration.
DEFAULT_CALIBRATION: dict[int, float] = {
    # Low-range: long-shot markets — slight edge from overpriced NO contracts
    5: 0.055,
    10: 0.108,
    15: 0.160,
    20: 0.213,
    25: 0.265,
    # Mid-range: live unresolved markets — small systematic edge (1-3%)
    30: 0.315,
    35: 0.365,
    40: 0.420,
    45: 0.470,
    50: 0.520,
    55: 0.575,
    60: 0.625,
    65: 0.675,
    70: 0.725,
    75: 0.775,
    80: 0.825,
    85: 0.875,
    # Near-certainty: historical calibration (resolved market data)
    90: 0.935,
    91: 0.945,
    92: 0.955,
    93: 0.965,
    94: 0.975,
    95: 0.982,
    96: 0.988,
    97: 0.993,
    98: 0.997,
    99: 0.999,
}


def load_calibration() -> dict[int, float]:
    """Return a calibration table with int-pct keys (e.g. 90, 91, …) -> float true prob.

    Reads data/calibration_historical.json.  Two JSON formats are accepted:

    1. Range-key bucket format (produced by backtester.py):
       {"90-95": {"true_rate": 0.924, "lo": 0.90, "hi": 0.95, ...}, ...}
       The lo/hi bounds are expanded into per-percentage-point entries via
       linear interpolation between adjacent bucket true_rates.

    2. Simple int-key format:
       {"90": 0.935, "91": 0.945, ...}

    Falls back to DEFAULT_CALIBRATION if the file is missing or unreadable.
    """
    if not _CALIBRATION_PATH.exists():
        logger.debug("Calibration file not found at %s — using defaults", _CALIBRATION_PATH)
        return DEFAULT_CALIBRATION.copy()

    try:
        raw: dict = json.loads(_CALIBRATION_PATH.read_text())
    except Exception as exc:
        logger.warning("Failed to read calibration file %s: %s — using defaults", _CALIBRATION_PATH, exc)
        return DEFAULT_CALIBRATION.copy()

    table: dict[int, float] = {}

    # Detect format by inspecting the first key
    first_key = next(iter(raw), None)
    if first_key is None:
        return DEFAULT_CALIBRATION.copy()

    if "-" in str(first_key):
        # Range-key bucket format: expand each bucket into per-point entries.
        # Collect (lo_pct, hi_pct, true_rate) tuples, sort, then interpolate.
        buckets: list[tuple[int, int, float]] = []
        for k, v in raw.items():
            if not isinstance(v, dict):
                continue
            true_rate = v.get("true_rate")
            if true_rate is None:
                continue
            try:
                parts = k.split("-")
                lo_pct, hi_pct = int(parts[0]), int(parts[1])
            except (ValueError, IndexError):
                continue
            buckets.append((lo_pct, hi_pct, float(true_rate)))

        buckets.sort(key=lambda b: b[0])

        # Assign the bucket's true_rate to every integer pct in [lo, hi).
        # For the boundary between buckets, linear-interpolate.
        for idx, (lo_pct, hi_pct, true_rate) in enumerate(buckets):
            if idx + 1 < len(buckets):
                next_true_rate = buckets[idx + 1][2]
            else:
                next_true_rate = true_rate

            span = hi_pct - lo_pct
            for offset in range(span):
                pct = lo_pct + offset
                t = offset / span
                table[pct] = true_rate + t * (next_true_rate - true_rate)

        # Include the final hi boundary of the last bucket
        if buckets:
            table[buckets[-1][1]] = buckets[-1][2]

    else:
        # Simple int-key format: {"90": 0.935, ...}
        for k, v in raw.items():
            try:
                table[int(k)] = float(v)
            except (ValueError, TypeError):
                continue

    if not table:
        logger.warning("Calibration file parsed but yielded no entries — using defaults")
        return DEFAULT_CALIBRATION.copy()

    logger.info(
        "Loaded calibration from %s (%d entries, pct range %d–%d)",
        _CALIBRATION_PATH,
        len(table),
        min(table),
        max(table),
    )
    return table


def get_true_prob(market_prob: float, table: dict[int, float] | None = None) -> float:
    """Return the calibrated true probability for a market price in [0, 1].

    Uses linear interpolation between adjacent int-pct entries in *table*.
    If *table* is None, load_calibration() is called on first use (cached).

    Args:
        market_prob: Market-implied probability as a float in [0, 1].
        table: Optional pre-loaded calibration table.  Pass None to use the
               module-level cached table (loaded lazily on first call).

    Returns:
        Calibrated true probability float.
    """
    if table is None:
        table = _cached_calibration()

    pct = market_prob * 100.0
    keys = sorted(table.keys())

    if not keys:
        return market_prob
    if pct <= keys[0]:
        # Below calibration range — return raw market_prob (don't inflate).
        # A 6% market should NOT be mapped to 88% just because the table
        # starts at 85%.
        return market_prob
    if pct >= keys[-1]:
        return table[keys[-1]]

    for i in range(len(keys) - 1):
        lo, hi = keys[i], keys[i + 1]
        if lo <= pct <= hi:
            t = (pct - lo) / (hi - lo)
            return table[lo] + t * (table[hi] - table[lo])

    return market_prob


# Module-level cache — populated on first call to get_true_prob(table=None)
_CACHED: dict[int, float] | None = None


def _cached_calibration() -> dict[int, float]:
    global _CACHED
    if _CACHED is None:
        _CACHED = load_calibration()
    return _CACHED
