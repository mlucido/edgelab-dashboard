"""
Analyzes funding rate data for tradeable signals.
Computes z-scores, detects spikes and sustained extremes.
"""
import logging
import statistics
from typing import Optional

from . import config

logger = logging.getLogger("funding_arb.analyzer")

# Cap implied 24hr price move
_MAX_IMPLIED_MOVE_PCT = 5.0


def analyze_funding(asset: str, history: list) -> Optional[dict]:
    """
    Given asset and list of historical FundingRate dicts (newest first),
    return a FundingSignal dict or None if no signal.

    FundingSignal: {
        asset, direction, magnitude, rate, rate_zscore,
        trend, confidence, implied_price_pressure
    }
    """
    if not history:
        logger.warning(f"No history provided for {asset}")
        return None

    rates = [h["rate"] for h in history if isinstance(h.get("rate"), (int, float))]
    if len(rates) < 2:
        logger.warning(f"Insufficient rate history for {asset}: {len(rates)} points")
        return None

    current_rate = rates[0]

    # ── Z-score vs last 30 periods ─────────────────────────────────────────────
    lookback = rates[:30] if len(rates) >= 30 else rates
    if len(lookback) < 2:
        z_score = 0.0
    else:
        mean_r = statistics.mean(lookback)
        std_r = statistics.stdev(lookback)
        z_score = (current_rate - mean_r) / std_r if std_r > 0 else 0.0

    # ── Consecutive extreme periods ────────────────────────────────────────────
    consecutive_positive = 0
    consecutive_negative = 0
    for r in rates[:config.LOOKBACK_PERIODS]:
        if r > config.FUNDING_EXTREME_THRESHOLD:
            consecutive_positive += 1
        else:
            break
    for r in rates[:config.LOOKBACK_PERIODS]:
        if r < -config.FUNDING_EXTREME_THRESHOLD:
            consecutive_negative += 1
        else:
            break

    # ── Signal detection ───────────────────────────────────────────────────────
    direction = None
    magnitude = None
    trend = False

    # SPIKE signals (single-period extremes with high z-score)
    if current_rate > config.FUNDING_SPIKE_THRESHOLD and z_score > 2.0:
        direction = "BEARISH"
        magnitude = "SPIKE"
    elif current_rate < -config.FUNDING_SPIKE_THRESHOLD and z_score < -2.0:
        direction = "BULLISH"
        magnitude = "SPIKE"

    # TREND signals (3+ consecutive extreme periods)
    if consecutive_positive >= 3:
        direction = "BEARISH"
        magnitude = "EXTREME" if current_rate > config.FUNDING_SPIKE_THRESHOLD else "ELEVATED"
        trend = True
    elif consecutive_negative >= 3:
        direction = "BULLISH"
        magnitude = "EXTREME" if current_rate < -config.FUNDING_SPIKE_THRESHOLD else "ELEVATED"
        trend = True

    # ELEVATED (above threshold but z-score not high enough for SPIKE)
    if direction is None:
        if current_rate > config.FUNDING_EXTREME_THRESHOLD and z_score > 1.0:
            direction = "BEARISH"
            magnitude = "ELEVATED"
        elif current_rate < -config.FUNDING_EXTREME_THRESHOLD and z_score < -1.0:
            direction = "BULLISH"
            magnitude = "ELEVATED"

    if direction is None:
        return None  # No signal

    # ── Confidence ────────────────────────────────────────────────────────────
    consecutive = consecutive_positive if direction == "BEARISH" else consecutive_negative
    if abs(z_score) > 3.0 and consecutive >= 3:
        confidence = "HIGH"
    elif abs(z_score) > 2.0 or consecutive >= 2:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    # ── Implied price pressure (funding * 3 * 100, capped at 5%) ──────────────
    implied = min(abs(current_rate) * 3 * 100, _MAX_IMPLIED_MOVE_PCT)

    signal = {
        "asset": asset,
        "direction": direction,
        "magnitude": magnitude,
        "rate": round(current_rate, 6),
        "rate_zscore": round(z_score, 3),
        "trend": trend,
        "confidence": confidence,
        "implied_price_pressure": round(implied, 4),
        "consecutive_extreme_periods": consecutive,
    }
    logger.info(
        f"[SIGNAL] {asset} {direction} {magnitude} | rate={current_rate:.6f} "
        f"z={z_score:.2f} conf={confidence}"
    )
    return signal
