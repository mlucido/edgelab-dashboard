"""
Calculates edge for funding rate arb trades on Kalshi range markets.

For range markets:
  BEARISH + "above $X where X ≈ current_price":
    P(NO) ≈ 0.5 + (funding_pressure * 0.3)
    Edge = P(NO) - current_NO_price - fees

Calibration: funding spike of 0.1% → ~18% probability shift over 4hr window
  (0.001 * 0.3 = 0.0003... adjusted via CALIBRATION_FACTOR below)
"""
import logging
from typing import Optional

from . import config

logger = logging.getLogger("funding_arb.edge")

FEES = 0.01          # Kalshi typical fee per contract
CALIBRATION_FACTOR = 180.0   # 0.001 rate * 180 = 0.18 (18% probability shift)
MAX_EDGE_PROBABILITY_SHIFT = 0.35   # Cap at 35% shift


def calculate_edge(signal: dict, market: dict) -> Optional[dict]:
    """
    Given a FundingSignal and a matched Kalshi market, compute edge.

    Returns:
        {
          implied_prob, market_price, edge_pct, side,
          confidence, expected_value, kelly_fraction
        }
    or None if edge < MIN_EDGE_PCT.
    """
    funding_rate = signal.get("rate", 0.0)
    direction = signal.get("direction")
    side = market.get("side")          # "YES" or "NO"
    confidence = signal.get("confidence", "LOW")

    if not direction or not side:
        return None

    # ── Probability shift from funding pressure ────────────────────────────────
    pressure = min(abs(funding_rate) * CALIBRATION_FACTOR, MAX_EDGE_PROBABILITY_SHIFT)

    # Base: range market is roughly 50/50 at boundary
    if direction == "BEARISH" and side == "NO":
        implied_prob = 0.50 + pressure
    elif direction == "BULLISH" and side == "YES":
        implied_prob = 0.50 + pressure
    else:
        # Direction / side mismatch — shouldn't happen via market_matcher
        implied_prob = 0.50

    implied_prob = min(max(implied_prob, 0.01), 0.99)

    # ── Current market price ───────────────────────────────────────────────────
    if side == "YES":
        market_price = market.get("yes_ask", 0.5)
    else:
        market_price = market.get("no_ask", 0.5)

    if not isinstance(market_price, (int, float)) or market_price <= 0:
        market_price = 0.5

    # ── Edge calculation ───────────────────────────────────────────────────────
    edge_pct = implied_prob - market_price - FEES
    if edge_pct < config.MIN_EDGE_PCT:
        return None

    # ── Expected value per $1 risked ──────────────────────────────────────────
    # Win: collect (1 - market_price), lose: pay market_price
    payout_if_win = (1.0 - market_price - FEES)
    ev = implied_prob * payout_if_win - (1 - implied_prob) * market_price

    # ── Kelly fraction (conservative half-Kelly) ───────────────────────────────
    b = payout_if_win / market_price if market_price > 0 else 1.0
    p = implied_prob
    q = 1 - p
    full_kelly = (b * p - q) / b if b > 0 else 0.0
    half_kelly = max(0.0, full_kelly * 0.5)

    result = {
        "implied_prob": round(implied_prob, 4),
        "market_price": round(market_price, 4),
        "edge_pct": round(edge_pct, 4),
        "side": side,
        "confidence": confidence,
        "expected_value": round(ev, 4),
        "kelly_fraction": round(half_kelly, 4),
        "funding_rate": funding_rate,
        "pressure": round(pressure, 4),
    }
    logger.debug(
        f"Edge: implied={implied_prob:.3f} mkt={market_price:.3f} "
        f"edge={edge_pct:.3f} ev={ev:.4f}"
    )
    return result
