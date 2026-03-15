"""
Position Sizer — Kelly Criterion with Hard Caps

Pure functions; no async required.

Exports:
    size_position(opportunity, capital) -> dict
    calculate_ev(price, true_prob, fee) -> float
    annualized_return(net_return, days_to_resolution) -> float
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

PLATFORM_FEE = 0.02
MAX_POSITION_FRACTION = 0.05   # 5% NAV hard cap per position
MAX_CORRELATED_FRACTION = 0.30  # 30% NAV hard cap for correlated exposure
KELLY_MULTIPLIER = 0.25        # Quarter-Kelly safety factor


# ---------------------------------------------------------------------------
# Core financial functions
# ---------------------------------------------------------------------------

def calculate_ev(price: float, true_prob: float, fee: float = PLATFORM_FEE) -> float:
    """
    Expected value of buying YES at `price` when true probability is `true_prob`.

    EV = true_prob * payout_after_fee / price - 1
       = true_prob * (1 - fee) / price - 1

    Returns net EV as a fraction (e.g., 0.05 = 5% edge).
    """
    if price <= 0 or price >= 1:
        return 0.0
    payout = (1.0 - fee)  # per dollar wagered, win returns (1-fee)/price net
    return true_prob * payout / price - 1.0


def annualized_return(net_return: float, days_to_resolution: float) -> float:
    """
    Convert a net return to an annualized rate.

    Uses compounding formula: (1 + r)^(365/days) - 1

    Returns 0.0 for invalid inputs.
    """
    if days_to_resolution <= 0 or net_return <= -1.0:
        return 0.0
    periods_per_year = 365.0 / days_to_resolution
    return (1.0 + net_return) ** periods_per_year - 1.0


def _kelly_fraction(price: float, true_prob: float, fee: float = PLATFORM_FEE) -> float:
    """
    Full Kelly fraction: f* = (p*b - (1-p)) / b

    where b = net odds on a win = (1/price - 1) * (1 - fee)
    i.e. for every $1 risked, you net b dollars if you win.

    Returns raw Kelly (may be <= 0 if there's no edge).
    """
    if price <= 0 or price >= 1:
        return 0.0
    gross = 1.0 / price - 1.0       # gross odds
    b = gross * (1.0 - fee)          # net odds after fee
    if b <= 0:
        return 0.0
    return (true_prob * b - (1.0 - true_prob)) / b


# ---------------------------------------------------------------------------
# Main sizer
# ---------------------------------------------------------------------------

def size_position(
    opportunity: dict[str, Any],
    capital: float = 10_000.0,
    correlated_exposure: float = 0.0,
) -> dict[str, Any]:
    """
    Compute recommended position size for an opportunity.

    Parameters
    ----------
    opportunity:
        Dict with at minimum: `current_price`, `implied_true_prob`.
        Optional: `days_to_resolution`, `liquidity`.
    capital:
        Total NAV available (default $10,000).
    correlated_exposure:
        Current dollar amount already allocated to correlated positions.
        Used to enforce the 30% correlated cap.

    Returns
    -------
    dict with:
        recommended_size   — dollar amount to deploy (after all caps)
        kelly_fraction     — raw Kelly f*
        kelly_quarter      — quarter-Kelly f* (pre-cap)
        capped_size        — size after individual position cap (pre-correlation cap)
        reason             — human-readable explanation of binding constraint
    """
    price = float(opportunity.get("current_price", 0))
    true_prob = float(opportunity.get("implied_true_prob", 0))
    days_to_res = float(opportunity.get("days_to_resolution", 30))
    liquidity = float(opportunity.get("liquidity", 0))

    # --- Guard rails ---
    if price <= 0 or price >= 1:
        return _no_position("Invalid price: must be in (0, 1)", price, true_prob, capital)
    if true_prob <= 0 or true_prob > 1:
        return _no_position("Invalid true_prob: must be in (0, 1]", price, true_prob, capital)

    kelly = _kelly_fraction(price, true_prob)

    if kelly <= 0:
        return _no_position(
            f"No edge: Kelly={kelly:.4f} (true_prob={true_prob:.3f}, price={price:.3f})",
            price, true_prob, capital,
        )

    kelly_quarter = KELLY_MULTIPLIER * kelly
    uncapped_size = kelly_quarter * capital

    # --- Hard cap 1: 5% NAV per position ---
    max_per_position = MAX_POSITION_FRACTION * capital
    capped_size = min(uncapped_size, max_per_position)
    reason = "quarter-Kelly"
    if uncapped_size > max_per_position:
        reason = f"capped at {MAX_POSITION_FRACTION*100:.0f}% NAV (Kelly suggested {uncapped_size:.2f})"

    # --- Hard cap 2: 30% correlated exposure ---
    max_correlated = MAX_CORRELATED_FRACTION * capital
    remaining_correlated_budget = max(max_correlated - correlated_exposure, 0.0)
    recommended_size = min(capped_size, remaining_correlated_budget)
    if capped_size > remaining_correlated_budget:
        reason = (
            f"capped at {MAX_CORRELATED_FRACTION*100:.0f}% correlated exposure limit "
            f"(correlated_exposure={correlated_exposure:.2f}, budget remaining={remaining_correlated_budget:.2f})"
        )

    # --- Liquidity sanity check (advisory, doesn't zero out) ---
    if liquidity > 0 and recommended_size > liquidity * 0.10:
        advisory = f"; note: size {recommended_size:.2f} > 10% of liquidity {liquidity:.2f}"
        reason += advisory
        logger.warning(
            "Position size %.2f exceeds 10%% of market liquidity %.2f for market %s",
            recommended_size,
            liquidity,
            opportunity.get("market_id", "unknown"),
        )

    ev = calculate_ev(price, true_prob)
    ann_ret = annualized_return(ev, days_to_res) if days_to_res > 0 else 0.0

    logger.debug(
        "size_position: market=%s price=%.3f true_prob=%.3f kelly=%.4f q_kelly=%.4f "
        "capped=%.2f recommended=%.2f ev=%.2f%% ann=%.0f%%",
        opportunity.get("market_id", "?"),
        price, true_prob, kelly, kelly_quarter,
        capped_size, recommended_size, ev * 100, ann_ret * 100,
    )

    return {
        "market_id": opportunity.get("market_id"),
        "strategy": opportunity.get("strategy"),
        "recommended_size": round(recommended_size, 2),
        "kelly_fraction": round(kelly, 6),
        "kelly_quarter": round(kelly_quarter, 6),
        "uncapped_size": round(uncapped_size, 2),
        "capped_size": round(capped_size, 2),
        "ev": round(ev, 4),
        "annualized_return": round(ann_ret, 4),
        "capital": capital,
        "reason": reason,
    }


def _no_position(
    reason: str,
    price: float,
    true_prob: float,
    capital: float,
) -> dict[str, Any]:
    return {
        "market_id": None,
        "strategy": None,
        "recommended_size": 0.0,
        "kelly_fraction": 0.0,
        "kelly_quarter": 0.0,
        "uncapped_size": 0.0,
        "capped_size": 0.0,
        "ev": calculate_ev(price, true_prob) if 0 < price < 1 and 0 < true_prob <= 1 else 0.0,
        "annualized_return": 0.0,
        "capital": capital,
        "reason": reason,
    }
