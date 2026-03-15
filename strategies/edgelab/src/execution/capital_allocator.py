"""
EdgeLab Capital Allocator — Strategy-weighted capital distribution.

Reads allocation weights from environment variables and provides helpers
for strategy-level capital budgeting on top of Kelly sizing.
"""
from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Allocation weights (from env with defaults)
# ---------------------------------------------------------------------------

ALLOC_RESOLUTION_LAG: float = float(os.environ.get("ALLOC_RESOLUTION_LAG", 0.60))
ALLOC_THRESHOLD: float      = float(os.environ.get("ALLOC_THRESHOLD",       0.25))
ALLOC_ARB: float            = float(os.environ.get("ALLOC_ARB",             0.15))

_ALLOCATIONS: dict[str, float] = {
    "resolution_lag": ALLOC_RESOLUTION_LAG,
    "threshold":      ALLOC_THRESHOLD,
    "arb":            ALLOC_ARB,
}

_TOLERANCE = 0.01  # sum must be within 1.0 ± 0.01


def _validate_allocations() -> None:
    """Raise ValueError if weights don't sum to 1.0 within tolerance."""
    total = sum(_ALLOCATIONS.values())
    if abs(total - 1.0) > _TOLERANCE:
        raise ValueError(
            f"Allocation weights must sum to 1.0 (got {total:.4f}). "
            f"Values: {_ALLOCATIONS}"
        )


# Validate on module load
_validate_allocations()

logger.info(
    "CapitalAllocator initialized — resolution_lag=%.0f%% threshold=%.0f%% arb=%.0f%%",
    ALLOC_RESOLUTION_LAG * 100,
    ALLOC_THRESHOLD * 100,
    ALLOC_ARB * 100,
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_allocation(strategy: str, total_capital: float) -> float:
    """Return the maximum capital available for *strategy*.

    Parameters
    ----------
    strategy:
        One of "resolution_lag", "threshold", or "arb".
    total_capital:
        Total portfolio NAV.

    Returns
    -------
    Dollar amount allocated to this strategy.

    Raises
    ------
    KeyError
        If *strategy* is not a recognised strategy name.
    """
    weight = _ALLOCATIONS[strategy]
    return total_capital * weight


def get_all_allocations(total_capital: float) -> dict[str, float]:
    """Return a dict of {strategy: allocated_capital} for all strategies.

    Parameters
    ----------
    total_capital:
        Total portfolio NAV.
    """
    return {strategy: total_capital * weight for strategy, weight in _ALLOCATIONS.items()}


def get_position_size(
    strategy: str,
    total_capital: float,
    kelly_fraction: float,
) -> float:
    """Compute position size, capped at both Kelly and strategy allocation.

    Applies quarter-Kelly sizing against the strategy's allocated capital
    slice, then enforces the allocation cap so a single strategy cannot
    consume more than its budgeted share of the portfolio.

    Parameters
    ----------
    strategy:
        One of "resolution_lag", "threshold", or "arb".
    total_capital:
        Total portfolio NAV.
    kelly_fraction:
        Full Kelly fraction (f*) from the sizer — quarter-Kelly is applied
        internally (× 0.25).

    Returns
    -------
    Recommended dollar size after Kelly and allocation cap.
    """
    allocated_capital = get_allocation(strategy, total_capital)

    # Quarter-Kelly applied against full capital for proper sizing
    kelly_size = 0.25 * kelly_fraction * total_capital

    # Cap at the strategy's allocated slice
    position_size = min(kelly_size, allocated_capital)
    return max(position_size, 0.0)
