"""
EdgeLab Threshold Strategy Config — Filters to improve win rate from 84.2% to 90%+.
Imported by threshold_scanner.py for runtime config.

Increasing MIN_VOLUME_FILTER and MAX_DAYS_TO_RESOLUTION narrows the trade
universe to higher-conviction, near-resolution markets — the primary driver
of the simulated 84.2% win rate shortfall vs resolution_lag's 100%.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Config values
# ---------------------------------------------------------------------------

MIN_VOLUME_FILTER: int = 50         # Lowered for paper mode — most live markets have low volume
MAX_DAYS_TO_RESOLUTION: int = 30     # Widened from 9 for live unresolved markets
THRESHOLD_PCT_OVERRIDE: float | None = None  # If set, overrides THRESHOLD_PCT from .env


# ---------------------------------------------------------------------------
# Filter function
# ---------------------------------------------------------------------------

def apply_filters(opportunity: dict[str, Any]) -> bool:
    """Return True if *opportunity* passes all threshold filters.

    Filters applied:
    1. Volume >= MIN_VOLUME_FILTER
    2. Days to resolution <= MAX_DAYS_TO_RESOLUTION

    Missing or unparseable fields default to *passing* the filter so that
    missing metadata never silently drops valid opportunities.

    Parameters
    ----------
    opportunity:
        Dict as published to the ``opportunities:threshold`` Redis stream.
        Expected keys: ``volume`` (float), ``end_date`` (datetime or ISO str).
    """
    # --- Volume filter ---
    volume = opportunity.get("volume")
    if volume is not None:
        try:
            if float(volume) < MIN_VOLUME_FILTER:
                return False
        except (TypeError, ValueError):
            pass  # Unparseable — default to pass

    # --- Days-to-resolution filter ---
    end_date = opportunity.get("end_date")
    if end_date is not None:
        try:
            if isinstance(end_date, str):
                end_date = end_date.replace("Z", "+00:00")
                if end_date.endswith("+00"):
                    end_date += ":00"
                end_date = datetime.fromisoformat(end_date)

            now = datetime.now(timezone.utc)
            # Ensure end_date is offset-aware for comparison
            if end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=timezone.utc)

            days_remaining = (end_date - now).total_seconds() / 86_400
            if days_remaining > MAX_DAYS_TO_RESOLUTION:
                return False
        except (TypeError, ValueError, AttributeError):
            pass  # Unparseable — default to pass

    return True
