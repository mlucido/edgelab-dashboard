"""
EdgeLab Arb Analyzer — Historical arbitrage opportunity analysis across Polymarket and Kalshi.

Loads historical data from both platforms, fuzzy-matches markets by name, computes
arb spreads and locked returns, and produces a summary report.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

PLATFORM_FEE = 0.02   # 2% per leg
MIN_SPREAD = 0.015    # 1.5% minimum locked return threshold

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
_POLY_PATH = os.path.join(_DATA_DIR, "polymarket_historical.json")
_KALSHI_PATH = os.path.join(_DATA_DIR, "kalshi_historical.json")
_OUTPUT_PATH = os.path.join(_DATA_DIR, "arb_analysis.json")


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def load_polymarket_historical(path: str = _POLY_PATH) -> list[dict[str, Any]]:
    """
    Load Polymarket historical markets from JSON.

    Expected schema per record:
        question, prob_yes, prob_no, resolved, liquidity, ...

    Returns an empty list with a logged warning if the file is missing or malformed.
    """
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        logger.warning("Polymarket historical file not found: %s", path)
        return []
    except json.JSONDecodeError as exc:
        logger.warning("Failed to parse Polymarket historical file: %s", exc)
        return []

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # Support wrapped formats: {"markets": [...]} or {"data": [...]}
        for key in ("markets", "data", "results"):
            if key in data and isinstance(data[key], list):
                return data[key]
        logger.warning("Unexpected Polymarket JSON structure; returning empty list.")
        return []

    logger.warning("Unexpected Polymarket data type %s; returning empty list.", type(data))
    return []


def load_kalshi_historical(path: str = _KALSHI_PATH) -> list[dict[str, Any]]:
    """
    Load Kalshi historical markets from JSON.

    Expected schema per record:
        title, yes_bid, yes_ask, result, category, ...

    Returns an empty list with a logged warning if the file is missing or malformed.
    """
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        logger.warning("Kalshi historical file not found: %s", path)
        return []
    except json.JSONDecodeError as exc:
        logger.warning("Failed to parse Kalshi historical file: %s", exc)
        return []

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("markets", "data", "results"):
            if key in data and isinstance(data[key], list):
                return data[key]
        logger.warning("Unexpected Kalshi JSON structure; returning empty list.")
        return []

    logger.warning("Unexpected Kalshi data type %s; returning empty list.", type(data))
    return []


# ---------------------------------------------------------------------------
# Fuzzy matching
# ---------------------------------------------------------------------------

def _fuzzy_score(a: str, b: str) -> float:
    """
    Return a similarity score in [0, 100] between two strings.

    Prefers rapidfuzz.fuzz.token_sort_ratio; falls back to a simple
    Jaccard keyword overlap if rapidfuzz is not installed.
    """
    try:
        from rapidfuzz import fuzz  # type: ignore
        return fuzz.token_sort_ratio(a.lower(), b.lower())
    except ImportError:
        logger.debug("rapidfuzz not available; using Jaccard keyword overlap.")
        tokens_a = set(a.lower().split())
        tokens_b = set(b.lower().split())
        if not tokens_a or not tokens_b:
            return 0.0
        intersection = tokens_a & tokens_b
        union = tokens_a | tokens_b
        return round(len(intersection) / len(union) * 100, 2)


def _compute_spread(
    poly_market: dict[str, Any],
    kalshi_market: dict[str, Any],
) -> tuple[float, float]:
    """
    Compute raw spread and locked return between a matched market pair.

    Polymarket: uses prob_yes (mid-price).
    Kalshi:     uses mid of yes_bid / yes_ask, falling back to prob_yes or price.

    Returns (spread, locked_return).  Both are 0.0 on invalid inputs.
    """
    # --- Polymarket price ---
    poly_yes = _safe_float(poly_market.get("prob_yes") or poly_market.get("price"))

    # --- Kalshi price ---
    kalshi_bid = _safe_float(kalshi_market.get("yes_bid"))
    kalshi_ask = _safe_float(kalshi_market.get("yes_ask"))
    if kalshi_bid > 0 and kalshi_ask > 0:
        kalshi_yes = (kalshi_bid + kalshi_ask) / 2.0
    else:
        kalshi_yes = _safe_float(
            kalshi_market.get("prob_yes") or kalshi_market.get("price") or kalshi_market.get("yes_bid") or kalshi_market.get("yes_ask")
        )

    if not (0 < poly_yes < 1) or not (0 < kalshi_yes < 1):
        return 0.0, 0.0

    spread = abs(poly_yes - kalshi_yes)
    locked_return = spread - 2 * PLATFORM_FEE
    return round(spread, 4), round(locked_return, 4)


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Market matching
# ---------------------------------------------------------------------------

def find_overlapping_markets(
    poly_markets: list[dict[str, Any]],
    kalshi_markets: list[dict[str, Any]],
    threshold: int = 70,
) -> list[dict[str, Any]]:
    """
    Fuzzy-match Polymarket questions against Kalshi titles.

    For each Polymarket market, finds the best-scoring Kalshi market.
    Only emits a pair when score >= threshold.

    Returns a list of match dicts:
        {
            poly_market:   dict,
            kalshi_market: dict,
            match_score:   float,   # 0–100
            spread:        float,
            locked_return: float,
        }
    """
    if not poly_markets:
        logger.info("No Polymarket markets to match.")
        return []
    if not kalshi_markets:
        logger.info("No Kalshi markets to match.")
        return []

    # Pre-build (title, market) pairs for Kalshi to avoid repeated dict lookups
    kalshi_index: list[tuple[str, dict[str, Any]]] = [
        (str(m.get("title") or m.get("question") or ""), m)
        for m in kalshi_markets
    ]

    matches: list[dict[str, Any]] = []

    for poly in poly_markets:
        poly_question = str(poly.get("question") or poly.get("title") or "")
        if not poly_question:
            continue

        best_score = -1.0
        best_kalshi: dict[str, Any] | None = None

        for kalshi_title, kalshi in kalshi_index:
            if not kalshi_title:
                continue
            score = _fuzzy_score(poly_question, kalshi_title)
            if score > best_score:
                best_score = score
                best_kalshi = kalshi

        if best_score < threshold or best_kalshi is None:
            continue

        spread, locked_return = _compute_spread(poly, best_kalshi)

        matches.append(
            {
                "poly_market": poly,
                "kalshi_market": best_kalshi,
                "match_score": best_score,
                "spread": spread,
                "locked_return": locked_return,
            }
        )

    logger.info("Found %d overlapping markets (threshold=%d).", len(matches), threshold)
    return matches


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def calculate_arb_stats(matches: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Compute summary statistics over the list of matched market pairs.

    Returns:
        {
            total_matches:          int,
            pct_above_threshold:    float,   # % with spread > MIN_SPREAD
            avg_spread:             float,
            avg_profitable_spread:  float,
            by_category:            dict[str, {count, avg_spread, pct_profitable}],
            top_categories:         list[str],  # sorted by avg_spread desc
        }
    """
    if not matches:
        return {
            "total_matches": 0,
            "pct_above_threshold": 0.0,
            "avg_spread": 0.0,
            "avg_profitable_spread": 0.0,
            "by_category": {},
            "top_categories": [],
        }

    total = len(matches)
    profitable = [m for m in matches if m["spread"] > MIN_SPREAD]
    pct_above = round(len(profitable) / total * 100, 2)
    avg_spread = round(sum(m["spread"] for m in matches) / total, 4)
    avg_profitable_spread = (
        round(sum(m["spread"] for m in profitable) / len(profitable), 4)
        if profitable else 0.0
    )

    # --- Per-category breakdown ---
    by_category: dict[str, dict[str, Any]] = {}
    for m in matches:
        cat = str(
            m["kalshi_market"].get("category")
            or m["poly_market"].get("category")
            or "unknown"
        )
        bucket = by_category.setdefault(cat, {"count": 0, "spread_sum": 0.0, "profitable": 0})
        bucket["count"] += 1
        bucket["spread_sum"] += m["spread"]
        if m["spread"] > MIN_SPREAD:
            bucket["profitable"] += 1

    category_stats: dict[str, dict[str, Any]] = {}
    for cat, bucket in by_category.items():
        n = bucket["count"]
        category_stats[cat] = {
            "count": n,
            "avg_spread": round(bucket["spread_sum"] / n, 4),
            "pct_profitable": round(bucket["profitable"] / n * 100, 2),
        }

    top_categories = sorted(
        category_stats.keys(),
        key=lambda c: category_stats[c]["avg_spread"],
        reverse=True,
    )

    return {
        "total_matches": total,
        "pct_above_threshold": pct_above,
        "avg_spread": avg_spread,
        "avg_profitable_spread": avg_profitable_spread,
        "by_category": category_stats,
        "top_categories": top_categories,
    }


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_analysis(
    poly_path: str = _POLY_PATH,
    kalshi_path: str = _KALSHI_PATH,
    output_path: str = _OUTPUT_PATH,
    threshold: int = 70,
) -> dict[str, Any]:
    """
    Full historical arb analysis pipeline.

    1. Load both datasets.
    2. Find overlapping markets via fuzzy matching.
    3. Compute arb statistics.
    4. Persist results to data/arb_analysis.json.
    5. Return the analysis dict.
    """
    poly_markets = load_polymarket_historical(poly_path)
    kalshi_markets = load_kalshi_historical(kalshi_path)

    missing: list[str] = []
    if not poly_markets:
        missing.append("polymarket")
    if not kalshi_markets:
        missing.append("kalshi")

    if missing:
        result: dict[str, Any] = {
            "status": "incomplete",
            "missing_data": missing,
            "explanation": (
                f"No data available for: {', '.join(missing)}. "
                "Ensure historical JSON files are present in data/."
            ),
            "stats": calculate_arb_stats([]),
            "matches": [],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        _save(result, output_path)
        return result

    matches = find_overlapping_markets(poly_markets, kalshi_markets, threshold=threshold)
    stats = calculate_arb_stats(matches)

    # Serialize matches — omit raw market blobs to keep output manageable
    slim_matches = [
        {
            "poly_question": m["poly_market"].get("question") or m["poly_market"].get("title"),
            "kalshi_title": m["kalshi_market"].get("title") or m["kalshi_market"].get("question"),
            "match_score": m["match_score"],
            "spread": m["spread"],
            "locked_return": m["locked_return"],
            "category": (
                m["kalshi_market"].get("category")
                or m["poly_market"].get("category")
                or "unknown"
            ),
        }
        for m in matches
    ]

    result = {
        "status": "ok",
        "missing_data": [],
        "explanation": "",
        "stats": stats,
        "matches": slim_matches,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    _save(result, output_path)
    logger.info("Arb analysis complete: %d matches, %.1f%% profitable.", stats["total_matches"], stats["pct_above_threshold"])
    return result


def _save(data: dict[str, Any], path: str) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
        logger.info("Arb analysis saved to %s", path)
    except OSError as exc:
        logger.error("Failed to save arb analysis: %s", exc)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run_analysis()
    print(json.dumps(result["stats"], indent=2))
