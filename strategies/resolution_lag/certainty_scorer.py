"""
Certainty Scorer — converts a resolved event + market context into a CertaintyScore.

Score breakdown:
  +0.40  Kalshi market finalized (authoritative settlement)
  +0.30  ESPN final score (official sports data)
  +0.25  Multiple independent sources agree
  +0.20  Government/official source (BLS, BEA, Fed, SEC)
  +0.15  Single reliable source (Reuters, AP, BBC)
  +0.10  News resolution language matched
  -0.20  Conflicting signals detected
  -0.30  Outcome could still change (preliminary data, overtime possible)

Only proceed if final score >= MIN_CERTAINTY_SCORE (0.85).
"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from . import config

logger = logging.getLogger("resolution_lag.certainty")


# Source trust tiers
_GOVERNMENT_DOMAINS = {"bls.gov", "bea.gov", "federalreserve.gov", "sec.gov",
                        "fred.stlouisfed.org", "census.gov"}
_RELIABLE_DOMAINS = {"reuters.com", "apnews.com", "bbc.com", "bloomberg.com",
                     "espn.com", "api.espn.com"}

_UNCERTAINTY_PHRASES = [
    "could still", "may change", "pending review", "preliminary", "subject to",
    "overtime possible", "protest possible", "recount", "unofficial",
    "not yet confirmed", "awaiting confirmation",
]

_RESOLUTION_LANGUAGE = [
    "officially declared", "has won", "final score", "confirmed",
    "signed into law", "approved", "rejected", "data released",
]


def score_certainty(resolved_event: dict, market: dict = None) -> dict:
    """
    Score the certainty of a resolved event.

    Args:
        resolved_event: dict from resolution_detector
        market: optional Kalshi/platform market dict for additional context

    Returns:
        CertaintyScore dict:
          {
            score: float,           # 0.0–1.0 (clamped)
            factors: list[str],     # human-readable scoring factors
            recommended_side: str,  # "YES" | "NO" | "SKIP"
            passes_threshold: bool,
          }
    """
    score = 0.0
    factors = []
    deductions = []

    source_method = resolved_event.get("_source_method", "")
    source_url = resolved_event.get("source_url", "").lower()
    description = (resolved_event.get("event_description", "") + " " +
                   resolved_event.get("outcome", "")).lower()

    # ── Positive factors ──────────────────────────────────────────────────────

    if source_method == "kalshi_finalized":
        # Kalshi's own settlement is ground truth — near-maximum certainty
        winning_side = resolved_event.get("_winning_side", "")
        if winning_side:
            # Finalized with a known result: 0.99 certainty (Kalshi = source of truth)
            score += 0.99
            factors.append("+0.99 Kalshi market finalized with result (ground truth — intra-Kalshi eligible)")
        else:
            score += 0.95
            factors.append("+0.95 Kalshi market finalized (authoritative settlement)")

    if source_method == "espn_final":
        score += 0.30
        factors.append("+0.30 ESPN final score confirmed")

    # Check for government/official domain
    is_govt_source = any(domain in source_url for domain in _GOVERNMENT_DOMAINS)
    if is_govt_source:
        score += 0.20
        factors.append("+0.20 Government/official source")

    # Check for reliable news source
    is_reliable = any(domain in source_url for domain in _RELIABLE_DOMAINS)
    if is_reliable and not is_govt_source and source_method != "espn_final":
        score += 0.15
        factors.append("+0.15 Reliable news source (Reuters/AP/BBC/Bloomberg)")

    # Multiple sources agreement (if event carries high confidence from detector)
    if resolved_event.get("outcome_confidence", 0) >= 0.90 and not source_method == "kalshi_finalized":
        score += 0.25
        factors.append("+0.25 High-confidence multi-source agreement")

    # News resolution language
    if source_method == "news_api":
        matched = resolved_event.get("_matched_phrases", [])
        if matched:
            score += 0.10
            factors.append(f"+0.10 News resolution language: {matched[0]!r}")

    # BLS/FRED economic data
    if source_method in ("bls_api", "fred_api"):
        score += 0.20
        factors.append("+0.20 Government statistical release (BLS/FRED)")

    # ── Negative factors ──────────────────────────────────────────────────────

    # Uncertainty language in description
    uncertainty_hits = [p for p in _UNCERTAINTY_PHRASES if p in description]
    if uncertainty_hits:
        score -= 0.30
        deductions.append(f"-0.30 Outcome could change: {uncertainty_hits[0]!r}")

    # Conflicting signals: if event has lower confidence from source
    base_confidence = resolved_event.get("outcome_confidence", 1.0)
    if base_confidence < 0.70:
        score -= 0.20
        deductions.append(f"-0.20 Conflicting/low-confidence source ({base_confidence:.0%})")
    elif base_confidence < 0.85:
        score -= 0.10
        deductions.append(f"-0.10 Moderate source confidence ({base_confidence:.0%})")

    # Clamp to [0, 1]
    final_score = max(0.0, min(1.0, score))
    all_factors = factors + deductions

    # ── Determine recommended side ────────────────────────────────────────────
    recommended_side = _determine_recommended_side(resolved_event, market)

    passes = final_score >= config.MIN_CERTAINTY_SCORE

    if passes:
        logger.info(
            f"Certainty score {final_score:.2f} PASSES for "
            f"{resolved_event.get('event_description', '')[:60]} "
            f"→ side={recommended_side}"
        )
    else:
        logger.debug(
            f"Certainty score {final_score:.2f} FAILS threshold "
            f"({config.MIN_CERTAINTY_SCORE}) for "
            f"{resolved_event.get('event_description', '')[:60]}"
        )

    return {
        "score": round(final_score, 3),
        "factors": all_factors,
        "recommended_side": recommended_side,
        "passes_threshold": passes,
    }


def _determine_recommended_side(resolved_event: dict, market: dict = None) -> str:
    """
    Infer whether the market should be traded YES or NO based on the resolved outcome.

    For Kalshi finalized: use _winning_side directly.
    For sports: check _winner vs market title keywords.
    For others: default to context clues.
    """
    source_method = resolved_event.get("_source_method", "")

    # Kalshi tells us directly
    if source_method == "kalshi_finalized":
        winning_side = resolved_event.get("_winning_side", "YES").upper()
        return winning_side if winning_side in ("YES", "NO") else "YES"

    # Sports: did the "winner" match the market's YES side?
    if source_method == "espn_final" and market:
        winner = resolved_event.get("_winner", "").lower()
        market_title = market.get("title", "").lower()
        # If market title contains the winner, trade YES; loser → NO
        if winner and winner in market_title:
            return "YES"
        loser = resolved_event.get("_loser", "").lower()
        if loser and loser in market_title:
            return "NO"

    # Economic releases: hard to map directly without knowing the question
    # Default to YES (most economic markets are phrased as "will X exceed Y?")
    # The lag scanner will validate this against current price
    if source_method in ("bls_api", "fred_api"):
        return "YES"

    # News: use resolution language sentiment
    if source_method == "news_api":
        outcome = resolved_event.get("outcome", "").lower()
        if any(w in outcome for w in ["rejected", "failed", "lost", "defeated", "fell"]):
            return "NO"
        return "YES"

    return "YES"
