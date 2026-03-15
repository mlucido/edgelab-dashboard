import logging
import re
from typing import Optional

from . import config

logger = logging.getLogger("news_arb.edge_calculator")

POSITIVE_WORDS = {"wins", "passes", "approved", "rises", "confirms", "elected",
                  "surges", "gains", "rallies", "beats", "succeeds", "accepts",
                  "endorses", "supports", "advances", "climbs", "jumps"}
NEGATIVE_WORDS = {"fails", "rejected", "falls", "resigns", "denied", "loses",
                  "crashes", "drops", "plunges", "declines", "opposes", "vetoes",
                  "collapses", "withdraws", "suspends", "blocks", "defeats"}


def compute_sentiment(text: str) -> float:
    """Keyword-based sentiment scoring. Returns value in [-1, +1]."""
    words = set(re.findall(r'\w+', text.lower()))
    pos = len(words & POSITIVE_WORDS)
    neg = len(words & NEGATIVE_WORDS)
    total = pos + neg
    if total == 0:
        return 0.0
    raw = (pos - neg) / total
    return max(-1.0, min(1.0, raw))


def implied_probability(sentiment: float) -> Optional[float]:
    """Convert sentiment to implied probability. Returns None if ambiguous."""
    if sentiment > 0.5:
        return 0.75 + (sentiment - 0.5) * 0.4
    elif sentiment < -0.5:
        return 0.25 - (abs(sentiment) - 0.5) * 0.4
    else:
        return None  # Ambiguous


def calculate_edge(article: dict, market_odds: float) -> Optional[dict]:
    """
    Given article and current market odds, compute edge.
    Returns dict with edge_pct, direction, confidence, or None if no edge.
    """
    text = f"{article.get('title', '')} {article.get('description', '')}"
    sentiment = compute_sentiment(text)
    impl_prob = implied_probability(sentiment)

    if impl_prob is None:
        return None

    edge = abs(impl_prob - market_odds)
    if edge < config.MIN_EDGE_PCT:
        return None

    direction = "YES" if impl_prob > market_odds else "NO"
    confidence = "HIGH" if edge > 0.15 else "MEDIUM" if edge > 0.08 else "LOW"

    return {
        "sentiment": round(sentiment, 3),
        "implied_prob": round(impl_prob, 3),
        "market_prob": round(market_odds, 3),
        "edge_pct": round(edge, 4),
        "direction": direction,
        "confidence": confidence,
    }
