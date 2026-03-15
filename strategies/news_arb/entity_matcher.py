import json
import logging
import re
from pathlib import Path

logger = logging.getLogger("news_arb.entity_matcher")

_entity_map = None

def _load_entity_map() -> dict:
    global _entity_map
    if _entity_map is None:
        map_path = Path(__file__).parent / "entity_map.json"
        with open(map_path) as f:
            _entity_map = json.load(f)
    return _entity_map


def _keyword_score(text: str, market_title: str) -> float:
    """Simple TF-IDF-style keyword overlap scoring."""
    text_words = set(re.findall(r'\w+', text.lower()))
    market_words = set(re.findall(r'\w+', market_title.lower()))
    if not text_words or not market_words:
        return 0.0
    overlap = text_words & market_words
    # Normalize by smaller set size
    return len(overlap) / min(len(text_words), len(market_words))


def match_news_to_markets(article: dict, open_markets: list) -> list:
    """
    Match a news article to open markets.
    Returns list of (market_id, match_score, match_reason).
    """
    entity_map = _load_entity_map()
    text = f"{article.get('title', '')} {article.get('description', '')}".lower()
    matches = []

    # Entity map matching
    for entity, prefixes in entity_map.items():
        if entity.lower() in text:
            for market in open_markets:
                mid = market.get("ticker", market.get("id", "")).upper()
                title = market.get("title", "")
                for prefix in prefixes:
                    if mid.startswith(prefix):
                        score = 0.8 + _keyword_score(text, title) * 0.2
                        matches.append((
                            mid, round(score, 3),
                            f"entity_map: '{entity}' → prefix '{prefix}'"
                        ))
                        break

    # Fuzzy keyword matching for markets not caught by entity map
    matched_ids = {m[0] for m in matches}
    for market in open_markets:
        mid = market.get("ticker", market.get("id", "")).upper()
        if mid in matched_ids:
            continue
        title = market.get("title", "")
        score = _keyword_score(text, title)
        if score > 0.15:
            matches.append((mid, round(score, 3), f"keyword_overlap: {score:.2f}"))

    # Sort by score descending
    matches.sort(key=lambda x: x[1], reverse=True)
    return matches[:10]  # Top 10
