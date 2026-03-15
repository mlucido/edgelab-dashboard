"""
Kalshi → Polymarket market name fuzzy matcher.

Trains on historical overlapping markets and provides a reusable matcher
that can be serialized to data/kalshi_matcher.pkl for fast lookups.

Falls back to rapidfuzz token_sort_ratio when no trained model is available.
"""
from __future__ import annotations

import json
import logging
import os
import pickle
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
_MATCHER_PATH = os.path.join(_DATA_DIR, "kalshi_matcher.pkl")

# Stopwords to strip for better matching
_STOPWORDS = frozenset({
    "will", "the", "a", "an", "be", "in", "on", "at", "to", "of", "for",
    "by", "is", "it", "or", "and", "this", "that", "with", "as", "from",
    "before", "after", "above", "below", "between", "does", "do", "did",
    "has", "have", "had", "was", "were", "are", "been", "being",
    "market", "contract", "event",
})


def _normalize_text(text: str) -> str:
    """Lowercase, strip punctuation, remove stopwords."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s]", " ", text)
    tokens = [t for t in text.split() if t not in _STOPWORDS]
    return " ".join(tokens)


def _fuzzy_score(a: str, b: str) -> float:
    """Token sort ratio via rapidfuzz, fallback to Jaccard."""
    try:
        from rapidfuzz import fuzz
        return fuzz.token_sort_ratio(a, b)
    except ImportError:
        tokens_a = set(a.split())
        tokens_b = set(b.split())
        if not tokens_a or not tokens_b:
            return 0.0
        return len(tokens_a & tokens_b) / len(tokens_a | tokens_b) * 100


class KalshiMatcher:
    """
    Fuzzy matcher trained on known Kalshi↔Polymarket market pairs.

    The "training" phase learns:
    - Normalized text pairs and their match scores
    - Category-specific score thresholds
    - Common synonym mappings (e.g. "bitcoin" ↔ "btc")

    At inference time, uses these learned patterns + rapidfuzz to match
    new Kalshi market titles to Polymarket questions.
    """

    def __init__(self, threshold: float = 65.0):
        self.threshold = threshold
        self.known_pairs: list[dict[str, Any]] = []
        self.synonyms: dict[str, str] = {
            "btc": "bitcoin",
            "eth": "ethereum",
            "sol": "solana",
            "pres": "president",
            "gop": "republican",
            "dem": "democrat",
            "scotus": "supreme court",
            "cpi": "consumer price index",
            "gdp": "gross domestic product",
            "fomc": "federal reserve",
            "nfl": "national football league",
            "nba": "national basketball association",
            "q1": "first quarter",
            "q2": "second quarter",
            "q3": "third quarter",
            "q4": "fourth quarter",
        }
        self.category_thresholds: dict[str, float] = {}
        self._poly_index: list[tuple[str, str, dict]] = []  # (normalized, raw, market)

    def _apply_synonyms(self, text: str) -> str:
        tokens = text.split()
        return " ".join(self.synonyms.get(t, t) for t in tokens)

    def _prep(self, text: str) -> str:
        return self._apply_synonyms(_normalize_text(text))

    def train(self, matched_pairs: list[dict[str, Any]]) -> None:
        """
        Train on known matched pairs from arb_analyzer output.

        Each pair should have:
            poly_question, kalshi_title, match_score, category (optional)
        """
        self.known_pairs = matched_pairs

        # Learn category-specific thresholds
        cat_scores: dict[str, list[float]] = {}
        for pair in matched_pairs:
            cat = pair.get("category", "unknown")
            score = pair.get("match_score", 0.0)
            cat_scores.setdefault(cat, []).append(score)

        for cat, scores in cat_scores.items():
            # Set threshold at 90th percentile of known matches
            sorted_scores = sorted(scores)
            idx = max(0, int(len(sorted_scores) * 0.1) - 1)
            self.category_thresholds[cat] = sorted_scores[idx]

        logger.info(
            "Matcher trained on %d pairs, %d categories",
            len(matched_pairs),
            len(self.category_thresholds),
        )

    def build_index(self, poly_markets: list[dict[str, Any]]) -> None:
        """Pre-index Polymarket markets for fast matching."""
        self._poly_index = []
        for m in poly_markets:
            raw = m.get("question") or m.get("title") or ""
            if raw:
                self._poly_index.append((self._prep(raw), raw, m))
        logger.info("Indexed %d Polymarket markets", len(self._poly_index))

    def match(self, kalshi_title: str) -> Optional[dict[str, Any]]:
        """
        Find best Polymarket match for a Kalshi market title.

        Returns dict with: poly_market, match_score, kalshi_title_normalized
        or None if no match above threshold.
        """
        if not self._poly_index:
            return None

        prep_kalshi = self._prep(kalshi_title)
        best_score = -1.0
        best_match: Optional[tuple] = None

        for norm_poly, raw_poly, poly_market in self._poly_index:
            score = _fuzzy_score(prep_kalshi, norm_poly)
            if score > best_score:
                best_score = score
                best_match = (raw_poly, poly_market)

        if best_score < self.threshold or best_match is None:
            return None

        return {
            "poly_question": best_match[0],
            "poly_market": best_match[1],
            "match_score": round(best_score, 2),
            "kalshi_title_normalized": prep_kalshi,
        }

    def batch_match(
        self, kalshi_markets: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Match a batch of Kalshi markets. Returns list of successful matches."""
        results = []
        for km in kalshi_markets:
            title = km.get("title") or km.get("question") or ""
            if not title:
                continue
            result = self.match(title)
            if result:
                result["kalshi_market"] = km
                results.append(result)
        return results

    def save(self, path: str = _MATCHER_PATH) -> None:
        """Serialize the trained matcher to disk."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        state = {
            "threshold": self.threshold,
            "known_pairs": self.known_pairs,
            "synonyms": self.synonyms,
            "category_thresholds": self.category_thresholds,
        }
        with open(path, "wb") as fh:
            pickle.dump(state, fh, protocol=pickle.HIGHEST_PROTOCOL)
        logger.info("Matcher saved to %s", path)

    @classmethod
    def load(cls, path: str = _MATCHER_PATH) -> "KalshiMatcher":
        """Load a previously trained matcher from disk."""
        with open(path, "rb") as fh:
            state = pickle.load(fh)
        matcher = cls(threshold=state.get("threshold", 65.0))
        matcher.known_pairs = state.get("known_pairs", [])
        matcher.synonyms = state.get("synonyms", matcher.synonyms)
        matcher.category_thresholds = state.get("category_thresholds", {})
        logger.info("Matcher loaded from %s (%d pairs)", path, len(matcher.known_pairs))
        return matcher


def train_and_save(arb_analysis_path: Optional[str] = None) -> KalshiMatcher:
    """
    Train a matcher from arb_analysis.json output and save to pickle.

    If no analysis data exists, creates an untrained matcher with synonym
    mappings only (still useful for basic matching).
    """
    if arb_analysis_path is None:
        arb_analysis_path = os.path.join(_DATA_DIR, "arb_analysis.json")

    matcher = KalshiMatcher()

    try:
        with open(arb_analysis_path, "r") as fh:
            analysis = json.load(fh)
        matches = analysis.get("matches", [])
        if matches:
            matcher.train(matches)
            logger.info("Trained matcher on %d historical matches", len(matches))
        else:
            logger.info("No historical matches to train on; matcher uses defaults")
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.warning("Could not load arb analysis for training: %s", exc)

    matcher.save()
    return matcher


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    m = train_and_save()
    print(f"Matcher saved with {len(m.known_pairs)} trained pairs")
    print(f"Synonyms: {len(m.synonyms)}")
    print(f"Category thresholds: {m.category_thresholds}")
