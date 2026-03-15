import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from .news_fetcher import fetch_all
from .entity_matcher import match_news_to_markets
from .edge_calculator import calculate_edge

logger = logging.getLogger("news_arb.signal_generator")
log_dir = Path(__file__).parent.parent.parent / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
sig_handler = logging.FileHandler(log_dir / "news_arb_signals.log")
sig_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
logger.addHandler(sig_handler)
logger.setLevel(logging.INFO)

_recent_signals = {}  # market_id -> timestamp of last signal


def _get_open_markets() -> list:
    """Fetch open markets from Kalshi. Returns list of market dicts."""
    try:
        import httpx
        import os
        with httpx.Client(timeout=15) as client:
            resp = client.get(
                "https://api.elections.kalshi.com/trade-api/v2/markets",
                params={"status": "open", "limit": 100},
            )
            if resp.status_code == 200:
                return resp.json().get("markets", [])
    except Exception as e:
        logger.error(f"Failed to fetch open markets: {e}")
    return []


def generate_signals() -> list:
    """Run one cycle: fetch news → match → calculate edge → emit signals."""
    articles = fetch_all()
    open_markets = _get_open_markets()

    if not open_markets:
        logger.warning("No open markets available")
        return []

    signals = []
    now = time.time()

    for article in articles:
        matches = match_news_to_markets(article, open_markets)
        for market_id, match_score, match_reason in matches:
            # Dedup: skip if we signaled this market in last 10 min
            if market_id in _recent_signals and (now - _recent_signals[market_id]) < 600:
                logger.info(f"SKIP (dedup): {market_id} — {article['title'][:60]}")
                continue

            # Get current odds from matched market
            market = next((m for m in open_markets
                          if m.get("ticker", m.get("id", "")).upper() == market_id), None)
            if not market:
                continue
            market_odds = market.get("yes_ask", market.get("last_price", 0.5))
            if not isinstance(market_odds, (int, float)):
                market_odds = 0.5

            edge_result = calculate_edge(article, market_odds)
            if edge_result is None:
                logger.info(f"REJECT (no edge): {market_id} — {article['title'][:60]}")
                continue

            signal = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "article_title": article["title"],
                "article_url": article.get("url", ""),
                "market_id": market_id,
                "platform": "kalshi",
                "direction": edge_result["direction"],
                "edge_pct": edge_result["edge_pct"],
                "implied_prob": edge_result["implied_prob"],
                "market_prob": edge_result["market_prob"],
                "confidence": edge_result["confidence"],
                "match_score": match_score,
                "match_reason": match_reason,
                "relevance_category": article.get("relevance_category", ""),
            }
            signals.append(signal)
            _recent_signals[market_id] = now
            logger.info(f"SIGNAL: {signal['direction']} {market_id} edge={signal['edge_pct']:.2%} — {article['title'][:60]}")

    return signals
