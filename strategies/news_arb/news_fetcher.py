import os
import time
import hashlib
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from collections import deque
from typing import Optional

try:
    import httpx
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx", "-q"])
    import httpx

from . import config

logger = logging.getLogger("news_arb.fetcher")
log_dir = Path(__file__).parent.parent.parent / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
handler = logging.FileHandler(log_dir / "news_arb.log")
handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

_seen_ids = deque(maxlen=1000)
_daily_api_calls = 0
_daily_reset_date = None


def _article_id(title: str, source: str) -> str:
    return hashlib.md5(f"{title}:{source}".encode()).hexdigest()


def _classify_relevance(text: str) -> Optional[str]:
    text_lower = text.lower()
    for category, keywords in config.RELEVANCE_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in text_lower:
                return category
    return None


def fetch_newsapi() -> list:
    """Fetch from NewsAPI. Respects rate limits."""
    global _daily_api_calls, _daily_reset_date
    today = datetime.now(timezone.utc).date()
    if _daily_reset_date != today:
        _daily_api_calls = 0
        _daily_reset_date = today

    if _daily_api_calls >= 95:  # Leave buffer below 100
        logger.warning("NewsAPI daily limit approaching, skipping")
        return []

    api_key = config.NEWS_API_KEY
    if not api_key:
        logger.warning("NEWS_API_KEY not set, skipping NewsAPI")
        return []

    articles = []
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.get(
                "https://newsapi.org/v2/top-headlines",
                params={"country": "us", "pageSize": 50, "apiKey": api_key}
            )
            _daily_api_calls += 1
            if resp.status_code == 429:
                logger.warning("NewsAPI rate limited (429)")
                return []
            resp.raise_for_status()
            data = resp.json()

            for art in data.get("articles", []):
                aid = _article_id(art.get("title", ""), art.get("source", {}).get("name", ""))
                if aid in _seen_ids:
                    continue
                _seen_ids.append(aid)
                text = f"{art.get('title', '')} {art.get('description', '')}"
                relevance = _classify_relevance(text)
                if relevance:
                    articles.append({
                        "article_id": aid,
                        "title": art.get("title", ""),
                        "description": art.get("description", ""),
                        "source": art.get("source", {}).get("name", ""),
                        "published_at": art.get("publishedAt", ""),
                        "url": art.get("url", ""),
                        "relevance_category": relevance,
                    })
    except Exception as e:
        logger.error(f"NewsAPI fetch error: {e}")

    logger.info(f"NewsAPI returned {len(articles)} relevant articles")
    return articles


def fetch_rss() -> list:
    """Fetch from working RSS feeds (BBC, Google News, NYT)."""
    feeds = [
        "https://feeds.bbci.co.uk/news/rss.xml",
        "https://news.google.com/rss",
        "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
    ]
    articles = []
    for feed_url in feeds:
        try:
            with httpx.Client(timeout=10, follow_redirects=True) as client:
                resp = client.get(feed_url)
                if resp.status_code != 200:
                    continue
                root = ET.fromstring(resp.text)
                for item in root.iter("item"):
                    title = item.findtext("title", "")
                    desc = item.findtext("description", "")
                    link = item.findtext("link", "")
                    pub_date = item.findtext("pubDate", "")
                    source = feed_url.split("/")[2]
                    aid = _article_id(title, source)
                    if aid in _seen_ids:
                        continue
                    _seen_ids.append(aid)
                    relevance = _classify_relevance(f"{title} {desc}")
                    if relevance:
                        articles.append({
                            "article_id": aid,
                            "title": title,
                            "description": desc,
                            "source": source,
                            "published_at": pub_date,
                            "url": link,
                            "relevance_category": relevance,
                        })
        except Exception as e:
            logger.error(f"RSS fetch error for {feed_url}: {e}")

    logger.info(f"RSS returned {len(articles)} relevant articles")
    return articles


def fetch_all() -> list:
    """Fetch from all sources, deduplicated."""
    articles = fetch_newsapi() + fetch_rss()
    logger.info(f"Total articles fetched: {len(articles)}")
    return articles
