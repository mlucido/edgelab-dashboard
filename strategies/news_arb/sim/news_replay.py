"""Replay historical news events for backtesting."""
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import httpx
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx", "-q"])
    import httpx

from .. import config

logger = logging.getLogger("news_arb.sim.replay")


def fetch_historical_headlines(days: int = 30) -> list:
    """Fetch historical headlines from NewsAPI (free tier: last 30 days)."""
    api_key = config.NEWS_API_KEY
    if not api_key:
        logger.warning("NEWS_API_KEY not set, using synthetic test data")
        return _generate_synthetic_headlines()

    articles = []
    try:
        from_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        with httpx.Client(timeout=30) as client:
            for page in range(1, 4):  # 3 pages max to stay in rate limits
                resp = client.get(
                    "https://newsapi.org/v2/everything",
                    params={
                        "q": "election OR bitcoin OR fed OR GDP OR inflation",
                        "from": from_date,
                        "language": "en",
                        "sortBy": "publishedAt",
                        "pageSize": 100,
                        "page": page,
                        "apiKey": api_key,
                    }
                )
                if resp.status_code != 200:
                    logger.warning(f"NewsAPI returned {resp.status_code}")
                    break
                data = resp.json()
                for art in data.get("articles", []):
                    articles.append({
                        "title": art.get("title", ""),
                        "description": art.get("description", ""),
                        "source": art.get("source", {}).get("name", ""),
                        "published_at": art.get("publishedAt", ""),
                        "url": art.get("url", ""),
                    })
                if len(data.get("articles", [])) < 100:
                    break
    except Exception as e:
        logger.error(f"Failed to fetch historical headlines: {e}")
        return _generate_synthetic_headlines()

    if not articles:
        return _generate_synthetic_headlines()
    return articles


def _generate_synthetic_headlines() -> list:
    """Generate synthetic headlines for backtesting when API is unavailable."""
    import random
    headlines = [
        ("Fed raises interest rate by 25 basis points", "economic", "positive"),
        ("Bitcoin ETF approved by SEC", "crypto", "positive"),
        ("President signs executive order on AI regulation", "political", "neutral"),
        ("GDP falls below expectations for Q3", "economic", "negative"),
        ("Unemployment rises to 4.2%", "economic", "negative"),
        ("Senate passes infrastructure bill", "political", "positive"),
        ("CPI inflation drops to 2.8%", "economic", "positive"),
        ("Russia-Ukraine ceasefire talks collapse", "political", "negative"),
        ("Supreme Court blocks student loan forgiveness", "political", "negative"),
        ("Bitcoin surges past $100,000", "crypto", "positive"),
        ("Fed signals rate cuts in 2026", "economic", "positive"),
        ("Election polls show tight race in swing states", "political", "neutral"),
        ("Ethereum ETF rejected by SEC", "crypto", "negative"),
        ("Jobs report beats expectations with 300K new jobs", "economic", "positive"),
        ("Congress fails to raise debt ceiling", "political", "negative"),
        ("Trump wins Republican primary", "political", "positive"),
        ("Oil prices drop as OPEC increases production", "economic", "negative"),
        ("NBA playoffs: Lakers eliminated in first round", "sports", "negative"),
        ("NFL draft: top pick signs record deal", "sports", "positive"),
        ("Trade war escalates with new China tariffs", "economic", "negative"),
        ("Hurricane warning issued for Florida coast", "political", "negative"),
        ("Tesla stock crashes after earnings miss", "crypto", "negative"),
        ("Google announces breakthrough AI model", "crypto", "positive"),
        ("Gold prices hit all-time high", "economic", "positive"),
        ("Government shutdown averted with last-minute deal", "political", "positive"),
        ("Federal Reserve chair resigns unexpectedly", "economic", "negative"),
        ("Bitcoin regulation bill passes House", "crypto", "neutral"),
        ("Inflation rises above Fed target at 3.5%", "economic", "negative"),
        ("Senate confirms new Supreme Court justice", "political", "positive"),
        ("Recession fears grow as yield curve inverts", "economic", "negative"),
    ]

    articles = []
    base = datetime.now(timezone.utc) - timedelta(days=30)
    for i, (title, category, sentiment) in enumerate(headlines):
        pub = base + timedelta(hours=i * 24)
        articles.append({
            "title": title,
            "description": f"Breaking: {title}. Market analysts react.",
            "source": random.choice(["Reuters", "AP", "Bloomberg", "CNBC"]),
            "published_at": pub.isoformat(),
            "url": f"https://example.com/article/{i}",
            "_category": category,
            "_sentiment": sentiment,
        })
    return articles
