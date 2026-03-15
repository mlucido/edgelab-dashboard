import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from EdgeLab root
_env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(dotenv_path=_env_path, override=False)

NEWS_POLL_INTERVAL_SECS = 30
LAG_WINDOW_SECS = 120
MIN_EDGE_PCT = 0.05
MIN_MARKET_LIQUIDITY_USD = 100
MAX_POSITION_USD = 10.0
STARTING_CAPITAL_USD = 25.0
MODE = os.environ.get("NEWS_ARB_MODE", "PAPER")  # No Kalshi order executor wired — PAPER only
PAPER_PROMOTION_WIN_RATE = 0.55
PAPER_PROMOTION_MIN_TRADES = 20
PAPER_PROMOTION_MIN_HOURS = 48

RELEVANCE_KEYWORDS = {
    "political": ["election", "president", "senate", "congress", "vote", "poll",
                  "appointed", "resign", "impeach", "executive order", "legislation",
                  "supreme court", "cabinet", "filibuster", "veto", "confirmation",
                  "midterm", "ballot", "primary", "speaker", "majority leader"],
    "economic": ["GDP", "CPI", "inflation", "Fed", "interest rate", "unemployment",
                 "jobs report", "FOMC", "rate hike", "rate cut", "basis points",
                 "retail sales", "housing starts", "trade deficit", "PMI",
                 "nonfarm payrolls", "consumer confidence", "core PCE", "durable goods"],
    "geopolitical": ["war", "ceasefire", "sanctions", "NATO", "treaty", "nuclear",
                     "invasion", "occupation", "airstrike", "peace talks", "embargo"],
    "crypto_regulatory": ["bitcoin ETF", "SEC", "crypto regulation", "stablecoin",
                          "CFTC", "digital asset", "crypto bill", "token approval"],
}

NEWS_SOURCES = ["reuters.com", "apnews.com", "bbc.com", "bloomberg.com", "cnbc.com"]

NEWS_API_KEY = os.environ.get("NEWSAPI_KEY", "")
KALSHI_API_KEY = os.environ.get("KALSHI_API_KEY", "")
KALSHI_KEY_ID = os.environ.get("KALSHI_KEY_ID", "")
POLYMARKET_API_KEY = os.environ.get("POLYMARKET_API_KEY", "")

# Liquid market targeting
USE_LIQUID_MARKETS_CACHE = True
LIQUID_MARKETS_CACHE_PATH = str(Path(__file__).parent.parent.parent / "framework" / "liquid_markets.json")
MIN_MARKET_VOLUME = 100
