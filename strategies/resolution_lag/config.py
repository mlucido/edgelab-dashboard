# PARKED (2026-03-15): Kalshi CLOBs clear atomically — no intra-Kalshi lag exists.
# Revival path: Polymarket US deposits reopen (cross-platform lag)
# No code changes needed — strategy is ready to activate when viable.

"""
Resolution Lag strategy configuration.
All credentials read from ~/Dropbox/EdgeLab/.env — never printed.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from EdgeLab root
_env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(dotenv_path=_env_path, override=False)

# ── Timing ──────────────────────────────────────────────────────────────────
SCAN_INTERVAL_SECS = 20
MAX_LAG_WINDOW_SECS = 1800   # 30 min: max time a lag opportunity stays valid
MIN_LAG_WINDOW_SECS = 60     # 1 min: ignore events resolved <60s ago

# ── Signal thresholds ───────────────────────────────────────────────────────
MIN_CERTAINTY_SCORE = 0.85
MIN_EDGE_PCT = 0.08          # Minimum 8% edge after fees + slippage

# ── Position sizing ─────────────────────────────────────────────────────────
MAX_POSITION_USD = 75.0
STARTING_CAPITAL_USD = 50.0

# ── Mode ────────────────────────────────────────────────────────────────────
MODE = os.environ.get("RESOLUTION_LAG_MODE", "PAPER")

# ── Promotion criteria ──────────────────────────────────────────────────────
PAPER_PROMOTION_WIN_RATE = 0.60
PAPER_PROMOTION_MIN_TRADES = 15
PAPER_PROMOTION_MIN_HOURS = 24

# ── Resolution source config ─────────────────────────────────────────────────
RESOLUTION_SOURCES = {
    "politics": {
        "sources": ["apnews.com", "reuters.com", "bbc.com"],
        "keywords": ["election called", "declared winner", "concedes", "sworn in",
                     "appointed", "confirmed", "signed into law", "vetoed", "passed"],
    },
    "economics": {
        "sources": ["bls.gov", "bea.gov", "federalreserve.gov", "fred.stlouisfed.org"],
        "keywords": ["jobs added", "unemployment rate", "CPI rose", "CPI fell",
                     "GDP grew", "GDP contracted", "Fed raised", "Fed cut", "basis points"],
        "scheduled_releases": {
            "Jobs Report": {"agency": "BLS", "series_id": "CES0000000001"},
            "CPI": {"agency": "BLS", "series_id": "CUUR0000SA0"},
            "GDP": {"agency": "BEA", "url": "https://www.bea.gov/data/gdp/gross-domestic-product"},
            "FOMC": {"agency": "FED", "url": "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"},
        },
    },
    "sports": {
        "sources": ["espn.com", "api.espn.com"],
        "keywords": ["final score", "game over", "wins", "defeats", "championship",
                     "eliminated", "advances"],
        "espn_sports": ["football", "basketball", "baseball", "hockey", "soccer"],
    },
    "crypto": {
        "sources": ["coindesk.com", "cointelegraph.com", "sec.gov"],
        "keywords": ["approved", "rejected", "launched", "halted", "ruling",
                     "ETF approved", "SEC approved"],
    },
}

# ── API credentials (read from env, never log) ───────────────────────────────
KALSHI_API_KEY = os.environ.get("KALSHI_API_KEY", "")
KALSHI_KEY_ID = os.environ.get("KALSHI_KEY_ID", "")
NEWS_API_KEY = os.environ.get("NEWSAPI_KEY", "")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")

# ── Fee / slippage model ─────────────────────────────────────────────────────
ESTIMATED_FEE_PCT = 0.01     # ~1% round-trip fee on Kalshi
ESTIMATED_SLIPPAGE_PCT = 0.01  # 1% slippage on thin post-resolution book
