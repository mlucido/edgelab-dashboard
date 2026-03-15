"""
Liquidity Provision Strategy — Configuration
All dollar values in USD. All pct values are decimal fractions.
"""
import os
from pathlib import Path

SCAN_INTERVAL_SECS = 30
MIN_SPREAD_PCT = 0.05             # 5 cents minimum spread
TARGET_SPREAD_CAPTURE_PCT = 0.03  # aim to capture 3 cents of every 5-cent spread
MAX_INVENTORY_USD = 30.0          # max one-sided exposure
MAX_POSITION_USD = 15.0           # per market
STARTING_CAPITAL_USD = 50.0
MODE = os.environ.get("LP_MODE", "PAPER")

PAPER_PROMOTION_WIN_RATE = 0.52
PAPER_PROMOTION_MIN_TRADES = 30
PAPER_PROMOTION_MIN_HOURS = 48

AVOID_CATEGORIES = ["sports", "weather", "entertainment", "crypto_price_range"]
PREFER_CATEGORIES = ["political_stable", "economic_release", "regulatory"]

MAX_TIME_TO_RESOLUTION_HOURS = 168   # 1 week
MIN_TIME_TO_RESOLUTION_HOURS = 24

# API keys (loaded from environment)
KALSHI_API_KEY = os.environ.get("KALSHI_API_KEY", "")
KALSHI_KEY_ID = os.environ.get("KALSHI_KEY_ID", "")

# Rate limiting
KALSHI_ORDERBOOK_RATE_LIMIT = 10   # requests per second

# Market targeting — focus on liquid categories
USE_LIQUID_MARKETS_CACHE = True
LIQUID_MARKETS_CACHE_PATH = str(Path(__file__).parent.parent.parent / "framework" / "liquid_markets.json")
