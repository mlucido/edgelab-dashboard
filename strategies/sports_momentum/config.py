import os

POLL_INTERVAL_SECS = 15
LAG_WINDOW_SECS = 90
MIN_EDGE_PCT = 0.06
MIN_MARKET_LIQUIDITY_USD = 200
MAX_POSITION_USD = 50.0
STARTING_CAPITAL_USD = 50.0
MODE = os.environ.get("SPORTS_MOMENTUM_MODE", "PAPER")

PAPER_PROMOTION_WIN_RATE = 0.55
PAPER_PROMOTION_MIN_TRADES = 20
PAPER_PROMOTION_MIN_HOURS = 48

ESPN_BASE_URL = "https://site.api.espn.com/apis/site/v2/sports"

SPORT_ENDPOINTS = {
    "nba":   "basketball/nba",
    "nfl":   "football/nfl",
    "mlb":   "baseball/mlb",
    "ncaab": "basketball/mens-college-basketball",
}

MOMENTUM_THRESHOLDS = {
    "nba": {
        "run_points": 8,
        "run_window_minutes": 3,
        "foul_trouble_threshold": 4,
    },
    "nfl": {
        "score_swing": 7,
        "turnover_weight": 1.5,
        "redzone_weight": 1.2,
    },
    "mlb": {
        "inning_runs": 3,
        "bases_loaded_weight": 1.3,
        "pitcher_era_weight": 1.1,
    },
    "ncaab": {
        "run_points": 10,
        "run_window_minutes": 4,
        "foul_trouble_threshold": 3,
    },
}

KALSHI_API_KEY = os.environ.get("KALSHI_API_KEY", "")
KALSHI_KEY_ID = os.environ.get("KALSHI_KEY_ID", "")

# Broadened to Event Momentum — political/economic momentum alongside sports
STRATEGY_DISPLAY_NAME = "Event Momentum"

# Only poll ESPN for sports where liquid Kalshi markets exist
REQUIRE_LIQUID_KALSHI_MARKET = True

# Economic momentum triggers (for news-driven signals on economic markets)
ECONOMIC_MOMENTUM_TRIGGERS = {
    "beat": ["beat", "exceeded", "above expectations", "stronger than", "surpassed"],
    "miss": ["missed", "below expectations", "weaker than", "disappointing", "fell short"],
    "surprise": ["surprise", "unexpected", "shocked", "historic", "unprecedented"],
}
