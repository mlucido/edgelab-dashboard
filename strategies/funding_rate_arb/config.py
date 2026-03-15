import os

SCAN_INTERVAL_SECS = 60
FUNDING_EXTREME_THRESHOLD = 0.0005   # 0.05%
FUNDING_SPIKE_THRESHOLD = 0.001      # 0.10%
MIN_EDGE_PCT = 0.05
MAX_POSITION_USD = 50.0
STARTING_CAPITAL_USD = 50.0
MODE = os.environ.get("FUNDING_ARB_MODE", "PAPER")
PAPER_PROMOTION_WIN_RATE = 0.55
PAPER_PROMOTION_MIN_TRADES = 20
PAPER_PROMOTION_MIN_HOURS = 48

FUNDING_SOURCES = {
    "binance": "https://fapi.binance.com/fapi/v1/fundingRate",
    "bybit": "https://api.bybit.com/v5/market/funding/history",
    "okx": "https://www.okx.com/api/v5/public/funding-rate",
}

ASSETS = {
    "BTC": {"binance_symbol": "BTCUSDT", "bybit_symbol": "BTCUSDT", "kalshi_prefix": "BTC-"},
    "ETH": {"binance_symbol": "ETHUSDT", "bybit_symbol": "ETHUSDT", "kalshi_prefix": "ETH-"},
    "SOL": {"binance_symbol": "SOLUSDT", "bybit_symbol": "SOLUSDT", "kalshi_prefix": "SOL-"},
}

LOOKBACK_PERIODS = 3
SIGNAL_WINDOW_HOURS = 4

# Cross-asset signal targets: funding rate → Kalshi economic markets
CROSS_ASSET_TARGETS = {
    "BTC_bearish": ["CPI", "INFL", "FED"],     # high BTC funding → inflation/hike fears
    "BTC_bullish": ["FED", "PIVOT", "CUT"],     # negative BTC funding → rate cut expectations
    "ETH_bearish": ["SEC", "CRYPTO", "REG"],    # ETH bearish → regulatory fear
}
MIN_FUNDING_RATE_FOR_CROSS_SIGNAL = 0.0003  # 0.03%
