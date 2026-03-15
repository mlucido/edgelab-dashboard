"""
config.py — All tunable parameters for the Polymarket Lag Arb Sim.
Change thresholds here; nothing else needs to be touched.
"""

# ── Bankroll ───────────────────────────────────────────────────────────────
STARTING_BANKROLL      = 497.00   # USD — matches real Kalshi balance
EMERGENCY_FLOOR_PCT    = 0.20     # halt if balance < 20% of starting bankroll
DAILY_LOSS_HALT_PCT    = 0.05     # halt if daily loss > 5% of current bankroll
MAX_POSITION_PCT       = 0.08     # max single position = 8% of bankroll
MAX_DEPLOYED_PCT       = 0.70     # never have more than 70% in open positions
MAX_OPEN_POSITIONS     = 5        # paranoia mode: max simultaneous open trades
KELLY_FRACTION         = 0.25     # use 25% of full Kelly

# ── Signal thresholds ─────────────────────────────────────────────────────
MOMENTUM_WINDOW_SECS   = 45       # primary look-back window for momentum calculation
MOMENTUM_WINDOW_90_SECS = 90      # secondary look-back window (doubles event capture)
MOMENTUM_MIN_PCT       = 0.15     # minimum % move to generate a signal (relaxed from 0.40%)
WEAK_SIGNAL_THRESHOLD  = 0.10     # weak signals (0.10-0.15%): half position size
WEAK_SIGNAL_MAX_SIZE   = 20.00    # max $20 for weak signals (vs $39 for strong)
VOLUME_MULTIPLIER      = 1.0      # disabled: rolling_volume_avg() is stubbed, always returns 1.0
MIN_EDGE_PCT           = 0.05     # minimum edge over market price (5% — relaxed from 10%)
MIN_MARKET_LIQUIDITY   = 250      # skip markets with < $250 total liquidity (relaxed from $500)
MAX_BID_ASK_SPREAD     = 0.03     # skip markets where spread > 3%
POLYMARKET_FEE         = 0.02     # 2% fee on winning trades

# ── Low-price contract recalibration ────────────────────────────────────
# Contracts below this price are "deep in the money" — cheap NO on far-OTM strikes.
# The displayed best-ask price is unreliable at size (thin books, massive slippage).
LOW_PRICE_THRESHOLD    = 0.10     # contracts priced below $0.10 get special handling
LOW_PRICE_MIN_MOMENTUM = 0.20     # match MOMENTUM_MIN_PCT — unblock cheap NO trades
LOW_PRICE_SLIPPAGE     = 0.04     # assume 4 cents of slippage on thin books
LOW_PRICE_MIN_STRENGTH = "MEDIUM" # minimum signal strength for sub-$0.10 contracts

# ── Trade management ──────────────────────────────────────────────────────
STOP_LOSS_PCT          = 0.04     # exit early if unrealized loss > 4% of position
REVERSAL_EXIT_PCT      = 0.005    # exit early if spot reverses 0.50% against us
LOCK_PROFIT_SECS       = 90       # exit early if < 90s to resolution AND profitable
STALE_DATA_SECS        = 3        # reject price data older than 3 seconds

# ── Assets (Paranoia Mode: BTC only for first 30 days) ────────────────────
ASSETS = ["BTC-USD", "ETH-USD", "SOL-USD"]   # BTC + ETH + SOL for maximum signal coverage
COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"

# ── Polymarket API ────────────────────────────────────────────────────────
POLYMARKET_CLOB_URL    = "https://clob.polymarket.com"
POLYMARKET_GAMMA_URL   = "https://gamma-api.polymarket.com"

# ── Kalshi API ───────────────────────────────────────────────────────────
KALSHI_API_URL         = "https://api.elections.kalshi.com/trade-api/v2"

# ── Keyword filters for finding crypto lag markets on Polymarket ──────────
# These match the short-duration BTC/ETH/SOL directional markets
MARKET_KEYWORDS = [
    "bitcoin", "btc", "ethereum", "eth", "solana", "sol",
    "crypto", "higher", "above", "price"
]
# Markets must close within this many hours to be candidates
MAX_MARKET_HOURS_TO_CLOSE = 2

# ── Database ──────────────────────────────────────────────────────────────
DB_PATH = "sim_results.db"

# ── Dashboard refresh ─────────────────────────────────────────────────────
DASHBOARD_REFRESH_SECS = 5

# ── Logging ───────────────────────────────────────────────────────────────
LOG_LEVEL = "DEBUG"
LOG_FILE  = "sim.log"

# ── Implied probability model ─────────────────────────────────────────────
# Maps momentum strength to implied true probability of "higher in next N min"
# Calibrated from historical BTC 15-min directional accuracy at various momentum levels
# Format: (min_momentum_pct, max_momentum_pct) -> implied_probability
MOMENTUM_PROB_TABLE = [
    (0.10, 0.15, 0.53),   # weak signal  → 53% continuation prob (exploratory)
    (0.15, 0.40, 0.56),   # very weak    → 56% continuation prob
    (0.40, 0.60, 0.62),   # weak signal  → 62% continuation prob
    (0.60, 0.90, 0.68),   # medium       → 68% continuation prob
    (0.90, 1.30, 0.74),   # strong       → 74% continuation prob
    (1.30, 2.00, 0.80),   # very strong  → 80% continuation prob
    (2.00, 99.0, 0.85),   # extreme      → 85% continuation prob
]

# For low-price (deep ITM) contracts, momentum adds only a MARGINAL edge
# over the market's already-high implied probability. This table maps
# momentum to the INCREMENTAL probability bonus (percentage points).
MOMENTUM_EDGE_BONUS = [
    (0.40, 0.60, 0.02),   # weak         → +2pp over market
    (0.60, 0.90, 0.04),   # medium       → +4pp over market
    (0.90, 1.30, 0.06),   # strong       → +6pp over market
    (1.30, 2.00, 0.08),   # very strong  → +8pp over market
    (2.00, 99.0, 0.10),   # extreme      → +10pp over market
]
# For DOWN moves, implied prob applies to NO contract (same table, mirrored)
