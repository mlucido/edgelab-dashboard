"""
Strategy Registry — single source of truth for all EdgeLab strategies.

Each entry maps a short key to display metadata. Status values:
  LIVE      — trading real capital
  PAPER     — paper-trading, collecting data
  BUILDING  — under construction / not yet running
  DISABLED  — parked indefinitely
"""

STRATEGY_REGISTRY = {
    "bot": {
        "name": "Kalshi Momentum Bot",
        "status": "LIVE",
        "db_path": "strategies/bot/sim_results.db",
        "description": "Crypto momentum",
    },
    "edgelab": {
        "name": "Event Arb Scanner",
        "status": "PAPER",
        "db_path": "strategies/edgelab/",
        "description": "Multi-platform arb",
    },
    "behavioral_bias": {
        "name": "Behavioral Bias",
        "status": "BUILDING",
        "db_path": None,
        "description": "Favorite-longshot bias",
    },
    "stale_quote": {
        "name": "Stale Quote",
        "status": "BUILDING",
        "db_path": None,
        "description": "Aged limit order lifter",
    },
    "resolution_lag": {
        "name": "Resolution Lag",
        "status": "BUILDING",
        "db_path": None,
        "description": "Already-resolved markets",
    },
    "alt_data": {
        "name": "Alt Data (Sports/Weather)",
        "status": "BUILDING",
        "db_path": None,
        "description": "ESPN + NWS + Congress",
    },
    "stat_pairs": {
        "name": "Stat Pairs",
        "status": "BUILDING",
        "db_path": None,
        "description": "Correlated market arb",
    },
    "theta_decay": {
        "name": "Theta Decay",
        "status": "BUILDING",
        "db_path": None,
        "description": "Time premium capture",
    },
    "clob_mm": {
        "name": "CLOB Market Making",
        "status": "BUILDING",
        "db_path": None,
        "description": "Polymarket bid-ask spread",
    },
    "political_base": {
        "name": "Political Base Rate",
        "status": "BUILDING",
        "db_path": None,
        "description": "Incumbent/base rate edge",
    },
    "cross_venue": {
        "name": "Cross-Venue Arb",
        "status": "BUILDING",
        "db_path": None,
        "description": "Kalshi vs Polymarket",
    },
    "vol_signal": {
        "name": "Volume Signal",
        "status": "BUILDING",
        "db_path": None,
        "description": "Whale flow detector",
    },
}
