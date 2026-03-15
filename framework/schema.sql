CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    market_id TEXT,
    platform TEXT,
    side TEXT,
    price REAL,
    size REAL,
    edge_pct REAL,
    signal_strength TEXT,
    market_conditions TEXT,
    outcome TEXT DEFAULT 'OPEN',
    pnl REAL DEFAULT 0.0,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS strategy_performance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_name TEXT NOT NULL,
    date TEXT NOT NULL,
    trades INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    losses INTEGER DEFAULT 0,
    pnl REAL DEFAULT 0.0,
    win_rate REAL DEFAULT 0.0,
    avg_edge REAL DEFAULT 0.0,
    capital_deployed REAL DEFAULT 0.0,
    roi REAL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS config_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_name TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    param_name TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    reason TEXT
);

CREATE TABLE IF NOT EXISTS capital_allocations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    allocated_pct REAL,
    allocated_usd REAL,
    reason TEXT
);
