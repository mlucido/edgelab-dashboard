import sqlite3
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "learning.db"
logger = logging.getLogger("framework.trade_journal")

def init_db():
    """Create tables if not exist. Safe to call multiple times."""
    schema_path = Path(__file__).parent / "schema.sql"
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(schema_path.read_text())

def log_trade(strategy_name, market_id, platform, side, price, size,
              edge_pct=0.0, signal_strength="MEDIUM",
              market_conditions=None, notes=""):
    """Call this after placing any trade across any strategy. Returns inserted trade ID."""
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute("""
            INSERT INTO trades (timestamp, strategy_name, market_id, platform,
                                side, price, size, edge_pct, signal_strength,
                                market_conditions, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now(timezone.utc).isoformat(),
            strategy_name, market_id, platform, side,
            price, size, edge_pct, signal_strength,
            json.dumps(market_conditions or {}), notes
        ))
        return cursor.lastrowid

def update_trade_outcome(trade_id, outcome, pnl):
    """Call when a trade resolves. outcome = 'WIN' or 'LOSS'."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            UPDATE trades SET outcome=?, pnl=? WHERE id=?
        """, (outcome, pnl, trade_id))

def get_recent_trades(strategy_name=None, limit=100):
    """Return recent trades, optionally filtered by strategy."""
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        if strategy_name:
            rows = conn.execute("""
                SELECT * FROM trades WHERE strategy_name=?
                ORDER BY timestamp DESC LIMIT ?
            """, (strategy_name, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM trades ORDER BY timestamp DESC LIMIT ?
            """, (limit,)).fetchall()
        return [dict(r) for r in rows]
