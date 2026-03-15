import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "learning.db"
logger = logging.getLogger("framework.monitor")

def get_strategy_status(strategy_name: str) -> dict:
    """Get current status summary for a strategy."""
    from .trade_journal import init_db
    init_db()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        # Last trade
        last = conn.execute(
            "SELECT * FROM trades WHERE strategy_name=? ORDER BY timestamp DESC LIMIT 1",
            (strategy_name,)
        ).fetchone()
        # Open positions
        opens = conn.execute(
            "SELECT COUNT(*) as cnt FROM trades WHERE strategy_name=? AND outcome='OPEN'",
            (strategy_name,)
        ).fetchone()
        # Today's PnL
        today = datetime.now(timezone.utc).date().isoformat()
        today_pnl = conn.execute(
            "SELECT COALESCE(SUM(pnl), 0) as pnl FROM trades WHERE strategy_name=? AND date(timestamp)=?",
            (strategy_name, today)
        ).fetchone()

    return {
        "strategy": strategy_name,
        "last_trade": dict(last) if last else None,
        "open_positions": opens["cnt"] if opens else 0,
        "today_pnl": today_pnl["pnl"] if today_pnl else 0.0,
    }

def get_all_status() -> list:
    """Get status for all known strategies."""
    from .performance_analyzer import get_all_strategies
    return [get_strategy_status(s) for s in get_all_strategies()]
