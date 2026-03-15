import sqlite3
import logging
import statistics
from datetime import datetime, timezone, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent / "learning.db"
logger = logging.getLogger("framework.performance")

def analyze_strategy(strategy_name: str, lookback_days: int = 30) -> dict:
    """Compute win_rate, avg_edge, total_pnl, Sharpe estimate for a strategy."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT outcome, pnl, edge_pct FROM trades
            WHERE strategy_name=? AND timestamp > ? AND outcome != 'OPEN'
        """, (strategy_name, cutoff)).fetchall()

    if not rows:
        return {"strategy": strategy_name, "trades": 0, "win_rate": 0.0, "total_pnl": 0.0, "avg_edge": 0.0, "sharpe": 0.0, "wins": 0, "losses": 0}

    outcomes = [r[0] for r in rows]
    pnls = [r[1] for r in rows]
    edges = [r[2] for r in rows]
    wins = outcomes.count("WIN")
    n = len(outcomes)
    win_rate = wins / n if n > 0 else 0.0
    total_pnl = sum(pnls)
    avg_edge = sum(edges) / len(edges) if edges else 0.0

    mean_pnl = statistics.mean(pnls)
    try:
        std_pnl = statistics.stdev(pnls) if len(pnls) > 1 else 1.0
        sharpe = mean_pnl / std_pnl if std_pnl > 0 else 0.0
    except Exception:
        sharpe = 0.0

    result = {
        "strategy": strategy_name,
        "trades": n,
        "wins": wins,
        "losses": n - wins,
        "win_rate": round(win_rate, 4),
        "avg_edge": round(avg_edge, 4),
        "total_pnl": round(total_pnl, 2),
        "sharpe": round(sharpe, 3)
    }

    today = datetime.now(timezone.utc).date().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT OR REPLACE INTO strategy_performance
            (strategy_name, date, trades, wins, losses, pnl, win_rate, avg_edge)
            VALUES (?,?,?,?,?,?,?,?)
        """, (strategy_name, today, n, wins, n - wins, total_pnl, win_rate, avg_edge))

    logger.info(f"[PERF] {strategy_name}: {n} trades, {win_rate:.1%} WR, ${total_pnl:.2f} PnL, Sharpe {sharpe:.2f}")
    return result

def get_all_strategies() -> list:
    """Return list of all known strategy names from trade log."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT DISTINCT strategy_name FROM trades").fetchall()
        return [r[0] for r in rows]
