"""
Calibration and paper trading tracker for EdgeLab.
Uses SQLite via aiosqlite for async persistence.
"""
from __future__ import annotations

import os
import aiosqlite
from datetime import datetime, timezone
from typing import Optional

DB_PATH = os.getenv(
    "EDGELAB_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "edgelab.db"),
)
os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)


async def init_db() -> None:
    """Create all tables if they do not exist."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS trades_paper (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id   TEXT    NOT NULL,
                entry_price REAL    NOT NULL,
                entry_prob  REAL    NOT NULL,
                strategy    TEXT    NOT NULL,
                size        REAL    NOT NULL,
                timestamp   TEXT    NOT NULL,
                status      TEXT    NOT NULL DEFAULT 'open',
                exit_price  REAL,
                pnl         REAL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS trades_live (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id   TEXT    NOT NULL,
                entry_price REAL    NOT NULL,
                entry_prob  REAL    NOT NULL,
                strategy    TEXT    NOT NULL,
                size        REAL    NOT NULL,
                timestamp   TEXT    NOT NULL,
                status      TEXT    NOT NULL DEFAULT 'open',
                exit_price  REAL,
                pnl         REAL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS market_outcomes (
                market_id       TEXT PRIMARY KEY,
                resolved_yes    INTEGER NOT NULL,
                resolution_time TEXT    NOT NULL
            )
        """)
        await db.commit()


def _trades_table(paper: bool) -> str:
    return "trades_paper" if paper else "trades_live"


async def record_opportunity(
    market_id: str,
    entry_price: float,
    strategy: str,
    size: float,
    paper: bool = True,
) -> int:
    """
    Log an identified opportunity as a new open trade.
    entry_price is treated as the implied probability (0–1).
    Returns the new trade id.
    """
    table = _trades_table(paper)
    ts = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            f"""
            INSERT INTO {table} (market_id, entry_price, entry_prob, strategy, size, timestamp, status)
            VALUES (?, ?, ?, ?, ?, ?, 'open')
            """,
            (market_id, entry_price, entry_price, strategy, size, ts),
        )
        await db.commit()
        return cursor.lastrowid


async def record_outcome(market_id: str, resolved_yes: bool) -> None:
    """Log the resolution of a market."""
    ts = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO market_outcomes (market_id, resolved_yes, resolution_time)
            VALUES (?, ?, ?)
            """,
            (market_id, int(resolved_yes), ts),
        )
        await db.commit()


async def close_trade(
    trade_id: int,
    exit_price: float,
    paper: bool = True,
) -> Optional[float]:
    """
    Close an open trade and compute P&L.
    P&L = (exit_price - entry_price) * size
    Returns computed pnl, or None if trade not found.
    """
    table = _trades_table(paper)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            f"SELECT entry_price, size FROM {table} WHERE id = ? AND status = 'open'",
            (trade_id,),
        ) as cursor:
            row = await cursor.fetchone()

        if row is None:
            return None

        pnl = (exit_price - row["entry_price"]) * row["size"]
        await db.execute(
            f"""
            UPDATE {table}
            SET status = 'closed', exit_price = ?, pnl = ?
            WHERE id = ?
            """,
            (exit_price, pnl, trade_id),
        )
        await db.commit()
        return pnl


async def get_open_trades(paper: bool = True) -> list:
    """Return all open positions as a list of dicts."""
    table = _trades_table(paper)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            f"SELECT * FROM {table} WHERE status = 'open' ORDER BY timestamp"
        ) as cursor:
            rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def get_closed_trades(paper: bool = True) -> list:
    """Return all closed positions with P&L as a list of dicts."""
    table = _trades_table(paper)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            f"SELECT * FROM {table} WHERE status = 'closed' ORDER BY timestamp"
        ) as cursor:
            rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def get_calibration_stats() -> dict:
    """
    Compare actual resolution rate vs priced probability by 5% bucket.
    Buckets cover entry_prob 0.50–1.00 in 5% increments.
    Returns a dict keyed by bucket label, e.g. "50-55".
    """
    buckets: dict[str, dict] = {}
    bucket_edges = [(i, i + 5) for i in range(50, 100, 5)]

    for lo, hi in bucket_edges:
        label = f"{lo}-{hi}"
        buckets[label] = {
            "lo": lo / 100,
            "hi": hi / 100,
            "midpoint": (lo + hi) / 200,
            "trade_count": 0,
            "resolved_yes_count": 0,
            "true_rate": None,
        }

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        # Join closed paper trades with market outcomes
        async with db.execute(
            """
            SELECT t.entry_prob, o.resolved_yes
            FROM trades_paper t
            JOIN market_outcomes o ON t.market_id = o.market_id
            WHERE t.status = 'closed'
            """
        ) as cursor:
            rows = await cursor.fetchall()

    for row in rows:
        prob = row["entry_prob"]
        resolved = bool(row["resolved_yes"])
        for lo, hi in bucket_edges:
            if (lo / 100) <= prob < (hi / 100):
                label = f"{lo}-{hi}"
                buckets[label]["trade_count"] += 1
                if resolved:
                    buckets[label]["resolved_yes_count"] += 1
                break

    for label, data in buckets.items():
        if data["trade_count"] > 0:
            data["true_rate"] = data["resolved_yes_count"] / data["trade_count"]
            data["edge"] = data["true_rate"] - data["midpoint"]
        else:
            data["true_rate"] = None
            data["edge"] = None

    return buckets


async def get_strategy_pnl(strategy_name: str) -> dict:
    """
    Return P&L attribution for a given strategy across both paper tables.
    Returns: win_rate, total_pnl, avg_return, trade_count
    """
    results = []
    for table in ("trades_paper", "trades_live"):
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                f"""
                SELECT pnl, size FROM {table}
                WHERE strategy = ? AND status = 'closed'
                """,
                (strategy_name,),
            ) as cursor:
                rows = await cursor.fetchall()
        results.extend(rows)

    if not results:
        return {
            "strategy": strategy_name,
            "trade_count": 0,
            "total_pnl": 0.0,
            "win_rate": 0.0,
            "avg_return": 0.0,
        }

    pnls = [r["pnl"] for r in results]
    sizes = [r["size"] for r in results]
    wins = sum(1 for p in pnls if p > 0)
    total_pnl = sum(pnls)
    avg_return = (
        sum(p / s for p, s in zip(pnls, sizes) if s != 0) / len(pnls)
        if pnls
        else 0.0
    )

    return {
        "strategy": strategy_name,
        "trade_count": len(pnls),
        "total_pnl": round(total_pnl, 4),
        "win_rate": round(wins / len(pnls), 4),
        "avg_return": round(avg_return, 4),
    }


async def get_pnl_curve(paper: bool = True) -> list:
    """
    Return running cumulative P&L over time for charting.
    Each element: {"timestamp": str, "pnl": float, "cumulative_pnl": float}
    """
    table = _trades_table(paper)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            f"""
            SELECT timestamp, pnl FROM {table}
            WHERE status = 'closed' AND pnl IS NOT NULL
            ORDER BY timestamp
            """
        ) as cursor:
            rows = await cursor.fetchall()

    curve = []
    cumulative = 0.0
    for row in rows:
        cumulative += row["pnl"]
        curve.append(
            {
                "timestamp": row["timestamp"],
                "pnl": round(row["pnl"], 4),
                "cumulative_pnl": round(cumulative, 4),
            }
        )
    return curve
