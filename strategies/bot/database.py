"""
database.py — SQLite setup and all read/write operations.
Every signal, every decision, every simulated trade is logged here.
"""

import sqlite3
import logging
import threading
from datetime import datetime, timezone
from typing import Optional
import config

log = logging.getLogger(__name__)

_write_lock = threading.Lock()


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(config.DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # allows concurrent reads
    return conn


def init_db():
    """Create all tables if they don't exist."""
    conn = get_conn()
    cur = conn.cursor()

    # Price snapshots (kept for rolling window calcs)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS price_snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          REAL    NOT NULL,           -- unix timestamp (UTC)
            asset       TEXT    NOT NULL,           -- e.g. BTCUSDT
            price       REAL    NOT NULL,
            volume_1m   REAL    NOT NULL DEFAULT 0  -- 1-min volume from Binance
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_snap_asset_ts ON price_snapshots(asset, ts)")

    # Raw signals detected by the signal engine
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ts              REAL    NOT NULL,
            asset           TEXT    NOT NULL,
            direction       TEXT    NOT NULL,       -- UP or DOWN
            momentum_pct    REAL    NOT NULL,       -- e.g. 0.55
            volume_ratio    REAL    NOT NULL,       -- actual / rolling avg
            implied_prob    REAL    NOT NULL,       -- our model's true prob estimate
            gate_result     TEXT    NOT NULL,       -- PASS or gate name that blocked
            signal_strength TEXT    NOT NULL        -- WEAK / MEDIUM / STRONG / VERY_STRONG
        )
    """)

    # Polymarket market snapshots (markets we scanned)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS poly_markets (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ts              REAL    NOT NULL,
            condition_id    TEXT    NOT NULL,
            question        TEXT    NOT NULL,
            asset           TEXT    NOT NULL,       -- BTC / ETH / SOL
            direction       TEXT    NOT NULL,       -- UP or DOWN
            yes_price       REAL    NOT NULL,
            no_price        REAL    NOT NULL,
            liquidity       REAL    NOT NULL,
            end_date_iso    TEXT    NOT NULL,
            hours_remaining REAL    NOT NULL
        )
    """)

    # Simulated trades
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sim_trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Entry
            entry_ts        REAL    NOT NULL,
            asset           TEXT    NOT NULL,
            direction       TEXT    NOT NULL,       -- UP / DOWN
            contract_side   TEXT    NOT NULL,       -- YES / NO
            condition_id    TEXT    NOT NULL,
            question        TEXT    NOT NULL,
            entry_price     REAL    NOT NULL,       -- price we paid per share
            position_size   REAL    NOT NULL,       -- USD deployed
            num_shares      REAL    NOT NULL,       -- position_size / entry_price
            implied_prob    REAL    NOT NULL,       -- our model estimate
            market_prob     REAL    NOT NULL,       -- polymarket's price (= their prob)
            edge_pct        REAL    NOT NULL,       -- implied_prob - market_prob
            kelly_fraction  REAL    NOT NULL,

            -- Spot context at entry
            spot_price_entry    REAL    NOT NULL,
            momentum_pct        REAL    NOT NULL,
            volume_ratio        REAL    NOT NULL,

            -- Exit
            exit_ts         REAL,
            exit_price      REAL,                  -- price at exit (0 or 1 at resolution)
            exit_reason     TEXT,                  -- RESOLUTION / STOP_LOSS / REVERSAL / LOCK_PROFIT
            gross_pnl       REAL,                  -- before fee
            fee             REAL,                  -- polymarket 2% fee on wins
            net_pnl         REAL,                  -- after fee
            outcome         TEXT,                  -- WIN / LOSS / PENDING

            -- Running account state at entry
            bankroll_at_entry   REAL    NOT NULL,
            bankroll_after_exit REAL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_ts ON sim_trades(entry_ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_outcome ON sim_trades(outcome)")

    # Daily summary (computed at end of each UTC day)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_summary (
            date_utc        TEXT    PRIMARY KEY,
            trades          INTEGER NOT NULL DEFAULT 0,
            wins            INTEGER NOT NULL DEFAULT 0,
            losses          INTEGER NOT NULL DEFAULT 0,
            gross_pnl       REAL    NOT NULL DEFAULT 0,
            net_pnl         REAL    NOT NULL DEFAULT 0,
            win_rate        REAL    NOT NULL DEFAULT 0,
            avg_edge        REAL    NOT NULL DEFAULT 0,
            bankroll_open   REAL    NOT NULL DEFAULT 0,
            bankroll_close  REAL    NOT NULL DEFAULT 0,
            halt_triggered  INTEGER NOT NULL DEFAULT 0
        )
    """)

    # Bot state (single row, upserted)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_state (
            key     TEXT PRIMARY KEY,
            value   TEXT NOT NULL
        )
    """)

    # Seed initial state
    for k, v in [
        ("bankroll",          str(config.STARTING_BANKROLL)),
        ("daily_loss",        "0.0"),
        ("daily_trade_count", "0"),
        ("halted",            "0"),
        ("halt_reason",       ""),
        ("started_at",        datetime.now(timezone.utc).isoformat()),
        ("total_trades",      "0"),
        ("total_wins",        "0"),
    ]:
        cur.execute(
            "INSERT OR IGNORE INTO bot_state(key, value) VALUES (?, ?)", (k, v)
        )

    conn.commit()
    conn.close()
    log.info("Database initialised at %s", config.DB_PATH)


# ── State helpers ──────────────────────────────────────────────────────────

def get_state(key: str) -> str:
    with get_conn() as c:
        row = c.execute("SELECT value FROM bot_state WHERE key=?", (key,)).fetchone()
        return row["value"] if row else ""

def set_state(key: str, value):
    with _write_lock, get_conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO bot_state(key, value) VALUES (?, ?)",
            (key, str(value))
        )

def get_bankroll() -> float:
    return float(get_state("bankroll"))

def set_bankroll(amount: float):
    set_state("bankroll", round(amount, 6))

def is_halted() -> bool:
    return get_state("halted") == "1"

def set_halt(reason: str):
    set_state("halted", "1")
    set_state("halt_reason", reason)
    log.warning("BOT HALTED: %s", reason)

def clear_halt():
    set_state("halted", "0")
    set_state("halt_reason", "")
    set_state("daily_loss", "0.0")
    set_state("daily_trade_count", "0")
    log.info("Halt cleared — new trading day")

def get_daily_loss() -> float:
    return float(get_state("daily_loss") or 0)

def add_daily_loss(amount: float):
    """Add to today's running loss (amount should be positive for a loss)."""
    current = get_daily_loss()
    set_state("daily_loss", round(current + amount, 6))


# ── Write helpers ──────────────────────────────────────────────────────────

def log_price(asset: str, price: float, volume_1m: float):
    ts = datetime.now(timezone.utc).timestamp()
    with _write_lock, get_conn() as c:
        c.execute(
            "INSERT INTO price_snapshots(ts, asset, price, volume_1m) VALUES (?,?,?,?)",
            (ts, asset, price, volume_1m)
        )

def log_signal(asset, direction, momentum_pct, volume_ratio,
               implied_prob, gate_result, signal_strength):
    ts = datetime.now(timezone.utc).timestamp()
    with _write_lock, get_conn() as c:
        c.execute("""
            INSERT INTO signals
            (ts, asset, direction, momentum_pct, volume_ratio,
             implied_prob, gate_result, signal_strength)
            VALUES (?,?,?,?,?,?,?,?)
        """, (ts, asset, direction, momentum_pct, volume_ratio,
              implied_prob, gate_result, signal_strength))

def open_sim_trade(asset, direction, contract_side, condition_id, question,
                   entry_price, position_size, implied_prob, market_prob,
                   edge_pct, kelly_fraction, spot_price, momentum_pct,
                   volume_ratio, bankroll) -> int:
    ts = datetime.now(timezone.utc).timestamp()
    num_shares = position_size / entry_price if entry_price > 0 else 0
    with _write_lock, get_conn() as c:
        cur = c.execute("""
            INSERT INTO sim_trades
            (entry_ts, asset, direction, contract_side, condition_id, question,
             entry_price, position_size, num_shares, implied_prob, market_prob,
             edge_pct, kelly_fraction, spot_price_entry, momentum_pct,
             volume_ratio, outcome, bankroll_at_entry)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (ts, asset, direction, contract_side, condition_id, question,
              entry_price, position_size, num_shares, implied_prob, market_prob,
              edge_pct, kelly_fraction, spot_price, momentum_pct,
              volume_ratio, "PENDING", bankroll))
        trade_id = cur.lastrowid
    # bump state counters
    total = int(get_state("total_trades") or 0) + 1
    set_state("total_trades", total)
    daily = int(get_state("daily_trade_count") or 0) + 1
    set_state("daily_trade_count", daily)
    return trade_id

def close_sim_trade(trade_id: int, exit_price: float, exit_reason: str,
                    new_bankroll: float):
    """Compute P&L and update the trade row."""
    with _write_lock, get_conn() as c:
        row = c.execute(
            "SELECT * FROM sim_trades WHERE id=?", (trade_id,)
        ).fetchone()
        if not row:
            log.error("close_sim_trade: trade_id %d not found", trade_id)
            return

        num_shares    = row["num_shares"]
        position_size = row["position_size"]
        entry_price   = row["entry_price"]

        gross_pnl = (exit_price - entry_price) * num_shares
        # Polymarket charges 2% fee only on winning trades (exit_price == 1.0)
        fee = position_size * config.POLYMARKET_FEE if exit_price >= 0.99 else 0.0
        net_pnl = gross_pnl - fee
        outcome = "WIN" if net_pnl > 0 else "LOSS"

        exit_ts = datetime.now(timezone.utc).timestamp()
        c.execute("""
            UPDATE sim_trades
            SET exit_ts=?, exit_price=?, exit_reason=?,
                gross_pnl=?, fee=?, net_pnl=?, outcome=?,
                bankroll_after_exit=?
            WHERE id=?
        """, (exit_ts, exit_price, exit_reason,
              round(gross_pnl, 6), round(fee, 6), round(net_pnl, 6),
              outcome, round(new_bankroll, 6), trade_id))

        if outcome == "WIN":
            wins = int(get_state("total_wins") or 0) + 1
            set_state("total_wins", wins)
        if net_pnl < 0:
            add_daily_loss(abs(net_pnl))

    log.info(
        "Trade #%d closed | %s | exit=%.4f | net_pnl=$%.4f | %s",
        trade_id, exit_reason, exit_price, net_pnl, outcome
    )


# ── Read / stats helpers ───────────────────────────────────────────────────

def get_recent_prices(asset: str, seconds: int) -> list:
    cutoff = datetime.now(timezone.utc).timestamp() - seconds
    with get_conn() as c:
        rows = c.execute(
            "SELECT ts, price, volume_1m FROM price_snapshots "
            "WHERE asset=? AND ts>=? ORDER BY ts ASC",
            (asset, cutoff)
        ).fetchall()
    return [dict(r) for r in rows]

def get_open_trades() -> list:
    with get_conn() as c:
        rows = c.execute(
            "SELECT * FROM sim_trades WHERE outcome='PENDING'"
        ).fetchall()
    return [dict(r) for r in rows]

def get_stats() -> dict:
    total  = int(get_state("total_trades") or 0)
    wins   = int(get_state("total_wins") or 0)
    losses = total - wins

    with get_conn() as c:
        row = c.execute("""
            SELECT
                COUNT(*)                        AS closed,
                SUM(CASE WHEN outcome='WIN'  THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN outcome='LOSS' THEN 1 ELSE 0 END) AS losses,
                AVG(net_pnl)                    AS avg_net_pnl,
                SUM(net_pnl)                    AS total_net_pnl,
                AVG(edge_pct)                   AS avg_edge,
                AVG(momentum_pct)               AS avg_momentum
            FROM sim_trades
            WHERE outcome != 'PENDING'
        """).fetchone()

    closed    = row["closed"] or 0
    win_rate  = (row["wins"] / closed * 100) if closed > 0 else 0.0
    bankroll  = get_bankroll()
    pnl_total = row["total_net_pnl"] or 0.0
    roi       = (pnl_total / config.STARTING_BANKROLL * 100) if config.STARTING_BANKROLL else 0

    return {
        "bankroll":      round(bankroll, 2),
        "total_trades":  total,
        "closed_trades": closed,
        "wins":          row["wins"] or 0,
        "losses":        row["losses"] or 0,
        "pending":       total - closed,
        "win_rate":      round(win_rate, 1),
        "avg_net_pnl":   round(row["avg_net_pnl"] or 0, 4),
        "total_net_pnl": round(pnl_total, 4),
        "roi_pct":       round(roi, 2),
        "avg_edge":      round((row["avg_edge"] or 0) * 100, 2),
        "daily_loss":    round(get_daily_loss(), 4),
        "halted":        is_halted(),
        "halt_reason":   get_state("halt_reason"),
    }
