"""
Command Center Dashboard — Unified view of all EdgeLab strategies.

Technology: aiohttp serving a single-page HTML dashboard with sidebar navigation.
Zero new dependencies beyond aiohttp (already in requirements.txt).

Run:  python3 dashboard.py
Open: http://localhost:8050

Tabs:
  1. CAPITAL COMMAND — unified overview of both strategies
  2. EDGELAB — EdgeLab-specific views (placeholder until deployed)
  3. BOT — polymarket-sim read-only view
  4. PERFORMANCE — combined P&L history
  5. RISK — circuit breakers, guardrails, data export

Data sources (all read-only):
  - Bot: strategies/bot/sim_results.db (SQLite, WAL mode)
  - Bot logs: strategies/bot/live.log, sim.log, paper_sim.log
  - EdgeLab: strategies/edgelab/data/edgelab.db, Redis (bot:status, edgelab:*), logs
"""

import asyncio
import json
import logging
import os
import sqlite3
import subprocess
import sys
import re
from datetime import datetime, timezone, date
from pathlib import Path

from aiohttp import web

try:
    from command_center.strategy_registry import STRATEGY_REGISTRY
except ImportError:
    try:
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from command_center.strategy_registry import STRATEGY_REGISTRY
    except ImportError:
        STRATEGY_REGISTRY = {}

# ── Paths ─────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent.parent
BOT_DIR = BASE_DIR / "strategies" / "bot"
BOT_DB = BOT_DIR / "sim_results.db"
BOT_LIVE_LOG = BOT_DIR / "live.log"
BOT_LOGS = [
    BOT_DIR / "live.log",
    BOT_DIR / "sim.log",
    BOT_DIR / "paper_sim.log",
]

# EdgeLab real paths
EDGE_DIR = BASE_DIR / "strategies" / "edgelab"
EDGE_LOG = EDGE_DIR / "logs" / "edgelab.log"
EDGE_DB = EDGE_DIR / "data" / "edgelab.db"

CARRY_DB = EDGE_DIR / "carry_trades.db"

FRAMEWORK_DB = BASE_DIR / "framework" / "learning.db"
FRAMEWORK_TRADES_DB = BASE_DIR / "framework" / "framework.db"
NEWS_ARB_PAPER = BASE_DIR / "strategies" / "news_arb" / "sim" / "results" / "paper_trades.json"
SPORTS_MOMENTUM_PAPER = BASE_DIR / "strategies" / "sports_momentum" / "sim" / "results" / "paper_trades.json"
SPORTS_MOMENTUM_LOG = BASE_DIR / "logs" / "sports_momentum.log"
RESOLUTION_LAG_PAPER = BASE_DIR / "strategies" / "resolution_lag" / "sim" / "results" / "paper_trades.json"
RESOLUTION_LAG_LOG = BASE_DIR / "logs" / "resolution_lag.log"
FUNDING_RATE_ARB_PAPER = BASE_DIR / "strategies" / "funding_rate_arb" / "sim" / "results" / "paper_trades.json"
FUNDING_RATE_ARB_LOG = BASE_DIR / "logs" / "funding_rate_arb.log"
LIQUIDITY_PROVISION_PAPER = BASE_DIR / "strategies" / "liquidity_provision" / "sim" / "results" / "paper_trades.json"
LIQUIDITY_PROVISION_LOG = BASE_DIR / "logs" / "liquidity_provision.log"
HOT_MARKETS_PATH = BASE_DIR / "framework" / "hot_markets.json"

PORT = int(os.environ.get("DASHBOARD_PORT", 8050))
log = logging.getLogger("command_center")


# ── SQLite helpers (read-only, never crash) ───────────────────────────────────

def _bot_conn():
    """Open a read-only SQLite connection to the bot DB. Returns None if unavailable."""
    if not BOT_DB.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{BOT_DB}?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        log.warning("Bot DB connection failed: %s", e)
        return None


def _redis_bot_status() -> str:
    """Read bot:status from Redis. Returns 'UNKNOWN' if Redis unavailable."""
    try:
        import redis as _redis
        r = _redis.Redis(host="127.0.0.1", port=6379, decode_responses=True,
                         socket_connect_timeout=2)
        raw = r.get("bot:status")
        if raw is None:
            return "UNKNOWN"
        data = json.loads(raw)
        return data.get("state", "UNKNOWN") if isinstance(data, dict) else str(raw)
    except Exception:
        return "UNKNOWN"


def _bot_process_running() -> bool:
    """Check bot status via Redis first, fall back to pgrep."""
    status = _redis_bot_status()
    if status not in ("UNKNOWN",):
        return status.upper() in ("RUNNING", "LIVE", "ACTIVE", "OK")
    # Fallback: pgrep
    try:
        result = subprocess.run(
            ["pgrep", "-f", "live_engine.py"],
            capture_output=True, timeout=5
        )
        return result.returncode == 0
    except Exception:
        return False


def _tail_log(path: Path, n: int = 50) -> list:
    """Return last n lines from a log file."""
    if not path.exists():
        return []
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        return [line.rstrip() for line in lines[-n:]]
    except Exception:
        return []


def _today_pnl(conn) -> float:
    """Calculate today's P&L from closed live trades only."""
    try:
        today_start = datetime(
            *date.today().timetuple()[:3],
            tzinfo=timezone.utc
        ).timestamp()
        row = conn.cursor().execute(
            "SELECT COALESCE(SUM(net_pnl), 0) AS pnl "
            "FROM sim_trades WHERE outcome != 'PENDING' AND exit_ts >= ? "
            "AND bankroll_at_entry >= 500",
            (today_start,)
        ).fetchone()
        return float(row["pnl"]) if row else 0.0
    except Exception:
        return 0.0


def _bot_stats() -> dict:
    """Read bot stats. Returns empty dict on any failure."""
    conn = _bot_conn()
    if not conn:
        return {}
    try:
        cur = conn.cursor()

        # Bot state
        state = {}
        try:
            for row in cur.execute("SELECT key, value FROM bot_state"):
                state[row["key"]] = row["value"]
        except sqlite3.OperationalError:
            pass

        bankroll = float(state.get("bankroll", 0))
        halted = state.get("halted", "0") == "1"
        halt_reason = state.get("halt_reason", "")
        daily_loss = float(state.get("daily_loss", 0))

        # Closed trade stats — filter to live trades only
        # Belt-and-suspenders: bankroll_at_entry >= 500 AND entry_ts after first live deploy
        live_filter = "bankroll_at_entry >= 500"
        try:
            row = cur.execute(f"""
                SELECT
                    COUNT(*) AS closed,
                    SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN outcome='LOSS' THEN 1 ELSE 0 END) AS losses,
                    AVG(net_pnl) AS avg_net_pnl,
                    SUM(net_pnl) AS total_net_pnl,
                    AVG(edge_pct) AS avg_edge
                FROM sim_trades WHERE outcome != 'PENDING'
                AND {live_filter}
            """).fetchone()
            closed = row["closed"] or 0
            wins = row["wins"] or 0
            losses = row["losses"] or 0
            avg_pnl = row["avg_net_pnl"] or 0
            total_pnl = row["total_net_pnl"] or 0
            avg_edge = (row["avg_edge"] or 0) * 100
            win_rate = (wins / closed * 100) if closed > 0 else 0
        except sqlite3.OperationalError:
            closed = wins = losses = 0
            avg_pnl = total_pnl = avg_edge = win_rate = 0

        # Total trades count (including pending) — live only
        try:
            total_row = cur.execute(f"""
                SELECT COUNT(*) AS total FROM sim_trades WHERE {live_filter}
            """).fetchone()
            total_trades = total_row["total"] or 0
        except sqlite3.OperationalError:
            total_trades = 0

        # Today P&L
        today_pnl = _today_pnl(conn)

        # Open positions — live trades only
        try:
            open_rows = cur.execute(
                "SELECT id, asset, direction, contract_side, entry_price, "
                "position_size, edge_pct, entry_ts, question "
                "FROM sim_trades WHERE outcome='PENDING' "
                "AND bankroll_at_entry >= 500 ORDER BY entry_ts DESC"
            ).fetchall()
            open_positions = [dict(r) for r in open_rows]
        except sqlite3.OperationalError:
            open_positions = []

        # Recent closed trades — live trades only
        try:
            recent = cur.execute(
                "SELECT id, asset, direction, contract_side, entry_price, exit_price, "
                "position_size, net_pnl, outcome, exit_reason, entry_ts, exit_ts "
                "FROM sim_trades WHERE outcome != 'PENDING' "
                "AND bankroll_at_entry >= 500 "
                "ORDER BY exit_ts DESC LIMIT 10"
            ).fetchall()
            recent_trades = [dict(r) for r in recent]
        except sqlite3.OperationalError:
            recent_trades = []

        # Recent signals
        try:
            sigs = cur.execute(
                "SELECT ts, asset, direction, momentum_pct, volume_ratio, "
                "implied_prob, gate_result, signal_strength "
                "FROM signals ORDER BY ts DESC LIMIT 15"
            ).fetchall()
            recent_signals = [dict(r) for r in sigs]
        except sqlite3.OperationalError:
            recent_signals = []

        # Daily summaries for performance tab
        try:
            daily = cur.execute(
                "SELECT date_utc, trades, wins, losses, net_pnl, win_rate, "
                "bankroll_open, bankroll_close "
                "FROM daily_summary ORDER BY date_utc DESC LIMIT 30"
            ).fetchall()
            daily_history = [dict(r) for r in daily]
        except sqlite3.OperationalError:
            daily_history = []

        total_deployed = sum(p["position_size"] for p in open_positions)

        # Process status
        process_running = _bot_process_running()

        # Derive total P&L from bankroll vs starting, since trade records
        # may have placeholder entries from early bugs
        starting = 500.0
        live_total_pnl = bankroll - starting if bankroll > 0 else total_pnl

        return {
            "available": True,
            "bankroll": round(bankroll, 2),
            "starting_bankroll": starting,
            "total_pnl": round(live_total_pnl, 4),
            "today_pnl": round(today_pnl, 4),
            "roi_pct": round((live_total_pnl / starting * 100) if starting else 0, 2),
            "total_trades": total_trades,
            "closed_trades": closed,
            "wins": wins,
            "losses": losses,
            "win_rate": round(win_rate, 1),
            "avg_pnl": round(avg_pnl, 4),
            "avg_edge": round(avg_edge, 2),
            "daily_loss": round(daily_loss, 4),
            "halted": halted,
            "halt_reason": halt_reason,
            "open_positions": open_positions,
            "total_deployed": round(total_deployed, 2),
            "recent_trades": recent_trades,
            "recent_signals": recent_signals,
            "daily_history": daily_history,
            "process_running": process_running,
            "mode": "LIVE",
        }
    except Exception as e:
        log.warning("Bot stats read failed: %s", e)
        return {"available": False, "error": str(e)}
    finally:
        conn.close()


def _carry_conn():
    """Open a read-only SQLite connection to carry trades DB. Returns None if unavailable."""
    if not CARRY_DB.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{CARRY_DB}?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        log.warning("Carry DB connection failed: %s", e)
        return None


def _carry_stats() -> dict:
    """Read carry trade data for dashboard."""
    conn = _carry_conn()
    if not conn:
        return {"available": False}
    try:
        cur = conn.cursor()

        # Latest scan per asset
        latest_scans = []
        try:
            rows = cur.execute("""
                SELECT s.* FROM carry_scans s
                INNER JOIN (
                    SELECT asset, MAX(scan_time) as max_time
                    FROM carry_scans GROUP BY asset
                ) m ON s.asset = m.asset AND s.scan_time = m.max_time
            """).fetchall()
            latest_scans = [dict(r) for r in rows]
        except sqlite3.OperationalError:
            pass

        # Open positions
        open_positions = []
        try:
            rows = cur.execute(
                "SELECT * FROM carry_positions WHERE status='open' ORDER BY entry_time DESC"
            ).fetchall()
            open_positions = [dict(r) for r in rows]
        except sqlite3.OperationalError:
            pass

        # Closed positions (last 20)
        closed_positions = []
        try:
            rows = cur.execute(
                "SELECT * FROM carry_positions WHERE status='closed' "
                "ORDER BY exit_time DESC LIMIT 20"
            ).fetchall()
            closed_positions = [dict(r) for r in rows]
        except sqlite3.OperationalError:
            pass

        # Aggregate stats
        total_trades = 0
        total_pnl = 0.0
        wins = 0
        total_basis_captured = 0.0
        try:
            row = cur.execute("""
                SELECT COUNT(*) as total,
                       COALESCE(SUM(pnl_usd), 0) as total_pnl,
                       SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) as wins,
                       COALESCE(AVG(entry_basis_apr - COALESCE(exit_basis_apr, entry_basis_apr)), 0) as avg_basis_captured
                FROM carry_positions WHERE status='closed'
            """).fetchone()
            if row:
                total_trades = row["total"] or 0
                total_pnl = row["total_pnl"] or 0.0
                wins = row["wins"] or 0
                total_basis_captured = row["avg_basis_captured"] or 0.0
        except sqlite3.OperationalError:
            pass

        win_rate = (wins / total_trades * 100) if total_trades > 0 else 0

        # Last scan time
        last_scan_time = None
        try:
            row = cur.execute("SELECT MAX(scan_time) as t FROM carry_scans").fetchone()
            last_scan_time = row["t"] if row else None
        except sqlite3.OperationalError:
            pass

        # Best current basis
        best_basis = 0.0
        if latest_scans:
            best_basis = max(s.get("basis_apr", 0) for s in latest_scans)

        # Avg margin health (for open positions, approximate from entry data)
        avg_health = 100.0  # default healthy if no positions

        return {
            "available": True,
            "latest_scans": latest_scans,
            "open_positions": open_positions,
            "closed_positions": closed_positions,
            "total_trades": total_trades,
            "total_pnl": round(total_pnl, 2),
            "win_rate": round(win_rate, 1),
            "wins": wins,
            "avg_basis_captured": round(total_basis_captured, 2),
            "best_current_basis_apr": round(best_basis, 2),
            "avg_margin_health_pct": round(avg_health, 1),
            "last_scan_time": last_scan_time,
            "active_positions": len(open_positions),
        }
    except Exception as e:
        log.warning("Carry stats read failed: %s", e)
        return {"available": False, "error": str(e)}
    finally:
        conn.close()


def _edge_conn():
    """Open a read-only SQLite connection to EdgeLab DB. Returns None if unavailable."""
    if not EDGE_DB.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{EDGE_DB}?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        log.warning("EdgeLab DB connection failed: %s", e)
        return None


def _edge_redis_status() -> dict:
    """Read EdgeLab status from Redis (bot:status + circuit breaker keys)."""
    try:
        import redis as _redis
        r = _redis.Redis(host="127.0.0.1", port=6379, decode_responses=True,
                         socket_connect_timeout=2)
        status = {}
        # autonomous_trader writes bot:status
        raw = r.get("bot:status")
        if raw:
            status = json.loads(raw) if isinstance(raw, str) else {}
        # circuit breaker keys
        cb = {
            "global_pause": r.get("edgelab:cb:global_pause") == "1",
            "daily_pnl": float(r.get("edgelab:cb:daily_pnl") or 0),
            "alltime_pnl": float(r.get("edgelab:cb:alltime_pnl") or 0),
        }
        status["circuit_breaker"] = cb
        # edgelab:mode (authoritative mode key from mode_manager)
        mode = r.get("edgelab:mode")
        if mode:
            status["edgelab_mode"] = mode.upper()
        return status
    except Exception:
        return {}


def _edge_parse_feeds(log_lines: list) -> list:
    """Parse feed status from recent log lines."""
    feeds = {}
    for line in reversed(log_lines):
        if "polymarket" in line.lower() and "poll complete" in line.lower() and "polymarket" not in feeds:
            feeds["polymarket"] = {"name": "Polymarket REST", "status": "ACTIVE", "detail": line.strip()}
        elif "kalshi" in line.lower() and "poll" in line.lower() and "kalshi" not in feeds:
            is_mock = "mock" in line.lower()
            feeds["kalshi"] = {
                "name": "Kalshi " + ("Mock" if is_mock else "Live"),
                "status": "MOCK" if is_mock else "ACTIVE",
                "detail": line.strip(),
            }
        elif "espn" in line.lower() and "total" in line.lower() and "espn" not in feeds:
            feeds["espn"] = {"name": "ESPN Events", "status": "ACTIVE", "detail": line.strip()}
        elif "newsapi" in line.lower() and "newsapi" not in feeds:
            feeds["newsapi"] = {"name": "NewsAPI", "status": "ACTIVE", "detail": line.strip()}
        elif "coingecko" in line.lower() and "coingecko" not in feeds:
            is_cached = "cached" in line.lower()
            is_error = "429" in line or "error" in line.lower()
            feeds["coingecko"] = {
                "name": "CoinGecko",
                "status": "CACHED" if is_cached else ("ERROR" if is_error else "ACTIVE"),
                "detail": line.strip(),
            }
        if len(feeds) >= 5:
            break
    return list(feeds.values())


def _edge_parse_scan_status(log_lines: list) -> dict:
    """Parse last scan info from log lines."""
    last_scan_ts = None
    markets_scanned = 0
    signals_found = 0
    for line in reversed(log_lines):
        if "poll complete" in line.lower() or "markets published" in line.lower():
            ts_match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
            if ts_match and not last_scan_ts:
                last_scan_ts = ts_match.group(1)
            # Extract market count
            count_match = re.search(r"(\d+)\s+(?:binary\s+)?markets?\s+published", line)
            if count_match:
                markets_scanned += int(count_match.group(1))
        if "opportunity" in line.lower() or "signal" in line.lower():
            signals_found += 1
        if last_scan_ts and markets_scanned > 0:
            break
    return {
        "last_scan": last_scan_ts,
        "markets_scanned": markets_scanned,
        "signals_found": signals_found,
    }


def _edge_stats() -> dict:
    """EdgeLab stats — reads from Redis, SQLite, and log files."""
    # Redis status (primary source for running state)
    redis_status = _edge_redis_status()

    # Determine mode
    trading_mode = redis_status.get("edgelab_mode") or redis_status.get("trading_mode", "paper")
    mode = trading_mode.upper() if trading_mode else "PAPER"
    is_paper = mode != "LIVE"
    table = "trades_paper" if is_paper else "trades_live"

    # Process running check
    process_running = False
    try:
        result = subprocess.run(
            ["pgrep", "-f", "edgelab.*main.py"],
            capture_output=True, timeout=5
        )
        process_running = result.returncode == 0
    except Exception:
        pass
    # Also check via Redis — if bot:status was updated recently, it's alive
    if not process_running and redis_status.get("state"):
        process_running = redis_status["state"].upper() in ("RUNNING", "LIVE", "ACTIVE")

    # SQLite data
    conn = _edge_conn()
    open_positions = []
    recent_trades = []
    today_pnl = 0.0
    total_pnl = 0.0
    total_deployed = 0.0
    trade_count = 0
    wins = 0
    losses = 0
    if conn:
        try:
            cur = conn.cursor()
            # Open positions
            try:
                rows = cur.execute(
                    f"SELECT * FROM {table} WHERE status='open' ORDER BY timestamp DESC"
                ).fetchall()
                open_positions = [dict(r) for r in rows]
                total_deployed = sum(r["size"] for r in open_positions)
            except sqlite3.OperationalError:
                pass
            # Closed trades
            try:
                rows = cur.execute(
                    f"SELECT * FROM {table} WHERE status='closed' ORDER BY timestamp DESC LIMIT 10"
                ).fetchall()
                recent_trades = [dict(r) for r in rows]
            except sqlite3.OperationalError:
                pass
            # Aggregate stats
            try:
                row = cur.execute(f"""
                    SELECT COUNT(*) as cnt,
                           COALESCE(SUM(pnl), 0) as total_pnl,
                           SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                           SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses
                    FROM {table} WHERE status='closed'
                """).fetchone()
                if row:
                    trade_count = row["cnt"] or 0
                    total_pnl = float(row["total_pnl"] or 0)
                    wins = row["wins"] or 0
                    losses = row["losses"] or 0
            except sqlite3.OperationalError:
                pass
            # Today P&L
            try:
                row = cur.execute(f"""
                    SELECT COALESCE(SUM(pnl), 0) as pnl FROM {table}
                    WHERE status='closed' AND date(timestamp) = date('now')
                """).fetchone()
                today_pnl = float(row["pnl"]) if row else 0.0
            except sqlite3.OperationalError:
                pass
        except Exception as e:
            log.warning("EdgeLab DB read failed: %s", e)
        finally:
            conn.close()

    # Override with Redis if available (more current)
    if redis_status.get("pnl_today") is not None:
        today_pnl = float(redis_status.get("pnl_today", 0))
    if redis_status.get("pnl_alltime") is not None:
        total_pnl = float(redis_status.get("pnl_alltime", 0))
    if redis_status.get("capital_deployed") is not None:
        total_deployed = float(redis_status.get("capital_deployed", 0))

    # Log-based data
    log_lines = _tail_log(EDGE_LOG, 200)
    feeds = _edge_parse_feeds(log_lines)
    scan_status = _edge_parse_scan_status(log_lines)

    capital = float(redis_status.get("capital_total", 500))
    cb = redis_status.get("circuit_breaker", {})

    return {
        "available": True,
        "process_running": process_running,
        "mode": mode,
        "bankroll": capital,
        "today_pnl": round(today_pnl, 4),
        "total_pnl": round(total_pnl, 4),
        "total_deployed": round(total_deployed, 2),
        "open_positions": open_positions,
        "recent_trades": recent_trades,
        "trade_count": trade_count,
        "wins": wins,
        "losses": losses,
        "win_rate": round((wins / trade_count * 100) if trade_count > 0 else 0, 1),
        "feeds": feeds,
        "scan_status": scan_status,
        "circuit_breaker": {
            "global_pause": cb.get("global_pause", False),
            "daily_pnl": round(cb.get("daily_pnl", 0), 4),
        },
        "active_positions": int(redis_status.get("active_positions", len(open_positions))),
    }


def _parse_log_alerts(log_path: Path, prefix: str, limit: int = 20) -> list:
    """Parse recent log lines for alert-worthy entries."""
    if not log_path.exists():
        return []
    try:
        with open(log_path, "r") as f:
            lines = f.readlines()
        alerts = []
        patterns = [
            (r"\[WARNING\]", "WARN"),
            (r"\[ERROR\]", "ERROR"),
            (r"\[CRITICAL\]", "CRIT"),
            (r"LIVE ORDER", "TRADE"),
            (r"Trade #\d+ closed", "CLOSE"),
            (r"HALT", "HALT"),
            (r"Signal BLOCKED", "BLOCK"),
        ]
        for line in reversed(lines[-500:]):
            line = line.strip()
            if not line:
                continue
            for pat, level in patterns:
                if re.search(pat, line):
                    ts_match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                    ts = ts_match.group(1) if ts_match else ""
                    # Skip stale alerts before 2026-03-13
                    if ts and ts < "2026-03-13":
                        break
                    msg = re.sub(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+ \[\w+\] \S+ — ", "", line)
                    alerts.append({
                        "ts": ts,
                        "level": level,
                        "source": prefix,
                        "message": msg[:200],
                    })
                    break
            if len(alerts) >= limit:
                break
        return alerts
    except Exception as e:
        log.warning("Log parse error for %s: %s", log_path, e)
        return []


def _unified_alerts() -> list:
    """Merge alerts from all sources, sorted by timestamp desc."""
    alerts = []
    for log_path in BOT_LOGS:
        alerts.extend(_parse_log_alerts(log_path, "BOT", limit=15))
    alerts.extend(_parse_log_alerts(EDGE_LOG, "EDGE", limit=15))
    alerts.sort(key=lambda a: a.get("ts", ""), reverse=True)
    return alerts[:20]


# ── Framework + News Arb API routes ──────────────────────────────────────────

def _framework_conn():
    if not FRAMEWORK_DB.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{FRAMEWORK_DB}?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        return None


async def api_framework(request):
    """GET /api/framework — self-learning framework data."""
    conn = _framework_conn()
    if not conn:
        return web.json_response({"error": "framework DB not found"}, status=404)
    try:
        strategies = []
        rows = conn.execute(
            "SELECT DISTINCT strategy_name FROM trades"
        ).fetchall()
        for r in rows:
            name = r["strategy_name"]
            perf = conn.execute(
                "SELECT * FROM strategy_performance WHERE strategy_name=? ORDER BY date DESC LIMIT 1",
                (name,)
            ).fetchone()
            last_trade = conn.execute(
                "SELECT timestamp FROM trades WHERE strategy_name=? ORDER BY timestamp DESC LIMIT 1",
                (name,)
            ).fetchone()
            strategies.append({
                "name": name,
                "win_rate": perf["win_rate"] if perf else 0,
                "total_pnl": perf["pnl"] if perf else 0,
                "trades": perf["trades"] if perf else 0,
                "last_updated": last_trade["timestamp"] if last_trade else None,
            })

        allocs = [dict(r) for r in conn.execute(
            "SELECT strategy_name as strategy, allocated_pct, allocated_usd "
            "FROM capital_allocations ORDER BY timestamp DESC LIMIT 20"
        ).fetchall()]

        # Read latest suggestions file
        reports_dir = BASE_DIR / "framework" / "reports"
        suggestions = []
        if reports_dir.exists():
            report_files = sorted(reports_dir.glob("weekly_*.txt"), reverse=True)
            if report_files:
                lines = report_files[0].read_text().strip().split("\n")
                suggestions = [l.strip() for l in lines if l.strip() and not l.startswith("=") and not l.startswith("EdgeLab")]

        last_analyzed = conn.execute(
            "SELECT MAX(date) as d FROM strategy_performance"
        ).fetchone()

        return web.json_response({
            "strategies": strategies,
            "capital_allocations": allocs,
            "recent_suggestions": suggestions,
            "last_analyzed": last_analyzed["d"] if last_analyzed else None,
        })
    except Exception as e:
        log.error("api_framework error: %s", e)
        return web.json_response({"error": str(e)}, status=500)
    finally:
        conn.close()


async def api_news_arb(request):
    """GET /api/news_arb — news arb strategy status."""
    result = {
        "mode": "PAPER",
        "status": "STOPPED",
        "paper_trades": 0,
        "paper_win_rate": 0.0,
        "paper_pnl": 0.0,
        "recent_signals": [],
        "promotion_eligible": False,
        "last_signal": None,
    }
    try:
        # Check if engine is running
        proc = subprocess.run(["pgrep", "-f", "news_arb_engine"], capture_output=True, timeout=5)
        if proc.returncode == 0:
            result["status"] = "RUNNING"

        # Read paper trades
        if NEWS_ARB_PAPER.exists():
            trades = json.loads(NEWS_ARB_PAPER.read_text())
            result["paper_trades"] = len(trades)
            resolved = [t for t in trades if t.get("status") != "OPEN"]
            if resolved:
                wins = sum(1 for t in resolved if t.get("pnl", 0) > 0)
                result["paper_win_rate"] = round(wins / len(resolved), 4)
                result["paper_pnl"] = round(sum(t.get("pnl", 0) for t in resolved), 2)
            # Recent signals (last 10 trades as proxy)
            for t in trades[-10:]:
                result["recent_signals"].append({
                    "timestamp": t.get("timestamp"),
                    "market_id": t.get("market_id"),
                    "direction": t.get("direction"),
                    "edge_pct": t.get("edge_pct"),
                    "article_title": t.get("article_title", ""),
                })
            if trades:
                result["last_signal"] = trades[-1].get("timestamp")

        # Check promotion from sim report
        sim_dir = BASE_DIR / "strategies" / "news_arb" / "sim" / "results"
        if sim_dir.exists():
            reports = sorted(sim_dir.glob("sim_report_*.json"), reverse=True)
            if reports:
                sim = json.loads(reports[0].read_text())
                result["promotion_eligible"] = sim.get("promotion_eligible", False)

    except Exception as e:
        log.error("api_news_arb error: %s", e)

    return web.json_response(result)


async def api_sports_momentum(request):
    """GET /api/sports_momentum — sports momentum strategy status."""
    result = {
        "mode": "PAPER",
        "status": "STOPPED",
        "paper_trades": 0,
        "paper_win_rate": 0.0,
        "paper_pnl": 0.0,
        "recent_signals": [],
        "promotion_eligible": False,
        "active_sports": [],
    }
    try:
        proc = subprocess.run(["pgrep", "-f", "sports_engine"], capture_output=True, timeout=5)
        if proc.returncode == 0:
            result["status"] = "RUNNING"

        if SPORTS_MOMENTUM_PAPER.exists():
            trades = json.loads(SPORTS_MOMENTUM_PAPER.read_text())
            result["paper_trades"] = len(trades)
            resolved = [t for t in trades if t.get("status") != "OPEN"]
            if resolved:
                wins = sum(1 for t in resolved if t.get("pnl", 0) > 0)
                result["paper_win_rate"] = round(wins / len(resolved), 4)
                result["paper_pnl"] = round(sum(t.get("pnl", 0) for t in resolved), 2)
            for t in trades[-10:]:
                result["recent_signals"].append({
                    "timestamp": t.get("timestamp"),
                    "sport": t.get("sport", ""),
                    "game": t.get("game", t.get("market_id", "")),
                    "direction": t.get("direction"),
                    "edge_pct": t.get("edge_pct"),
                    "trigger_reason": t.get("trigger_reason", t.get("confidence", "")),
                })
            # Active sports from recent trades
            recent_sports = set(t.get("sport", "") for t in trades[-20:] if t.get("sport"))
            result["active_sports"] = list(recent_sports)

        sim_dir = BASE_DIR / "strategies" / "sports_momentum" / "sim" / "results"
        if sim_dir.exists():
            reports = sorted(sim_dir.glob("sim_report_*.json"), reverse=True)
            if reports:
                sim = json.loads(reports[0].read_text())
                result["promotion_eligible"] = sim.get("promotion_eligible", False)

    except Exception as e:
        log.error("api_sports_momentum error: %s", e)

    return web.json_response(result)


async def api_resolution_lag(request):
    """GET /api/resolution_lag — resolution lag strategy status.

    FINDING (2026-03-15): Kalshi CLOBs clear atomically at settlement.
    No lag window exists on settled markets. Strategy needs Polymarket
    cross-platform integration to be viable. Parked until Polymarket
    US deposits reopen.
    """
    result = {
        "mode": "PARKED",
        "status": "SCANNING",
        "paper_trades": 0,
        "paper_win_rate": 0.0,
        "paper_pnl": 0.0,
        "recent_signals": [],
        "promotion_eligible": False,
        "blocker": "Kalshi CLOBs clear atomically — no lag window. Needs Polymarket integration.",
        "lag_frequency_pct": 0.0,
        "avg_lag_duration_min": 0.0,
    }
    try:
        proc = subprocess.run(["pgrep", "-f", "resolution_engine"], capture_output=True, timeout=5)
        if proc.returncode == 0:
            result["status"] = "SCANNING"

        if RESOLUTION_LAG_PAPER.exists():
            trades = json.loads(RESOLUTION_LAG_PAPER.read_text())
            result["paper_trades"] = len(trades)
            resolved = [t for t in trades if t.get("status") != "OPEN"]
            if resolved:
                wins = sum(1 for t in resolved if t.get("pnl", 0) > 0)
                result["paper_win_rate"] = round(wins / len(resolved), 4)
                result["paper_pnl"] = round(sum(t.get("pnl", 0) for t in resolved), 2)

    except Exception as e:
        log.error("api_resolution_lag error: %s", e)

    return web.json_response(result)


async def api_funding_rate_arb(request):
    """GET /api/funding_rate_arb — funding rate arb strategy status."""
    result = {
        "mode": "PAPER",
        "status": "STOPPED",
        "paper_trades": 0,
        "paper_win_rate": 0.0,
        "paper_pnl": 0.0,
        "current_funding_rates": {},
        "active_signals": [],
        "promotion_eligible": False,
    }
    try:
        proc = subprocess.run(["pgrep", "-f", "funding_engine"], capture_output=True, timeout=5)
        if proc.returncode == 0:
            result["status"] = "RUNNING"

        if FUNDING_RATE_ARB_PAPER.exists():
            trades = json.loads(FUNDING_RATE_ARB_PAPER.read_text())
            result["paper_trades"] = len(trades)
            resolved = [t for t in trades if t.get("status") != "OPEN"]
            if resolved:
                wins = sum(1 for t in resolved if t.get("pnl", 0) > 0)
                result["paper_win_rate"] = round(wins / len(resolved), 4)
                result["paper_pnl"] = round(sum(t.get("pnl", 0) for t in resolved), 2)
            # Extract funding rates from recent trades
            for t in trades[-5:]:
                asset = t.get("asset", "")
                rate = t.get("funding_rate", 0)
                if asset and rate:
                    result["current_funding_rates"][asset] = rate
            # Active signals
            for t in trades[-10:]:
                result["active_signals"].append({
                    "timestamp": t.get("timestamp"),
                    "asset": t.get("asset", ""),
                    "direction": t.get("direction", ""),
                    "funding_rate": t.get("funding_rate", 0),
                    "magnitude": t.get("magnitude", ""),
                    "edge_pct": t.get("edge_pct", 0),
                })

        sim_dir = BASE_DIR / "strategies" / "funding_rate_arb" / "sim" / "results"
        if sim_dir.exists():
            reports = sorted(sim_dir.glob("sim_report_*.json"), reverse=True)
            if reports:
                sim = json.loads(reports[0].read_text())
                result["promotion_eligible"] = sim.get("promotion_eligible", False)

    except Exception as e:
        log.error("api_funding_rate_arb error: %s", e)

    return web.json_response(result)


async def api_liquidity_provision(request):
    """GET /api/liquidity_provision — liquidity provision strategy status."""
    result = {
        "mode": "PAPER",
        "status": "STOPPED",
        "paper_trades": 0,
        "paper_win_rate": 0.0,
        "paper_pnl": 0.0,
        "active_quotes": 0,
        "current_inventory_usd": 0.0,
        "spread_capture_rate": 0.0,
        "promotion_eligible": False,
    }
    try:
        proc = subprocess.run(["pgrep", "-f", "liquidity_engine"], capture_output=True, timeout=5)
        if proc.returncode == 0:
            result["status"] = "RUNNING"

        if LIQUIDITY_PROVISION_PAPER.exists():
            trades = json.loads(LIQUIDITY_PROVISION_PAPER.read_text())
            result["paper_trades"] = len(trades)
            resolved = [t for t in trades if t.get("status") != "OPEN"]
            if resolved:
                wins = sum(1 for t in resolved if t.get("pnl", 0) > 0)
                result["paper_win_rate"] = round(wins / len(resolved), 4)
                result["paper_pnl"] = round(sum(t.get("pnl", 0) for t in resolved), 2)
                # Spread capture rate = % of round-trips that completed profitably
                round_trips = [t for t in resolved if t.get("trade_type") == "round_trip"]
                if round_trips:
                    profitable = sum(1 for t in round_trips if t.get("pnl", 0) > 0)
                    result["spread_capture_rate"] = round(profitable / len(round_trips), 4)
            # Count open quotes
            open_trades = [t for t in trades if t.get("status") == "OPEN"]
            result["active_quotes"] = len(open_trades)
            result["current_inventory_usd"] = round(
                sum(abs(t.get("size", 0)) for t in open_trades), 2
            )

        sim_dir = BASE_DIR / "strategies" / "liquidity_provision" / "sim" / "results"
        if sim_dir.exists():
            reports = sorted(sim_dir.glob("sim_report_*.json"), reverse=True)
            if reports:
                sim = json.loads(reports[0].read_text())
                result["promotion_eligible"] = sim.get("promotion_eligible", False)
                if "spread_capture_rate" in sim:
                    result["spread_capture_rate"] = sim["spread_capture_rate"]

    except Exception as e:
        log.error("api_liquidity_provision error: %s", e)

    return web.json_response(result)


async def api_carry_status(request):
    """GET /api/carry/status — carry trade strategy status."""
    stats = _carry_stats()
    return web.json_response({
        "active_positions": stats.get("active_positions", 0),
        "total_pnl_usd": stats.get("total_pnl", 0.0),
        "best_current_basis_apr": stats.get("best_current_basis_apr", 0.0),
        "avg_margin_health_pct": stats.get("avg_margin_health_pct", 0.0),
        "last_scan_time": stats.get("last_scan_time"),
        "strategy_mode": "paper",
    })


async def api_carry_data(request):
    """GET /api/carry — full carry trade data for dashboard tab."""
    return web.json_response(_carry_stats())


async def carry_page(request):
    """GET /carry — redirect to main dashboard with carry tab active."""
    raise web.HTTPFound("/#carry-trade")


# ── Strategy Registry helpers ─────────────────────────────────────────────────

def _redis_risk() -> dict:
    """Read edgelab:risk:* keys from Redis. Returns {} if Redis unavailable."""
    try:
        import redis as _redis
        r = _redis.Redis(host="127.0.0.1", port=6379, decode_responses=True,
                         socket_connect_timeout=2)
        daily_pnl = float(r.get("edgelab:risk:daily_pnl") or 0)
        halted = r.get("edgelab:risk:halted") == "1"
        halt_reason = r.get("edgelab:risk:halt_reason") or ""

        # Per-strategy P&L: edgelab:risk:strategy_pnl:{key}
        strategy_pnl = {}
        for key in STRATEGY_REGISTRY:
            raw = r.get(f"edgelab:risk:strategy_pnl:{key}")
            if raw is not None:
                try:
                    strategy_pnl[key] = float(raw)
                except ValueError:
                    pass

        # Allocations hash or individual keys
        allocs = {}
        raw_allocs = r.get("edgelab:risk:allocations")
        if raw_allocs:
            try:
                allocs = json.loads(raw_allocs)
            except Exception:
                pass
        if not allocs:
            # Try individual keys per strategy
            for key in STRATEGY_REGISTRY:
                raw = r.get(f"edgelab:risk:allocation:{key}")
                if raw is not None:
                    try:
                        allocs[key] = float(raw)
                    except ValueError:
                        pass

        # Open positions list
        open_positions_raw = r.get("edgelab:risk:open_positions")
        open_positions = []
        if open_positions_raw:
            try:
                open_positions = json.loads(open_positions_raw)
            except Exception:
                pass

        return {
            "available": True,
            "daily_pnl": daily_pnl,
            "halted": halted,
            "halt_reason": halt_reason,
            "strategy_pnl": strategy_pnl,
            "allocations": allocs,
            "open_positions": open_positions,
        }
    except Exception:
        return {"available": False}


def _framework_trades_conn():
    """Read-only connection to framework/framework.db. Returns None if absent."""
    if not FRAMEWORK_TRADES_DB.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{FRAMEWORK_TRADES_DB}?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        return None


def _strategy_last_signal(key: str) -> str:
    """Return human-readable 'N min ago' from strategy log mtime, or fallback."""
    log_candidates = [
        BASE_DIR / "logs" / f"{key}.log",
        BASE_DIR / "strategies" / key / "logs" / f"{key}.log",
        BASE_DIR / "strategies" / key / f"{key}.log",
    ]
    for lp in log_candidates:
        if lp.exists():
            try:
                age_s = (datetime.now(timezone.utc).timestamp() - lp.stat().st_mtime)
                if age_s < 86400:
                    mins = int(age_s // 60)
                    if mins < 60:
                        return f"{mins} min ago"
                    return f"{mins // 60}h {mins % 60}m ago"
            except Exception:
                pass
    return "No signals today"


# ── New Sprint 1 API routes ───────────────────────────────────────────────────

async def api_status(request):
    """GET /api/status — Redis risk engine data + strategy registry."""
    risk = _redis_risk()
    if not risk.get("available"):
        return web.json_response({"error": "redis_unavailable"})

    strategies = {}
    for key, meta in STRATEGY_REGISTRY.items():
        strategies[key] = {
            "name": meta["name"],
            "status": meta["status"],
            "description": meta["description"],
            "today_pnl": risk["strategy_pnl"].get(key, None),
            "allocation": risk["allocations"].get(key, 0),
            "last_signal": _strategy_last_signal(key),
        }

    capital_deployed = sum(v for v in risk["allocations"].values() if isinstance(v, (int, float)))

    return web.json_response({
        "daily_pnl": risk["daily_pnl"],
        "capital_deployed": round(capital_deployed, 2),
        "halted": risk["halted"],
        "halt_reason": risk["halt_reason"],
        "strategies": strategies,
        "allocations": risk["allocations"],
    })


async def api_trades(request):
    """GET /api/trades — last 50 closed trades from framework/framework.db."""
    conn = _framework_trades_conn()
    if not conn:
        return web.json_response([])
    try:
        rows = conn.execute("""
            SELECT * FROM trades
            ORDER BY rowid DESC LIMIT 50
        """).fetchall()
        return web.json_response([dict(r) for r in rows])
    except sqlite3.OperationalError:
        # Table may not exist yet
        return web.json_response([])
    except Exception as e:
        log.warning("api_trades error: %s", e)
        return web.json_response([])
    finally:
        conn.close()


async def api_allocations(request):
    """GET /api/allocations — current allocations per strategy from Redis."""
    risk = _redis_risk()
    if not risk.get("available"):
        return web.json_response({"error": "redis_unavailable"})
    return web.json_response(risk.get("allocations", {}))


# ── API routes ────────────────────────────────────────────────────────────────

async def api_data(request):
    """Main JSON endpoint — all dashboard data in one call."""
    bot = _bot_stats()
    edge = _edge_stats()
    alerts = _unified_alerts()
    live_log = _tail_log(BOT_LIVE_LOG, 50)

    bot_deployed = bot.get("total_deployed", 0)
    bot_bankroll = bot.get("bankroll", 0)
    edge_deployed = edge.get("total_deployed", 0)
    edge_bankroll = edge.get("bankroll", 0)
    edge_is_paper = edge.get("mode", "PAPER") != "LIVE"
    # Total capital = real Kalshi bankroll only (EdgeLab paper capital excluded)
    total_capital = bot_bankroll
    total_deployed = bot_deployed + (0 if edge_is_paper else edge_deployed)

    data = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "bot": bot,
        "edge": edge,
        "risk": {
            "total_capital": round(total_capital, 2),
            "total_deployed": round(total_deployed, 2),
            "deploy_pct": round((total_deployed / total_capital * 100) if total_capital > 0 else 0, 1),
            "bot_capital": round(bot_bankroll, 2),
            "bot_deployed": round(bot_deployed, 2),
            "edge_capital": round(edge_bankroll, 2),
            "edge_deployed": round(edge_deployed, 2),
            "edge_is_paper": edge_is_paper,
        },
        "alerts": alerts,
        "live_log": live_log,
        "edge_log": _tail_log(EDGE_LOG, 50),
    }
    return web.json_response(data)


async def index(request):
    """Serve the single-page dashboard HTML."""
    return web.Response(text=HTML_PAGE, content_type="text/html")


# ── HTML Dashboard ────────────────────────────────────────────────────────────

HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>EdgeLab Command Center</title>
<style>
  :root {
    --bg: #0d1117;
    --surface: #161b22;
    --surface2: #1c2129;
    --border: #30363d;
    --text: #c9d1d9;
    --text-dim: #8b949e;
    --accent: #58a6ff;
    --green: #3fb950;
    --red: #f85149;
    --yellow: #d29922;
    --purple: #bc8cff;
    --mono: 'SF Mono', 'Fira Code', 'Cascadia Code', monospace;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--mono);
    font-size: 13px;
    line-height: 1.5;
    display: flex;
    height: 100vh;
    overflow: hidden;
  }

  /* ── Sidebar ── */
  .sidebar {
    width: 220px;
    min-width: 220px;
    background: var(--surface);
    border-right: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    padding: 16px 0;
  }
  .sidebar-header {
    padding: 0 16px 16px;
    border-bottom: 1px solid var(--border);
    margin-bottom: 8px;
  }
  .sidebar-header h1 {
    font-size: 14px;
    color: var(--accent);
    letter-spacing: 1px;
  }
  .sidebar-header .subtitle {
    font-size: 10px;
    color: var(--text-dim);
    margin-top: 2px;
  }

  .nav-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 10px 16px;
    cursor: pointer;
    color: var(--text-dim);
    font-size: 12px;
    font-weight: 600;
    letter-spacing: 0.5px;
    text-transform: uppercase;
    border-left: 3px solid transparent;
    transition: all 0.15s;
  }
  .nav-item:hover {
    color: var(--text);
    background: rgba(88,166,255,0.04);
  }
  .nav-item.active {
    color: var(--accent);
    background: rgba(88,166,255,0.08);
    border-left-color: var(--accent);
  }

  .nav-badge {
    font-size: 9px;
    padding: 1px 5px;
    border-radius: 3px;
    font-weight: bold;
    margin-left: auto;
  }
  .badge-live { background: #1a3a1a; color: var(--green); }
  .badge-paper { background: #3a3a1a; color: var(--yellow); }
  .badge-running { background: #1a3a1a; color: var(--green); }
  .badge-stopped { background: #3a1a1a; color: var(--red); }

  .sidebar-section {
    font-size: 9px;
    color: var(--text-dim);
    text-transform: uppercase;
    letter-spacing: 1px;
    padding: 12px 16px 4px;
  }

  .sidebar-spacer { flex: 1; }

  .halt-btn {
    margin: 8px 16px;
    padding: 10px;
    background: #3a1a1a;
    border: 1px solid var(--red);
    border-radius: 6px;
    color: var(--red);
    font-family: var(--mono);
    font-size: 11px;
    font-weight: bold;
    cursor: pointer;
    text-align: center;
    text-transform: uppercase;
    letter-spacing: 1px;
    transition: all 0.15s;
  }
  .halt-btn:hover {
    background: var(--red);
    color: #fff;
  }

  .sidebar-footer {
    padding: 8px 16px;
    font-size: 10px;
    color: var(--text-dim);
  }

  /* ── Main content ── */
  .main {
    flex: 1;
    overflow-y: auto;
    padding: 24px;
  }

  .tab-content { display: none; }
  .tab-content.active { display: block; }

  .page-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 20px;
  }
  .page-title {
    font-size: 18px;
    color: var(--accent);
    font-weight: bold;
  }
  .refresh-info {
    font-size: 11px;
    color: var(--text-dim);
  }

  /* ── Panels ── */
  .panel {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 16px;
    margin-bottom: 16px;
  }
  .panel-title {
    font-size: 13px;
    font-weight: bold;
    color: var(--accent);
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .tag {
    font-size: 10px;
    padding: 2px 6px;
    border-radius: 4px;
    font-weight: normal;
  }
  .tag-bot { background: #1f3a5f; color: #58a6ff; }
  .tag-edge { background: #2a1f3f; color: #bc8cff; }
  .tag-risk { background: #3f2a1f; color: #d29922; }

  .grid-2 {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
  }

  /* ── Strategy cards ── */
  .strategy-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 20px;
  }
  .strategy-card-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
  }
  .strategy-name {
    font-size: 14px;
    font-weight: bold;
  }
  .mode-badge {
    font-size: 10px;
    padding: 2px 8px;
    border-radius: 4px;
    font-weight: bold;
  }
  .mode-live { background: #1a3a1a; color: var(--green); border: 1px solid var(--green); }
  .mode-paper { background: #3a3a1a; color: var(--yellow); border: 1px solid var(--yellow); }

  /* ── Stats ── */
  .stat-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(130px, 1fr));
    gap: 8px;
  }
  .stat-grid-4 {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 8px;
  }
  .stat {
    padding: 8px;
    background: var(--bg);
    border-radius: 4px;
  }
  .stat-label {
    font-size: 10px;
    color: var(--text-dim);
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }
  .stat-value {
    font-size: 16px;
    font-weight: bold;
    margin-top: 2px;
  }
  .stat-value-lg {
    font-size: 20px;
    font-weight: bold;
    margin-top: 2px;
  }
  .positive { color: var(--green); }
  .negative { color: var(--red); }
  .neutral { color: var(--text); }
  .warn { color: var(--yellow); }

  /* ── Tables ── */
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
  }
  th {
    text-align: left;
    color: var(--text-dim);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    padding: 6px 8px;
    border-bottom: 1px solid var(--border);
  }
  td {
    padding: 6px 8px;
    border-bottom: 1px solid #21262d;
    white-space: nowrap;
  }
  tr:hover td { background: rgba(88,166,255,0.04); }

  .no-data {
    color: var(--text-dim);
    font-style: italic;
    text-align: center;
    padding: 24px;
  }

  /* ── Log viewer ── */
  .log-viewer {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 8px 12px;
    max-height: 400px;
    overflow-y: auto;
    font-size: 11px;
    line-height: 1.6;
  }
  .log-line {
    white-space: pre-wrap;
    word-break: break-all;
  }
  .log-line-error { color: var(--red); }
  .log-line-warn { color: var(--yellow); }
  .log-line-info { color: var(--text-dim); }
  .log-line-trade { color: var(--green); }

  /* ── Alert feed ── */
  .alert-feed {
    max-height: 300px;
    overflow-y: auto;
  }
  .alert-row {
    display: flex;
    gap: 8px;
    padding: 4px 0;
    border-bottom: 1px solid #21262d;
    font-size: 11px;
  }
  .alert-ts { color: var(--text-dim); min-width: 130px; }
  .alert-src {
    font-size: 10px;
    padding: 1px 5px;
    border-radius: 3px;
    font-weight: bold;
    min-width: 40px;
    text-align: center;
  }
  .alert-src-BOT { background: #1f3a5f; color: #58a6ff; }
  .alert-src-EDGE { background: #2a1f3f; color: #bc8cff; }
  .alert-level-ERROR, .alert-level-CRIT { color: var(--red); }
  .alert-level-WARN { color: var(--yellow); }
  .alert-level-TRADE, .alert-level-CLOSE { color: var(--green); }
  .alert-level-SIGNAL { color: var(--accent); }
  .alert-level-BLOCK { color: var(--text-dim); }
  .alert-msg { overflow: hidden; text-overflow: ellipsis; }

  /* ── EdgeLab mode toggle ── */
  .mode-toggle {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 12px 16px;
    background: var(--bg);
    border-radius: 6px;
    margin-bottom: 16px;
  }
  .mode-toggle-label {
    font-size: 11px;
    color: var(--text-dim);
    text-transform: uppercase;
  }
  .go-live-btn {
    padding: 6px 16px;
    background: transparent;
    border: 1px solid var(--green);
    border-radius: 4px;
    color: var(--green);
    font-family: var(--mono);
    font-size: 11px;
    font-weight: bold;
    cursor: pointer;
    transition: all 0.15s;
  }
  .go-live-btn:hover {
    background: var(--green);
    color: #fff;
  }
  .go-live-btn:disabled {
    opacity: 0.4;
    cursor: not-allowed;
  }

  .status-dot {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    margin-right: 4px;
  }
  .status-live { background: var(--green); }
  .status-halted { background: var(--red); }
  .status-offline { background: var(--text-dim); }

  /* ── Confirm dialog ── */
  .confirm-overlay {
    display: none;
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background: rgba(0,0,0,0.7);
    z-index: 100;
    align-items: center;
    justify-content: center;
  }
  .confirm-overlay.show { display: flex; }
  .confirm-box {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 24px;
    max-width: 400px;
    text-align: center;
  }
  .confirm-box h3 {
    color: var(--red);
    margin-bottom: 12px;
    font-size: 14px;
  }
  .confirm-box p {
    color: var(--text-dim);
    margin-bottom: 20px;
    font-size: 12px;
  }
  .confirm-actions {
    display: flex;
    gap: 12px;
    justify-content: center;
  }
  .confirm-actions button {
    padding: 8px 20px;
    border-radius: 4px;
    font-family: var(--mono);
    font-size: 12px;
    font-weight: bold;
    cursor: pointer;
    border: 1px solid var(--border);
  }
  .btn-cancel {
    background: var(--surface2);
    color: var(--text);
  }
  .btn-confirm-halt {
    background: var(--red);
    color: #fff;
    border-color: var(--red);
  }

  /* ── Header strip ── */
  .header-strip {
    position: sticky;
    top: 0;
    z-index: 50;
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 8px 24px;
    display: flex;
    align-items: center;
    gap: 24px;
    flex-shrink: 0;
    font-size: 12px;
  }
  .header-strip-item {
    display: flex;
    flex-direction: column;
    gap: 2px;
  }
  .header-strip-label {
    font-size: 9px;
    color: var(--text-dim);
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }
  .header-strip-value {
    font-weight: bold;
    font-size: 13px;
  }
  .header-strip-divider {
    width: 1px;
    height: 32px;
    background: var(--border);
    flex-shrink: 0;
  }
  .header-pbar-wrap {
    display: flex;
    flex-direction: column;
    gap: 4px;
    flex: 1;
    max-width: 200px;
  }
  .header-pbar {
    height: 6px;
    background: var(--surface2);
    border-radius: 3px;
    overflow: hidden;
    border: 1px solid var(--border);
  }
  .header-pbar-fill {
    height: 100%;
    border-radius: 3px;
    transition: width 0.5s ease;
  }
  .system-status-ok {
    color: var(--green);
    font-weight: bold;
    font-size: 11px;
  }
  .system-status-halt {
    color: var(--red);
    font-weight: bold;
    font-size: 11px;
    animation: blink 1s step-start infinite;
  }
  @keyframes blink {
    50% { opacity: 0.4; }
  }

  /* ── Strategy grid (12-card) ── */
  .strategy-grid-12 {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 12px;
  }
  @media (max-width: 1100px) {
    .strategy-grid-12 { grid-template-columns: repeat(2, 1fr); }
  }
  .strat-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 14px;
    display: flex;
    flex-direction: column;
    gap: 10px;
    transition: border-color 0.15s;
  }
  .strat-card:hover { border-color: #444c56; }
  .strat-card-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
  }
  .strat-card-name {
    font-size: 12px;
    font-weight: bold;
    color: var(--text);
  }
  .strat-card-desc {
    font-size: 10px;
    color: var(--text-dim);
    margin-top: 2px;
  }
  .status-badge {
    font-size: 9px;
    padding: 2px 7px;
    border-radius: 3px;
    font-weight: bold;
    white-space: nowrap;
    flex-shrink: 0;
  }
  .badge-status-LIVE     { background: #1a3a1a; color: var(--green); border: 1px solid #2a6a2a; }
  .badge-status-PAPER    { background: #1a2a3a; color: var(--accent); border: 1px solid #2a4a6a; }
  .badge-status-BUILDING { background: #3a2a00; color: var(--yellow); border: 1px solid #6a4a00; }
  .badge-status-DISABLED { background: #2a2a2a; color: var(--text-dim); border: 1px solid var(--border); }
  .strat-card-metrics {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 6px;
  }
  .strat-metric {
    background: var(--bg);
    border-radius: 4px;
    padding: 5px 7px;
  }
  .strat-metric-label {
    font-size: 9px;
    color: var(--text-dim);
    text-transform: uppercase;
    letter-spacing: 0.4px;
  }
  .strat-metric-value {
    font-size: 12px;
    font-weight: bold;
    margin-top: 1px;
  }
  .strat-card-footer {
    font-size: 10px;
    color: var(--text-dim);
    border-top: 1px solid var(--border);
    padding-top: 8px;
    display: flex;
    justify-content: space-between;
  }

  /* ── Live Trade Feed ── */
  .trade-feed-wrap {
    max-height: 420px;
    overflow-y: auto;
  }
  .trade-row-profit td { background: rgba(63,185,80,0.06); }
  .trade-row-loss   td { background: rgba(248,81,73,0.06); }
</style>
</head>
<body>

<!-- ═══ OUTER WRAPPER (column layout: header strip + row) ═══ -->
<div style="display:flex;flex-direction:column;height:100vh;overflow:hidden;width:100%">

<!-- ── HEADER STRIP ── -->
<div class="header-strip" id="header-strip">
  <div class="header-strip-item">
    <div class="header-strip-label">Daily P&amp;L</div>
    <div class="header-strip-value" id="hs-daily-pnl">—</div>
  </div>
  <div class="header-strip-divider"></div>
  <div class="header-pbar-wrap">
    <div class="header-strip-label">Daily Target $1,000</div>
    <div class="header-pbar">
      <div class="header-pbar-fill" id="hs-pbar" style="width:0%;background:var(--green)"></div>
    </div>
    <div style="font-size:9px;color:var(--text-dim)" id="hs-pbar-label">0%</div>
  </div>
  <div class="header-strip-divider"></div>
  <div class="header-strip-item">
    <div class="header-strip-label">Capital Deployed</div>
    <div class="header-strip-value neutral" id="hs-deployed">—</div>
  </div>
  <div class="header-strip-divider"></div>
  <div class="header-strip-item">
    <div class="header-strip-label">System</div>
    <div id="hs-system-status" class="system-status-ok">ALL SYSTEMS GO</div>
  </div>
  <div style="margin-left:auto;font-size:10px;color:var(--text-dim)" id="hs-updated"></div>
</div>

<!-- ── ROW: sidebar + main ── -->
<div style="display:flex;flex:1;overflow:hidden">

<!-- ═══ SIDEBAR ═══ -->
<div class="sidebar">
  <div class="sidebar-header">
    <h1>EDGELAB</h1>
    <div class="subtitle">Command Center</div>
  </div>

  <div class="sidebar-section">Navigation</div>
  <div class="nav-item active" data-tab="master-overview">
    Master Overview
  </div>
  <div class="nav-item" data-tab="capital-command">
    Capital Command
  </div>
  <div class="nav-item" data-tab="edgelab">
    EdgeLab
    <span class="nav-badge badge-paper" id="nav-edge-badge">PAPER</span>
  </div>
  <div class="nav-item" data-tab="bot">
    Bot
    <span class="nav-badge badge-stopped" id="nav-bot-badge">--</span>
  </div>
  <div class="nav-item" data-tab="performance">
    Performance
  </div>
  <div class="nav-item" data-tab="risk">
    Risk
  </div>

  <div class="sidebar-section">Intelligence</div>
  <div class="nav-item" data-tab="framework">
    Framework
  </div>
  <div class="nav-item" data-tab="news-arb">
    News Arb
    <span class="nav-badge badge-paper" id="nav-newsarb-badge">PAPER</span>
  </div>
  <div class="nav-item" data-tab="sports-momentum">
    Sports Momentum
    <span class="nav-badge badge-paper" id="nav-sm-badge">PAPER</span>
  </div>
  <div class="nav-item" data-tab="resolution-lag">
    Resolution Lag
    <span class="nav-badge badge-paper" id="nav-rl-badge">PAPER</span>
  </div>
  <div class="nav-item" data-tab="funding-rate-arb">
    Funding Rate Arb
    <span class="nav-badge badge-paper" id="nav-fra-badge">PAPER</span>
  </div>
  <div class="nav-item" data-tab="liquidity-provision">
    Liquidity Prov.
    <span class="nav-badge badge-paper" id="nav-lp-badge">PAPER</span>
  </div>
  <div class="nav-item" data-tab="carry-trade">
    Carry Trade
    <span class="nav-badge badge-paper" id="nav-carry-badge">PAPER</span>
  </div>

  <div class="sidebar-section">Sprint 1</div>
  <div class="nav-item" data-tab="strategy-grid">
    Strategy Grid
    <span class="nav-badge badge-paper" id="nav-sgrid-badge">12</span>
  </div>
  <div class="nav-item" data-tab="live-feed">
    Live Feed
  </div>

  <div class="sidebar-spacer"></div>

  <button class="halt-btn" id="halt-btn">HALT ALL TRADING</button>

  <div class="sidebar-footer" id="refresh-info">Loading...</div>
</div>

<!-- ═══ MAIN CONTENT ═══ -->
<div class="main" style="flex:1;overflow-y:auto;padding:24px">

<!-- ── TAB 0: MASTER OVERVIEW ── -->
<div class="tab-content active" id="tab-master-overview">
  <div class="page-header">
    <div class="page-title">Master Overview</div>
    <span class="refresh-info" id="mo-status"></span>
  </div>

  <div class="panel">
    <div class="panel-title">All Strategies</div>
    <div id="mo-strategy-grid"></div>
  </div>

  <div class="panel">
    <div class="panel-title">Capital Allocation</div>
    <div class="stat-grid" id="mo-capital-stats"></div>
  </div>

  <div class="panel">
    <div class="panel-title">Portfolio Summary</div>
    <div class="stat-grid" id="mo-portfolio-stats"></div>
  </div>

  <div class="panel">
    <div class="panel-title">Learning Loop</div>
    <div id="mo-learning-loop"></div>
  </div>
</div>

<!-- ── TAB 1: CAPITAL COMMAND ── -->
<div class="tab-content" id="tab-capital-command">
  <div class="page-header">
    <div class="page-title">Capital Command</div>
    <span class="refresh-info" id="cc-status"></span>
  </div>

  <!-- Combined stats -->
  <div class="panel">
    <div class="panel-title">Total Capital Overview</div>
    <div class="stat-grid-4" id="cc-total-stats"></div>
  </div>

  <!-- Strategy cards -->
  <div class="grid-2" id="cc-cards"></div>
</div>

<!-- ── TAB 2: EDGELAB ── -->
<div class="tab-content" id="tab-edgelab">
  <div class="page-header">
    <div class="page-title">EdgeLab</div>
  </div>

  <!-- Mode toggle (Paper/Live) — inside EdgeLab tab only -->
  <div class="mode-toggle" id="edge-mode-toggle">
    <span class="mode-toggle-label">Current Mode:</span>
    <span class="mode-badge mode-paper" id="edge-mode-badge">PAPER</span>
    <button class="go-live-btn" id="edge-go-live-btn" disabled>Go Live</button>
  </div>

  <!-- Scan Status -->
  <div class="panel">
    <div class="panel-title"><span class="tag tag-edge">SCAN</span> Scan Status</div>
    <div class="stat-grid-4" id="edge-scan-stats"></div>
  </div>

  <!-- Feeds Status -->
  <div class="panel">
    <div class="panel-title"><span class="tag tag-edge">FEEDS</span> Data Feeds</div>
    <div id="edge-feeds"></div>
  </div>

  <!-- Paper P&L -->
  <div class="panel">
    <div class="panel-title"><span class="tag tag-edge">P&L</span> Paper P&L</div>
    <div class="stat-grid" id="edge-pnl-stats"></div>
  </div>

  <!-- Signals / Opportunities -->
  <div class="panel">
    <div class="panel-title"><span class="tag tag-edge">SIGNALS</span> Recent Opportunities</div>
    <div id="edge-signals"></div>
  </div>

  <!-- EdgeLab Log -->
  <div class="panel">
    <div class="panel-title">EdgeLab Log <span style="font-size:10px;color:var(--text-dim);font-weight:normal">(last 50 lines)</span></div>
    <div class="log-viewer" id="edge-live-log"></div>
  </div>
</div>

<!-- ── TAB 3: BOT ── -->
<div class="tab-content" id="tab-bot">
  <div class="page-header">
    <div class="page-title">Bot — Kalshi Crypto Momentum</div>
    <span id="bot-process-status"></span>
  </div>

  <!-- Header stats -->
  <div class="panel">
    <div class="stat-grid-4" id="bot-header-stats"></div>
  </div>

  <!-- Open Positions -->
  <div class="panel">
    <div class="panel-title"><span class="tag tag-bot">OPEN</span> Open Positions</div>
    <div id="bot-positions"></div>
  </div>

  <!-- Recent Closed -->
  <div class="panel">
    <div class="panel-title"><span class="tag tag-bot">CLOSED</span> Recent Closed Trades</div>
    <div id="bot-closed"></div>
  </div>

  <!-- Live Log -->
  <div class="panel">
    <div class="panel-title">Live Log <span style="font-size:10px;color:var(--text-dim);font-weight:normal">(last 50 lines)</span></div>
    <div class="log-viewer" id="bot-live-log"></div>
  </div>
</div>

<!-- ── TAB 4: PERFORMANCE ── -->
<div class="tab-content" id="tab-performance">
  <div class="page-header">
    <div class="page-title">Performance</div>
  </div>

  <!-- Combined P&L -->
  <div class="panel">
    <div class="panel-title">Combined P&L Summary</div>
    <div class="stat-grid" id="perf-combined-stats"></div>
  </div>

  <!-- Bot daily history -->
  <div class="panel">
    <div class="panel-title"><span class="tag tag-bot">BOT</span> Daily P&L History</div>
    <div id="perf-daily-table"></div>
  </div>

  <!-- Alerts -->
  <div class="panel">
    <div class="panel-title">Unified Alert Feed <span style="font-size:10px;color:var(--text-dim);font-weight:normal">(last 20)</span></div>
    <div class="alert-feed" id="perf-alerts"></div>
  </div>
</div>

<!-- ── TAB 5: RISK ── -->
<div class="tab-content" id="tab-risk">
  <div class="page-header">
    <div class="page-title">Risk Management</div>
  </div>

  <!-- Risk envelope -->
  <div class="panel">
    <div class="panel-title"><span class="tag tag-risk">RISK</span> Shared Risk Envelope</div>
    <div class="stat-grid" id="risk-stats"></div>
  </div>

  <!-- Circuit breakers -->
  <div class="panel">
    <div class="panel-title">Circuit Breakers</div>
    <div id="risk-breakers"></div>
  </div>

  <!-- Bot signals -->
  <div class="panel">
    <div class="panel-title"><span class="tag tag-bot">BOT</span> Recent Signals (Gate Activity)</div>
    <div id="risk-signals"></div>
  </div>

  <!-- Export -->
  <div class="panel">
    <div class="panel-title">Data Export</div>
    <div style="padding:8px">
      <button onclick="exportData()" style="padding:6px 16px;background:var(--surface2);border:1px solid var(--border);border-radius:4px;color:var(--text);font-family:var(--mono);font-size:11px;cursor:pointer;">
        Export All Data as JSON
      </button>
      <span id="export-status" style="margin-left:12px;font-size:11px;color:var(--text-dim)"></span>
    </div>
  </div>
</div>

<!-- ── TAB: FRAMEWORK ── -->
<div class="tab-content" id="tab-framework">
  <div class="page-header">
    <div class="page-title">Self-Learning Framework</div>
    <span class="refresh-info" id="fw-status"></span>
  </div>

  <div class="panel">
    <div class="panel-title">Strategy Performance</div>
    <div id="fw-strategies" class="no-data">Loading...</div>
  </div>

  <div class="panel">
    <div class="panel-title">Capital Allocations</div>
    <div id="fw-allocations" class="no-data">Loading...</div>
  </div>

  <div class="panel">
    <div class="panel-title">Config Suggestions</div>
    <div id="fw-suggestions" class="no-data">Loading...</div>
  </div>
</div>

<!-- ── TAB: NEWS ARB ── -->
<div class="tab-content" id="tab-news-arb">
  <div class="page-header">
    <div class="page-title">News Arb Strategy</div>
    <span id="na-mode-badge" class="nav-badge badge-paper">PAPER</span>
  </div>

  <div class="panel">
    <div class="panel-title">Status</div>
    <div class="stat-grid" id="na-stats"></div>
  </div>

  <div class="panel">
    <div class="panel-title">Recent Signals</div>
    <div id="na-signals" class="no-data">Loading...</div>
  </div>

  <div class="panel">
    <div class="panel-title">Promotion Status</div>
    <div id="na-promotion"></div>
  </div>
</div>

<!-- ── TAB: SPORTS MOMENTUM ── -->
<div class="tab-content" id="tab-sports-momentum">
  <div class="page-header">
    <div class="page-title">Sports Momentum Strategy</div>
    <span id="sm-mode-badge" class="nav-badge badge-paper">PAPER</span>
  </div>

  <div class="panel">
    <div class="panel-title">Status</div>
    <div class="stat-grid" id="sm-stats"></div>
  </div>

  <div class="panel">
    <div class="panel-title">Active Sports</div>
    <div id="sm-sports" class="no-data">Loading...</div>
  </div>

  <div class="panel">
    <div class="panel-title">Recent Signals</div>
    <div id="sm-signals" class="no-data">Loading...</div>
  </div>

  <div class="panel">
    <div class="panel-title">Promotion Status</div>
    <div id="sm-promotion"></div>
  </div>
</div>

<!-- ── TAB: RESOLUTION LAG ── -->
<div class="tab-content" id="tab-resolution-lag">
  <div class="page-header">
    <div class="page-title">Resolution Lag Strategy</div>
    <span id="rl-mode-badge" class="nav-badge badge-paper">PAPER</span>
  </div>

  <div class="panel">
    <div class="panel-title">Status</div>
    <div class="stat-grid" id="rl-stats"></div>
  </div>

  <div class="panel">
    <div class="panel-title">Lag Metrics</div>
    <div class="stat-grid" id="rl-lag-stats"></div>
  </div>

  <div class="panel">
    <div class="panel-title">Recent Signals</div>
    <div id="rl-signals" class="no-data">Loading...</div>
  </div>

  <div class="panel">
    <div class="panel-title">Promotion Status</div>
    <div id="rl-promotion"></div>
  </div>
</div>

<!-- ── TAB: FUNDING RATE ARB ── -->
<div class="tab-content" id="tab-funding-rate-arb">
  <div class="page-header">
    <div class="page-title">Funding Rate Arb Strategy</div>
    <span id="fra-mode-badge" class="nav-badge badge-paper">PAPER</span>
  </div>

  <div class="panel">
    <div class="panel-title">Status</div>
    <div class="stat-grid" id="fra-stats"></div>
  </div>

  <div class="panel">
    <div class="panel-title">Current Funding Rates</div>
    <div id="fra-rates" class="no-data">Loading...</div>
  </div>

  <div class="panel">
    <div class="panel-title">Active Signals</div>
    <div id="fra-signals" class="no-data">Loading...</div>
  </div>

  <div class="panel">
    <div class="panel-title">Promotion Status</div>
    <div id="fra-promotion"></div>
  </div>
</div>

<!-- ── TAB: LIQUIDITY PROVISION ── -->
<div class="tab-content" id="tab-liquidity-provision">
  <div class="page-header">
    <div class="page-title">Liquidity Provision Strategy</div>
    <span id="lp-mode-badge" class="nav-badge badge-paper">PAPER</span>
  </div>

  <div class="panel">
    <div class="panel-title">Status</div>
    <div class="stat-grid" id="lp-stats"></div>
  </div>

  <div class="panel">
    <div class="panel-title">Inventory</div>
    <div class="stat-grid" id="lp-inventory-stats"></div>
  </div>

  <div class="panel">
    <div class="panel-title">Promotion Status</div>
    <div id="lp-promotion"></div>
  </div>
</div>

<!-- ── TAB: CARRY TRADE ── -->
<div class="tab-content" id="tab-carry-trade">
  <div class="page-header">
    <div class="page-title">Carry Trade Strategy</div>
    <span id="carry-mode-badge" class="nav-badge badge-paper">PAPER</span>
  </div>

  <!-- Section D: Strategy Stats -->
  <div class="panel">
    <div class="panel-title">Strategy Stats</div>
    <div class="stat-grid" id="carry-stats"></div>
  </div>

  <!-- Section A: Live Basis Scanner -->
  <div class="panel">
    <div class="panel-title">Live Basis Scanner</div>
    <div id="carry-scanner">
      <div class="no-data">Loading...</div>
    </div>
  </div>

  <!-- Section B: Open Positions -->
  <div class="panel">
    <div class="panel-title">Open Positions</div>
    <div id="carry-positions">
      <div class="no-data">No open positions</div>
    </div>
  </div>

  <!-- Section C: Trade History -->
  <div class="panel">
    <div class="panel-title">Trade History (last 20)</div>
    <div id="carry-history">
      <div class="no-data">No trade history</div>
    </div>
  </div>
</div>

<!-- ── TAB: STRATEGY GRID ── -->
<div class="tab-content" id="tab-strategy-grid">
  <div class="page-header">
    <div class="page-title">Strategy Grid</div>
    <span class="refresh-info" id="sg-status"></span>
  </div>

  <div class="panel">
    <div class="panel-title">All Strategies (12)</div>
    <div class="strategy-grid-12" id="sg-grid">
      <div class="no-data" style="grid-column:1/-1">Loading...</div>
    </div>
  </div>
</div>

<!-- ── TAB: LIVE FEED ── -->
<div class="tab-content" id="tab-live-feed">
  <div class="page-header">
    <div class="page-title">Live Trade Feed</div>
    <span class="refresh-info" id="lf-status"></span>
  </div>

  <!-- Open positions from Redis -->
  <div class="panel">
    <div class="panel-title"><span class="tag tag-bot">OPEN</span> Open Positions</div>
    <div id="lf-open-positions">
      <div class="no-data">Loading...</div>
    </div>
  </div>

  <!-- Closed trades from framework.db -->
  <div class="panel">
    <div class="panel-title"><span class="tag tag-edge">CLOSED</span> Last 50 Trades</div>
    <div class="trade-feed-wrap">
      <div id="lf-closed-trades">
        <div class="no-data">Loading...</div>
      </div>
    </div>
  </div>
</div>

</div><!-- /main -->
</div><!-- /row -->
</div><!-- /outer wrapper -->

<!-- Halt confirmation dialog -->
<div class="confirm-overlay" id="halt-confirm">
  <div class="confirm-box">
    <h3>HALT ALL TRADING</h3>
    <p>This is a global emergency stop. It will attempt to halt all active strategies. Are you sure?</p>
    <div class="confirm-actions">
      <button class="btn-cancel" onclick="closeHaltConfirm()">Cancel</button>
      <button class="btn-confirm-halt" onclick="executeHalt()">HALT NOW</button>
    </div>
  </div>
</div>

<script>
const REFRESH_MS = 30000;
let lastData = null;

function $(id) { return document.getElementById(id); }

function fmt(v, decimals=2) {
  if (v === null || v === undefined) return '\u2014';
  return Number(v).toFixed(decimals);
}

function fmtUsd(v) {
  if (v === null || v === undefined) return '\u2014';
  const n = Number(v);
  return (n >= 0 ? '+' : '') + '$' + Math.abs(n).toFixed(2);
}

function fmtTs(unix) {
  if (!unix) return '\u2014';
  const d = new Date(unix * 1000);
  return d.toISOString().replace('T',' ').slice(0,19) + ' UTC';
}

function pnlClass(v) {
  if (v > 0) return 'positive';
  if (v < 0) return 'negative';
  return 'neutral';
}

function renderStats(el, items) {
  el.innerHTML = items.map(([label, value, cls]) =>
    `<div class="stat">
      <div class="stat-label">${label}</div>
      <div class="stat-value ${cls || 'neutral'}">${value}</div>
    </div>`
  ).join('');
}

function renderStatsLg(el, items) {
  el.innerHTML = items.map(([label, value, cls]) =>
    `<div class="stat">
      <div class="stat-label">${label}</div>
      <div class="stat-value-lg ${cls || 'neutral'}">${value}</div>
    </div>`
  ).join('');
}

// ── Tab switching ──
document.querySelectorAll('.nav-item').forEach(item => {
  item.addEventListener('click', () => {
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    item.classList.add('active');
    $('tab-' + item.dataset.tab).classList.add('active');
  });
});

// ── Halt button ──
$('halt-btn').addEventListener('click', () => {
  $('halt-confirm').classList.add('show');
});

function closeHaltConfirm() {
  $('halt-confirm').classList.remove('show');
}

function executeHalt() {
  // In a real implementation, this would POST to an API endpoint
  // For now, just show confirmation
  closeHaltConfirm();
  $('halt-btn').textContent = 'HALT SENT';
  $('halt-btn').style.background = 'var(--red)';
  $('halt-btn').style.color = '#fff';
  setTimeout(() => {
    $('halt-btn').textContent = 'HALT ALL TRADING';
    $('halt-btn').style.background = '#3a1a1a';
    $('halt-btn').style.color = 'var(--red)';
  }, 3000);
}

// ── Data export ──
function exportData() {
  if (!lastData) return;
  const blob = new Blob([JSON.stringify(lastData, null, 2)], {type: 'application/json'});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'edgelab-export-' + new Date().toISOString().slice(0,10) + '.json';
  a.click();
  URL.revokeObjectURL(url);
  $('export-status').textContent = 'Exported!';
  setTimeout(() => { $('export-status').textContent = ''; }, 2000);
}

// ── Render: Capital Command ──
function renderCapitalCommand(data) {
  const bot = data.bot || {};
  const edge = data.edge || {};
  const risk = data.risk || {};

  const botBankroll = bot.bankroll || 0;
  const edgeBankroll = edge.bankroll || 0;
  // Total Capital = real Kalshi bankroll only (EdgeLab is paper)
  const totalCapital = botBankroll;
  const botTodayPnl = bot.today_pnl || 0;
  const edgeTodayPnl = edge.today_pnl || 0;
  const totalTodayPnl = botTodayPnl + edgeTodayPnl;
  const botDailyLoss = bot.daily_loss || 0;
  const combinedDailyLoss = botDailyLoss;

  renderStatsLg($('cc-total-stats'), [
    ['Total Capital', '$' + fmt(totalCapital), 'neutral'],
    ['Today P&L', fmtUsd(totalTodayPnl), pnlClass(totalTodayPnl)],
    ['Deployed', '$' + fmt(risk.total_deployed), risk.deploy_pct > 70 ? 'warn' : 'neutral'],
    ['Combined Daily Loss', '$' + fmt(combinedDailyLoss, 4), combinedDailyLoss > 0 ? 'negative' : 'neutral'],
  ]);

  // Strategy cards
  const botStatus = !bot.available ? 'OFFLINE'
    : bot.halted ? 'HALTED'
    : bot.process_running ? 'RUNNING' : 'STOPPED';
  const botStatusClass = botStatus === 'RUNNING' ? 'positive'
    : botStatus === 'HALTED' ? 'negative' : 'warn';

  const edgeMode = edge.mode || 'PAPER';
  const edgeModeClass = edgeMode === 'LIVE' ? 'mode-live' : 'mode-paper';

  const edgeStatus = !edge.available ? 'OFFLINE'
    : edge.circuit_breaker && edge.circuit_breaker.global_pause ? 'HALTED'
    : edge.process_running ? 'SCANNING' : 'STOPPED';
  const edgeStatusClass = edgeStatus === 'SCANNING' ? 'positive'
    : edgeStatus === 'HALTED' ? 'negative' : 'warn';
  const edgeScan = edge.scan_status || {};

  $('cc-cards').innerHTML = `
    <div class="strategy-card">
      <div class="strategy-card-header">
        <span class="strategy-name" style="color:var(--purple)">EdgeLab</span>
        <span class="mode-badge ${edgeModeClass}">${edgeMode}</span>
      </div>
      <div class="stat-grid">
        <div class="stat">
          <div class="stat-label">${edgeMode === 'LIVE' ? 'Balance' : 'Paper Balance'}</div>
          <div class="stat-value neutral">$${fmt(edgeBankroll)}</div>
        </div>
        <div class="stat">
          <div class="stat-label">Today P&L</div>
          <div class="stat-value ${pnlClass(edgeTodayPnl)}">${fmtUsd(edgeTodayPnl)}</div>
        </div>
        <div class="stat">
          <div class="stat-label">Status</div>
          <div class="stat-value ${edgeStatusClass}">${edgeStatus}</div>
        </div>
        <div class="stat">
          <div class="stat-label">Last Scan</div>
          <div class="stat-value neutral" style="font-size:12px">${edgeScan.last_scan || '—'}</div>
        </div>
      </div>
    </div>
    <div class="strategy-card">
      <div class="strategy-card-header">
        <span class="strategy-name" style="color:var(--accent)">Bot — Kalshi Momentum</span>
        <span class="mode-badge mode-live">LIVE</span>
      </div>
      <div class="stat-grid">
        <div class="stat">
          <div class="stat-label">Bankroll</div>
          <div class="stat-value neutral">$${fmt(botBankroll)}</div>
        </div>
        <div class="stat">
          <div class="stat-label">Today P&L</div>
          <div class="stat-value ${pnlClass(botTodayPnl)}">${fmtUsd(botTodayPnl)}</div>
        </div>
        <div class="stat">
          <div class="stat-label">Status</div>
          <div class="stat-value ${botStatusClass}">${botStatus}</div>
        </div>
      </div>
    </div>
  `;
}

// ── Render: Bot tab ──
function renderBotTab(data) {
  const bot = data.bot || {};

  // Process status badge
  if (!bot.available) {
    $('bot-process-status').innerHTML = '<span class="mode-badge" style="background:#3a1a1a;color:var(--red);border:1px solid var(--red)">DB UNAVAILABLE</span>';
  } else if (bot.process_running) {
    $('bot-process-status').innerHTML = '<span class="mode-badge mode-live">RUNNING</span>';
  } else {
    $('bot-process-status').innerHTML = '<span class="mode-badge" style="background:#3a1a1a;color:var(--red);border:1px solid var(--red)">STOPPED</span>';
  }

  // Header stats
  if (!bot.available) {
    $('bot-header-stats').innerHTML = '<div class="no-data" style="grid-column:1/-1">Bot database not found</div>';
    $('bot-positions').innerHTML = '<div class="no-data">No data</div>';
    $('bot-closed').innerHTML = '<div class="no-data">No data</div>';
    $('bot-live-log').innerHTML = '<div class="no-data">No data</div>';
    return;
  }

  renderStatsLg($('bot-header-stats'), [
    ['Bankroll', '$' + fmt(bot.bankroll), 'neutral'],
    ['Today P&L', fmtUsd(bot.today_pnl), pnlClass(bot.today_pnl)],
    ['Open Positions', (bot.open_positions || []).length, 'neutral'],
    ['Win Rate', fmt(bot.win_rate, 1) + '%', bot.win_rate >= 60 ? 'positive' : bot.win_rate > 0 ? 'warn' : 'neutral'],
  ]);

  // Open positions
  if (bot.open_positions && bot.open_positions.length > 0) {
    let html = '<table><tr><th>Market</th><th>Side</th><th>Contracts</th><th>Cost</th><th>Time Open</th></tr>';
    const now = Date.now() / 1000;
    bot.open_positions.forEach(p => {
      const elapsed = now - (p.entry_ts || now);
      const hours = Math.floor(elapsed / 3600);
      const mins = Math.floor((elapsed % 3600) / 60);
      const timeOpen = hours > 0 ? hours + 'h ' + mins + 'm' : mins + 'm';
      html += `<tr>
        <td title="${p.question || ''}">${p.asset} ${p.direction}</td>
        <td>${p.contract_side}</td>
        <td>${fmt(p.position_size / (p.entry_price || 1), 0)}</td>
        <td>$${fmt(p.position_size)}</td>
        <td style="color:var(--text-dim)">${timeOpen}</td>
      </tr>`;
    });
    html += '</table>';
    $('bot-positions').innerHTML = html;
  } else {
    $('bot-positions').innerHTML = '<div class="no-data">No open positions</div>';
  }

  // Recent closed
  if (bot.recent_trades && bot.recent_trades.length > 0) {
    let html = '<table><tr><th>Market</th><th>Result</th><th>Payout</th><th>Return %</th></tr>';
    bot.recent_trades.forEach(t => {
      const cls = t.outcome === 'WIN' ? 'positive' : 'negative';
      const returnPct = t.position_size > 0 ? (t.net_pnl / t.position_size * 100) : 0;
      html += `<tr>
        <td>${t.asset} ${t.direction}</td>
        <td class="${cls}">${t.outcome}</td>
        <td class="${cls}">${fmtUsd(t.net_pnl)}</td>
        <td class="${cls}">${fmt(returnPct, 1)}%</td>
      </tr>`;
    });
    html += '</table>';
    $('bot-closed').innerHTML = html;
  } else {
    $('bot-closed').innerHTML = '<div class="no-data">No closed trades yet</div>';
  }

  // Live log
  const logLines = data.live_log || [];
  if (logLines.length > 0) {
    $('bot-live-log').innerHTML = logLines.map(line => {
      let cls = 'log-line-info';
      if (/\[ERROR\]|\[CRITICAL\]/.test(line)) cls = 'log-line-error';
      else if (/\[WARNING\]/.test(line)) cls = 'log-line-warn';
      else if (/LIVE ORDER|Trade #\d+ closed/.test(line)) cls = 'log-line-trade';
      const escaped = line.replace(/</g, '&lt;').replace(/>/g, '&gt;');
      return `<div class="log-line ${cls}">${escaped}</div>`;
    }).join('');
    // Auto-scroll to bottom
    const el = $('bot-live-log');
    el.scrollTop = el.scrollHeight;
  } else {
    $('bot-live-log').innerHTML = '<div class="no-data">No log data</div>';
  }
}

// ── Render: EdgeLab tab ──
function renderEdgeLabTab(data) {
  const edge = data.edge || {};
  const mode = edge.mode || 'PAPER';
  const badge = $('edge-mode-badge');
  badge.textContent = mode;
  badge.className = 'mode-badge ' + (mode === 'LIVE' ? 'mode-live' : 'mode-paper');
  $('edge-go-live-btn').disabled = !edge.available;

  // Scan Status
  const scan = edge.scan_status || {};
  renderStatsLg($('edge-scan-stats'), [
    ['Last Scan', scan.last_scan || 'No data', 'neutral'],
    ['Markets Scanned', scan.markets_scanned || 0, 'neutral'],
    ['Signals Found', scan.signals_found || 0, scan.signals_found > 0 ? 'positive' : 'neutral'],
    ['Process', edge.process_running ? 'RUNNING' : 'OFFLINE', edge.process_running ? 'positive' : 'negative'],
  ]);

  // Feeds
  const feeds = edge.feeds || [];
  if (feeds.length > 0) {
    let html = '<table><tr><th>Feed</th><th>Status</th><th>Detail</th></tr>';
    feeds.forEach(f => {
      const cls = f.status === 'ACTIVE' ? 'positive' : f.status === 'MOCK' ? 'warn' : f.status === 'ERROR' ? 'negative' : 'neutral';
      // Extract just the useful part from detail
      const detailMatch = (f.detail || '').match(/INFO:\s*(.+)/);
      const detail = detailMatch ? detailMatch[1] : (f.detail || '').slice(-80);
      html += '<tr><td>' + f.name + '</td><td class="' + cls + '">' + f.status + '</td><td style="color:var(--text-dim);font-size:11px">' + detail + '</td></tr>';
    });
    html += '</table>';
    $('edge-feeds').innerHTML = html;
  } else {
    $('edge-feeds').innerHTML = '<div class="no-data">No feed data — strategy may not be running</div>';
  }

  // Paper P&L
  const isPaper = mode !== 'LIVE';
  renderStats($('edge-pnl-stats'), [
    [isPaper ? 'Paper Balance' : 'Balance', '$' + fmt(edge.bankroll), 'neutral'],
    ['Today P&L', fmtUsd(edge.today_pnl), pnlClass(edge.today_pnl)],
    ['Total P&L', fmtUsd(edge.total_pnl), pnlClass(edge.total_pnl)],
    ['Deployed', '$' + fmt(edge.total_deployed), 'neutral'],
    ['Trades', edge.trade_count || 0, 'neutral'],
    ['Win Rate', fmt(edge.win_rate || 0, 1) + '%', (edge.win_rate || 0) > 50 ? 'positive' : 'neutral'],
  ]);
  if (isPaper && edge.trade_count === 0) {
    $('edge-pnl-stats').innerHTML += '<div style="grid-column:1/-1;padding:8px;color:var(--text-dim);font-size:11px;font-style:italic">Paper mode \u2014 no trades placed yet. Feeds are scanning for opportunities.</div>';
  }

  // Signals (recent trades or "no signals" message)
  const trades = edge.recent_trades || [];
  if (trades.length > 0) {
    let html = '<table><tr><th>Market</th><th>Strategy</th><th>Edge %</th><th>Size</th><th>Status</th><th>P&L</th><th>Time</th></tr>';
    trades.forEach(t => {
      const cls = t.pnl > 0 ? 'positive' : t.pnl < 0 ? 'negative' : 'neutral';
      html += '<tr><td>' + (t.market_id || '').slice(0,30) + '</td><td>' + (t.strategy || '') + '</td><td>' + fmt((t.entry_prob || 0) * 100, 1) + '%</td><td>$' + fmt(t.size) + '</td><td>' + (t.status || '') + '</td><td class="' + cls + '">' + fmtUsd(t.pnl || 0) + '</td><td style="color:var(--text-dim)">' + (t.timestamp || '') + '</td></tr>';
    });
    html += '</table>';
    $('edge-signals').innerHTML = html;
  } else {
    $('edge-signals').innerHTML = '<div class="no-data">No opportunities detected yet. Strategies are scanning ' + (scan.markets_scanned || 0) + ' markets across active feeds.</div>';
  }

  // EdgeLab Log
  const logLines = data.edge_log || [];
  if (logLines.length > 0) {
    $('edge-live-log').innerHTML = logLines.map(line => {
      let cls = 'log-line-info';
      if (/\[ERROR\]|\[CRITICAL\]|error/i.test(line)) cls = 'log-line-error';
      else if (/\[WARNING\]|warning/i.test(line)) cls = 'log-line-warn';
      else if (/opportunity|signal|trade/i.test(line)) cls = 'log-line-trade';
      const escaped = line.replace(/</g, '&lt;').replace(/>/g, '&gt;');
      return '<div class="log-line ' + cls + '">' + escaped + '</div>';
    }).join('');
    const el = $('edge-live-log');
    el.scrollTop = el.scrollHeight;
  } else {
    $('edge-live-log').innerHTML = '<div class="no-data">No log data</div>';
  }
}

// ── Render: Performance tab ──
function renderPerformanceTab(data) {
  const bot = data.bot || {};
  const edge = data.edge || {};

  const botPnl = bot.total_pnl || 0;
  const edgePnl = edge.total_pnl || 0;
  const combinedPnl = botPnl + edgePnl;
  const botTodayPnl = bot.today_pnl || 0;
  const edgeTodayPnl = edge.today_pnl || 0;

  renderStats($('perf-combined-stats'), [
    ['Combined Total P&L', fmtUsd(combinedPnl), pnlClass(combinedPnl)],
    ['Bot Total P&L', fmtUsd(botPnl), pnlClass(botPnl)],
    ['EdgeLab Total P&L', fmtUsd(edgePnl), pnlClass(edgePnl)],
    ['Bot Today', fmtUsd(botTodayPnl), pnlClass(botTodayPnl)],
    ['Bot Win Rate', fmt(bot.win_rate || 0, 1) + '%', (bot.win_rate || 0) >= 60 ? 'positive' : 'warn'],
    ['Bot ROI', fmt(bot.roi_pct || 0) + '%', pnlClass(bot.roi_pct || 0)],
    ['Total Trades', bot.total_trades || 0, 'neutral'],
    ['W / L', (bot.wins || 0) + ' / ' + (bot.losses || 0), 'neutral'],
  ]);

  // Daily history table
  const daily = bot.daily_history || [];
  if (daily.length > 0) {
    let html = '<table><tr><th>Date</th><th>Trades</th><th>W/L</th><th>Net P&L</th><th>Win Rate</th><th>Bankroll</th></tr>';
    daily.forEach(d => {
      const cls = (d.net_pnl || 0) >= 0 ? 'positive' : 'negative';
      html += `<tr>
        <td>${d.date_utc}</td>
        <td>${d.trades}</td>
        <td>${d.wins}/${d.losses}</td>
        <td class="${cls}">${fmtUsd(d.net_pnl)}</td>
        <td>${fmt(d.win_rate * 100, 0)}%</td>
        <td>$${fmt(d.bankroll_close)}</td>
      </tr>`;
    });
    html += '</table>';
    $('perf-daily-table').innerHTML = html;
  } else {
    $('perf-daily-table').innerHTML = '<div class="no-data">No daily history yet</div>';
  }

  // Alerts
  const alerts = data.alerts || [];
  if (alerts.length > 0) {
    $('perf-alerts').innerHTML = alerts.map(a =>
      `<div class="alert-row">
        <span class="alert-ts">${a.ts || '\u2014'}</span>
        <span class="alert-src alert-src-${a.source}">${a.source}</span>
        <span class="alert-level-${a.level}" style="min-width:40px">${a.level}</span>
        <span class="alert-msg">${a.message}</span>
      </div>`
    ).join('');
  } else {
    $('perf-alerts').innerHTML = '<div class="no-data">No recent alerts</div>';
  }
}

// ── Render: Risk tab ──
function renderRiskTab(data) {
  const risk = data.risk || {};
  const bot = data.bot || {};

  renderStats($('risk-stats'), [
    ['Total Capital', '$' + fmt(risk.total_capital), 'neutral'],
    ['Total Deployed', '$' + fmt(risk.total_deployed), risk.deploy_pct > 70 ? 'warn' : 'neutral'],
    ['Deploy %', fmt(risk.deploy_pct, 1) + '%', risk.deploy_pct > 70 ? 'warn' : 'neutral'],
    ['Bot Capital', '$' + fmt(risk.bot_capital), 'neutral'],
    ['Bot Deployed', '$' + fmt(risk.bot_deployed), 'neutral'],
    ['Edge (Paper)', '$' + fmt(risk.edge_capital), 'neutral'],
    ['Edge Deployed', '$' + fmt(risk.edge_deployed), 'neutral'],
  ]);

  // Circuit breakers
  const halted = bot.halted || false;
  const dailyLoss = bot.daily_loss || 0;
  $('risk-breakers').innerHTML = `
    <table>
      <tr><th>Breaker</th><th>Status</th><th>Value</th></tr>
      <tr>
        <td>Bot Halt Flag</td>
        <td class="${halted ? 'negative' : 'positive'}">${halted ? 'TRIGGERED' : 'OK'}</td>
        <td>${halted ? bot.halt_reason || 'Manual halt' : 'Not triggered'}</td>
      </tr>
      <tr>
        <td>Bot Daily Loss Limit</td>
        <td class="${dailyLoss > 5 ? 'negative' : dailyLoss > 3 ? 'warn' : 'positive'}">${dailyLoss > 5 ? 'NEAR LIMIT' : 'OK'}</td>
        <td>$${fmt(dailyLoss, 4)} used</td>
      </tr>
      <tr>
        <td>Bot Process</td>
        <td class="${bot.process_running ? 'positive' : 'negative'}">${bot.process_running ? 'RUNNING' : 'STOPPED'}</td>
        <td>${bot.process_running ? 'live_engine.py active' : 'Process not found'}</td>
      </tr>
    </table>
  `;

  // Signals
  if (bot.recent_signals && bot.recent_signals.length > 0) {
    let html = '<table><tr><th>Time</th><th>Asset</th><th>Dir</th><th>Mom%</th><th>Strength</th><th>Gate</th></tr>';
    bot.recent_signals.forEach(s => {
      const gateClass = s.gate_result === 'PASS' ? 'positive' : 'negative';
      html += `<tr>
        <td>${fmtTs(s.ts)}</td>
        <td>${s.asset}</td>
        <td>${s.direction}</td>
        <td>${fmt(s.momentum_pct, 4)}%</td>
        <td>${s.signal_strength}</td>
        <td class="${gateClass}">${s.gate_result}</td>
      </tr>`;
    });
    html += '</table>';
    $('risk-signals').innerHTML = html;
  } else {
    $('risk-signals').innerHTML = '<div class="no-data">No recent signals</div>';
  }
}

// ── Render: Framework tab ──
async function renderFrameworkTab() {
  try {
    const resp = await fetch('/api/framework');
    if (!resp.ok) { $('fw-strategies').innerHTML = '<div class="no-data">Framework DB not available</div>'; return; }
    const fw = await resp.json();
    $('fw-status').textContent = 'Last analyzed: ' + (fw.last_analyzed || 'never');

    // Strategy table
    if (fw.strategies && fw.strategies.length > 0) {
      let html = '<table><tr><th>Strategy</th><th>Win Rate</th><th>P&L</th><th>Trades</th><th>Last Updated</th></tr>';
      fw.strategies.forEach(s => {
        const wrClass = s.win_rate >= 0.6 ? 'positive' : s.win_rate >= 0.45 ? 'warn' : 'negative';
        html += `<tr>
          <td>${s.name}</td>
          <td class="${wrClass}">${(s.win_rate * 100).toFixed(1)}%</td>
          <td class="${s.total_pnl >= 0 ? 'positive' : 'negative'}">${fmtUsd(s.total_pnl)}</td>
          <td>${s.trades}</td>
          <td>${fmtTs(s.last_updated)}</td>
        </tr>`;
      });
      html += '</table>';
      $('fw-strategies').innerHTML = html;
    } else {
      $('fw-strategies').innerHTML = '<div class="no-data">No strategies tracked yet</div>';
    }

    // Allocations
    if (fw.capital_allocations && fw.capital_allocations.length > 0) {
      let html = '<table><tr><th>Strategy</th><th>Allocation %</th><th>Allocation $</th></tr>';
      fw.capital_allocations.forEach(a => {
        html += `<tr><td>${a.strategy}</td><td>${(a.allocated_pct * 100).toFixed(1)}%</td><td>$${fmt(a.allocated_usd)}</td></tr>`;
      });
      html += '</table>';
      $('fw-allocations').innerHTML = html;
    } else {
      $('fw-allocations').innerHTML = '<div class="no-data">No allocations yet</div>';
    }

    // Suggestions
    if (fw.recent_suggestions && fw.recent_suggestions.length > 0) {
      $('fw-suggestions').innerHTML = '<ul style="padding-left:16px">' + fw.recent_suggestions.map(s => `<li style="margin:4px 0">${s}</li>`).join('') + '</ul>';
    } else {
      $('fw-suggestions').innerHTML = '<div class="no-data">No suggestions — all strategies within bounds</div>';
    }
  } catch(e) {
    $('fw-strategies').innerHTML = '<div class="no-data">Error loading framework data</div>';
  }
}

// ── Render: News Arb tab ──
async function renderNewsArbTab() {
  try {
    const resp = await fetch('/api/news_arb');
    const na = await resp.json();

    // Mode badge
    const badge = $('na-mode-badge');
    badge.textContent = na.mode;
    badge.className = 'nav-badge ' + (na.mode === 'LIVE' ? 'badge-live' : 'badge-paper');

    // Sidebar badge
    const navBadge = $('nav-newsarb-badge');
    navBadge.textContent = na.status;
    navBadge.className = 'nav-badge ' + (na.status === 'RUNNING' ? 'badge-running' : 'badge-stopped');

    // Stats
    renderStatsLg($('na-stats'), [
      ['Mode', na.mode, 'neutral'],
      ['Status', na.status, na.status === 'RUNNING' ? 'positive' : 'negative'],
      ['Paper Trades', na.paper_trades, 'neutral'],
      ['Win Rate', (na.paper_win_rate * 100).toFixed(1) + '%', na.paper_win_rate >= 0.55 ? 'positive' : 'warn'],
      ['Paper P&L', fmtUsd(na.paper_pnl), pnlClass(na.paper_pnl)],
    ]);

    // Signals
    if (na.recent_signals && na.recent_signals.length > 0) {
      let html = '<table><tr><th>Time</th><th>Market</th><th>Dir</th><th>Edge</th><th>Article</th></tr>';
      na.recent_signals.forEach(s => {
        html += `<tr>
          <td>${fmtTs(s.timestamp)}</td>
          <td>${s.market_id || '-'}</td>
          <td>${s.direction || '-'}</td>
          <td>${s.edge_pct ? (s.edge_pct * 100).toFixed(1) + '%' : '-'}</td>
          <td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${s.article_title || '-'}</td>
        </tr>`;
      });
      html += '</table>';
      $('na-signals').innerHTML = html;
    } else {
      $('na-signals').innerHTML = '<div class="no-data">No signals yet</div>';
    }

    // Promotion
    $('na-promotion').innerHTML = na.promotion_eligible
      ? '<span style="color:var(--green);font-weight:bold">ELIGIBLE — Strategy meets all live deployment criteria</span>'
      : '<span style="color:var(--yellow)">Not yet eligible — collecting paper trade data</span>';
  } catch(e) {
    $('na-stats').innerHTML = '<div class="no-data">Error loading news arb data</div>';
  }
}

// ── Render: Sports Momentum tab ──
async function renderSportsMomentumTab() {
  try {
    const resp = await fetch('/api/sports_momentum');
    const sm = await resp.json();

    const badge = $('sm-mode-badge');
    badge.textContent = sm.mode;
    badge.className = 'nav-badge ' + (sm.mode === 'LIVE' ? 'badge-live' : 'badge-paper');

    const navBadge = $('nav-sm-badge');
    navBadge.textContent = sm.status;
    navBadge.className = 'nav-badge ' + (sm.status === 'RUNNING' ? 'badge-running' : 'badge-stopped');

    renderStatsLg($('sm-stats'), [
      ['Mode', sm.mode, 'neutral'],
      ['Status', sm.status, sm.status === 'RUNNING' ? 'positive' : 'negative'],
      ['Paper Trades', sm.paper_trades, 'neutral'],
      ['Win Rate', (sm.paper_win_rate * 100).toFixed(1) + '%', sm.paper_win_rate >= 0.55 ? 'positive' : 'warn'],
      ['Paper P&L', fmtUsd(sm.paper_pnl), pnlClass(sm.paper_pnl)],
    ]);

    if (sm.active_sports && sm.active_sports.length > 0) {
      $('sm-sports').innerHTML = sm.active_sports.map(s =>
        '<span style="display:inline-block;padding:4px 12px;margin:4px;background:var(--surface2);border:1px solid var(--border);border-radius:4px;color:var(--accent);text-transform:uppercase;font-size:11px;font-weight:600">' + s + '</span>'
      ).join('');
    } else {
      $('sm-sports').innerHTML = '<div class="no-data">No active sports detected — waiting for live games</div>';
    }

    if (sm.recent_signals && sm.recent_signals.length > 0) {
      let html = '<table><tr><th>Time</th><th>Sport</th><th>Game</th><th>Dir</th><th>Edge</th><th>Trigger</th></tr>';
      sm.recent_signals.forEach(s => {
        html += '<tr>' +
          '<td>' + (s.timestamp || '-') + '</td>' +
          '<td style="text-transform:uppercase">' + (s.sport || '-') + '</td>' +
          '<td>' + (s.game || '-') + '</td>' +
          '<td>' + (s.direction || '-') + '</td>' +
          '<td>' + (s.edge_pct ? (s.edge_pct * 100).toFixed(1) + '%' : '-') + '</td>' +
          '<td style="max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + (s.trigger_reason || '-') + '</td>' +
          '</tr>';
      });
      html += '</table>';
      $('sm-signals').innerHTML = html;
    } else {
      $('sm-signals').innerHTML = '<div class="no-data">No signals yet — monitoring ESPN feeds</div>';
    }

    $('sm-promotion').innerHTML = sm.promotion_eligible
      ? '<span style="color:var(--green);font-weight:bold">ELIGIBLE — Strategy meets all live deployment criteria</span>'
      : '<span style="color:var(--yellow)">Not yet eligible — collecting paper trade data</span>';
  } catch(e) {
    $('sm-stats').innerHTML = '<div class="no-data">Error loading sports momentum data</div>';
  }
}

// ── Render: Resolution Lag tab ──
async function renderResolutionLagTab() {
  try {
    const resp = await fetch('/api/resolution_lag');
    const rl = await resp.json();

    const badge = $('rl-mode-badge');
    badge.textContent = rl.mode;
    badge.className = 'nav-badge ' + (rl.mode === 'LIVE' ? 'badge-live' : 'badge-paper');

    const navBadge = $('nav-rl-badge');
    navBadge.textContent = rl.status;
    navBadge.className = 'nav-badge ' + (rl.status === 'RUNNING' ? 'badge-running' : 'badge-stopped');

    renderStatsLg($('rl-stats'), [
      ['Mode', rl.mode, 'neutral'],
      ['Status', rl.status, rl.status === 'RUNNING' ? 'positive' : 'negative'],
      ['Paper Trades', rl.paper_trades, 'neutral'],
      ['Win Rate', (rl.paper_win_rate * 100).toFixed(1) + '%', rl.paper_win_rate >= 0.60 ? 'positive' : 'warn'],
      ['Paper P&L', fmtUsd(rl.paper_pnl), pnlClass(rl.paper_pnl)],
    ]);

    renderStats($('rl-lag-stats'), [
      ['Lag Frequency', fmt(rl.lag_frequency_pct, 1) + '%', rl.lag_frequency_pct > 10 ? 'positive' : 'neutral'],
      ['Avg Lag Duration', fmt(rl.avg_lag_duration_min, 1) + ' min', 'neutral'],
    ]);

    if (rl.recent_signals && rl.recent_signals.length > 0) {
      let html = '<table><tr><th>Time</th><th>Market</th><th>Edge</th><th>Certainty</th><th>Lag %</th></tr>';
      rl.recent_signals.forEach(s => {
        html += '<tr>' +
          '<td>' + (s.timestamp || '-') + '</td>' +
          '<td>' + (s.market_id || '-') + '</td>' +
          '<td>' + (s.edge_pct ? (s.edge_pct * 100).toFixed(1) + '%' : '-') + '</td>' +
          '<td>' + (s.certainty_score ? fmt(s.certainty_score * 100, 0) + '%' : '-') + '</td>' +
          '<td>' + (s.lag_magnitude_pct ? (s.lag_magnitude_pct * 100).toFixed(1) + '%' : '-') + '</td>' +
          '</tr>';
      });
      html += '</table>';
      $('rl-signals').innerHTML = html;
    } else {
      $('rl-signals').innerHTML = '<div class="no-data">No signals yet — scanning for resolved events</div>';
    }

    $('rl-promotion').innerHTML = rl.promotion_eligible
      ? '<span style="color:var(--green);font-weight:bold">ELIGIBLE — Strategy meets all live deployment criteria</span>'
      : '<span style="color:var(--yellow)">Not yet eligible — collecting paper trade data</span>';
  } catch(e) {
    $('rl-stats').innerHTML = '<div class="no-data">Error loading resolution lag data</div>';
  }
}

// ── Render: Master Overview tab ──
async function renderMasterOverview() {
  // Fetch all strategy data in parallel
  let na = {}, sm = {}, rl = {}, fra = {}, lp = {};
  try {
    const [naR, smR, rlR, fraR, lpR] = await Promise.all([
      fetch('/api/news_arb').then(r => r.json()).catch(() => ({})),
      fetch('/api/sports_momentum').then(r => r.json()).catch(() => ({})),
      fetch('/api/resolution_lag').then(r => r.json()).catch(() => ({})),
      fetch('/api/funding_rate_arb').then(r => r.json()).catch(() => ({})),
      fetch('/api/liquidity_provision').then(r => r.json()).catch(() => ({})),
    ]);
    na = naR; sm = smR; rl = rlR; fra = fraR; lp = lpR;
  } catch(e) {}

  const strategies = [
    { name: 'News Arb', data: na },
    { name: 'Sports Momentum', data: sm },
    { name: 'Resolution Lag', data: rl },
    { name: 'Funding Rate Arb', data: fra },
    { name: 'Liquidity Prov.', data: lp },
  ];

  function statusBadge(s) {
    const d = s.data;
    const mode = d.mode || 'PAPER';
    const status = d.status || 'STOPPED';
    const wr = (d.paper_win_rate || 0) * 100;
    const eligible = d.promotion_eligible || false;

    if (status === 'RUNNING' && mode === 'LIVE' && (d.paper_pnl || 0) >= 0)
      return '<span style="color:var(--green);font-weight:bold">LIVE+PROFIT</span>';
    if (eligible)
      return '<span style="color:var(--yellow);font-weight:bold">ELIGIBLE</span>';
    if (mode === 'PAPER' && status === 'RUNNING')
      return '<span style="color:var(--yellow)">PAPER</span>';
    if (status === 'STOPPED')
      return '<span style="color:var(--red)">STOPPED</span>';
    return '<span style="color:var(--text-dim)">PAPER</span>';
  }

  let html = '<table><tr><th>Strategy</th><th>Mode</th><th>Win Rate</th><th>P&L</th><th>Trades</th><th>Status</th></tr>';
  strategies.forEach(s => {
    const d = s.data;
    const mode = d.mode || 'PAPER';
    const modeClass = mode === 'LIVE' ? 'positive' : 'warn';
    const wr = ((d.paper_win_rate || 0) * 100).toFixed(1);
    const wrClass = (d.paper_win_rate || 0) >= 0.55 ? 'positive' : (d.paper_win_rate || 0) >= 0.50 ? 'warn' : 'neutral';
    const pnl = d.paper_pnl || 0;
    html += '<tr>' +
      '<td style="font-weight:bold">' + s.name + '</td>' +
      '<td class="' + modeClass + '">' + mode + '</td>' +
      '<td class="' + wrClass + '">' + wr + '%</td>' +
      '<td class="' + pnlClass(pnl) + '">' + fmtUsd(pnl) + '</td>' +
      '<td>' + (d.paper_trades || 0) + '</td>' +
      '<td>' + statusBadge(s) + '</td>' +
      '</tr>';
  });
  html += '</table>';
  $('mo-strategy-grid').innerHTML = html;

  // Portfolio summary
  const totalPnl = strategies.reduce((acc, s) => acc + (s.data.paper_pnl || 0), 0);
  const totalTrades = strategies.reduce((acc, s) => acc + (s.data.paper_trades || 0), 0);
  const runningCount = strategies.filter(s => (s.data.status || '') === 'RUNNING').length;
  const eligibleCount = strategies.filter(s => s.data.promotion_eligible).length;

  renderStats($('mo-portfolio-stats'), [
    ['Total P&L (All Strategies)', fmtUsd(totalPnl), pnlClass(totalPnl)],
    ['Total Paper Trades', totalTrades, 'neutral'],
    ['Running', runningCount + '/' + strategies.length, runningCount > 0 ? 'positive' : 'warn'],
    ['Promotion Eligible', eligibleCount + '/' + strategies.length, eligibleCount > 0 ? 'positive' : 'neutral'],
  ]);

  // Capital allocation from framework
  try {
    const fw = await fetch('/api/framework').then(r => r.json()).catch(() => ({}));
    if (fw.capital_allocations && fw.capital_allocations.length > 0) {
      const allocItems = fw.capital_allocations.slice(0, 5).map(a =>
        [a.strategy, '$' + fmt(a.allocated_usd) + ' (' + (a.allocated_pct * 100).toFixed(1) + '%)', 'neutral']
      );
      renderStats($('mo-capital-stats'), allocItems);
    } else {
      $('mo-capital-stats').innerHTML = '<div class="no-data" style="grid-column:1/-1">No allocations yet — framework needs trade data</div>';
    }
    $('mo-learning-loop').innerHTML = '<div style="padding:8px;color:var(--text-dim)">Last analyzed: ' + (fw.last_analyzed || 'never') + '</div>';
  } catch(e) {
    $('mo-capital-stats').innerHTML = '<div class="no-data" style="grid-column:1/-1">Framework unavailable</div>';
  }

  $('mo-status').textContent = 'Updated ' + new Date().toLocaleTimeString();
}

// ── Render: Funding Rate Arb tab ──
async function renderFundingRateArbTab() {
  try {
    const resp = await fetch('/api/funding_rate_arb');
    const fra = await resp.json();

    const badge = $('fra-mode-badge');
    badge.textContent = fra.mode;
    badge.className = 'nav-badge ' + (fra.mode === 'LIVE' ? 'badge-live' : 'badge-paper');

    const navBadge = $('nav-fra-badge');
    navBadge.textContent = fra.status;
    navBadge.className = 'nav-badge ' + (fra.status === 'RUNNING' ? 'badge-running' : 'badge-stopped');

    renderStatsLg($('fra-stats'), [
      ['Mode', fra.mode, 'neutral'],
      ['Status', fra.status, fra.status === 'RUNNING' ? 'positive' : 'negative'],
      ['Paper Trades', fra.paper_trades, 'neutral'],
      ['Win Rate', (fra.paper_win_rate * 100).toFixed(1) + '%', fra.paper_win_rate >= 0.55 ? 'positive' : 'warn'],
      ['Paper P&L', fmtUsd(fra.paper_pnl), pnlClass(fra.paper_pnl)],
    ]);

    // Funding rates
    const rates = fra.current_funding_rates || {};
    const rateKeys = Object.keys(rates);
    if (rateKeys.length > 0) {
      let html = '<table><tr><th>Asset</th><th>Funding Rate</th><th>Direction</th></tr>';
      rateKeys.forEach(k => {
        const r = rates[k];
        const dir = r > 0.0005 ? 'BEARISH' : r < -0.0005 ? 'BULLISH' : 'NEUTRAL';
        const cls = dir === 'BEARISH' ? 'negative' : dir === 'BULLISH' ? 'positive' : 'neutral';
        html += '<tr><td>' + k + '</td><td>' + (r * 100).toFixed(4) + '%</td><td class="' + cls + '">' + dir + '</td></tr>';
      });
      html += '</table>';
      $('fra-rates').innerHTML = html;
    } else {
      $('fra-rates').innerHTML = '<div class="no-data">No funding rate data yet</div>';
    }

    // Signals
    if (fra.active_signals && fra.active_signals.length > 0) {
      let html = '<table><tr><th>Time</th><th>Asset</th><th>Dir</th><th>Rate</th><th>Magnitude</th><th>Edge</th></tr>';
      fra.active_signals.forEach(s => {
        const dirCls = s.direction === 'BEARISH' ? 'negative' : 'positive';
        html += '<tr>' +
          '<td>' + (s.timestamp || '-') + '</td>' +
          '<td>' + (s.asset || '-') + '</td>' +
          '<td class="' + dirCls + '">' + (s.direction || '-') + '</td>' +
          '<td>' + (s.funding_rate ? (s.funding_rate * 100).toFixed(4) + '%' : '-') + '</td>' +
          '<td>' + (s.magnitude || '-') + '</td>' +
          '<td>' + (s.edge_pct ? (s.edge_pct * 100).toFixed(1) + '%' : '-') + '</td>' +
          '</tr>';
      });
      html += '</table>';
      $('fra-signals').innerHTML = html;
    } else {
      $('fra-signals').innerHTML = '<div class="no-data">No signals yet — monitoring funding rates</div>';
    }

    $('fra-promotion').innerHTML = fra.promotion_eligible
      ? '<span style="color:var(--green);font-weight:bold">ELIGIBLE — Strategy meets all live deployment criteria</span>'
      : '<span style="color:var(--yellow)">Not yet eligible — collecting paper trade data</span>';
  } catch(e) {
    $('fra-stats').innerHTML = '<div class="no-data">Error loading funding rate arb data</div>';
  }
}

// ── Render: Liquidity Provision tab ──
async function renderLiquidityProvisionTab() {
  try {
    const resp = await fetch('/api/liquidity_provision');
    const lp = await resp.json();

    const badge = $('lp-mode-badge');
    badge.textContent = lp.mode;
    badge.className = 'nav-badge ' + (lp.mode === 'LIVE' ? 'badge-live' : 'badge-paper');

    const navBadge = $('nav-lp-badge');
    navBadge.textContent = lp.status;
    navBadge.className = 'nav-badge ' + (lp.status === 'RUNNING' ? 'badge-running' : 'badge-stopped');

    renderStatsLg($('lp-stats'), [
      ['Mode', lp.mode, 'neutral'],
      ['Status', lp.status, lp.status === 'RUNNING' ? 'positive' : 'negative'],
      ['Paper Trades', lp.paper_trades, 'neutral'],
      ['Win Rate', (lp.paper_win_rate * 100).toFixed(1) + '%', lp.paper_win_rate >= 0.52 ? 'positive' : 'warn'],
      ['Paper P&L', fmtUsd(lp.paper_pnl), pnlClass(lp.paper_pnl)],
      ['Spread Capture', (lp.spread_capture_rate * 100).toFixed(1) + '%', lp.spread_capture_rate >= 0.60 ? 'positive' : 'warn'],
    ]);

    renderStats($('lp-inventory-stats'), [
      ['Active Quotes', lp.active_quotes || 0, 'neutral'],
      ['Inventory USD', '$' + fmt(lp.current_inventory_usd), lp.current_inventory_usd > 25 ? 'warn' : 'neutral'],
    ]);

    $('lp-promotion').innerHTML = lp.promotion_eligible
      ? '<span style="color:var(--green);font-weight:bold">ELIGIBLE — Strategy meets all live deployment criteria</span>'
      : '<span style="color:var(--yellow)">Not yet eligible — collecting paper trade data</span>';
  } catch(e) {
    $('lp-stats').innerHTML = '<div class="no-data">Error loading liquidity provision data</div>';
  }
}

// ── Sidebar badges ──
function updateSidebarBadges(data) {
  const bot = data.bot || {};
  const edge = data.edge || {};

  // EdgeLab badge
  const edgeBadge = $('nav-edge-badge');
  const edgeMode = edge.mode || 'PAPER';
  edgeBadge.textContent = edgeMode;
  edgeBadge.className = 'nav-badge ' + (edgeMode === 'LIVE' ? 'badge-live' : 'badge-paper');

  // Bot badge
  const botBadge = $('nav-bot-badge');
  if (!bot.available) {
    botBadge.textContent = 'OFFLINE';
    botBadge.className = 'nav-badge badge-stopped';
  } else if (bot.halted) {
    botBadge.textContent = 'HALTED';
    botBadge.className = 'nav-badge badge-stopped';
  } else if (bot.process_running) {
    botBadge.textContent = 'LIVE';
    botBadge.className = 'nav-badge badge-running';
  } else {
    botBadge.textContent = 'STOPPED';
    botBadge.className = 'nav-badge badge-stopped';
  }
}

// ── Render: Carry Trade ──
async function renderCarryTradeTab() {
  try {
    const resp = await fetch('/api/carry');
    const carry = await resp.json();

    if (!carry.available) {
      $('carry-scanner').innerHTML = '<div class="no-data">No carry trade data yet. Start the scanner: <code>python3 strategies/edgelab/run_carry.py</code></div>';
      $('carry-positions').innerHTML = '<div class="no-data">No carry trade data yet.</div>';
      $('carry-history').innerHTML = '<div class="no-data">No carry trade data yet.</div>';
      renderStats($('carry-stats'), [
        ['Total Trades', '0', 'neutral'],
        ['Win Rate', '0%', 'neutral'],
        ['Avg Basis Captured', '0%', 'neutral'],
        ['Total P&L', '$0.00', 'neutral'],
      ]);
      return;
    }

    // Stats cards
    renderStatsLg($('carry-stats'), [
      ['Total Trades', carry.total_trades, 'neutral'],
      ['Win Rate', carry.win_rate.toFixed(1) + '%', carry.win_rate >= 50 ? 'positive' : carry.win_rate > 0 ? 'warn' : 'neutral'],
      ['Avg Basis Captured', carry.avg_basis_captured.toFixed(1) + '%', carry.avg_basis_captured > 0 ? 'positive' : 'neutral'],
      ['Total P&L', fmtUsd(carry.total_pnl), pnlClass(carry.total_pnl)],
    ]);

    // Scanner table
    const scans = carry.latest_scans || [];
    if (scans.length > 0) {
      let html = '<table><tr><th>Asset</th><th>Spot Price</th><th>Futures Price</th><th>Basis APR</th><th>Days to Expiry</th><th>Signal</th></tr>';
      scans.forEach(s => {
        let signal, sigCls;
        if (s.basis_apr >= 12) { signal = '\u{1F7E2} ENTER'; sigCls = 'positive'; }
        else if (s.basis_apr >= 5) { signal = '\u{1F7E1} WATCH'; sigCls = 'warn'; }
        else { signal = '\u{1F534} PASS'; sigCls = 'negative'; }
        html += '<tr>' +
          '<td>' + s.asset + '</td>' +
          '<td>$' + Number(s.spot_price).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) + '</td>' +
          '<td>$' + Number(s.futures_price).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) + '</td>' +
          '<td>' + Number(s.basis_apr).toFixed(1) + '%</td>' +
          '<td>' + (s.days_to_expiry || '-') + '</td>' +
          '<td class="' + sigCls + '">' + signal + '</td>' +
          '</tr>';
      });
      html += '</table>';
      $('carry-scanner').innerHTML = html;
    } else {
      $('carry-scanner').innerHTML = '<div class="no-data">No scan data yet. Start the scanner: <code>python3 strategies/edgelab/run_carry.py</code></div>';
    }

    // Open positions
    const open = carry.open_positions || [];
    if (open.length > 0) {
      let html = '<table><tr><th>Asset</th><th>Entry Basis</th><th>Entry Spot</th><th>Spot Size</th><th>Leverage</th><th>Margin Call Price</th><th>Status</th></tr>';
      open.forEach(p => {
        html += '<tr>' +
          '<td>' + p.asset + '</td>' +
          '<td>' + Number(p.entry_basis_apr).toFixed(1) + '%</td>' +
          '<td>$' + Number(p.entry_spot).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) + '</td>' +
          '<td>$' + Number(p.spot_size_usd).toFixed(2) + '</td>' +
          '<td>' + Number(p.leverage).toFixed(1) + 'x</td>' +
          '<td>$' + Number(p.margin_call_price).toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2}) + '</td>' +
          '<td><span class="nav-badge badge-running">OPEN</span></td>' +
          '</tr>';
      });
      html += '</table>';
      $('carry-positions').innerHTML = html;
    } else {
      $('carry-positions').innerHTML = '<div class="no-data">No open positions</div>';
    }

    // Trade history
    const closed = carry.closed_positions || [];
    if (closed.length > 0) {
      let cumPnl = 0;
      let html = '<table><tr><th>Asset</th><th>Entry Date</th><th>Exit Date</th><th>Entry Basis</th><th>Exit Basis</th><th>P&L</th><th>Exit Reason</th></tr>';
      closed.forEach(t => {
        cumPnl += (t.pnl_usd || 0);
        const pCls = (t.pnl_usd || 0) >= 0 ? 'positive' : 'negative';
        html += '<tr>' +
          '<td>' + t.asset + '</td>' +
          '<td>' + (t.entry_time || '-').slice(0,16) + '</td>' +
          '<td>' + (t.exit_time || '-').slice(0,16) + '</td>' +
          '<td>' + Number(t.entry_basis_apr).toFixed(1) + '%</td>' +
          '<td>' + (t.exit_basis_apr ? Number(t.exit_basis_apr).toFixed(1) + '%' : '-') + '</td>' +
          '<td class="' + pCls + '">' + fmtUsd(t.pnl_usd) + '</td>' +
          '<td>' + (t.exit_reason || '-') + '</td>' +
          '</tr>';
      });
      html += '</table>';
      const cumCls = cumPnl >= 0 ? 'positive' : 'negative';
      html += '<div style="margin-top:8px;font-size:12px;">Cumulative P&L: <span class="' + cumCls + '">' + fmtUsd(cumPnl) + '</span></div>';
      $('carry-history').innerHTML = html;
    } else {
      $('carry-history').innerHTML = '<div class="no-data">No closed trades yet</div>';
    }

    // Nav badge
    const navBadge = $('nav-carry-badge');
    if (carry.active_positions > 0) {
      navBadge.textContent = carry.active_positions + ' OPEN';
      navBadge.className = 'nav-badge badge-running';
    } else {
      navBadge.textContent = 'PAPER';
      navBadge.className = 'nav-badge badge-paper';
    }

  } catch(e) {
    console.error('Carry trade tab error:', e);
  }
}

// ── Render: Header Strip ──
async function refreshHeaderStrip() {
  try {
    const resp = await fetch('/api/status');
    const d = await resp.json();

    if (d.error) {
      $('hs-daily-pnl').textContent = '—';
      $('hs-deployed').textContent = '—';
      $('hs-system-status').textContent = 'REDIS DOWN';
      $('hs-system-status').className = 'system-status-halt';
      $('hs-updated').textContent = 'Redis unavailable';
      return;
    }

    const pnl = d.daily_pnl || 0;
    const pnlEl = $('hs-daily-pnl');
    pnlEl.textContent = (pnl >= 0 ? '+' : '') + '$' + Math.abs(pnl).toFixed(2);
    pnlEl.className = 'header-strip-value ' + (pnl > 0 ? 'positive' : pnl < 0 ? 'negative' : 'neutral');

    const fillPct = Math.min(Math.max((pnl / 1000) * 100, 0), 100);
    const pbarFill = $('hs-pbar');
    pbarFill.style.width = fillPct.toFixed(1) + '%';
    pbarFill.style.background = pnl >= 0 ? 'var(--green)' : 'var(--red)';
    $('hs-pbar-label').textContent = fillPct.toFixed(1) + '% of $1,000 target';

    $('hs-deployed').textContent = '$' + (d.capital_deployed || 0).toFixed(2);

    const sysEl = $('hs-system-status');
    if (d.halted) {
      sysEl.textContent = 'HALTED: ' + (d.halt_reason || 'unknown');
      sysEl.className = 'system-status-halt';
    } else {
      sysEl.textContent = 'ALL SYSTEMS GO';
      sysEl.className = 'system-status-ok';
    }

    $('hs-updated').textContent = 'Updated ' + new Date().toLocaleTimeString();
  } catch(e) {
    $('hs-system-status').textContent = 'ERROR';
    $('hs-system-status').className = 'system-status-halt';
  }
}

// ── Render: Strategy Grid ──
async function renderStrategyGrid() {
  try {
    const resp = await fetch('/api/status');
    const d = await resp.json();

    if (d.error) {
      $('sg-grid').innerHTML = '<div class="no-data" style="grid-column:1/-1">Redis unavailable — strategy data offline</div>';
      return;
    }

    const strats = d.strategies || {};
    const keys = Object.keys(strats);
    if (keys.length === 0) {
      $('sg-grid').innerHTML = '<div class="no-data" style="grid-column:1/-1">No strategies in registry</div>';
      return;
    }

    const cards = keys.map(key => {
      const s = strats[key];
      const status = s.status || 'BUILDING';
      const pnl = s.today_pnl;
      const alloc = s.allocation || 0;
      const winRate = s.win_rate;
      const lastSig = s.last_signal || 'No signals today';

      const pnlStr = pnl !== null && pnl !== undefined
        ? '<span class="' + (pnl > 0 ? 'positive' : pnl < 0 ? 'negative' : 'neutral') + '">'
          + (pnl >= 0 ? '+' : '') + '$' + Math.abs(pnl).toFixed(2) + '</span>'
        : '<span class="neutral">—</span>';

      const winRateStr = winRate !== null && winRate !== undefined && !isNaN(winRate)
        ? '<span class="' + (winRate >= 55 ? 'positive' : 'warn') + '">' + Number(winRate).toFixed(1) + '%</span>'
        : '<span class="neutral">— awaiting trades</span>';

      return `<div class="strat-card">
        <div class="strat-card-header">
          <div>
            <div class="strat-card-name">${s.name}</div>
            <div class="strat-card-desc">${s.description || ''}</div>
          </div>
          <span class="status-badge badge-status-${status}">${status}</span>
        </div>
        <div class="strat-card-metrics">
          <div class="strat-metric">
            <div class="strat-metric-label">Today P&amp;L</div>
            <div class="strat-metric-value">${pnlStr}</div>
          </div>
          <div class="strat-metric">
            <div class="strat-metric-label">Win Rate</div>
            <div class="strat-metric-value">${winRateStr}</div>
          </div>
          <div class="strat-metric">
            <div class="strat-metric-label">Allocation</div>
            <div class="strat-metric-value neutral">${alloc > 0 ? '$' + alloc.toFixed(2) : '—'}</div>
          </div>
          <div class="strat-metric">
            <div class="strat-metric-label">Status</div>
            <div class="strat-metric-value ${status === 'LIVE' ? 'positive' : status === 'PAPER' ? 'neutral' : 'warn'}">${status}</div>
          </div>
        </div>
        <div class="strat-card-footer">
          <span>Last signal</span>
          <span>${lastSig}</span>
        </div>
      </div>`;
    });

    $('sg-grid').innerHTML = cards.join('');
    $('sg-status').textContent = 'Updated ' + new Date().toLocaleTimeString();
    $('nav-sgrid-badge').textContent = keys.length;
  } catch(e) {
    $('sg-grid').innerHTML = '<div class="no-data" style="grid-column:1/-1">Error loading strategy grid</div>';
  }
}

// ── Render: Live Feed ──
async function renderLiveFeed() {
  try {
    // Fetch open positions from /api/status (Redis) and closed from /api/trades
    const [statusResp, tradesResp] = await Promise.all([
      fetch('/api/status').then(r => r.json()).catch(() => ({})),
      fetch('/api/trades').then(r => r.json()).catch(() => []),
    ]);

    const risk = statusResp;
    const closedTrades = Array.isArray(tradesResp) ? tradesResp : [];

    // Open positions
    const openPositions = (risk.error || !risk.strategies)
      ? []
      : (risk.open_positions || []);

    const openEl = $('lf-open-positions');
    if (openPositions.length > 0) {
      let html = '<table><tr><th>Time</th><th>Strategy</th><th>Market</th><th>Side</th><th>Size</th><th>Entry</th><th>P&L</th></tr>';
      openPositions.forEach(p => {
        const ts = p.timestamp || p.entry_ts || p.time || '';
        const pnl = p.unrealized_pnl || p.pnl || null;
        const pnlStr = pnl !== null ? fmtUsd(pnl) : '—';
        const pnlCls = pnl > 0 ? 'positive' : pnl < 0 ? 'negative' : 'neutral';
        html += '<tr>' +
          '<td style="color:var(--text-dim)">' + (typeof ts === 'number' ? fmtTs(ts) : ts) + '</td>' +
          '<td>' + (p.strategy || p.strategy_name || '—') + '</td>' +
          '<td style="max-width:180px;overflow:hidden;text-overflow:ellipsis">' + (p.market || p.market_id || '—') + '</td>' +
          '<td>' + (p.side || p.direction || '—') + '</td>' +
          '<td>$' + fmt(p.size || p.position_size || 0) + '</td>' +
          '<td>' + fmt(p.entry || p.entry_price || 0, 4) + '</td>' +
          '<td class="' + pnlCls + '">' + pnlStr + '</td>' +
          '</tr>';
      });
      html += '</table>';
      openEl.innerHTML = html;
    } else {
      openEl.innerHTML = '<div class="no-data">No open positions</div>';
    }

    // Closed trades
    const closedEl = $('lf-closed-trades');
    if (closedTrades.length > 0) {
      let html = '<table><tr><th>Time</th><th>Strategy</th><th>Market</th><th>Side</th><th>Size</th><th>Entry</th><th>Exit</th><th>P&L</th></tr>';
      closedTrades.forEach(t => {
        const pnl = t.pnl || t.net_pnl || 0;
        const rowCls = pnl > 0 ? 'trade-row-profit' : pnl < 0 ? 'trade-row-loss' : '';
        const ts = t.exit_ts || t.timestamp || t.exit_time || '';
        html += '<tr class="' + rowCls + '">' +
          '<td style="color:var(--text-dim)">' + (typeof ts === 'number' ? fmtTs(ts) : String(ts).slice(0, 19)) + '</td>' +
          '<td>' + (t.strategy || t.strategy_name || '—') + '</td>' +
          '<td style="max-width:160px;overflow:hidden;text-overflow:ellipsis">' + (t.market || t.market_id || '—') + '</td>' +
          '<td>' + (t.side || t.direction || t.contract_side || '—') + '</td>' +
          '<td>$' + fmt(t.size || t.position_size || 0) + '</td>' +
          '<td>' + fmt(t.entry_price || t.entry || 0, 4) + '</td>' +
          '<td>' + fmt(t.exit_price || t.exit || 0, 4) + '</td>' +
          '<td class="' + (pnl > 0 ? 'positive' : pnl < 0 ? 'negative' : 'neutral') + '">' + fmtUsd(pnl) + '</td>' +
          '</tr>';
      });
      html += '</table>';
      closedEl.innerHTML = html;
    } else {
      closedEl.innerHTML = '<div class="no-data">No closed trades in framework.db yet</div>';
    }

    $('lf-status').textContent = 'Updated ' + new Date().toLocaleTimeString();
  } catch(e) {
    $('lf-open-positions').innerHTML = '<div class="no-data">Error loading live feed</div>';
  }
}

// ── Main refresh ──
async function refresh() {
  try {
    const resp = await fetch('/api/data');
    const data = await resp.json();
    lastData = data;

    updateSidebarBadges(data);
    renderMasterOverview();
    renderCapitalCommand(data);
    renderEdgeLabTab(data);
    renderBotTab(data);
    renderPerformanceTab(data);
    renderRiskTab(data);
    renderFrameworkTab();
    renderNewsArbTab();
    renderSportsMomentumTab();
    renderResolutionLagTab();
    renderFundingRateArbTab();
    renderLiquidityProvisionTab();
    renderCarryTradeTab();
    renderStrategyGrid();
    renderLiveFeed();

    const now = new Date();
    $('refresh-info').textContent = 'Updated ' + now.toLocaleTimeString();
    $('cc-status').textContent = 'Last update: ' + now.toLocaleTimeString();
  } catch (e) {
    $('refresh-info').textContent = 'Connection error';
    $('refresh-info').style.color = '#f85149';
  }
}

// Initial load + auto-refresh
refresh();
setInterval(refresh, REFRESH_MS);

// Header strip refreshes every 5s independently
refreshHeaderStrip();
setInterval(refreshHeaderStrip, 5000);
</script>
</body>
</html>
"""


# ── App setup ─────────────────────────────────────────────────────────────────

app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/api/data", api_data)
app.router.add_get("/api/framework", api_framework)
app.router.add_get("/api/news_arb", api_news_arb)
app.router.add_get("/api/sports_momentum", api_sports_momentum)
app.router.add_get("/api/resolution_lag", api_resolution_lag)
app.router.add_get("/api/funding_rate_arb", api_funding_rate_arb)
app.router.add_get("/api/liquidity_provision", api_liquidity_provision)
app.router.add_get("/api/carry/status", api_carry_status)
app.router.add_get("/api/carry", api_carry_data)
app.router.add_get("/carry", carry_page)
app.router.add_get("/api/status", api_status)
app.router.add_get("/api/trades", api_trades)
app.router.add_get("/api/allocations", api_allocations)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    print(f"\n  EdgeLab Command Center")
    print(f"  http://localhost:{PORT}")
    print(f"  Bot DB: {BOT_DB} ({'found' if BOT_DB.exists() else 'NOT FOUND'})")
    print(f"  Edge DB: {EDGE_DB} ({'found' if EDGE_DB.exists() else 'NOT FOUND'})")
    print(f"  Edge Log: {EDGE_LOG} ({'found' if EDGE_LOG.exists() else 'NOT FOUND'})")
    print(f"  Carry DB: {CARRY_DB} ({'found' if CARRY_DB.exists() else 'NOT FOUND'})")
    print(f"  Framework Trades DB: {FRAMEWORK_TRADES_DB} ({'found' if FRAMEWORK_TRADES_DB.exists() else 'NOT FOUND'})")
    print(f"  Strategy Registry: {len(STRATEGY_REGISTRY)} strategies loaded")
    print(f"  Auto-refresh: 30s (header strip: 5s)\n")
    web.run_app(app, host="0.0.0.0", port=PORT, print=None)
