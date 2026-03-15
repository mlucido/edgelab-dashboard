"""
EdgeLab Morning Report — overnight paper trading summary + GO/NO-GO.

Reads from SQLite (trades_paper) and logs to produce a clean terminal report.

Usage:
    python scripts/morning_report.py
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

DB_PATH = os.getenv("EDGELAB_DB_PATH", str(PROJECT_ROOT / "data" / "edgelab.db"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
ALERT_LOG = PROJECT_ROOT / "logs" / "overnight_alerts.log"
MAIN_LOG = PROJECT_ROOT / "logs" / "edgelab.log"

# GO/NO-GO thresholds
MIN_WIN_RATE = 0.60
MIN_SHARPE = 2.0


def _header(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _section(title: str) -> None:
    print(f"\n--- {title} ---")


def _load_trades(hours: int = 12) -> list[dict]:
    """Load paper trades from the last N hours."""
    if not Path(DB_PATH).exists():
        print(f"  Database not found at {DB_PATH}")
        return []
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    rows = conn.execute(
        "SELECT * FROM trades_paper WHERE timestamp >= ? ORDER BY timestamp",
        (cutoff,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _load_all_trades() -> list[dict]:
    """Load all paper trades."""
    if not Path(DB_PATH).exists():
        return []
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM trades_paper ORDER BY timestamp").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _calc_sharpe(pnls: list[float], periods_per_day: int = 48) -> float | None:
    """Annualized Sharpe from a list of per-trade P&Ls."""
    if len(pnls) < 2:
        return None
    import math
    mean = sum(pnls) / len(pnls)
    var = sum((p - mean) ** 2 for p in pnls) / (len(pnls) - 1)
    std = math.sqrt(var) if var > 0 else 0
    if std == 0:
        return float("inf") if mean > 0 else 0.0
    return (mean / std) * math.sqrt(periods_per_day * 365)


def _check_circuit_breakers() -> tuple[bool, str]:
    """Check Redis for active circuit breakers."""
    try:
        import redis as sync_redis
        r = sync_redis.from_url(REDIS_URL, socket_connect_timeout=3)
        paused = r.get("edgelab:cb:global_pause")
        if paused and paused.decode() == "1":
            reason = r.get("edgelab:cb:global_pause_reason")
            r.close()
            return True, reason.decode() if reason else "unknown reason"
        r.close()
        return False, "none"
    except Exception:
        return False, "Redis unavailable — cannot verify"


def _scan_overnight_alerts() -> list[str]:
    """Pull ALERT lines from overnight_alerts.log in last 12 hours."""
    alerts = []
    if not ALERT_LOG.exists():
        return alerts
    cutoff = datetime.now() - timedelta(hours=12)
    for line in ALERT_LOG.read_text(errors="replace").splitlines():
        if "ALERT" not in line and "CRITICAL" not in line and "ERROR" not in line:
            continue
        try:
            ts_str = line[:19]
            ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            if ts >= cutoff:
                alerts.append(line.strip())
        except (ValueError, IndexError):
            alerts.append(line.strip())
    return alerts


def _scan_error_log() -> int:
    """Count ERROR lines in edgelab.log in last 12 hours."""
    if not MAIN_LOG.exists():
        return 0
    cutoff = datetime.now() - timedelta(hours=12)
    count = 0
    lines = MAIN_LOG.read_text(errors="replace").splitlines()[-2000:]
    for line in lines:
        if " ERROR:" not in line and " ERROR " not in line:
            continue
        try:
            ts_str = line[:23]
            ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f")
            if ts >= cutoff:
                count += 1
        except (ValueError, IndexError):
            count += 1
    return count


def main() -> None:
    start = time.time()
    now = datetime.now()

    _header(f"EDGELAB MORNING REPORT — {now.strftime('%Y-%m-%d %H:%M')}")

    # Load trades
    overnight_trades = _load_trades(hours=12)
    all_trades = _load_all_trades()

    closed_overnight = [t for t in overnight_trades if t.get("status") == "closed" and t.get("pnl") is not None]
    open_overnight = [t for t in overnight_trades if t.get("status") == "open"]

    # --- Trade summary ---
    _section("TRADE SUMMARY (Last 12 Hours)")
    total_placed = len(overnight_trades)
    total_closed = len(closed_overnight)
    total_open = len(open_overnight)
    print(f"  Trades placed:   {total_placed}")
    print(f"  Trades closed:   {total_closed}")
    print(f"  Trades open:     {total_open}")

    # --- Win rate ---
    _section("PERFORMANCE")
    wins = [t for t in closed_overnight if (t.get("pnl") or 0) > 0]
    losses = [t for t in closed_overnight if (t.get("pnl") or 0) <= 0]
    win_rate = len(wins) / len(closed_overnight) if closed_overnight else 0.0
    print(f"  Wins:            {len(wins)}")
    print(f"  Losses:          {len(losses)}")
    print(f"  Win rate:        {win_rate:.1%}")

    # --- P&L ---
    pnls = [(t.get("pnl") or 0.0) for t in closed_overnight]
    total_pnl = sum(pnls)
    print(f"  Total P&L:       ${total_pnl:+.2f}")

    # --- Sharpe ---
    sharpe = _calc_sharpe(pnls) if pnls else None
    sharpe_str = f"{sharpe:.2f}" if sharpe is not None else "N/A"
    print(f"  Sharpe ratio:    {sharpe_str}")

    # --- P&L by strategy ---
    _section("P&L BY STRATEGY")
    strategy_pnl: dict[str, list[float]] = {}
    for t in closed_overnight:
        s = t.get("strategy", "unknown")
        strategy_pnl.setdefault(s, []).append(t.get("pnl") or 0.0)
    if strategy_pnl:
        for strat, pnl_list in sorted(strategy_pnl.items()):
            s_total = sum(pnl_list)
            s_wins = sum(1 for p in pnl_list if p > 0)
            s_wr = s_wins / len(pnl_list) if pnl_list else 0
            print(f"  {strat:20s}  ${s_total:+8.2f}  ({len(pnl_list)} trades, {s_wr:.0%} WR)")
    else:
        print("  No closed trades by strategy")

    # --- Best / worst trade ---
    _section("NOTABLE TRADES")
    if closed_overnight:
        best = max(closed_overnight, key=lambda t: t.get("pnl") or 0)
        worst = min(closed_overnight, key=lambda t: t.get("pnl") or 0)
        print(f"  Best:   ${best['pnl']:+.2f}  [{best['strategy']}]  {best['market_id'][:40]}")
        print(f"  Worst:  ${worst['pnl']:+.2f}  [{worst['strategy']}]  {worst['market_id'][:40]}")
    else:
        print("  No closed trades to report")

    # --- Circuit breakers ---
    _section("CIRCUIT BREAKERS")
    cb_tripped, cb_reason = _check_circuit_breakers()
    if cb_tripped:
        print(f"  *** TRIPPED: {cb_reason}")
    else:
        print(f"  Status: {cb_reason}")

    # --- Errors & anomalies ---
    _section("ERRORS & ANOMALIES")
    error_count = _scan_error_log()
    print(f"  Errors in edgelab.log (12h): {error_count}")
    alerts = _scan_overnight_alerts()
    if alerts:
        print(f"  Overnight alerts: {len(alerts)}")
        for a in alerts[-5:]:  # show last 5
            print(f"    > {a[:100]}")
    else:
        print("  Overnight alerts: 0")

    # --- All-time stats ---
    _section("ALL-TIME PAPER TRADING")
    all_closed = [t for t in all_trades if t.get("status") == "closed" and t.get("pnl") is not None]
    all_pnl = sum(t.get("pnl", 0) for t in all_closed)
    all_wr = sum(1 for t in all_closed if (t.get("pnl") or 0) > 0) / len(all_closed) if all_closed else 0
    print(f"  Total trades:    {len(all_closed)}")
    print(f"  Total P&L:       ${all_pnl:+.2f}")
    print(f"  Win rate:        {all_wr:.1%}")

    # --- GO / NO-GO ---
    _header("GO / NO-GO RECOMMENDATION")
    reasons_no = []
    reasons_go = []

    # Win rate
    if win_rate >= MIN_WIN_RATE:
        reasons_go.append(f"Win rate {win_rate:.1%} >= {MIN_WIN_RATE:.0%}")
    else:
        reasons_no.append(f"Win rate {win_rate:.1%} < {MIN_WIN_RATE:.0%}")

    # Circuit breakers
    if not cb_tripped:
        reasons_go.append("No circuit breakers active")
    else:
        reasons_no.append(f"Circuit breaker tripped: {cb_reason}")

    # Sharpe
    if sharpe is not None and sharpe >= MIN_SHARPE:
        reasons_go.append(f"Sharpe {sharpe:.2f} >= {MIN_SHARPE}")
    elif sharpe is None:
        reasons_no.append("Sharpe ratio unavailable (no closed trades)")
    else:
        reasons_no.append(f"Sharpe {sharpe:.2f} < {MIN_SHARPE}")

    # Minimum trade count
    if total_closed < 5:
        reasons_no.append(f"Only {total_closed} closed trades — insufficient sample")

    # Error check
    if error_count > 100:
        reasons_no.append(f"{error_count} errors overnight — investigate before going live")

    verdict = "GO" if not reasons_no else "NO-GO"
    color = "\033[92m" if verdict == "GO" else "\033[91m"
    reset = "\033[0m"

    print(f"\n  {'>' * 3} {color}{verdict}{reset} {'<' * 3}\n")

    if reasons_go:
        for r in reasons_go:
            print(f"  [+] {r}")
    if reasons_no:
        for r in reasons_no:
            print(f"  [-] {r}")

    elapsed = time.time() - start
    print(f"\n  Report generated in {elapsed:.1f}s")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
