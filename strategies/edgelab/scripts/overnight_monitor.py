"""
EdgeLab Overnight Health Monitor — runs every 5 minutes via APScheduler.

Checks:
  1. main.py process alive (via PID file)
  2. Redis connectivity
  3. Last DB write within 10 minutes
  4. No circuit breakers tripped
  5. No error spam in logs (>50 ERROR lines in last 10 min)

On failure: writes alert to logs/overnight_alerts.log
If main.py dies: auto-restarts (max 3 times total)

Usage:
    python scripts/overnight_monitor.py
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure project root is on sys.path for imports
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

from apscheduler.schedulers.blocking import BlockingScheduler

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = os.getenv("EDGELAB_DB_PATH", str(PROJECT_ROOT / "data" / "edgelab.db"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
PID_FILE = PROJECT_ROOT / "logs" / "edgelab.pid"
MAIN_LOG = PROJECT_ROOT / "logs" / "edgelab.log"
ALERT_LOG = PROJECT_ROOT / "logs" / "overnight_alerts.log"
MAIN_SCRIPT = PROJECT_ROOT / "main.py"

MAX_RESTARTS = 3
DB_STALE_MINUTES = 10
ERROR_SPAM_THRESHOLD = 50  # errors in 10-min window

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
ALERT_LOG.parent.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("edgelab.overnight")
logger.setLevel(logging.INFO)

_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
logger.addHandler(_sh)

_fh = logging.FileHandler(ALERT_LOG, mode="a")
_fh.setFormatter(_fmt)
logger.addHandler(_fh)

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
_restart_count = 0


# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------

def _check_process_alive() -> tuple[bool, str]:
    """Check if main.py is running via PID file."""
    if not PID_FILE.exists():
        return False, "PID file missing — main.py not running"
    try:
        pid = int(PID_FILE.read_text().strip())
        os.kill(pid, 0)  # signal 0 = existence check
        return True, f"main.py alive (PID {pid})"
    except (ValueError, ProcessLookupError, PermissionError):
        return False, "main.py process is dead"


def _check_redis() -> tuple[bool, str]:
    """Check Redis connectivity (sync, no new deps)."""
    try:
        import redis as sync_redis
        r = sync_redis.from_url(REDIS_URL, socket_connect_timeout=3)
        r.ping()
        r.close()
        return True, "Redis alive"
    except Exception as exc:
        return False, f"Redis unreachable: {exc}"


def _check_db_freshness() -> tuple[bool, str]:
    """Check that the last paper trade was written within DB_STALE_MINUTES."""
    if not Path(DB_PATH).exists():
        return False, f"Database not found at {DB_PATH}"
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.execute(
            "SELECT MAX(timestamp) FROM trades_paper"
        )
        row = cur.fetchone()
        conn.close()
        if row is None or row[0] is None:
            return True, "No paper trades yet — OK for first run"
        last_ts = row[0]
        # Parse ISO timestamp
        try:
            last_dt = datetime.fromisoformat(last_ts)
        except ValueError:
            last_dt = datetime.strptime(last_ts, "%Y-%m-%d %H:%M:%S")
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=timezone.utc)
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=DB_STALE_MINUTES)
        if last_dt < cutoff:
            age = int((datetime.now(timezone.utc) - last_dt).total_seconds() / 60)
            return False, f"Last DB write {age} min ago — stale (>{DB_STALE_MINUTES} min)"
        return True, "DB writes fresh"
    except Exception as exc:
        return False, f"DB check failed: {exc}"


def _check_circuit_breakers() -> tuple[bool, str]:
    """Check if any circuit breakers are tripped in Redis."""
    try:
        import redis as sync_redis
        r = sync_redis.from_url(REDIS_URL, socket_connect_timeout=3)
        paused = r.get("edgelab:cb:global_pause")
        if paused and paused.decode() == "1":
            reason = r.get("edgelab:cb:global_pause_reason")
            reason_str = reason.decode() if reason else "unknown"
            r.close()
            return False, f"Circuit breaker TRIPPED: {reason_str}"
        # Check strategy pauses
        paused_raw = r.get("edgelab:cb:strategy_paused")
        if paused_raw:
            paused_dict = json.loads(paused_raw.decode())
            now = time.time()
            active = {s: t for s, t in paused_dict.items() if t > now}
            if active:
                r.close()
                return False, f"Strategies paused: {list(active.keys())}"
        r.close()
        return True, "No circuit breakers active"
    except Exception as exc:
        return False, f"CB check failed (Redis?): {exc}"


def _check_error_spam() -> tuple[bool, str]:
    """Count ERROR lines in the last 10 minutes of edgelab.log."""
    if not MAIN_LOG.exists():
        return True, "No log file yet"
    try:
        cutoff = datetime.now() - timedelta(minutes=10)
        error_count = 0
        # Read last 500 lines (tail) to keep it fast
        lines = MAIN_LOG.read_text(errors="replace").splitlines()[-500:]
        for line in lines:
            if " ERROR:" not in line and " ERROR " not in line:
                continue
            # Try to parse timestamp from log line: "2026-03-11 02:15:00,123 ..."
            try:
                ts_str = line[:23]
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f")
                if ts >= cutoff:
                    error_count += 1
            except (ValueError, IndexError):
                error_count += 1  # can't parse = count it conservatively
        if error_count >= ERROR_SPAM_THRESHOLD:
            return False, f"{error_count} ERROR lines in last 10 min (threshold: {ERROR_SPAM_THRESHOLD})"
        return True, f"{error_count} errors in last 10 min — OK"
    except Exception as exc:
        return False, f"Log scan failed: {exc}"


# ---------------------------------------------------------------------------
# Auto-restart
# ---------------------------------------------------------------------------

def _restart_main() -> bool:
    """Attempt to restart main.py. Returns True on success."""
    global _restart_count
    if _restart_count >= MAX_RESTARTS:
        logger.critical(
            "MAX RESTARTS (%d) EXCEEDED — will not restart again. Manual intervention required.",
            MAX_RESTARTS,
        )
        return False
    _restart_count += 1
    logger.warning("Restarting main.py (attempt %d/%d)...", _restart_count, MAX_RESTARTS)
    try:
        proc = subprocess.Popen(
            [sys.executable, str(MAIN_SCRIPT)],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        PID_FILE.parent.mkdir(parents=True, exist_ok=True)
        PID_FILE.write_text(str(proc.pid))
        logger.info("main.py restarted — new PID %d", proc.pid)
        return True
    except Exception as exc:
        logger.error("Restart failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Main health check job
# ---------------------------------------------------------------------------

def run_health_check() -> None:
    """Run all health checks and log results."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("=" * 60)
    logger.info("OVERNIGHT HEALTH CHECK — %s", now)
    logger.info("=" * 60)

    all_ok = True
    checks = [
        ("Process", _check_process_alive),
        ("Redis", _check_redis),
        ("DB Freshness", _check_db_freshness),
        ("Circuit Breakers", _check_circuit_breakers),
        ("Error Spam", _check_error_spam),
    ]

    results: list[tuple[str, bool, str]] = []
    for name, fn in checks:
        ok, msg = fn()
        results.append((name, ok, msg))
        status = "OK" if ok else "ALERT"
        logger.info("  [%s] %s: %s", status, name, msg)
        if not ok:
            all_ok = False

    # Auto-restart if main.py is dead
    proc_ok = results[0][1]
    if not proc_ok and "dead" in results[0][2].lower():
        _restart_main()

    if all_ok:
        logger.info("STATUS: ALL SYSTEMS HEALTHY")
    else:
        logger.warning("STATUS: ISSUES DETECTED — review alerts above")
    logger.info("")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("EdgeLab Overnight Monitor starting — checks every 5 minutes")
    logger.info("Alerts → %s", ALERT_LOG)
    logger.info("Max auto-restarts: %d", MAX_RESTARTS)

    # Run first check immediately
    run_health_check()

    # Schedule recurring checks
    scheduler = BlockingScheduler()
    scheduler.add_job(run_health_check, "interval", minutes=5, id="health_check")

    def _shutdown(signum, frame):
        logger.info("Shutting down overnight monitor.")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Overnight monitor stopped.")


if __name__ == "__main__":
    main()
