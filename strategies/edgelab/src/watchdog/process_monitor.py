"""
EdgeLab Process Monitor — Auto-restart watchdog for main.py.

Monitors that main.py is running via PID file at logs/edgelab.pid.
Restarts automatically on crash, with a rate limiter that halts after
3 restarts in 10 minutes and publishes a BOT_DOWN alert to Redis.

Run as:
    python -m src.watchdog.process_monitor
    python src/watchdog/process_monitor.py
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import redis.asyncio as aioredis

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
PID_FILE = Path("logs/edgelab.pid")
WATCHDOG_LOG = Path("logs/watchdog.log")
MAIN_SCRIPT = "main.py"
POLL_INTERVAL = 10          # seconds between liveness checks
RESTART_DELAY = 10          # seconds to wait before restarting
RESTART_WINDOW = 600        # 10 minutes in seconds
MAX_RESTARTS_IN_WINDOW = 3  # trip threshold

# ---------------------------------------------------------------------------
# Logging — writes to both stdout and logs/watchdog.log
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("edgelab.watchdog")

_file_handler: logging.FileHandler | None = None


def _ensure_log_handler() -> None:
    global _file_handler
    if _file_handler is not None:
        return
    WATCHDOG_LOG.parent.mkdir(parents=True, exist_ok=True)
    _file_handler = logging.FileHandler(WATCHDOG_LOG)
    _file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    logger.addHandler(_file_handler)


# ---------------------------------------------------------------------------
# PID helpers
# ---------------------------------------------------------------------------

def start_monitored_process() -> subprocess.Popen:
    """Start main.py as a subprocess and write its PID to logs/edgelab.pid."""
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(
        [sys.executable, MAIN_SCRIPT],
        cwd=Path(__file__).resolve().parents[2],  # repo root
    )
    PID_FILE.write_text(str(proc.pid))
    logger.info("Started main.py — PID %d written to %s", proc.pid, PID_FILE)
    return proc


def check_process(pid: int) -> bool:
    """Return True if the process with the given PID is alive."""
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def _read_pid() -> int | None:
    """Read and return the PID from the PID file, or None if unavailable."""
    try:
        raw = PID_FILE.read_text().strip()
        return int(raw) if raw else None
    except (FileNotFoundError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------

async def _publish_alert(event: str, details: dict | None = None) -> None:
    """Fire-and-forget Redis alert publish."""
    try:
        r = await aioredis.from_url(REDIS_URL, decode_responses=True)
        payload = json.dumps({"event": event, "details": details or {}, "ts": time.time()})
        await r.publish("alerts", payload)
        await r.aclose()
    except Exception as exc:
        logger.warning("Could not publish alert to Redis: %s", exc)


# ---------------------------------------------------------------------------
# Main watchdog loop
# ---------------------------------------------------------------------------

class RestartTracker:
    """Tracks restarts within a rolling time window."""

    def __init__(self, window: int = RESTART_WINDOW, max_restarts: int = MAX_RESTARTS_IN_WINDOW):
        self._window = window
        self._max = max_restarts
        self._times: list[float] = []

    def record(self) -> None:
        now = time.time()
        self._times.append(now)
        self._times = [t for t in self._times if now - t <= self._window]

    def exceeded(self) -> bool:
        now = time.time()
        recent = [t for t in self._times if now - t <= self._window]
        return len(recent) >= self._max

    def count(self) -> int:
        now = time.time()
        return len([t for t in self._times if now - t <= self._window])


async def run_watchdog() -> None:
    """Main watchdog loop — polls every POLL_INTERVAL seconds."""
    _ensure_log_handler()
    tracker = RestartTracker()
    halted = False

    logger.info("Process monitor started. Watching for %s via %s", MAIN_SCRIPT, PID_FILE)

    while True:
        await asyncio.sleep(POLL_INTERVAL)

        if halted:
            # Already published BOT_DOWN — keep sleeping but don't restart
            continue

        pid = _read_pid()

        if pid is None:
            logger.warning("PID file missing or unreadable — process may not have started yet.")
            continue

        if check_process(pid):
            # All good
            continue

        # Process is dead
        logger.warning("Process PID %d is no longer alive.", pid)

        if tracker.exceeded():
            msg = (
                f"Bot has crashed {tracker.count()} times in the last "
                f"{RESTART_WINDOW // 60} minutes — halting auto-restart."
            )
            logger.critical("WATCHDOG HALT: %s", msg)
            halted = True
            await _publish_alert("BOT_DOWN", {"reason": msg, "restart_count": tracker.count()})
            continue

        # Wait before restarting
        logger.info("Waiting %ds before restarting...", RESTART_DELAY)
        await asyncio.sleep(RESTART_DELAY)

        tracker.record()
        logger.info(
            "Restarting main.py (attempt %d in window)...", tracker.count()
        )
        try:
            start_monitored_process()
            await _publish_alert(
                "BOT_RESTARTED",
                {"attempt": tracker.count(), "pid": _read_pid()},
            )
        except Exception as exc:
            logger.error("Failed to restart main.py: %s", exc)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def main() -> None:
    # Graceful shutdown on SIGINT/SIGTERM
    loop = asyncio.new_event_loop()

    def _shutdown(sig: signal.Signals) -> None:
        logger.info("Watchdog received %s — shutting down.", sig.name)
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    try:
        loop.run_until_complete(run_watchdog())
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()
