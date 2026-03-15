"""EdgeLab — Prediction Market Arbitrage & Edge Detection System.

Async orchestrator that starts all feeds and strategy engines concurrently.
Handles graceful shutdown on SIGINT/SIGTERM.
Optional modules (telegram, execution) are loaded with import guards.
"""

import asyncio
import atexit
import json
import logging
import os
import signal
import sys

from dotenv import load_dotenv

load_dotenv()

os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/edgelab.log", mode="a"),
    ],
)
logger = logging.getLogger("edgelab")


def _try_import(module_path: str):
    """Import a module by dotted path; return None and log a warning on failure."""
    import importlib
    try:
        return importlib.import_module(module_path)
    except ImportError as exc:
        logger.warning("Optional module %s not available: %s — skipping", module_path, exc)
        return None


async def _save_state() -> None:
    """Persist minimal shutdown state to data/state.json."""
    state_path = "data/state.json"
    try:
        state = {
            "shutdown_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
            "pid": os.getpid(),
        }
        with open(state_path, "w") as f:
            json.dump(state, f)
        logger.info("State saved to %s", state_path)
    except Exception as exc:
        logger.warning("Failed to save state: %s", exc)


async def main():
    from src.feeds import polymarket as poly_feed
    from src.feeds import kalshi as kalshi_feed
    from src.feeds import events as event_monitor
    from src.strategies.resolution_lag import ResolutionLagDetector
    # DISABLED: threshold strategy not viable — only 1 trade/30 days at 88c floor
    # from src.strategies.threshold_scanner import ThresholdScanner
    from src.strategies.arb_detector import ArbDetector
    from src.strategies.whale_tracker import WhaleTracker
    from src.calibration.tracker import init_db

    await init_db()

    # Write PID file for overnight_monitor watchdog
    pid_path = os.path.join("logs", "edgelab.pid")
    with open(pid_path, "w") as f:
        f.write(str(os.getpid()))
    logger.info("PID file written: %s (pid=%d)", pid_path, os.getpid())

    def _cleanup_pid():
        try:
            os.remove(pid_path)
            logger.info("PID file removed: %s", pid_path)
        except OSError:
            pass

    atexit.register(_cleanup_pid)

    shutdown_event = asyncio.Event()

    def handle_signal():
        logger.info("Shutdown signal received, stopping...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    resolution_lag = ResolutionLagDetector()
    # threshold_scanner = ThresholdScanner()  # DISABLED
    arb_detector = ArbDetector()
    whale_tracker = WhaleTracker()

    tasks: list[asyncio.Task] = [
        asyncio.create_task(poly_feed.run(), name="polymarket-feed"),
        asyncio.create_task(kalshi_feed.run(), name="kalshi-feed"),
        asyncio.create_task(event_monitor.run(), name="event-monitor"),
        asyncio.create_task(resolution_lag.run(), name="resolution-lag"),
        # asyncio.create_task(threshold_scanner.run(), name="threshold-scanner"),  # DISABLED
        asyncio.create_task(arb_detector.run(), name="arb-detector"),
        asyncio.create_task(whale_tracker.run(), name="whale-tracker"),
    ]

    # Optional: execution engine (autonomous trader, position monitor, circuit breaker)
    for mod_path, task_name, fn_name in [
        ("src.execution.autonomous_trader", "autonomous-trader", "run"),
        ("src.execution.position_monitor", "position-monitor", "run"),
        ("src.execution.circuit_breaker", "circuit-breaker", "watchdog"),
    ]:
        mod = _try_import(mod_path)
        if mod is not None:
            fn = getattr(mod, fn_name, None)
            if fn is not None:
                tasks.append(asyncio.create_task(fn(), name=task_name))
                logger.info("Loaded optional module: %s", mod_path)

    # Optional: Telegram alerts bot
    telegram_mod = _try_import("src.alerts.telegram_bot")
    if telegram_mod is not None:
        fn = getattr(telegram_mod, "run", None)
        if fn is not None:
            tasks.append(asyncio.create_task(fn(), name="telegram-bot"))
            logger.info("Loaded optional module: src.alerts.telegram_bot")

    logger.info("EdgeLab started — %d tasks running", len(tasks))
    logger.info("Dashboard: streamlit run dashboard/app.py")

    await shutdown_event.wait()

    logger.info("Cancelling tasks...")
    for task in tasks:
        task.cancel()

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for task, result in zip(tasks, results):
        if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
            logger.error("Task %s failed: %s", task.get_name(), result)

    await _save_state()
    _cleanup_pid()
    logger.info("EdgeLab shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
