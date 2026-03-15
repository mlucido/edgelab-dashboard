import logging
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("framework.learning_loop")

# Configure file logging
log_dir = Path(__file__).parent.parent / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
handler = logging.FileHandler(log_dir / "learning_loop.log")
handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class LearningLoop:
    """Background orchestrator. Checks every 5 minutes."""

    def __init__(self):
        from .trade_journal import init_db
        init_db()
        self._last_trade_counts = {}
        self._last_alloc_date = None
        self._last_suggest_weekday = None
        self._running = False
        self._coordinator = None

    def run(self):
        """Main loop. Runs forever (intended for daemon thread)."""
        self._running = True
        logger.info("Learning loop started")
        while self._running:
            try:
                self._tick()
            except Exception as e:
                logger.error(f"Learning loop tick error: {e}")
            time.sleep(300)  # 5 minutes

    def stop(self):
        self._running = False

    def _tick(self):
        import sqlite3
        from .performance_analyzer import analyze_strategy, get_all_strategies
        from .capital_allocator import compute_allocations
        from .config_suggester import generate_suggestions
        from .strategy_coordinator import StrategyCoordinator

        # Run strategy coordinator cycle
        if self._coordinator is None:
            self._coordinator = StrategyCoordinator()
        try:
            self._coordinator.run_cycle()
        except Exception as e:
            logger.error(f"Strategy coordinator error: {e}")

        now = datetime.now(timezone.utc)
        strategies = get_all_strategies()

        # Check for 20 new trades per strategy
        db_path = Path(__file__).parent / "learning.db"
        with sqlite3.connect(db_path) as conn:
            for s in strategies:
                count = conn.execute(
                    "SELECT COUNT(*) FROM trades WHERE strategy_name=?", (s,)
                ).fetchone()[0]
                last = self._last_trade_counts.get(s, 0)
                if count - last >= 20:
                    logger.info(f"20 new trades for {s}, running analysis")
                    analyze_strategy(s)
                    self._last_trade_counts[s] = count

        # Daily at 9 AM PST (17:00 UTC)
        if now.hour == 17 and self._last_alloc_date != now.date():
            logger.info("Daily capital allocation run")
            compute_allocations()
            self._last_alloc_date = now.date()

        # Weekly on Sunday
        if now.weekday() == 6 and self._last_suggest_weekday != now.date():
            logger.info("Weekly config suggestions run")
            generate_suggestions()
            self._last_suggest_weekday = now.date()
