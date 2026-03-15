"""
Strategy Coordinator — master coordinator preventing strategy conflicts.

Runs a 60-second cycle:
  1. Read active signals from all strategies via trade_journal
  2. Build "hot markets" set — markets with active signals from ANY strategy
  3. Write hot_markets to framework/hot_markets.json
  4. Rule: if market is hot AND requester is liquidity_provision → block
  5. Daily capital rebalancing log at midnight UTC
"""

import json
import logging
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("framework.strategy_coordinator")

BASE_DIR = Path(__file__).resolve().parent.parent
HOT_MARKETS_PATH = Path(__file__).parent / "hot_markets.json"
ALLOCATION_LOG_PATH = Path(__file__).parent / "allocation_log.json"

# Directional strategies that take priority over LP
DIRECTIONAL_STRATEGIES = {"news_arb", "sports_momentum", "resolution_lag", "funding_rate_arb"}

# Configure file logging
log_dir = BASE_DIR / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
handler = logging.FileHandler(log_dir / "strategy_coordinator.log")
handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class StrategyCoordinator:
    """Master strategy coordinator. Prevents conflicts between strategies."""

    def __init__(self):
        self._hot_markets = {}
        self._last_daily_log_date = None
        self._running = False
        self._lock = threading.Lock()

    def run(self):
        """Main loop — runs forever (intended for daemon thread)."""
        self._running = True
        logger.info("Strategy coordinator started")
        while self._running:
            try:
                self.run_cycle()
            except Exception as e:
                logger.error(f"Coordinator cycle error: {e}")
            time.sleep(60)

    def stop(self):
        self._running = False

    def run_cycle(self):
        """Single coordinator cycle. Called every 60s or from learning_loop."""
        self._update_hot_markets()
        self._check_daily_rebalance()

    def _update_hot_markets(self):
        """Read active trades/signals from all strategies, build hot markets set."""
        from .trade_journal import get_recent_trades

        hot = {}
        try:
            # Get recent trades from all directional strategies (last 100)
            trades = get_recent_trades(limit=200)
            for t in trades:
                strategy = t.get("strategy_name", "")
                if strategy not in DIRECTIONAL_STRATEGIES:
                    continue

                market_id = t.get("market_id", "")
                if not market_id:
                    continue

                # Only consider trades that are still OPEN or very recent (within 4 hours)
                outcome = t.get("outcome")
                ts_str = t.get("timestamp", "")
                if outcome and outcome != "OPEN":
                    # Check if trade resolved recently (within last hour — still hot)
                    try:
                        trade_time = datetime.fromisoformat(ts_str)
                        age_hours = (datetime.now(timezone.utc) - trade_time).total_seconds() / 3600
                        if age_hours > 1:
                            continue
                    except (ValueError, TypeError):
                        continue

                # Determine direction from trade side
                side = t.get("side", "")
                direction = "LONG" if side.upper() in ("YES", "BUY", "LONG") else "SHORT"

                hot[market_id] = {
                    "strategy": strategy,
                    "signal_time": ts_str,
                    "direction": direction,
                }

            with self._lock:
                self._hot_markets = hot

            # Write to shared file
            try:
                HOT_MARKETS_PATH.write_text(json.dumps(hot, indent=2))
            except Exception as e:
                logger.warning(f"Failed to write hot_markets.json: {e}")

            if hot:
                logger.info(f"Hot markets updated: {len(hot)} active from directional strategies")

        except Exception as e:
            logger.error(f"Failed to update hot markets: {e}")

    def _check_daily_rebalance(self):
        """At midnight UTC, log capital allocation state."""
        now = datetime.now(timezone.utc)
        if now.hour == 0 and self._last_daily_log_date != now.date():
            self._last_daily_log_date = now.date()
            self._log_daily_allocation()

    def _log_daily_allocation(self):
        """Log current capital deployment per strategy."""
        try:
            from .capital_allocator import compute_allocations
            allocations = compute_allocations()

            log_entry = {
                "date": datetime.now(timezone.utc).date().isoformat(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "allocations": allocations,
                "hot_markets_count": len(self._hot_markets),
            }

            # Append to allocation log
            log_data = []
            if ALLOCATION_LOG_PATH.exists():
                try:
                    log_data = json.loads(ALLOCATION_LOG_PATH.read_text())
                except (json.JSONDecodeError, Exception):
                    log_data = []

            log_data.append(log_entry)
            # Keep last 90 days
            log_data = log_data[-90:]
            ALLOCATION_LOG_PATH.write_text(json.dumps(log_data, indent=2))

            logger.info(f"Daily allocation log written: {len(allocations)} strategies")
            for a in allocations:
                logger.info(f"  {a['strategy']}: {a['allocated_pct']*100:.1f}% (${a['allocated_usd']:.2f})")

        except Exception as e:
            logger.error(f"Daily allocation log failed: {e}")

    def is_market_hot(self, market_id: str) -> bool:
        """Check if a market has active directional signals."""
        with self._lock:
            return market_id in self._hot_markets

    def get_hot_markets(self) -> dict:
        """Return current hot markets dict."""
        with self._lock:
            return dict(self._hot_markets)

    def should_block_lp(self, market_id: str) -> bool:
        """
        Check if LP should be blocked for a market.
        Returns True if directional strategy has priority.
        """
        with self._lock:
            if market_id in self._hot_markets:
                signal = self._hot_markets[market_id]
                logger.info(
                    f"LP blocked on {market_id} — "
                    f"active signal from {signal['strategy']} ({signal['direction']})"
                )
                return True
        return False


def load_hot_markets() -> dict:
    """
    Read hot_markets.json from disk.
    Any strategy can call this to check for conflicts.
    Returns empty dict if file missing or corrupt.
    """
    try:
        if HOT_MARKETS_PATH.exists():
            return json.loads(HOT_MARKETS_PATH.read_text())
    except (json.JSONDecodeError, Exception):
        pass
    return {}
