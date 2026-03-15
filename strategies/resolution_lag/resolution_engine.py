"""
Resolution Lag Engine — main orchestrator.

Pipeline every SCAN_INTERVAL_SECS:
  1. detect_resolved_events()       ← resolution_detector
  2. score_certainty(event, market) ← certainty_scorer
  3. scan_for_lag(event, certainty) ← lag_scanner
  4. paper trade + persist + log

Safety:
  - Circuit breaker: 1hr pause if daily loss > -$10
  - Position timeout: if open > MAX_LAG_WINDOW_SECS → close as TIMEOUT (LOSS)
  - Promotion check every 10 cycles

MODE = PAPER (override via env RESOLUTION_LAG_MODE)
"""
import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from . import config
from .resolution_detector import detect_resolved_events
from .certainty_scorer import score_certainty
from .lag_scanner import scan_for_lag

logger = logging.getLogger("resolution_lag.engine")

# ── Logging setup ─────────────────────────────────────────────────────────────
log_dir = Path(__file__).parent.parent.parent / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
_file_handler = logging.FileHandler(log_dir / "resolution_lag.log")
_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
))
logger.addHandler(_file_handler)
_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter(
    "%(asctime)s [%(name)s] %(message)s"
))
logger.addHandler(_console)
logger.setLevel(logging.INFO)

PAPER_TRADES_PATH = Path(__file__).parent / "sim" / "results" / "paper_trades.json"


class ResolutionLagEngine:
    def __init__(self):
        self.mode = config.MODE
        # Honor live promotion env var
        if os.environ.get("RESOLUTION_LAG_PROMOTE_TO_LIVE") == "true" and self.mode == "PAPER":
            self.mode = "LIVE"

        self.paper_trades: list[dict] = []
        self.paper_capital = config.STARTING_CAPITAL_USD
        self.paused_until: float = 0.0
        self.reduced_size: bool = False
        self.consecutive_losses: int = 0
        self.start_time = time.time()
        self._load_paper_trades()

        logger.info(f"ResolutionLagEngine initialized in {self.mode} mode | "
                    f"{len(self.paper_trades)} existing paper trades loaded")

    # ── Persistence ────────────────────────────────────────────────────────────

    def _load_paper_trades(self):
        if PAPER_TRADES_PATH.exists():
            try:
                self.paper_trades = json.loads(PAPER_TRADES_PATH.read_text())
            except Exception as e:
                logger.warning(f"Could not load paper trades: {e}")
                self.paper_trades = []

    def _save_paper_trades(self):
        PAPER_TRADES_PATH.parent.mkdir(parents=True, exist_ok=True)
        try:
            PAPER_TRADES_PATH.write_text(json.dumps(self.paper_trades, indent=2))
        except Exception as e:
            logger.error(f"Failed to save paper trades: {e}")

    # ── Paper trading ──────────────────────────────────────────────────────────

    def _paper_trade(self, opportunity: dict) -> None:
        """Record a paper trade for a lag opportunity."""
        if time.time() < self.paused_until:
            remaining = self.paused_until - time.time()
            logger.warning(f"Engine paused by circuit breaker ({remaining:.0f}s remaining)")
            return

        position_size = config.MAX_POSITION_USD
        if self.reduced_size:
            position_size *= 0.5

        side = opportunity["expected_resolution"]
        fill_price = opportunity["current_price"]
        # Slippage: buying YES add cost, buying NO is 1 - current + slippage
        if side == "YES":
            fill_price = min(fill_price + config.ESTIMATED_SLIPPAGE_PCT, 0.99)
        else:
            fill_price = max(fill_price - config.ESTIMATED_SLIPPAGE_PCT, 0.01)

        trade = {
            "trade_id": f"rl_{int(time.time() * 1000)}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "market_id": opportunity["market_id"],
            "platform": opportunity["platform"],
            "side": side,
            "fill_price": round(fill_price, 4),
            "position_size": position_size,
            "edge_pct": opportunity["edge_pct"],
            "certainty_score": opportunity["certainty_score"],
            "urgency": opportunity["urgency"],
            "time_since_resolution": opportunity["time_since_resolution"],
            "source_method": opportunity.get("source_method", "unknown"),
            "market_title": opportunity.get("market_title", "")[:100],
            "event_description": opportunity.get("event_description", "")[:100],
            "status": "OPEN",
            "pnl": 0.0,
            "opened_at_epoch": time.time(),
        }

        self.paper_trades.append(trade)
        self._save_paper_trades()

        logger.info(
            f"PAPER TRADE: {side} {opportunity['market_id']} @ {fill_price:.3f} "
            f"size=${position_size} | edge={opportunity['edge_pct']:.1%} "
            f"certainty={opportunity['certainty_score']:.2f} urgency={opportunity['urgency']}"
        )

        # Also log to framework trade journal
        try:
            from framework.trade_journal import log_trade
            log_trade(
                strategy_name="resolution_lag",
                market_id=opportunity["market_id"],
                platform=opportunity["platform"],
                side=side,
                price=fill_price,
                size=position_size,
                edge_pct=opportunity["edge_pct"],
                signal_strength=opportunity["urgency"],
                market_conditions={
                    "certainty_score": opportunity["certainty_score"],
                    "time_since_resolution": opportunity["time_since_resolution"],
                    "source_method": opportunity.get("source_method", "unknown"),
                },
                notes=opportunity.get("event_description", "")[:200],
            )
        except Exception as e:
            logger.warning(f"Failed to log to framework trade journal: {e}")

    # ── Position management ────────────────────────────────────────────────────

    def _expire_stale_positions(self) -> None:
        """
        Close any OPEN positions that have been open longer than MAX_LAG_WINDOW_SECS.
        These count as TIMEOUT (LOSS) — the lag opportunity has expired.
        """
        now = time.time()
        changed = False

        for trade in self.paper_trades:
            if trade.get("status") != "OPEN":
                continue

            opened_at = trade.get("opened_at_epoch", now)
            age_secs = now - opened_at

            if age_secs > config.MAX_LAG_WINDOW_SECS:
                # Estimate loss: we paid fill_price, market didn't move in 30min
                fill_price = trade.get("fill_price", 0.5)
                position_size = trade.get("position_size", config.MAX_POSITION_USD)
                # Conservative: assume price moved 50% of the way to expected but
                # we exit at a small loss (missed the window)
                exit_price = fill_price - 0.05  # modest adverse move
                pnl = (exit_price - fill_price) * position_size
                pnl = max(pnl, -position_size * 0.15)  # cap loss at 15%

                trade["status"] = "TIMEOUT"
                trade["pnl"] = round(pnl, 2)
                trade["closed_at"] = datetime.now(timezone.utc).isoformat()
                changed = True

                logger.warning(
                    f"TIMEOUT: {trade['market_id']} opened {age_secs:.0f}s ago — "
                    f"closed as LOSS pnl=${pnl:.2f}"
                )

        if changed:
            self._save_paper_trades()

    # ── Circuit breaker ────────────────────────────────────────────────────────

    def _check_circuit_breaker(self) -> None:
        today = datetime.now(timezone.utc).date().isoformat()
        today_pnl = sum(
            t.get("pnl", 0) for t in self.paper_trades
            if t.get("timestamp", "")[:10] == today and t.get("status") != "OPEN"
        )

        if today_pnl < -10.0:
            self.paused_until = time.time() + 3600
            logger.warning(
                f"CIRCUIT BREAKER: daily PnL=${today_pnl:.2f} — pausing 1 hour"
            )

        # 5 consecutive closed losses → half size
        recent_closed = [
            t for t in self.paper_trades
            if t.get("status") not in ("OPEN",)
        ][-5:]
        if len(recent_closed) == 5 and all(t.get("pnl", 0) < 0 for t in recent_closed):
            self.reduced_size = True
            logger.warning("5 consecutive losses — reducing position size 50%")
        else:
            self.reduced_size = False

    # ── Performance logging ───────────────────────────────────────────────────

    def _log_performance(self) -> None:
        closed = [t for t in self.paper_trades if t.get("status") not in ("OPEN",)]
        n = len(closed)
        if n == 0:
            logger.info(f"PAPER PERF: {len(self.paper_trades)} open, 0 resolved")
            return

        wins = sum(1 for t in closed if t.get("pnl", 0) > 0)
        total_pnl = sum(t.get("pnl", 0) for t in closed)
        wr = wins / n

        logger.info(
            f"PAPER PERF: {len(self.paper_trades)} total | {n} resolved | "
            f"WR={wr:.1%} | PnL=${total_pnl:.2f} | "
            f"reduced_size={self.reduced_size}"
        )

    # ── Promotion check ───────────────────────────────────────────────────────

    def _check_promotion(self) -> bool:
        closed = [t for t in self.paper_trades if t.get("status") not in ("OPEN",)]
        if len(closed) < config.PAPER_PROMOTION_MIN_TRADES:
            return False

        hours_running = (time.time() - self.start_time) / 3600
        if hours_running < config.PAPER_PROMOTION_MIN_HOURS:
            return False

        wins = sum(1 for t in closed if t.get("pnl", 0) > 0)
        win_rate = wins / len(closed)

        if win_rate >= config.PAPER_PROMOTION_WIN_RATE:
            logger.info(
                f"PROMOTION ELIGIBLE: WR={win_rate:.1%} over {len(closed)} trades, "
                f"{hours_running:.1f}h running — consider LIVE deployment"
            )
            return True

        return False

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run(self) -> None:
        logger.info(f"Starting ResolutionLagEngine in {self.mode} mode "
                    f"(scan every {config.SCAN_INTERVAL_SECS}s)")
        cycle = 0

        while True:
            try:
                cycle += 1
                logger.debug(f"Cycle {cycle} starting")

                # Step 1: Detect resolved events
                try:
                    resolved_events = detect_resolved_events()
                except Exception as e:
                    logger.error(f"detect_resolved_events failed: {e}")
                    resolved_events = []

                # Step 2 + 3: Score certainty → scan for lag
                for event in resolved_events:
                    try:
                        certainty = score_certainty(event)
                        if not certainty["passes_threshold"]:
                            continue
                    except Exception as e:
                        logger.error(f"score_certainty failed: {e}")
                        continue

                    try:
                        opportunities = scan_for_lag(event, certainty)
                    except Exception as e:
                        logger.error(f"scan_for_lag failed: {e}")
                        opportunities = []

                    # Step 4: Trade
                    for opp in opportunities[:3]:  # Max 3 trades per event
                        if self.mode == "PAPER":
                            self._paper_trade(opp)
                        # LIVE mode: would execute via Kalshi executor (not implemented here)
                        # elif self.mode == "LIVE":
                        #     self._live_trade(opp)

                # Expire stale positions
                try:
                    self._expire_stale_positions()
                except Exception as e:
                    logger.error(f"expire_stale_positions failed: {e}")

                # Every 10 cycles: housekeeping
                if cycle % 10 == 0:
                    try:
                        self._check_circuit_breaker()
                        self._log_performance()
                        if self.mode == "PAPER":
                            self._check_promotion()
                    except Exception as e:
                        logger.error(f"Housekeeping failed: {e}")

            except Exception as e:
                logger.error(f"Engine cycle {cycle} unhandled error: {e}")

            time.sleep(config.SCAN_INTERVAL_SECS)


def main():
    engine = ResolutionLagEngine()
    engine.run()


if __name__ == "__main__":
    main()
