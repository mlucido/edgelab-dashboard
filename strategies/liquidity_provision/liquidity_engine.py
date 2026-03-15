"""
Liquidity Engine — main orchestrator for Strategy 7 (Liquidity Provision).
Runs in PAPER mode by default. Requires LP_PROMOTE_TO_LIVE=true to go live.

Loop:
  scan_for_spreads() → risk_guard → quote_engine → inventory_manager → log_trade()

Circuit breaker: if session inventory loss > -$5 → pause 2 hours.
"""
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add EdgeLab root to path for framework imports
_edgelab_root = str(Path(__file__).parent.parent.parent)
sys.path.insert(0, _edgelab_root)

from . import config
from .spread_scanner import scan_for_spreads
from .inventory_manager import InventoryManager
from .quote_engine import QuoteEngine
from .risk_guard import RiskGuard

logger = logging.getLogger("liquidity_provision.engine")

_log_dir = Path(__file__).parent.parent.parent / "logs"
_log_dir.mkdir(parents=True, exist_ok=True)
_fh = logging.FileHandler(_log_dir / "liquidity_provision.log")
_fh.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
logger.addHandler(_fh)
_ch = logging.StreamHandler()
_ch.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(message)s"))
logger.addHandler(_ch)
logger.setLevel(logging.INFO)

PAPER_TRADES_PATH = Path(__file__).parent / "sim" / "results" / "paper_trades.json"
CIRCUIT_BREAKER_LOSS_USD = -5.0
CIRCUIT_BREAKER_PAUSE_SECS = 2 * 3600   # 2 hours


class LiquidityEngine:
    def __init__(self):
        self.mode = config.MODE
        if os.environ.get("LP_PROMOTE_TO_LIVE") == "true" and self.mode == "PAPER":
            self.mode = "LIVE"
            logger.info("LP_PROMOTE_TO_LIVE=true — switching to LIVE mode")

        self.inventory = InventoryManager()
        self.quote_engine = QuoteEngine(self.inventory)
        self.risk_guard = RiskGuard(self.inventory)

        self.paper_trades: list = []
        self.session_pnl: float = 0.0
        self.paused_until: float = 0.0
        self.start_time: float = time.time()
        self.cycle_count: int = 0

        self._load_paper_trades()
        logger.info(f"LiquidityEngine initialized in {self.mode} mode")

    # ------------------------------------------------------------------
    # Paper trade persistence
    # ------------------------------------------------------------------

    def _load_paper_trades(self):
        if PAPER_TRADES_PATH.exists():
            try:
                self.paper_trades = json.loads(PAPER_TRADES_PATH.read_text())
                logger.info(f"Loaded {len(self.paper_trades)} existing paper trades")
            except Exception:
                self.paper_trades = []

    def _save_paper_trades(self):
        PAPER_TRADES_PATH.parent.mkdir(parents=True, exist_ok=True)
        try:
            PAPER_TRADES_PATH.write_text(json.dumps(self.paper_trades, indent=2))
        except Exception as e:
            logger.error(f"Failed to save paper trades: {e}")

    # ------------------------------------------------------------------
    # Circuit breaker
    # ------------------------------------------------------------------

    def _check_circuit_breaker(self):
        if self.session_pnl < CIRCUIT_BREAKER_LOSS_USD:
            self.paused_until = time.time() + CIRCUIT_BREAKER_PAUSE_SECS
            logger.warning(
                f"CIRCUIT BREAKER: session PnL ${self.session_pnl:.2f} < "
                f"${CIRCUIT_BREAKER_LOSS_USD:.2f} — pausing {CIRCUIT_BREAKER_PAUSE_SECS//3600}h"
            )

    def _is_paused(self) -> bool:
        if time.time() < self.paused_until:
            remaining = int(self.paused_until - time.time())
            logger.warning(f"Engine paused (circuit breaker): {remaining}s remaining")
            return True
        return False

    # ------------------------------------------------------------------
    # Promotion check
    # ------------------------------------------------------------------

    def _check_promotion(self):
        resolved = [t for t in self.paper_trades if t.get("status") != "OPEN"]
        if len(resolved) < config.PAPER_PROMOTION_MIN_TRADES:
            return

        hours_in_paper = (time.time() - self.start_time) / 3600
        if hours_in_paper < config.PAPER_PROMOTION_MIN_HOURS:
            return

        wins = sum(1 for t in resolved if t.get("pnl", 0.0) > 0)
        win_rate = wins / len(resolved) if resolved else 0
        if win_rate >= config.PAPER_PROMOTION_WIN_RATE:
            logger.info(
                f"PROMOTION ELIGIBLE: {win_rate:.1%} WR over {len(resolved)} trades, "
                f"{hours_in_paper:.1f} hours"
            )

    # ------------------------------------------------------------------
    # Performance logging
    # ------------------------------------------------------------------

    def _log_performance(self):
        resolved = [t for t in self.paper_trades if t.get("status") != "OPEN"]
        total_pnl = sum(t.get("pnl", 0.0) for t in resolved)
        n = len(resolved)
        wins = sum(1 for t in resolved if t.get("pnl", 0.0) > 0)
        wr = wins / n if n > 0 else 0.0
        active = len(self.quote_engine.get_active_quotes())
        inv_risk = self.inventory.get_inventory_risk()
        logger.info(
            f"PERF: {len(self.paper_trades)} total | {n} resolved | WR={wr:.1%} | "
            f"PnL=${total_pnl:.2f} | session=${self.session_pnl:.2f} | "
            f"active_quotes={active} | inv_risk=${inv_risk:.2f}"
        )

    # ------------------------------------------------------------------
    # Core cycle
    # ------------------------------------------------------------------

    def _process_opportunity(self, opp):
        """Process a single spread opportunity through the full pipeline."""
        hours_remaining = opp.days_to_resolution * 24
        mid = round((opp.best_bid + opp.best_ask) / 2, 4)

        # Risk check
        allowed, reason = self.risk_guard.pre_quote_check(
            market_id=opp.market_id,
            current_mid=mid,
            hours_remaining=hours_remaining,
            proposed_size_usd=config.MAX_POSITION_USD,
        )
        if not allowed:
            logger.debug(f"Skip {opp.market_id}: {reason}")
            return

        # Check if existing quote needs refresh
        quote = self.quote_engine.get_active_quotes().get(opp.market_id)
        if quote and not self.quote_engine.should_refresh_quote(
            opp.market_id, opp.best_bid, opp.best_ask
        ):
            # Still valid — just check for simulated fills
            fills = self.quote_engine.simulate_fills(opp.market_id)
            self._handle_fills(fills)
            return

        # Cancel stale quote if exists
        if quote:
            self.quote_engine.cancel_quote(opp.market_id)

        # Post new quote
        new_quote = self.quote_engine.post_quotes(opp.market_id, opp.best_bid, opp.best_ask)
        if not new_quote:
            return

        # Immediately attempt fill simulation (probabilistic)
        fills = self.quote_engine.simulate_fills(opp.market_id)
        self._handle_fills(fills)

    def _handle_fills(self, fills: list):
        """Record fills as paper trades and update session PnL."""
        if not fills:
            return

        try:
            from framework.trade_journal import log_trade
        except Exception:
            log_trade = None

        for fill in fills:
            trade = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "market_id": fill.market_id,
                "side": fill.side,
                "price": fill.price,
                "size_usd": fill.size_usd,
                "pnl_if_pair_fills": fill.pnl_if_pair_fills,
                "status": "OPEN",
                "pnl": 0.0,
            }
            self.paper_trades.append(trade)

            # Estimate realized PnL: if we filled both sides we captured the spread
            pos = self.inventory.get_position(fill.market_id)
            if pos and pos["yes_inventory"] > 0 and pos["no_inventory"] > 0:
                spread_capture = fill.pnl_if_pair_fills * min(
                    pos["yes_inventory"], pos["no_inventory"]
                )
                realized = round(spread_capture, 4)
                trade["pnl"] = realized
                trade["status"] = "CLOSED"
                self.session_pnl += realized
                logger.info(
                    f"SPREAD CAPTURED: {fill.market_id} "
                    f"YES@{pos['avg_entry_yes']:.3f} / NO@{pos['avg_entry_no']:.3f} "
                    f"capture=${realized:.4f}"
                )

            # Log to framework trade journal
            if log_trade:
                try:
                    log_trade(
                        strategy_name="liquidity_provision",
                        market_id=fill.market_id,
                        platform="kalshi",
                        side=fill.side,
                        price=fill.price,
                        size=fill.size_usd,
                        edge_pct=fill.pnl_if_pair_fills,
                        signal_strength="LOW",
                        market_conditions={"mode": self.mode},
                        notes=f"LP fill {fill.side}",
                    )
                except Exception as e:
                    logger.error(f"Failed to log trade to journal: {e}")

        self._save_paper_trades()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self):
        logger.info(f"Starting LiquidityEngine in {self.mode} mode")
        while True:
            try:
                if self._is_paused():
                    time.sleep(60)
                    continue

                self.cycle_count += 1
                opportunities = scan_for_spreads()

                for opp in opportunities[:20]:   # top 20 by lp_score
                    try:
                        self._process_opportunity(opp)
                    except Exception as e:
                        logger.error(f"Error processing {opp.market_id}: {e}")

                # Every 10 cycles: check circuit breaker, log performance, check promotion
                if self.cycle_count % 10 == 0:
                    self._check_circuit_breaker()
                    self._log_performance()
                    if self.mode == "PAPER":
                        self._check_promotion()

            except Exception as e:
                logger.error(f"Engine cycle error: {e}")

            time.sleep(config.SCAN_INTERVAL_SECS)


def main():
    engine = LiquidityEngine()
    engine.run()


if __name__ == "__main__":
    main()
