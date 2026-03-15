"""
live_engine.py — Live trading execution layer.

Mirrors sim_engine.py exactly, but replaces simulated fills with real
Kalshi order execution via KalshiExecutor.

Safety: defaults to DRY_RUN=true. Set DRY_RUN=false to trade real money.
"""

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiohttp

# Wire into EdgeLab self-learning framework
sys.path.insert(0, os.path.expanduser('~/Dropbox/EdgeLab'))
try:
    from framework.trade_journal import log_trade as _fw_log_trade
    _FW_AVAILABLE = True
except Exception:
    _FW_AVAILABLE = False

import config
import database as db
from data_ingestor import DataIngestor, get_state as get_price_state
from kalshi_client import KalshiMarket, fetch_crypto_markets, get_market_snapshot
from risk_manager import GateResult, check_all_gates, check_exit_conditions
from signal_engine import Signal, compute_kelly_size, evaluate

log = logging.getLogger(__name__)

# ── Live mode toggle ──────────────────────────────────────────────────────
LIVE_MODE = os.environ.get("DRY_RUN", "true").lower() != "true"

# How often to scan for new markets (seconds)
MARKET_SCAN_INTERVAL = 60

# How often to check open positions for exit conditions (seconds)
POSITION_CHECK_INTERVAL = 15

# How often to evaluate new signals (seconds)
SIGNAL_EVAL_INTERVAL = 5

# How often to sync bankroll with Kalshi balance (seconds)
BANKROLL_SYNC_INTERVAL = 300  # 5 minutes


class LiveEngine:
    """
    Live trading engine. Mirrors SimEngine but executes real orders.

    Usage:
        engine = LiveEngine()
        await engine.run()
    """

    def __init__(self):
        self.ingestor       = DataIngestor()
        self._live_markets: List[KalshiMarket] = []
        self._session:      Optional[aiohttp.ClientSession] = None
        self._stop          = asyncio.Event()
        self.open_orders:   Dict[int, str] = {}  # {trade_id: order_id}
        self._last_trade_time: Dict[str, float] = {}  # per-asset cooldown tracker

        # Lazy import — kalshi_executor.py will exist when integrated
        from kalshi_executor import KalshiExecutor
        self.executor = KalshiExecutor()

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def start(self):
        self._session = aiohttp.ClientSession()
        await self.ingestor.start()

        if LIVE_MODE:
            log.warning("=" * 60)
            log.warning("⚠️  LIVE TRADING MODE - REAL MONEY AT RISK ⚠️")
            log.warning("=" * 60)
        else:
            log.info("LiveEngine started in DRY RUN mode (no real orders)")

        log.info("Waiting for price feeds to warm up...")
        for i in range(12, 0, -1):
            await asyncio.sleep(5)
            log.info("Warming up... %ds remaining", i * 5)

        # Sync bankroll with Kalshi on startup
        await self._sync_bankroll()

    async def stop(self):
        self._stop.set()
        await self.ingestor.stop()
        if self._session:
            await self._session.close()
        log.info("LiveEngine stopped.")

    # ── Main loop ──────────────────────────────────────────────────────────

    async def run(self):
        await self.start()

        tasks = [
            asyncio.create_task(self._market_scan_loop(),    name="market-scan"),
            asyncio.create_task(self._signal_loop(),         name="signal-eval"),
            asyncio.create_task(self._position_monitor(),    name="position-monitor"),
            asyncio.create_task(self._daily_reset_loop(),    name="daily-reset"),
            asyncio.create_task(self._bankroll_sync_loop(),  name="bankroll-sync"),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            for t in tasks:
                t.cancel()
            await self.stop()

    # ── Market scanner ─────────────────────────────────────────────────────

    async def _market_scan_loop(self):
        """Refresh the list of live Kalshi markets periodically."""
        while not self._stop.is_set():
            try:
                markets = await fetch_crypto_markets(self._session)
                self._live_markets = markets
                log.debug("Market scan: %d markets found", len(markets))
            except Exception as e:
                log.error("Market scan error: %s", e)
            await asyncio.sleep(MARKET_SCAN_INTERVAL)

    # ── Signal evaluation loop ─────────────────────────────────────────────

    async def _signal_loop(self):
        """Evaluate signals for all configured assets."""
        while not self._stop.is_set():
            try:
                if not db.is_halted():
                    for asset in config.ASSETS:
                        await self._evaluate_asset(asset)
            except Exception as e:
                log.error("Signal loop error: %s", e)
            await asyncio.sleep(SIGNAL_EVAL_INTERVAL)

    async def _evaluate_asset(self, asset: str):
        if not self.ingestor.is_ready(asset):
            return

        spot_price    = self.ingestor.get_price(asset)
        volume_ratio  = self.ingestor.get_volume_ratio(asset)

        # ── Dual momentum windows: 45s (primary) and 90s (secondary) ─────
        momentum_45 = self.ingestor.get_momentum(asset, window_secs=config.MOMENTUM_WINDOW_SECS)
        momentum_90 = self.ingestor.get_momentum(asset, window_secs=config.MOMENTUM_WINDOW_90_SECS)

        if momentum_45 is None and momentum_90 is None:
            return

        # Periodic momentum telemetry (every ~30s per asset)
        import time
        if not hasattr(self, '_last_telemetry'):
            self._last_telemetry = {}
        last = self._last_telemetry.get(asset, 0)
        if time.time() - last > 30:
            self._last_telemetry[asset] = time.time()
            mom_45_str = f"{momentum_45:+.4f}%" if momentum_45 is not None else "N/A"
            mom_90_str = f"{momentum_90:+.4f}%" if momentum_90 is not None else "N/A"
            log.info(
                "[%s] spot=$%.2f mom_45=%s mom_90=%s vol_r=%.2f markets=%d",
                asset, spot_price, mom_45_str, mom_90_str, volume_ratio, len(self._live_markets),
            )

        # Try both windows — pick the stronger signal, or whichever fires
        signal = None
        for mom_pct, tag in [
            (momentum_45, "window_45s"),
            (momentum_90, "window_90s"),
        ]:
            if mom_pct is None:
                continue
            candidate = evaluate(
                asset        = asset,
                spot_price   = spot_price,
                momentum_pct = mom_pct,
                volume_ratio = volume_ratio,
                markets      = self._live_markets,
                window_tag   = tag,
            )
            if candidate is not None:
                if signal is None or abs(candidate.momentum_pct) > abs(signal.momentum_pct):
                    signal = candidate

        if signal is None:
            return

        # Compute Kelly size
        bankroll    = db.get_bankroll()
        kelly_size  = compute_kelly_size(
            signal.implied_prob,
            signal.market_price,
            bankroll
        )

        # ── Weak signal sizing: half position, capped at $20 ─────────────
        if signal.signal_tier == "WEAK_SIGNAL":
            kelly_size = min(kelly_size * 0.5, config.WEAK_SIGNAL_MAX_SIZE)
            log.info("[%s] WEAK_SIGNAL tier: position capped to $%.2f", asset, kelly_size)

        # Run through all 6 gates
        gate_result: GateResult = check_all_gates(signal, kelly_size)

        # Log signal regardless of gate outcome
        db.log_signal(
            asset           = asset,
            direction       = signal.direction,
            momentum_pct    = signal.momentum_pct,
            volume_ratio    = volume_ratio,
            implied_prob    = signal.implied_prob,
            gate_result     = gate_result.gate if not gate_result.passed else "PASS",
            signal_strength = signal.signal_strength,
        )

        if not gate_result.passed:
            log.info("Signal BLOCKED by %s: %s", gate_result.gate, gate_result.reason)
            return

        # Per-asset cooldown: 60s minimum between trades on the same asset
        now = time.time()
        last = self._last_trade_time.get(asset, 0)
        if now - last < 60:
            log.info("Cooldown active for %s (%.0fs remaining), skipping signal",
                     asset, 60 - (now - last))
            return

        # All gates passed — open live trade
        final_size = gate_result.adjusted_size or kelly_size
        log.info(
            "[%s] %s | %s | mom=%+.4f%% | size=$%.2f",
            asset, signal.signal_tier, signal.window_tag, signal.momentum_pct, final_size,
        )
        self._last_trade_time[asset] = now
        await self._open_trade(signal, final_size, gate_result)

    # ── Trade execution ────────────────────────────────────────────────────

    async def _open_trade(self, signal: Signal, size: float, gate: GateResult):
        order_id = None

        if LIVE_MODE:
            try:
                order_id = await self.executor.place_order(signal, size)
                # Defensive: ensure order_id is a string UUID, not a raw API dict
                if isinstance(order_id, dict):
                    order_id = order_id.get('order_id') or order_id.get('id') or str(order_id)
            except Exception as e:
                log.error(
                    "LIVE ORDER FAILED: %s %s $%.2f — %s",
                    signal.asset, signal.direction, size, e
                )
                return  # Do NOT log to DB on order failure
        else:
            order_id = f"DRY-{int(time.time() * 1000)}"

        # Order placed (or dry run) — log to DB using same sim_trade functions
        bankroll = db.get_bankroll()
        trade_id = db.open_sim_trade(
            asset          = signal.asset,
            direction      = signal.direction,
            contract_side  = signal.contract_side,
            condition_id   = signal.best_market.condition_id,
            question       = signal.best_market.question,
            entry_price    = signal.market_price,
            position_size  = size,
            implied_prob   = signal.implied_prob,
            market_prob    = signal.market_price,
            edge_pct       = signal.edge_pct,
            kelly_fraction = config.KELLY_FRACTION,
            spot_price     = signal.spot_price,
            momentum_pct   = signal.momentum_pct,
            volume_ratio   = signal.volume_ratio,
            bankroll       = bankroll,
        )

        self.open_orders[trade_id] = order_id

        # Log to self-learning framework
        if _FW_AVAILABLE:
            try:
                _fw_log_trade(
                    strategy_name="kalshi_crypto_momentum",
                    market_id=signal.best_market.condition_id,
                    platform="kalshi",
                    side=signal.direction,
                    price=signal.market_price,
                    size=size,
                    edge_pct=signal.edge_pct,
                    signal_strength=signal.signal_strength,
                    market_conditions={"asset": signal.asset, "momentum": signal.momentum_pct},
                )
            except Exception as fw_err:
                log.warning("Framework log_trade failed: %s", fw_err)

        log.info(
            "LIVE ORDER placed: %s %s $%.2f @ %.4f | order_id=%s | trade_id=%d",
            signal.asset, signal.direction, size, signal.market_price,
            order_id, trade_id
        )

    # ── Position monitor ───────────────────────────────────────────────────

    async def _position_monitor(self):
        """Check all open positions for exit conditions."""
        while not self._stop.is_set():
            try:
                open_trades = db.get_open_trades()
                for trade in open_trades:
                    await self._check_trade_exit(trade)
            except Exception as e:
                log.error("Position monitor error: %s", e)
            await asyncio.sleep(POSITION_CHECK_INTERVAL)

    async def _check_trade_exit(self, trade: dict):
        trade_id     = trade["id"]
        order_id     = self.open_orders.get(trade_id)
        # Defensive: skip if order_id is a dict (stale bug) — can't query API with it
        if isinstance(order_id, dict):
            log.error("Corrupt order_id for trade %d (dict instead of str) — skipping", trade_id)
            return
        asset        = trade["asset"]
        direction    = trade["direction"]
        entry_price  = trade["entry_price"]
        contract_side = trade["contract_side"]
        spot_entry   = trade["spot_price_entry"]
        current_spot = self.ingestor.get_price(asset)

        # ── Check real order status from Kalshi ──────────────────────────
        if LIVE_MODE and order_id:
            try:
                status = await self.executor.get_order_status(order_id)
            except Exception as e:
                log.error("Failed to get order status for %s: %s", order_id, e)
                return

            # If the market has resolved on Kalshi
            if status.get("settled"):
                settlement_price = status.get("settlement_price", 0.0)
                if contract_side == "YES":
                    exit_price = settlement_price
                else:
                    exit_price = 1.0 - settlement_price
                exit_reason = "RESOLUTION_WIN" if exit_price >= 0.99 else "RESOLUTION_LOSS"
                self._close_trade(trade, exit_price, exit_reason)
                return

        # ── Early exit checks (same logic as sim) ────────────────────────
        now = time.time()
        entry_ts = trade["entry_ts"]
        time_in_trade = now - entry_ts
        max_hold_secs = 15 * 60

        spot_change_pct = ((current_spot - spot_entry) / spot_entry * 100
                           if spot_entry > 0 else 0)

        if contract_side == "YES":
            favorable = spot_change_pct > 0
        else:
            favorable = spot_change_pct < 0

        time_fraction = min(time_in_trade / max_hold_secs, 1.0)

        if favorable:
            simulated_price = entry_price + (1.0 - entry_price) * time_fraction * 0.7
        else:
            simulated_price = entry_price * (1 - time_fraction * 0.8)

        simulated_price = max(0.01, min(0.99, simulated_price))
        seconds_to_res = max(0, max_hold_secs - time_in_trade)

        if contract_side == "YES":
            reversal = max(0, -spot_change_pct)
        else:
            reversal = max(0, spot_change_pct)

        exit_reason = check_exit_conditions(
            trade               = trade,
            current_yes_price   = simulated_price if contract_side == "YES" else 1 - simulated_price,
            seconds_to_resolution = seconds_to_res,
            spot_reversal_pct   = reversal,
        )

        # Force resolution if time is up
        if time_in_trade >= max_hold_secs:
            exit_reason = "RESOLUTION_WIN" if favorable else "RESOLUTION_LOSS"
            # Fetch real orderbook price instead of hardcoded 1.0/0.0
            if LIVE_MODE and self._session:
                ticker = trade.get("condition_id", "")
                try:
                    snapshot = await get_market_snapshot(self._session, ticker)
                    if snapshot:
                        real_yes, real_no = snapshot
                        if real_yes == 0.50 and real_no == 0.50:
                            log.warning(
                                "Exit deferred: empty orderbook for %s "
                                "(trade #%d, max hold reached) — retry next cycle",
                                ticker, trade_id,
                            )
                            return
                        exit_price = real_yes if contract_side == "YES" else real_no
                        log.info(
                            "Max-hold exit: fetched real price for %s → exit_price=%.4f",
                            ticker, exit_price,
                        )
                    else:
                        log.warning(
                            "Exit deferred: empty orderbook for %s "
                            "(trade #%d, max hold reached) — retry next cycle",
                            ticker, trade_id,
                        )
                        return
                except Exception as e:
                    log.warning(
                        "Exit deferred: orderbook fetch error for %s (trade #%d): %s "
                        "— retry next cycle",
                        ticker, trade_id, e,
                    )
                    return
            else:
                exit_price = 1.0 if favorable else 0.0
        elif exit_reason:
            # In LIVE mode, fetch the real Kalshi orderbook price instead of
            # using a synthetic simulated_price (which can be a 0.50 placeholder)
            if LIVE_MODE and self._session:
                ticker = trade.get("condition_id", "")
                try:
                    snapshot = await get_market_snapshot(self._session, ticker)
                    if snapshot:
                        real_yes, real_no = snapshot
                        # Reject the 0.50 placeholder — means empty orderbook
                        if real_yes == 0.50 and real_no == 0.50:
                            log.warning(
                                "EXIT SKIPPED trade #%d: orderbook empty for %s "
                                "(got placeholder 0.50) — will retry next cycle",
                                trade_id, ticker,
                            )
                            return  # Retry on next scan cycle
                        exit_price = real_yes if contract_side == "YES" else real_no
                        log.info(
                            "Fetched real Kalshi price for exit: %s yes=%.4f no=%.4f → exit_price=%.4f",
                            ticker, real_yes, real_no, exit_price,
                        )
                    else:
                        log.warning(
                            "EXIT SKIPPED trade #%d: failed to fetch orderbook for %s "
                            "— will retry next cycle",
                            trade_id, ticker,
                        )
                        return  # Retry on next scan cycle
                except Exception as e:
                    log.warning(
                        "EXIT SKIPPED trade #%d: orderbook fetch error for %s: %s "
                        "— will retry next cycle",
                        trade_id, ticker, e,
                    )
                    return  # Retry on next scan cycle
            else:
                # DRY RUN / sim mode: still attempt a real orderbook fetch so
                # exit_price reflects market reality instead of a simulated value
                # that equals entry_price at trade open (net_pnl = 0 bug).
                ticker = trade.get("condition_id", "")
                if ticker and self._session:
                    try:
                        snapshot = await get_market_snapshot(self._session, ticker)
                        if snapshot:
                            real_yes, real_no = snapshot
                            # 0.50/0.50 means empty orderbook — use conservative fallback
                            if real_yes == 0.50 and real_no == 0.50:
                                exit_price = entry_price * 0.98
                                log.warning(
                                    "DRY RUN exit: empty orderbook for %s "
                                    "(trade #%d) — using entry * 0.98 fallback: %.4f",
                                    ticker, trade_id, exit_price,
                                )
                            else:
                                exit_price = real_yes if contract_side == "YES" else real_no
                                log.info(
                                    "DRY RUN exit: fetched real price for %s → exit_price=%.4f",
                                    ticker, exit_price,
                                )
                        else:
                            exit_price = entry_price * 0.98
                            log.warning(
                                "DRY RUN exit: orderbook fetch failed for %s "
                                "(trade #%d) — using entry * 0.98 fallback: %.4f",
                                ticker, trade_id, exit_price,
                            )
                    except Exception as e:
                        exit_price = entry_price * 0.98
                        log.warning(
                            "DRY RUN exit: orderbook error for %s (trade #%d): %s "
                            "— using entry * 0.98 fallback: %.4f",
                            ticker, trade_id, e, exit_price,
                        )
                else:
                    exit_price = simulated_price

            # Early exit on live: cancel the order and close at market
            if LIVE_MODE and order_id:
                try:
                    await self.executor.cancel_order(order_id)
                    log.info("Cancelled order %s for early exit: %s", order_id, exit_reason)
                except Exception as e:
                    log.error("Failed to cancel order %s: %s", order_id, e)
        else:
            return  # Hold

        self._close_trade(trade, exit_price, exit_reason)

    def _close_trade(self, trade: dict, exit_price: float, exit_reason: str):
        """Close trade, update bankroll, check halt conditions."""
        trade_id      = trade["id"]
        entry_price   = trade["entry_price"]
        position_size = trade["position_size"]
        num_shares    = trade["num_shares"]

        bankroll  = db.get_bankroll()
        gross_pnl = (exit_price - entry_price) * num_shares
        fee       = position_size * config.POLYMARKET_FEE if exit_price >= 0.99 else 0.0
        net_pnl   = gross_pnl - fee
        new_bankroll = round(bankroll + net_pnl, 6)

        db.set_bankroll(new_bankroll)
        db.close_sim_trade(
            trade_id    = trade_id,
            exit_price  = exit_price,
            exit_reason = exit_reason,
            new_bankroll= new_bankroll,
        )

        # Clean up order tracking
        self.open_orders.pop(trade_id, None)

        emoji = "✅" if net_pnl > 0 else "❌"
        log.info(
            "%s Trade #%d | %s | exit=%.4f | pnl=$%.4f | bankroll=$%.2f",
            emoji, trade_id, exit_reason, exit_price, net_pnl, new_bankroll
        )

        # Check halt conditions post-close
        daily_loss = db.get_daily_loss()
        daily_limit = new_bankroll * config.DAILY_LOSS_HALT_PCT
        if daily_loss >= daily_limit:
            db.set_halt(
                f"Daily loss halt: ${daily_loss:.2f} >= ${daily_limit:.2f}"
            )

        floor = config.STARTING_BANKROLL * config.EMERGENCY_FLOOR_PCT
        if new_bankroll <= floor:
            db.set_halt(
                f"Emergency floor: bankroll ${new_bankroll:.2f} <= ${floor:.2f}"
            )

    # ── Bankroll sync ──────────────────────────────────────────────────────

    async def _sync_bankroll(self):
        """Sync local bankroll with real Kalshi balance."""
        if not LIVE_MODE:
            log.info("Bankroll sync skipped (dry run mode)")
            return

        try:
            real_balance = await self.executor.get_balance()
            if real_balance < 0:
                log.warning("Bankroll sync: Kalshi returned error balance (%.2f) — skipping sync", real_balance)
                return
            local_balance = db.get_bankroll()
            diff = abs(real_balance - local_balance)

            if diff > 0.50:
                log.warning(
                    "Bankroll discrepancy: Kalshi=$%.2f, local=$%.2f (diff=$%.2f) — syncing to Kalshi",
                    real_balance, local_balance, diff
                )
                db.set_bankroll(real_balance)
            else:
                log.info("Bankroll in sync: $%.2f (diff=$%.2f)", local_balance, diff)
        except Exception as e:
            log.error("Bankroll sync failed: %s", e)

    async def _bankroll_sync_loop(self):
        """Periodically sync bankroll with Kalshi every 5 minutes."""
        while not self._stop.is_set():
            await asyncio.sleep(BANKROLL_SYNC_INTERVAL)
            await self._sync_bankroll()

    # ── Daily reset ────────────────────────────────────────────────────────

    async def _daily_reset_loop(self):
        """Reset daily loss counter and clear non-emergency halts at midnight UTC."""
        while not self._stop.is_set():
            now = datetime.now(timezone.utc)
            seconds_until_midnight = (
                (24 - now.hour) * 3600
                - now.minute * 60
                - now.second
            )
            await asyncio.sleep(max(seconds_until_midnight, 60))
            db.clear_halt()
            log.info("Daily reset: loss counter cleared, halt lifted (if daily)")
