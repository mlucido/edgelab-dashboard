"""
sim_engine.py — The simulation execution layer.

Wires together: signal → risk gates → simulated order → position monitor → close.
All trades are SIMULATED — no real orders, no real money.

The simulation models:
  - Realistic fill at current order-book best ask
  - Slippage model (small positions, assume full fill at quoted price)
  - Resolution outcome: based on whether spot moved as predicted
  - Early exit: stop-loss, reversal, lock-profit
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import List, Optional

import aiohttp

import config
import database as db


def init_schema(db_path):
    """Create tables if not exist."""
    import sqlite3
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS sim_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, market_id TEXT, side TEXT,
                price REAL, size REAL, pnl REAL, signal_strength TEXT
            );
            CREATE TABLE IF NOT EXISTS bot_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, bankroll REAL,
                open_positions TEXT, daily_pnl REAL
            );
        """)
from data_ingestor import DataIngestor, get_state as get_price_state
from polymarket_client import PolyMarket, fetch_crypto_markets as _poly_fetch, get_market_snapshot as _poly_snapshot
from kalshi_client import KalshiMarket, fetch_crypto_markets as _kalshi_fetch, get_market_snapshot as _kalshi_snapshot
from risk_manager import GateResult, check_all_gates, check_exit_conditions
from signal_engine import Signal, compute_kelly_size, evaluate

log = logging.getLogger(__name__)

# ── Exchange toggle ──────────────────────────────────────────────────────
USE_KALSHI = True   # True = Kalshi, False = Polymarket

fetch_crypto_markets = _kalshi_fetch if USE_KALSHI else _poly_fetch
get_market_snapshot  = _kalshi_snapshot if USE_KALSHI else _poly_snapshot

# How often to scan Polymarket for new markets (seconds)
MARKET_SCAN_INTERVAL = 60

# How often to check open positions for exit conditions (seconds)
POSITION_CHECK_INTERVAL = 15

# How often to evaluate new signals (seconds)
SIGNAL_EVAL_INTERVAL = 5


class SimEngine:
    """
    Main simulation loop. Run with asyncio.

    Usage:
        engine = SimEngine()
        await engine.run()
    """

    def __init__(self):
        init_schema(config.DB_PATH)
        self.ingestor       = DataIngestor()
        self._live_markets: List[PolyMarket] = []
        self._session:      Optional[aiohttp.ClientSession] = None
        self._stop          = asyncio.Event()

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def start(self):
        self._session = aiohttp.ClientSession()
        await self.ingestor.start()
        log.info("SimEngine started. Waiting for price feeds to warm up...")

        # Give feeds 60 seconds to accumulate history
        for i in range(12, 0, -1):
            await asyncio.sleep(5)
            log.info("Warming up... %ds remaining", i * 5)

    async def stop(self):
        self._stop.set()
        await self.ingestor.stop()
        if self._session:
            await self._session.close()
        log.info("SimEngine stopped.")

    # ── Main loop ──────────────────────────────────────────────────────────

    async def run(self):
        await self.start()

        tasks = [
            asyncio.create_task(self._market_scan_loop(),  name="market-scan"),
            asyncio.create_task(self._signal_loop(),       name="signal-eval"),
            asyncio.create_task(self._position_monitor(),  name="position-monitor"),
            asyncio.create_task(self._daily_reset_loop(),  name="daily-reset"),
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
        """Refresh the list of live Polymarket markets periodically."""
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
        momentum_pct  = self.ingestor.get_momentum(asset)
        volume_ratio  = self.ingestor.get_volume_ratio(asset)

        if momentum_pct is None:
            return

        signal = evaluate(
            asset        = asset,
            spot_price   = spot_price,
            momentum_pct = momentum_pct,
            volume_ratio = volume_ratio,
            markets      = self._live_markets,
        )

        if signal is None:
            return

        # Compute Kelly size
        bankroll    = db.get_bankroll()
        kelly_size  = compute_kelly_size(
            signal.implied_prob,
            signal.market_price,
            bankroll
        )

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

        # All gates passed — open simulated trade
        final_size = gate_result.adjusted_size or kelly_size
        await self._open_trade(signal, final_size, gate_result)

    async def _open_trade(self, signal: Signal, size: float, gate: GateResult):
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
        log.info(
            "✅ SIM TRADE #%d opened | %s %s | $%.2f @ %.4f | edge=%.1f%%",
            trade_id, signal.asset, signal.direction,
            size, signal.market_price, signal.edge_pct * 100
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
        now = time.time()
        entry_ts    = trade["entry_ts"]
        condition_id = trade["condition_id"]
        yes_token   = None   # we'd store this in a real system

        # ── Simulated resolution check ─────────────────────────────────────
        # In the sim, we estimate resolution based on spot price movement
        # since entry. A "YES" contract wins if BTC is higher now than at entry.
        asset       = trade["asset"]
        direction   = trade["direction"]
        spot_entry  = trade["spot_price_entry"]
        current_spot = self.ingestor.get_price(asset)

        # Check if we've hit the estimated resolution time
        # (markets end_date wasn't stored — use a proxy: 15 mins = 900s)
        # In production this would check the actual end_date
        time_in_trade = now - entry_ts
        max_hold_secs = 15 * 60   # 15-minute markets

        # Simulated current contract price based on remaining time + spot move
        spot_change_pct = ((current_spot - spot_entry) / spot_entry * 100
                           if spot_entry > 0 else 0)

        # Simulate a YES price decay/grow based on spot movement
        # Simple model: entry_price + (edge if moving right) - (decay if wrong)
        entry_price = trade["entry_price"]
        contract_side = trade["contract_side"]

        if contract_side == "YES":
            favorable = spot_change_pct > 0
        else:
            favorable = spot_change_pct < 0

        time_fraction = min(time_in_trade / max_hold_secs, 1.0)

        if favorable:
            # Price drifts toward 1.0 as we approach resolution
            simulated_price = entry_price + (1.0 - entry_price) * time_fraction * 0.7
        else:
            # Price drifts toward 0.0
            simulated_price = entry_price * (1 - time_fraction * 0.8)

        simulated_price = max(0.01, min(0.99, simulated_price))

        # Seconds to (estimated) resolution
        seconds_to_res = max(0, max_hold_secs - time_in_trade)

        # Spot reversal since entry (how much it went AGAINST us)
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
            # Final resolution: did spot move in the right direction?
            if favorable:
                exit_price  = 1.0   # WIN
                exit_reason = "RESOLUTION_WIN"
            else:
                exit_price  = 0.0   # LOSS
                exit_reason = "RESOLUTION_LOSS"
        elif exit_reason:
            # Early exit — use simulated current price
            exit_price = simulated_price
        else:
            return   # Hold

        # Close trade and update bankroll
        bankroll = db.get_bankroll()
        position_size = trade["position_size"]
        num_shares    = trade["num_shares"]

        gross_pnl = (exit_price - entry_price) * num_shares
        fee       = position_size * config.POLYMARKET_FEE if exit_price >= 0.99 else 0.0
        net_pnl   = gross_pnl - fee
        new_bankroll = round(bankroll + net_pnl, 6)

        db.set_bankroll(new_bankroll)
        db.close_sim_trade(
            trade_id    = trade["id"],
            exit_price  = exit_price,
            exit_reason = exit_reason,
            new_bankroll= new_bankroll,
        )

        emoji = "✅" if net_pnl > 0 else "❌"
        log.info(
            "%s Trade #%d | %s | exit=%.4f | pnl=$%.4f | bankroll=$%.2f",
            emoji, trade["id"], exit_reason, exit_price, net_pnl, new_bankroll
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

    # ── Daily reset ────────────────────────────────────────────────────────

    async def _daily_reset_loop(self):
        """Reset daily loss counter and clear non-emergency halts at midnight UTC."""
        while not self._stop.is_set():
            now = datetime.now(timezone.utc)
            # Sleep until next midnight UTC
            seconds_until_midnight = (
                (24 - now.hour) * 3600
                - now.minute * 60
                - now.second
            )
            await asyncio.sleep(max(seconds_until_midnight, 60))
            db.clear_halt()
            log.info("Daily reset: loss counter cleared, halt lifted (if daily)")
