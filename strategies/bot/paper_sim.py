"""
paper_sim.py — 15-minute paper simulation with recalibrated model.

Uses LIVE Coinbase WebSocket prices + synthetic Kalshi-like contracts
to test the full signal → gate → trade pipeline without Kalshi API auth.

Generates realistic contracts:
  - Strikes at $250/$500/$1000 intervals around BTC spot
  - Strikes at $10/$25/$50 intervals around ETH spot
  - NO prices based on distance from spot (closer = more expensive)
"""

import asyncio
import logging
import sys
import time
from datetime import datetime, timezone, timedelta

import config
import database as db
from data_ingestor import DataIngestor
from kalshi_client import KalshiMarket
from signal_engine import Signal, evaluate, compute_kelly_size
from risk_manager import check_all_gates

SIM_DURATION = 15 * 60  # 15 minutes
SIGNAL_INTERVAL = 5     # check every 5 seconds
WARMUP_SECS = 60        # 1 minute warmup for price feed

log = logging.getLogger("paper_sim")


def _generate_synthetic_markets(asset: str, spot: float) -> list[KalshiMarket]:
    """
    Generate realistic Kalshi-like contracts around the current spot price.
    Mimics the real market structure: strikes at regular intervals, NO prices
    based on distance from spot.
    """
    markets = []
    now = datetime.now(timezone.utc)
    end = now + timedelta(hours=1)

    if "BTC" in asset:
        base = "KXBTC"
        # Generate strikes from -2% to +2% of spot in $250 steps
        step = 250
        low = int(spot * 0.98 / step) * step
        high = int(spot * 1.02 / step) * step + step
    else:
        base = "KXETH"
        step = 10
        low = int(spot * 0.98 / step) * step
        high = int(spot * 1.02 / step) * step + step

    for strike in range(low, high, step):
        distance_pct = abs(strike - spot) / spot
        # NO price for "above X" contract: cheaper when strike is further above spot
        if strike > spot:
            # Strike above spot → NO wins if price stays below → deep ITM for NO
            # Price the NO cheaply (more OTM the strike, cheaper the NO)
            no_price = max(0.02, min(0.15, 0.02 + distance_pct * 2))
            yes_price = round(1.0 - no_price, 2)
        else:
            # Strike below spot → YES likely wins → NO is expensive
            no_price = max(0.70, min(0.98, 0.98 - distance_pct * 3))
            yes_price = round(1.0 - no_price, 2)

        no_price = round(no_price, 2)
        ticker = f"{base}-26MAR1316-B{strike}"

        # Liquidity: closer to spot = more liquid
        liq = max(300, 2000 * (1 - distance_pct * 10))

        markets.append(KalshiMarket(
            condition_id=ticker,
            question=f"{'Bitcoin' if 'BTC' in asset else 'Ethereum'} price at 4pm UTC?",
            asset=asset.split("-")[0],
            direction="UP",
            yes_token_id=f"{ticker}_yes",
            no_token_id=f"{ticker}_no",
            yes_price=yes_price,
            no_price=no_price,
            spread=round(abs(yes_price + no_price - 1.0), 4),
            liquidity=round(liq, 2),
            end_date=end,
            hours_remaining=1.0,
            strike_price=float(strike),
        ))

    return markets


async def run_paper_sim():
    fmt = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler("paper_sim.log", mode="w"),
            logging.StreamHandler(sys.stdout),
        ]
    )
    # Suppress noisy loggers
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("signal_engine").setLevel(logging.INFO)

    db.init_db()

    # Record starting state
    start_bankroll = db.get_bankroll()
    start_trades = int(db.get_state("total_trades") or 0)

    print()
    print("█" * 60)
    print(f"█{'':^58}█")
    print(f"█{'PAPER SIMULATION — RECALIBRATED MODEL':^58}█")
    print(f"█{'15-minute test with live Coinbase prices':^58}█")
    print(f"█{f'Bankroll: ${start_bankroll:.2f}':^58}█")
    print(f"█{'':^58}█")
    print("█" * 60)
    print()

    ingestor = DataIngestor()
    await ingestor.start()

    # Warmup
    log.info("Warming up price feeds (%ds)...", WARMUP_SECS)
    await asyncio.sleep(WARMUP_SECS)

    # Verify we have data
    for asset in config.ASSETS:
        if not ingestor.is_ready(asset):
            log.warning("%s feed not ready after warmup", asset)

    sim_start = time.time()
    signals_seen = 0
    signals_blocked = 0
    signals_passed = 0
    trades_opened = 0
    low_price_rejected = 0

    log.info("Simulation started — running for %d minutes", SIM_DURATION // 60)

    try:
        while time.time() - sim_start < SIM_DURATION:
            elapsed = time.time() - sim_start
            remaining = SIM_DURATION - elapsed

            # Periodic status
            if int(elapsed) % 60 < SIGNAL_INTERVAL:
                log.info(
                    "--- %d:%02d elapsed | %d:%02d remaining | "
                    "signals=%d blocked=%d passed=%d trades=%d ---",
                    int(elapsed) // 60, int(elapsed) % 60,
                    int(remaining) // 60, int(remaining) % 60,
                    signals_seen, signals_blocked, signals_passed, trades_opened,
                )

            for asset in config.ASSETS:
                if not ingestor.is_ready(asset):
                    continue

                spot = ingestor.get_price(asset)
                mom = ingestor.get_momentum(asset)
                vol = ingestor.get_volume_ratio(asset)

                if mom is None or spot is None:
                    continue

                # Generate synthetic markets around current spot
                markets = _generate_synthetic_markets(asset, spot)

                signal = evaluate(
                    asset=asset,
                    spot_price=spot,
                    momentum_pct=mom,
                    volume_ratio=vol,
                    markets=markets,
                )

                if signal is None:
                    continue

                signals_seen += 1

                # Kelly sizing
                bankroll = db.get_bankroll()
                kelly = compute_kelly_size(signal.implied_prob, signal.market_price, bankroll)

                # Gates
                gate = check_all_gates(signal, kelly)
                if not gate.passed:
                    signals_blocked += 1
                    log.info(
                        "BLOCKED %s: %s | %s %s @ $%.2f | mom=%.3f%% edge=%.1f%%",
                        gate.gate, gate.reason,
                        signal.asset, signal.direction, signal.market_price,
                        signal.momentum_pct, signal.edge_pct * 100,
                    )
                    continue

                signals_passed += 1
                final_size = gate.adjusted_size or kelly

                # Open the trade (paper)
                trade_id = db.open_sim_trade(
                    asset=signal.asset,
                    direction=signal.direction,
                    contract_side=signal.contract_side,
                    condition_id=signal.best_market.condition_id,
                    question=signal.best_market.question,
                    entry_price=signal.market_price,
                    position_size=final_size,
                    implied_prob=signal.implied_prob,
                    market_prob=signal.market_price,
                    edge_pct=signal.edge_pct,
                    kelly_fraction=config.KELLY_FRACTION,
                    spot_price=signal.spot_price,
                    momentum_pct=signal.momentum_pct,
                    volume_ratio=vol,
                    bankroll=bankroll,
                )
                trades_opened += 1

                log.info(
                    "TRADE OPENED #%d | %s %s %s @ $%.2f | "
                    "$%.2f deployed | mom=%.3f%% | edge=%.1f%% | %s",
                    trade_id, signal.asset, signal.direction, signal.contract_side,
                    signal.market_price, final_size,
                    signal.momentum_pct, signal.edge_pct * 100,
                    signal.signal_strength,
                )

            # Check open positions for simulated resolution
            open_trades = db.get_open_trades()
            for trade in open_trades:
                age = time.time() - trade["entry_ts"]
                asset = trade["asset"]
                if not ingestor.is_ready(asset):
                    continue

                current_spot = ingestor.get_price(asset)
                spot_entry = trade["spot_price_entry"]
                entry_price = trade["entry_price"]
                contract_side = trade["contract_side"]

                spot_change = (current_spot - spot_entry) / spot_entry * 100

                if contract_side == "NO":
                    favorable = spot_change < 0 if trade["direction"] == "DOWN" else spot_change > 0
                else:
                    favorable = spot_change > 0 if trade["direction"] == "UP" else spot_change < 0

                # Simulate contract price movement
                max_hold = 15 * 60
                time_frac = min(age / max_hold, 1.0)

                if favorable:
                    sim_price = entry_price + (1.0 - entry_price) * time_frac * 0.7
                else:
                    sim_price = entry_price * (1 - time_frac * 0.8)
                sim_price = max(0.01, min(0.99, sim_price))

                # Reversal check
                if contract_side == "NO":
                    reversal = max(0, spot_change) if trade["direction"] == "DOWN" else max(0, -spot_change)
                else:
                    reversal = max(0, -spot_change) if trade["direction"] == "UP" else max(0, spot_change)

                from risk_manager import check_exit_conditions
                exit_reason = check_exit_conditions(
                    trade=trade,
                    current_yes_price=sim_price if contract_side == "YES" else 1 - sim_price,
                    seconds_to_resolution=max(0, max_hold - age),
                    spot_reversal_pct=reversal,
                )

                # Force resolution at max hold
                if age >= max_hold:
                    if favorable:
                        exit_price = 1.0
                        exit_reason = "RESOLUTION_WIN"
                    else:
                        exit_price = 0.0
                        exit_reason = "RESOLUTION_LOSS"
                elif exit_reason:
                    exit_price = sim_price
                else:
                    continue

                # Close trade
                bankroll = db.get_bankroll()
                num_shares = trade["num_shares"]
                position_size = trade["position_size"]
                gross_pnl = (exit_price - entry_price) * num_shares
                fee = position_size * config.POLYMARKET_FEE if exit_price >= 0.99 else 0.0
                net_pnl = gross_pnl - fee
                new_bankroll = round(bankroll + net_pnl, 6)

                db.set_bankroll(new_bankroll)
                db.close_sim_trade(trade["id"], exit_price, exit_reason, new_bankroll)

                emoji = "WIN" if net_pnl > 0 else "LOSS"
                log.info(
                    "TRADE CLOSED #%d | %s | %s %s | exit=$%.2f | "
                    "pnl=$%+.2f | bankroll=$%.2f | spot_move=%+.3f%%",
                    trade["id"], emoji, trade["asset"], exit_reason,
                    exit_price, net_pnl, new_bankroll, spot_change,
                )

            await asyncio.sleep(SIGNAL_INTERVAL)

    except KeyboardInterrupt:
        log.info("Interrupted by user")

    await ingestor.stop()

    # Force-close any remaining open positions
    final_open = db.get_open_trades()
    if final_open:
        log.info("Force-closing %d remaining positions at end of sim...", len(final_open))
        for trade in final_open:
            asset = trade["asset"]
            spot_entry = trade["spot_price_entry"]
            current_spot = ingestor.get_price(asset) if ingestor.is_ready(asset) else spot_entry

            spot_change = (current_spot - spot_entry) / spot_entry * 100
            contract_side = trade["contract_side"]
            if contract_side == "NO":
                favorable = spot_change < 0 if trade["direction"] == "DOWN" else spot_change > 0
            else:
                favorable = spot_change > 0

            if favorable:
                exit_price = 1.0
                exit_reason = "SIM_END_WIN"
            else:
                exit_price = 0.0
                exit_reason = "SIM_END_LOSS"

            bankroll = db.get_bankroll()
            num_shares = trade["num_shares"]
            position_size = trade["position_size"]
            gross_pnl = (exit_price - trade["entry_price"]) * num_shares
            fee = position_size * config.POLYMARKET_FEE if exit_price >= 0.99 else 0.0
            net_pnl = gross_pnl - fee
            new_bankroll = round(bankroll + net_pnl, 6)
            db.set_bankroll(new_bankroll)
            db.close_sim_trade(trade["id"], exit_price, exit_reason, new_bankroll)

            log.info(
                "FORCE CLOSED #%d | %s | pnl=$%+.2f | bankroll=$%.2f",
                trade["id"], exit_reason, net_pnl, new_bankroll,
            )

    # Final report
    end_bankroll = db.get_bankroll()
    end_trades = int(db.get_state("total_trades") or 0)
    new_trades = end_trades - start_trades
    pnl = end_bankroll - start_bankroll

    print()
    print("=" * 60)
    print("  PAPER SIMULATION RESULTS — RECALIBRATED MODEL")
    print("=" * 60)
    print(f"  Duration          : {SIM_DURATION // 60} minutes")
    print(f"  Starting bankroll : ${start_bankroll:.2f}")
    print(f"  Ending bankroll   : ${end_bankroll:.2f}")
    print(f"  Net P&L           : ${pnl:+.2f} ({pnl/start_bankroll*100:+.1f}%)")
    print(f"  Signals generated : {signals_seen}")
    print(f"  Signals blocked   : {signals_blocked}")
    print(f"  Signals passed    : {signals_passed}")
    print(f"  Trades opened     : {trades_opened}")
    print()

    # Show individual trades from this session
    import sqlite3
    conn = sqlite3.connect(config.DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    rows = conn.execute(
        "SELECT * FROM sim_trades WHERE id > ? ORDER BY id",
        (start_trades,)
    ).fetchall()

    if rows:
        print("  TRADE LOG:")
        print("  " + "-" * 56)
        for r in rows:
            outcome = r["outcome"]
            net = r["net_pnl"] or 0
            print(
                f"  #{r['id']:2d} | {r['asset']:7s} {r['direction']:4s} {r['contract_side']:3s} | "
                f"entry=${r['entry_price']:.2f} | "
                f"${r['position_size']:.2f} | "
                f"mom={r['momentum_pct']:+.3f}% | "
                f"edge={r['edge_pct']*100:.0f}% | "
                f"{outcome:7s} ${net:+.2f}"
            )
    else:
        print("  No trades opened (recalibrated model filtered everything)")
        print()
        print("  This is expected behavior if momentum stayed below 0.60%")
        print("  (MEDIUM+ required for low-price contracts)")

    print()
    print("  LOW-PRICE FILTER STATS:")
    print(f"  Contracts < ${config.LOW_PRICE_THRESHOLD:.2f} rejected (weak signal): see paper_sim.log")
    print("=" * 60)
    print()

    conn.close()


if __name__ == "__main__":
    asyncio.run(run_paper_sim())
