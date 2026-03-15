#!/usr/bin/env python3
"""
Carry Trade Runner — Standalone entry point for the paper carry trade strategy.

Run:  python3 strategies/edgelab/run_carry.py
"""

import asyncio
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from strategies.edgelab.strategies.carry_trade import (
    CarryTradeStrategy,
    CARRY_ASSETS,
    CARRY_ENTRY_BASIS_APR,
    CARRY_EXIT_BASIS_APR,
    CARRY_MAX_LEVERAGE,
    CARRY_MARGIN_BUFFER_PCT,
    CARRY_POSITION_SIZE_USD,
    CARRY_SCAN_INTERVAL_SECS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("run_carry")


def format_usd(val):
    """Format USD value with commas."""
    if abs(val) >= 1000:
        return f"${val:,.0f}"
    return f"${val:,.2f}"


def print_banner():
    """Print config banner."""
    print()
    print("=" * 60)
    print("  CARRY TRADE STRATEGY  [paper mode]")
    print("=" * 60)
    print(f"  Assets:          {', '.join(CARRY_ASSETS)}")
    print(f"  Entry Threshold: {CARRY_ENTRY_BASIS_APR:.1f}% APR")
    print(f"  Exit Threshold:  {CARRY_EXIT_BASIS_APR:.1f}% APR")
    print(f"  Max Leverage:    {CARRY_MAX_LEVERAGE:.1f}x")
    print(f"  Margin Buffer:   {CARRY_MARGIN_BUFFER_PCT:.0f}%")
    print(f"  Position Size:   {format_usd(CARRY_POSITION_SIZE_USD)}")
    print(f"  Scan Interval:   {CARRY_SCAN_INTERVAL_SECS}s")
    print("=" * 60)
    print()


def signal_badge(basis_apr):
    """Return signal emoji and label."""
    if basis_apr >= CARRY_ENTRY_BASIS_APR:
        return "\U0001f7e2 ENTER"  # green circle
    elif basis_apr >= 5.0:
        return "\U0001f7e1 WATCH"  # yellow circle
    else:
        return "\U0001f534 PASS"   # red circle


def health_bar(health_pct):
    """Return colored health indicator."""
    if health_pct >= 75:
        return f"\U0001f7e2 {health_pct:.0f}%"
    elif health_pct >= 50:
        return f"\U0001f7e1 {health_pct:.0f}%"
    else:
        return f"\U0001f534 {health_pct:.0f}%"


def print_scan_table(scan_data):
    """Print live scan table."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    opportunities = scan_data.get("opportunities", [])

    print(f"\n  CARRY TRADE SCANNER  [paper mode]  {now}")
    print("  " + "-" * 56)
    print(f"  {'Asset':<6} {'Spot':>10} {'Futures':>10} {'Basis APR':>10} {'Signal':>14}")
    print("  " + "-" * 56)

    for opp in opportunities:
        asset = opp["asset"]
        spot = format_usd(opp["spot"])
        futures = format_usd(opp["futures_mark"])
        basis = f"{opp['basis_apr']:.1f}%"
        sig = signal_badge(opp["basis_apr"])
        sim_tag = " [sim]" if opp.get("simulated") else ""
        print(f"  {asset:<6} {spot:>10} {futures:>10} {basis:>10} {sig:>14}{sim_tag}")

    print("  " + "-" * 56)


def print_positions(strategy, opportunities):
    """Print open positions with margin health."""
    if not strategy.positions:
        return

    print("\n  OPEN POSITIONS")
    print("  " + "-" * 70)
    print(f"  {'Asset':<6} {'Entry Basis':>12} {'Curr Basis':>12} {'Net PnL':>10} {'Health':>12} {'Days Left':>10}")
    print("  " + "-" * 70)

    for asset, pos in strategy.positions.items():
        # Find current basis data for this asset
        current = next((o for o in opportunities if o["asset"] == asset), None)
        if not current:
            continue

        pnl = strategy.calculate_pnl(pos, current)
        health = strategy.check_margin_health(pos, current["spot"])

        entry_basis = f"{pos['entry_basis_apr']:.1f}%"
        curr_basis = f"{current['basis_apr']:.1f}%"
        net_pnl = format_usd(pnl["net_pnl"])
        hlth = health_bar(health["health_pct"])
        days = str(current["days_to_expiry"])

        print(f"  {asset:<6} {entry_basis:>12} {curr_basis:>12} {net_pnl:>10} {hlth:>12} {days:>10}")

    print("  " + "-" * 70)


def print_position_detail(position):
    """Print full position dict when a trade is entered."""
    print("\n  NEW POSITION OPENED")
    print("  " + "-" * 50)
    for k, v in position.items():
        if k in ("simulated",):
            continue
        if isinstance(v, float):
            print(f"  {k:<25} {v:>20.4f}")
        else:
            print(f"  {k:<25} {str(v):>20}")
    print("  " + "-" * 50)


async def main():
    print_banner()

    strategy = CarryTradeStrategy()

    # Track positions to detect new entries
    prev_positions = set(strategy.positions.keys())

    # Graceful shutdown
    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event()

    def handle_signal():
        log.info("Shutdown signal received")
        strategy.stop()
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    cycle = 0
    async for scan_data in strategy.paper_trade_loop():
        cycle += 1

        if "error" in scan_data:
            print(f"\n  [ERROR] Cycle {cycle}: {scan_data['error']}")
            continue

        print_scan_table(scan_data)

        # Check for newly opened positions
        current_positions = set(scan_data.get("open_positions", {}).keys())
        new_entries = current_positions - prev_positions
        for asset in new_entries:
            pos = scan_data["open_positions"][asset]
            print_position_detail(pos)

            # Print margin health for new position
            opp = next((o for o in scan_data["opportunities"] if o["asset"] == asset), None)
            if opp:
                health = strategy.check_margin_health(pos, opp["spot"])
                print(f"\n  MARGIN HEALTH CHECK: {health_bar(health['health_pct'])}")
                print(f"  Margin call price: {format_usd(health['margin_call_price'])}")
                print(f"  Current spot:      {format_usd(health['current_spot'])}")

        prev_positions = current_positions

        # Print open positions
        print_positions(strategy, scan_data.get("opportunities", []))

        print(f"\n  Cycle {cycle} complete. Next scan in {CARRY_SCAN_INTERVAL_SECS}s...")

        if shutdown_event.is_set():
            break

    await strategy.close()
    print("\n  Carry trade strategy stopped gracefully.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n  Interrupted. Exiting.")
