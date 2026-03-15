#!/usr/bin/env python3
"""
EdgeLab Simulation Runner

Usage:
    python scripts/run_simulation.py                  # standard config (default)
    python scripts/run_simulation.py --conservative   # conservative config
    python scripts/run_simulation.py --aggressive     # aggressive config
    python scripts/run_simulation.py --config standard
    python scripts/run_simulation.py --days 30        # last 30 days only
    python scripts/run_simulation.py --refresh        # force re-fetch from API
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

# Allow running from repo root or scripts/ directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.simulation.backsim import run_simulation, CONFIGS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run EdgeLab backsimulation against Polymarket historical data."
    )

    # Config selection (mutually exclusive shortcuts)
    config_group = parser.add_mutually_exclusive_group()
    config_group.add_argument(
        "--conservative",
        action="store_const",
        const="conservative",
        dest="config",
        help="Use conservative risk config (max $25/trade, 40%% deploy cap)",
    )
    config_group.add_argument(
        "--standard",
        action="store_const",
        const="standard",
        dest="config",
        help="Use standard risk config (max $50/trade, 60%% deploy cap) [default]",
    )
    config_group.add_argument(
        "--aggressive",
        action="store_const",
        const="aggressive",
        dest="config",
        help="Use aggressive risk config (max $100/trade, 80%% deploy cap)",
    )
    config_group.add_argument(
        "--config",
        choices=list(CONFIGS.keys()),
        dest="config",
        help="Risk config name (alternative to shorthand flags)",
    )

    parser.add_argument(
        "--days",
        type=int,
        default=90,
        metavar="N",
        help="Number of historical days to simulate (default: 90)",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Force re-fetch market data from Gamma API (ignore 24h cache)",
    )

    args = parser.parse_args()

    # Default config
    if args.config is None:
        args.config = "standard"

    return args


async def main() -> None:
    args = parse_args()

    print(f"\nEdgeLab Simulation — config={args.config}, days={args.days}")
    print(f"Config params: {CONFIGS[args.config]}\n")

    result = await run_simulation(
        config_name=args.config,
        days=args.days,
        force_refetch=args.refresh,
    )

    if not result:
        print("\nSimulation returned no results. Check API connectivity.")
        sys.exit(1)

    # Exit with non-zero if the selected config is a NO-GO
    pnl = result.get("total_pnl", 0)
    sharpe = result.get("sharpe_ratio", 0)
    dd = result.get("max_drawdown_pct", 100)
    wr = result.get("win_rate", 0) * 100
    cb = result.get("circuit_breaker_triggers", 99)

    passes = sum([sharpe > 1.0, dd < 20.0, wr > 55.0, pnl > 0, cb < 5])
    if passes < 3 or pnl <= 0:
        sys.exit(2)  # NO-GO signal for CI pipelines


if __name__ == "__main__":
    asyncio.run(main())
