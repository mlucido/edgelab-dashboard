"""
main.py — Entry point for the Polymarket Lag Arb Simulator.

Run:
    python main.py           # live sim (connects to real Binance + Polymarket feeds)
    python main.py --report  # print stats from an existing sim_results.db
    python main.py --reset   # wipe the database and start fresh
"""

from dotenv import load_dotenv
load_dotenv()

import argparse
import asyncio
import logging
import sys

import config
import database as db
from dashboard import Dashboard
from sim_engine import SimEngine


def setup_logging():
    fmt = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL, logging.INFO),
        format=fmt,
        handlers=[
            logging.FileHandler(config.LOG_FILE),
            logging.StreamHandler(sys.stdout),
        ]
    )
    # Quiet noisy libraries
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


def print_report():
    stats = db.get_stats()
    print("\n" + "═" * 55)
    print("  POLYMARKET SIM — PERFORMANCE REPORT")
    print("═" * 55)
    print(f"  Starting bankroll : ${config.STARTING_BANKROLL:,.2f}")
    print(f"  Current bankroll  : ${stats['bankroll']:,.2f}")
    print(f"  Total P&L         : ${stats['total_net_pnl']:+,.4f}  ({stats['roi_pct']:+.2f}%)")
    print(f"  Total signals     : {stats['total_trades']}")
    print(f"  Closed trades     : {stats['closed_trades']}")
    print(f"  Open (pending)    : {stats['pending']}")
    print(f"  Wins              : {stats['wins']}")
    print(f"  Losses            : {stats['losses']}")
    print(f"  Win rate          : {stats['win_rate']:.1f}%")
    print(f"  Avg edge          : {stats['avg_edge']:.2f}%")
    print(f"  Avg net P&L/trade : ${stats['avg_net_pnl']:+.4f}")
    print(f"  Bot halted        : {'YES — ' + stats['halt_reason'] if stats['halted'] else 'No'}")

    # Phase check
    b = stats["bankroll"]
    if b >= 100_000:
        phase = "🏆 TARGET HIT — $100k reached!"
    elif b >= 50_000:
        phase = "⚡ Phase 4 — Halfway to target"
    elif b >= 10_000:
        phase = "💪 Phase 4 — Full suite active"
    elif b >= 5_000:
        phase = "🔓 Phase 3 — Bonds unlocked"
    elif b >= 2_500:
        phase = "🔓 Phase 3 — Market making unlocked"
    elif b >= 1_000:
        phase = "🌿 Phase 2 — First double achieved"
    else:
        phase = "🌱 Phase 2 — Seed stage"
    print(f"  Milestone         : {phase}")

    # Validation targets
    print("\n  VALIDATION TARGETS:")
    wr_ok  = stats['win_rate'] >= 68 or stats['closed_trades'] < 50
    edge_ok = stats['avg_edge'] >= 8.0 or stats['closed_trades'] < 50
    trades_ok = stats['closed_trades'] >= 200
    print(f"  Win rate >= 68%  : {'PASS' if wr_ok else 'FAIL'} ({stats['win_rate']:.1f}%)")
    print(f"  Avg edge >= 8%   : {'PASS' if edge_ok else 'FAIL'} ({stats['avg_edge']:.2f}%)")
    trades_label = "PASS" if trades_ok else f"FAIL ({stats['closed_trades']}/200)"
    print(f"  200+ closed trades: {trades_label}")

    if wr_ok and edge_ok and trades_ok:
        print("\n  🚀 SIM PASSED — Ready to go live with real $500!")
    else:
        print("\n  ⏳ Keep running — not enough data yet.")
    print("═" * 55 + "\n")


async def main():
    setup_logging()
    log = logging.getLogger("main")

    parser = argparse.ArgumentParser(description="Polymarket Lag Arb Simulator")
    parser.add_argument("--report", action="store_true", help="Print stats and exit")
    parser.add_argument("--reset",  action="store_true", help="Reset database and start fresh")
    args = parser.parse_args()

    if args.reset:
        import os
        if os.path.exists(config.DB_PATH):
            os.remove(config.DB_PATH)
            print(f"Database {config.DB_PATH} deleted.")
    
    # Always init DB (creates if not exists)
    db.init_db()

    if args.report:
        print_report()
        return

    log.info("=" * 60)
    log.info("  Polymarket Lag Arb Simulator — Starting")
    log.info("  Starting bankroll : $%.2f", config.STARTING_BANKROLL)
    log.info("  Target            : $100,000")
    log.info("  Assets            : %s", config.ASSETS)
    log.info("  Min edge          : %.1f%%", config.MIN_EDGE_PCT * 100)
    log.info("  Max position      : %.1f%% of bankroll", config.MAX_POSITION_PCT * 100)
    log.info("=" * 60)

    dashboard = Dashboard()
    dashboard.start()

    engine = SimEngine()
    try:
        await engine.run()
    except KeyboardInterrupt:
        log.info("Keyboard interrupt — stopping...")
    finally:
        dashboard.stop()
        await engine.stop()
        print("\n")
        print_report()


if __name__ == "__main__":
    asyncio.run(main())
