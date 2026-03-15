"""
live_main.py — Entry point for the LIVE Kalshi trading bot.

Run:
    python3 live_main.py                # dry-run mode (default)
    python3 live_main.py --dry-run      # explicit dry-run
    python3 live_main.py --go-live      # real money (requires confirmation)
    python3 live_main.py --report       # print stats and exit
    python3 live_main.py --balance      # show Kalshi vs DB bankroll
"""

from dotenv import load_dotenv
load_dotenv()

import argparse
import asyncio
import logging
import os
import sys

import aiohttp

import config
import database as db
import alert_manager
from kalshi_client import _api_get, _auth_headers


def setup_logging():
    fmt = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL, logging.INFO),
        format=fmt,
        handlers=[
            logging.FileHandler("live.log"),
            logging.StreamHandler(sys.stdout),
        ]
    )
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


def print_report():
    stats = db.get_stats()
    print("\n" + "═" * 55)
    print("  KALSHI LIVE — PERFORMANCE REPORT")
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
    print("═" * 55 + "\n")


def print_banner(dry_run: bool):
    mode = "DRY RUN" if dry_run else "LIVE — REAL MONEY"
    border = "█" * 55
    print()
    print(border)
    print(f"█{'':^53}█")
    print(f"█{'KALSHI TRADING BOT':^53}█")
    print(f"█{'── ' + mode + ' ──':^53}█")
    print(f"█{'':^53}█")
    print(border)
    print()


async def show_balance():
    """Show Kalshi real balance vs DB bankroll, flag discrepancies."""
    db.init_db()
    db_bankroll = db.get_bankroll()

    print("\n" + "═" * 55)
    print("  BALANCE CHECK")
    print("═" * 55)
    print(f"  DB bankroll       : ${db_bankroll:,.2f}")

    # Fetch real Kalshi balance
    try:
        async with aiohttp.ClientSession() as session:
            data = await _api_get(session, "/portfolio/balance")
            if data:
                # Kalshi returns balance in cents
                balance_cents = data.get("balance", 0)
                real_balance = balance_cents / 100.0
                print(f"  Kalshi balance    : ${real_balance:,.2f}")

                diff = abs(real_balance - db_bankroll)
                if diff > 1.0:
                    print(f"  ⚠️  DISCREPANCY    : ${diff:,.2f} difference!")
                else:
                    print(f"  Status            : In sync (Δ ${diff:.2f})")
            else:
                print("  Kalshi balance    : UNAVAILABLE (API error)")
    except Exception as e:
        print(f"  Kalshi balance    : UNAVAILABLE ({e})")

    # Show open positions
    open_trades = db.get_open_trades()
    if open_trades:
        deployed = sum(t["position_size"] for t in open_trades)
        print(f"  Open positions    : {len(open_trades)} (${deployed:,.2f} deployed)")
    else:
        print(f"  Open positions    : 0")

    print("═" * 55 + "\n")


async def main():
    setup_logging()
    log = logging.getLogger("live_main")

    parser = argparse.ArgumentParser(description="Kalshi Live Trading Bot")
    parser.add_argument("--report",   action="store_true", help="Print stats and exit")
    parser.add_argument("--balance",  action="store_true", help="Show Kalshi vs DB balance")
    parser.add_argument("--dry-run",  action="store_true", help="Run in dry-run mode (default)")
    parser.add_argument("--go-live",  action="store_true", help="Run with real money (requires confirmation)")
    parser.add_argument("--reset",    action="store_true", help="Reset database and start fresh")
    args = parser.parse_args()

    if args.reset:
        if os.path.exists(config.DB_PATH):
            os.remove(config.DB_PATH)
            print(f"Database {config.DB_PATH} deleted.")

    db.init_db()

    if args.report:
        print_report()
        return

    if args.balance:
        await show_balance()
        return

    # Determine mode
    if args.go_live:
        dry_run = False
        print("\n⚠️  REAL MONEY MODE. Type CONFIRM to proceed:")
        user_input = input("> ").strip()
        if user_input != "CONFIRM":
            print("Aborted. Use --dry-run for safe testing.")
            return
        os.environ["DRY_RUN"] = "false"
    else:
        dry_run = True
        os.environ["DRY_RUN"] = "true"

    print_banner(dry_run)

    bankroll = db.get_bankroll()
    log.info("=" * 60)
    log.info("  Kalshi Live Trading Bot — Starting")
    log.info("  Mode              : %s", "DRY RUN" if dry_run else "LIVE")
    log.info("  Starting bankroll : $%.2f", bankroll)
    log.info("  Assets            : %s", config.ASSETS)
    log.info("  Min edge          : %.1f%%", config.MIN_EDGE_PCT * 100)
    log.info("  Max position      : %.1f%% of bankroll", config.MAX_POSITION_PCT * 100)
    log.info("  Telegram alerts   : %s", "ON" if alert_manager.TELEGRAM_ENABLED else "OFF")
    log.info("=" * 60)

    await alert_manager.startup(bankroll, dry_run)

    # LiveEngine import — will be built next
    try:
        from live_engine import LiveEngine
    except ImportError:
        log.error("live_engine.py not found — build it next!")
        print("\n⚠️  live_engine.py not yet built. Bot cannot start.")
        print("   alert_manager + live_main are ready. Build live_engine.py next.\n")
        return

    engine = LiveEngine()
    try:
        await engine.run()
    except KeyboardInterrupt:
        log.info("Keyboard interrupt — stopping...")
    finally:
        await engine.stop()
        # Send daily summary on shutdown
        stats = db.get_stats()
        await alert_manager.daily_summary(stats)
        print("\n")
        print_report()


if __name__ == "__main__":
    asyncio.run(main())
