"""
alert_manager.py — Telegram + console alerts for the live Kalshi trading bot.

Reads TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID from environment.
Falls back to console-only if either is missing. Never crashes.
"""

import logging
import os
from datetime import datetime, timezone

import aiohttp

log = logging.getLogger(__name__)

# ── Telegram config ──────────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

if not TELEGRAM_ENABLED:
    log.info("Telegram not configured — alerts will be console-only")


# ── Core send ────────────────────────────────────────────────────────────────

async def send_alert(message: str, level: str = "INFO"):
    """
    Send an alert via Telegram (if configured) and always print to console.
    level: INFO / WARNING / CRITICAL
    Never raises exceptions.
    """
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    console_msg = f"[{ts}] [{level}] {message}"
    print(console_msg)

    if not TELEGRAM_ENABLED:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.warning("Telegram send failed (%d): %s", resp.status, body[:200])
    except Exception as e:
        log.warning("Telegram send error: %s", e)


# ── Pre-built alert messages ────────────────────────────────────────────────

async def trade_opened(signal, size: float, order_id: str, dry_run: bool):
    """Alert when a new trade is opened."""
    tag = " [DRY RUN]" if dry_run else ""
    msg = (
        f"\U0001f514 {signal.get('asset', '?')} {signal.get('direction', '?')} "
        f"| ${size:.2f} @ {signal.get('entry_price', 0):.2f} "
        f"| edge={signal.get('edge_pct', 0) * 100:.1f}% "
        f"| id={order_id}{tag}"
    )
    await send_alert(msg)


async def trade_closed(trade_id, outcome: str, net_pnl: float, new_bankroll: float):
    """Alert when a trade is closed."""
    if outcome == "WIN":
        emoji = "\u2705"
        sign = f"+${net_pnl:.2f}"
    else:
        emoji = "\u274c"
        sign = f"-${abs(net_pnl):.2f}"
    msg = f"{emoji} {outcome} {sign} | bankroll=${new_bankroll:.2f}"
    await send_alert(msg)


async def halt_triggered(reason: str, bankroll: float):
    """Alert when the bot halts."""
    msg = f"\U0001f6d1 HALT: {reason} | bankroll=${bankroll:.2f}"
    await send_alert(msg, level="CRITICAL")


async def daily_summary(stats_dict: dict):
    """End-of-day summary alert."""
    wins = stats_dict.get("wins", 0)
    losses = stats_dict.get("losses", 0)
    total = wins + losses
    wr = (wins / total * 100) if total > 0 else 0.0
    pnl = stats_dict.get("total_net_pnl", 0)
    bankroll = stats_dict.get("bankroll", 0)
    msg = (
        f"\U0001f4ca Day summary: {wins}W/{losses}L ({wr:.1f}%) "
        f"| P&L=${pnl:+.2f} | bankroll=${bankroll:.2f}"
    )
    await send_alert(msg)


async def startup(bankroll: float, dry_run_mode: bool):
    """Alert on bot startup."""
    mode = "DRY RUN" if dry_run_mode else "LIVE"
    msg = f"\U0001f680 Bot started | mode={mode} | bankroll=${bankroll:.2f}"
    await send_alert(msg)
