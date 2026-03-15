"""
Alerting — Telegram Bot via httpx (no python-telegram-bot dependency).

Alert events:
    CIRCUIT_BREAKER_TRIGGERED  — trading halted
    DAILY_MILESTONE            — P&L milestone hit
    STRATEGY_PAUSED            — strategy paused after consecutive losses
    BOT_DOWN                   — main loop crashed
    TRADE_PLACED               — trade executed (off by default)
    BOT_RESTARTED              — bot process restarted
    LIVE_MODE_ACTIVATED        — live trading mode enabled
    TEST                       — test alert

Configuration (env vars):
    TELEGRAM_TOKEN    — bot token from BotFather
    TELEGRAM_CHAT_ID  — chat/group ID to send to

If either env var is missing, all alerts are written to logs/alerts.log.

Exports:
    send_alert(event_type, message, data=None)  — synchronous, direct send
    send_alert_async(event_type, details)        — async, fire-and-forget safe
    run()                                        — async loop subscribing to Redis "alerts" channel
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CHANNEL = "alerts"

# TRADE_PLACED alerts are off by default — set to "1" to enable
TRADE_ALERTS_ENABLED = os.getenv("TRADE_ALERTS_ENABLED", "0") == "1"

# Log file fallback when Telegram credentials are absent
_LOG_DIR = Path(__file__).parent.parent.parent / "logs"
_ALERT_LOG = _LOG_DIR / "alerts.log"

_USE_TELEGRAM = bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)

# ---------------------------------------------------------------------------
# Emoji mapping
# ---------------------------------------------------------------------------

EMOJI_MAP: dict[str, str] = {
    "CIRCUIT_BREAKER": "\u26a0\ufe0f",
    "CIRCUIT_BREAKER_TRIGGERED": "\u26a0\ufe0f",
    "STRATEGY_PAUSED": "\U0001f534",
    "BOT_RESTARTED": "\U0001f504",
    "DAILY_MILESTONE": "\U0001f4b0",
    "LIVE_MODE_ACTIVATED": "\U0001f6a8",
    "TRADE_PLACED": "\U0001f4ca",
    "BOT_DOWN": "\U0001f6a8",
    "TEST": "\U0001f9ea",
}

# ---------------------------------------------------------------------------
# Message templates
# ---------------------------------------------------------------------------

_TEMPLATES: dict[str, str] = {
    "CIRCUIT_BREAKER_TRIGGERED": (
        "\u26a0\ufe0f EdgeLab: Circuit breaker fired. Reason: {reason}. Trading paused."
    ),
    "DAILY_MILESTONE": (
        "\U0001f4b0 EdgeLab: Daily P&L hit +${daily_pnl:.2f}. Total all-time: ${alltime_pnl:.2f}"
    ),
    "STRATEGY_PAUSED": (
        "\U0001f534 EdgeLab: {strategy} paused after 3 consecutive losses."
    ),
    "BOT_DOWN": (
        "\U0001f6a8 EdgeLab: Main loop stopped unexpectedly. Check server."
    ),
    "TRADE_PLACED": (
        "\U0001f4ca EdgeLab: Trade placed. {market}. ${size:.2f} at {prob:.0f}%"
    ),
}


def _format_message(event_type: str, details: dict[str, Any]) -> str:
    """Build a human-readable alert message from event type + details dict."""
    template = _TEMPLATES.get(event_type)
    if template is None:
        return f"EdgeLab alert: {event_type} — {json.dumps(details)}"
    try:
        return template.format(**details)
    except (KeyError, ValueError):
        # Fall back to raw representation when details keys don't match template
        return f"EdgeLab alert: {event_type} — {json.dumps(details)}"


# ---------------------------------------------------------------------------
# Delivery backends
# ---------------------------------------------------------------------------

async def _send_telegram(message: str) -> None:
    """POST a message to Telegram. Logs but never raises."""
    url = TELEGRAM_API_URL.format(token=TELEGRAM_TOKEN)
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(url, json=payload)
        if response.status_code != 200:
            logger.error(
                "Telegram send failed: status=%d body=%s",
                response.status_code,
                response.text[:200],
            )
        else:
            logger.debug("Telegram alert sent: %s", message[:80])


def _write_alert_log(event_type: str, message: str) -> None:
    """Append alert to logs/alerts.log with UTC timestamp."""
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"[{ts}] [{event_type}] {message}\n"
    try:
        with _ALERT_LOG.open("a") as fh:
            fh.write(line)
    except OSError as exc:
        logger.error("Failed to write alert log: %s", exc)


# ---------------------------------------------------------------------------
# Public API — synchronous direct send
# ---------------------------------------------------------------------------

def send_alert(event_type: str, message: str, data: dict[str, Any] | None = None) -> bool:
    """
    Send an alert synchronously — can be called from any non-async context.

    Loads TELEGRAM_TOKEN and TELEGRAM_CHAT_ID fresh from the environment on
    each call so that tests can patch os.environ without restarting the module.

    Parameters
    ----------
    event_type:
        Alert category, e.g. LIVE_MODE_ACTIVATED, CIRCUIT_BREAKER, TEST.
    message:
        Human-readable message body.
    data:
        Optional extra payload (unused for delivery, available for future use).

    Returns True if the alert was delivered (Telegram) or logged successfully.
    Returns False only if both Telegram and log fallback failed.
    """
    # TRADE_PLACED is opt-in
    if event_type == "TRADE_PLACED":
        trade_alerts = os.getenv("TRADE_ALERTS_ENABLED", "0") == "1"
        if not trade_alerts:
            logger.debug("TRADE_PLACED alert suppressed (TRADE_ALERTS_ENABLED=0)")
            return False

    emoji = EMOJI_MAP.get(event_type, "")

    # Special formatting for TRADE_PLACED
    if event_type == "TRADE_PLACED" and data:
        market_q = data.get("market_question", data.get("market", ""))[:40]
        size = data.get("size", data.get("size_usdc", "?"))
        prob = data.get("prob", data.get("entry_prob", "?"))
        strategy = data.get("strategy", "")
        if isinstance(prob, float):
            prob = f"{prob * 100:.0f}" if prob <= 1.0 else f"{prob:.0f}"
        formatted = f"{emoji} {market_q} | ${size} @ {prob}% | {strategy}"
    else:
        formatted = f"{emoji} EdgeLab: {message}".strip()

    token = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

    if token and chat_id:
        url = TELEGRAM_API_URL.format(token=token)
        payload = {"chat_id": chat_id, "text": formatted, "parse_mode": "HTML"}
        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.post(url, json=payload)
            if response.status_code == 200:
                logger.debug("Telegram alert sent: %s", formatted[:80])
                return True
            else:
                logger.error(
                    "Telegram send failed: status=%d body=%s — falling back to log",
                    response.status_code,
                    response.text[:200],
                )
                _write_alert_log(event_type, formatted)
                return False
        except Exception as exc:
            logger.error("Telegram alert error (%s): %s — falling back to log", event_type, exc)
            _write_alert_log(event_type, formatted)
            return False
    else:
        _write_alert_log(event_type, formatted)
        logger.info("Alert logged (no Telegram): [%s] %s", event_type, formatted)
        return True


# ---------------------------------------------------------------------------
# Public API — async (Redis subscriber + legacy callers)
# ---------------------------------------------------------------------------

async def send_alert_async(event_type: str, details: dict[str, Any]) -> None:
    """
    Dispatch a single alert asynchronously (used by Redis subscriber loop).

    Parameters
    ----------
    event_type:
        One of CIRCUIT_BREAKER_TRIGGERED, DAILY_MILESTONE, STRATEGY_PAUSED,
        BOT_DOWN, TRADE_PLACED (or any custom string).
    details:
        Template variables. Must match the keys in _TEMPLATES[event_type].
    """
    # TRADE_PLACED is opt-in
    if event_type == "TRADE_PLACED" and not TRADE_ALERTS_ENABLED:
        logger.debug("TRADE_PLACED alert suppressed (TRADE_ALERTS_ENABLED=0)")
        return

    message = _format_message(event_type, details)

    if _USE_TELEGRAM:
        try:
            await _send_telegram(message)
        except Exception as exc:
            logger.error("Telegram alert error (%s): %s — falling back to log", event_type, exc)
            _write_alert_log(event_type, message)
    else:
        _write_alert_log(event_type, message)
        logger.info("Alert logged (no Telegram): [%s] %s", event_type, message)


async def run() -> None:
    """
    Subscribe to the Redis 'alerts' channel and forward each message.

    Publishers send: redis.publish("alerts", json.dumps({"type": "...", "details": {...}}))

    Runs forever; handles reconnection on errors.
    """
    import redis.asyncio as aioredis  # local import — optional at module level

    logger.info("Alert subscriber starting on channel '%s'", REDIS_CHANNEL)

    while True:
        try:
            r = await aioredis.from_url(REDIS_URL, decode_responses=True)
            pubsub = r.pubsub()
            await pubsub.subscribe(REDIS_CHANNEL)
            logger.info("Subscribed to Redis channel '%s'", REDIS_CHANNEL)

            async for raw_message in pubsub.listen():
                if raw_message["type"] != "message":
                    continue
                data = raw_message.get("data", "")
                try:
                    payload = json.loads(data)
                    event_type = payload.get("type", "UNKNOWN")
                    details = payload.get("details", {})
                    await send_alert_async(event_type, details)
                except (json.JSONDecodeError, AttributeError) as exc:
                    logger.warning("Malformed alert payload '%s': %s", data[:100], exc)

        except asyncio.CancelledError:
            logger.info("Alert subscriber shutting down.")
            return
        except Exception as exc:
            logger.error("Alert subscriber error: %s — reconnecting in 5s", exc)
            await asyncio.sleep(5)
