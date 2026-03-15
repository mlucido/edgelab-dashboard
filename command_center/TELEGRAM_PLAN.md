# Telegram Unification Plan

## Current State

The bot's Telegram alerting lives in `polymarket-sim/alert_manager.py`:
- Uses `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` from environment
- Sends alerts via `aiohttp` POST to `https://api.telegram.org/bot{token}/sendMessage`
- Pre-built message functions: `trade_opened()`, `trade_closed()`, `halt_triggered()`, `daily_summary()`, `startup()`
- All messages go through `send_alert(message, level)` — single choke point

EdgeLab has **no alerting** currently (no alert code exists anywhere in the EdgeLab directory).

## Proposed Prefix Format

All Telegram messages should be prefixed with a source tag:

| Source | Prefix | Example |
|--------|--------|---------|
| Bot (Kalshi) | `[BOT]` | `[BOT] 🔔 BTC-USD UP \| $12.00 @ 0.04` |
| EdgeLab | `[EDGE]` | `[EDGE] 📡 New signal: AAPL breakout detected` |
| System | `[SYS]` | `[SYS] ⚠️ Dashboard offline` |

## Implementation Steps

### Step 1: Add prefix to existing bot alerts

**File**: `polymarket-sim/alert_manager.py`
**Change**: Modify `send_alert()` to prepend `[BOT]` to all messages.

```python
# Line 35 — change:
console_msg = f"[{ts}] [{level}] {message}"

# To:
console_msg = f"[{ts}] [{level}] [BOT] {message}"

# Line 46 — change:
payload = {
    "chat_id": TELEGRAM_CHAT_ID,
    "text": message,

# To:
payload = {
    "chat_id": TELEGRAM_CHAT_ID,
    "text": f"[BOT] {message}",
```

### Step 2: Create EdgeLab alert sender

Create a new file: `edge/alert_sender.py` (when EdgeLab strategy is built)

```python
"""Reuse the bot's Telegram config to send [EDGE] alerts."""
import os
import aiohttp

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

async def send_edge_alert(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[EDGE] {message}")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": f"[EDGE] {message}",
        "parse_mode": "HTML",
    }
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10))
    except Exception:
        pass
```

### Step 3: Wire EdgeLab alerts into strategy code

When EdgeLab strategy is built, call `send_edge_alert()` at these points:
- Signal detected: `[EDGE] 📡 {asset} {signal_type} — {details}`
- Trade opened: `[EDGE] 🔔 Opened {asset} @ ${price}`
- Trade closed: `[EDGE] ✅/❌ {outcome} ${pnl}`
- Error/halt: `[EDGE] 🛑 {reason}`

### Step 4: Same Telegram chat

Both `[BOT]` and `[EDGE]` alerts go to the **same** `TELEGRAM_CHAT_ID`. This gives a unified view in one Telegram thread. The prefixes make it easy to distinguish source at a glance.

If volume gets too high, consider separate chat IDs per strategy using `TELEGRAM_CHAT_ID_BOT` and `TELEGRAM_CHAT_ID_EDGE`.
