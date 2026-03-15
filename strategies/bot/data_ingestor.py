"""
data_ingestor.py — Live Coinbase Advanced Trade WebSocket price feed.

Subscribes to the "ticker" channel for real-time price + 24h volume.
No API key required — ticker is a public channel.

Calls database.log_price() on every new tick so the signal engine
always has fresh data to work with.
"""

import asyncio
import json
import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import Callable, Dict, Optional

import websockets

import config
import database as db

log = logging.getLogger(__name__)


class PriceState:
    """Thread-safe in-memory price state for one asset."""

    def __init__(self, asset: str):
        self.asset       = asset
        self.price       = 0.0
        self.volume_1m   = 0.0           # approximated from 24h volume
        self.last_update = 0.0           # unix timestamp
        # Rolling price history: (timestamp, price) tuples
        self._history: deque = deque(maxlen=600)   # ~10 min at 1/sec

    def update(self, price: float, ts: float):
        self.price       = price
        self.last_update = ts
        self._history.append((ts, price))

    def update_volume(self, volume: float):
        self.volume_1m = volume

    def is_stale(self) -> bool:
        return (time.time() - self.last_update) > config.STALE_DATA_SECS

    def price_n_seconds_ago(self, seconds: int) -> Optional[float]:
        """Return the price closest to `seconds` ago, or None if not enough history."""
        target_ts = time.time() - seconds
        # Walk backwards through history to find the closest point
        for ts, price in reversed(self._history):
            if ts <= target_ts:
                return price
        return None

    def rolling_volume_avg(self, periods: int = 5) -> float:
        """Average of the last N 1-min volumes. Returns current volume if unavailable."""
        return self.volume_1m   # simplified: full rolling avg needs multi-minute klines

    def momentum(self, window_secs: int = None) -> Optional[float]:
        """
        % price change over the last `window_secs` seconds.
        Returns None if not enough history.
        Positive = UP, Negative = DOWN.
        """
        window_secs = window_secs or config.MOMENTUM_WINDOW_SECS
        old_price = self.price_n_seconds_ago(window_secs)
        if old_price is None or old_price == 0:
            return None
        if self.is_stale():
            return None
        return (self.price - old_price) / old_price * 100   # as %


# Global state registry
_states: Dict[str, PriceState] = {}

def get_state(asset: str) -> PriceState:
    if asset not in _states:
        _states[asset] = PriceState(asset)
    return _states[asset]


async def _stream_asset(asset: str, stop_event: asyncio.Event):
    """
    Opens a Coinbase Advanced Trade WebSocket for one asset.
    Subscribes to the "ticker" channel for real-time price + volume.
    Reconnects automatically on disconnect.
    """
    url = config.COINBASE_WS_URL
    state = get_state(asset)
    backoff = 1
    first_price_logged = False

    while not stop_event.is_set():
        try:
            log.info("[%s] Connecting to Coinbase stream...", asset)
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                backoff = 1
                # Subscribe to ticker channel
                subscribe_msg = {
                    "type": "subscribe",
                    "product_ids": [asset],
                    "channel": "ticker",
                }
                await ws.send(json.dumps(subscribe_msg))
                log.info("[%s] Subscribed to ticker channel.", asset)

                async for raw in ws:
                    if stop_event.is_set():
                        break
                    try:
                        msg = json.loads(raw)

                        # Skip subscription confirmations and heartbeats
                        if msg.get("channel") != "ticker":
                            continue

                        events = msg.get("events", [])
                        for event in events:
                            tickers = event.get("tickers", [])
                            for ticker in tickers:
                                if ticker.get("product_id") != asset:
                                    continue

                                price = float(ticker["price"])
                                ts = time.time()
                                state.update(price, ts)

                                # Use 24h volume as volume proxy
                                vol_24h = float(ticker.get("volume_24_h", 0))
                                if vol_24h > 0:
                                    state.update_volume(vol_24h)

                                db.log_price(asset, price, state.volume_1m)

                                if not first_price_logged:
                                    log.info("[%s] First price received: $%.2f", asset, price)
                                    first_price_logged = True

                    except (KeyError, ValueError, json.JSONDecodeError) as e:
                        log.debug("Parse error on %s: %s", asset, e)

        except (websockets.exceptions.ConnectionClosed,
                websockets.exceptions.WebSocketException,
                ConnectionRefusedError, OSError) as e:
            log.warning("[%s] WS disconnected: %s. Reconnecting in %ds...", asset, e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except asyncio.CancelledError:
            break

    log.info("[%s] Stream stopped.", asset)


class DataIngestor:
    """
    Manages WebSocket streams for all configured assets.
    Call start() to launch all streams as background tasks.
    """

    def __init__(self):
        self._tasks = []
        self._stop  = asyncio.Event()

    async def start(self):
        self._stop.clear()
        for asset in config.ASSETS:
            task = asyncio.create_task(
                _stream_asset(asset, self._stop),
                name=f"stream-{asset}"
            )
            self._tasks.append(task)
        log.info("DataIngestor started for assets: %s", config.ASSETS)

    async def stop(self):
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        log.info("DataIngestor stopped.")

    def is_ready(self, asset: str) -> bool:
        """True once we have at least enough price history for the primary window."""
        state = get_state(asset)
        return (
            state.price > 0
            and not state.is_stale()
            and state.price_n_seconds_ago(config.MOMENTUM_WINDOW_SECS) is not None
        )

    def get_price(self, asset: str) -> float:
        return get_state(asset).price

    def get_momentum(self, asset: str, window_secs: int = None) -> Optional[float]:
        return get_state(asset).momentum(window_secs=window_secs)

    def get_volume_ratio(self, asset: str) -> float:
        state = get_state(asset)
        avg = state.rolling_volume_avg()
        if avg == 0:
            return 1.0
        return state.volume_1m / avg
