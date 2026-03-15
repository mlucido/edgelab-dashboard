import asyncio
import json
import logging
import time

import websockets

from .base import PriceFeed

logger = logging.getLogger(__name__)

KRAKEN_WS_URL = "wss://ws.kraken.com/v2"
SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD", "BTC/EUR", "ETH/EUR", "SOL/EUR"]


class KrakenFeed(PriceFeed):
    """Kraken WebSocket v2 price feed."""

    def __init__(self):
        super().__init__()
        self._prices: dict[str, float] = {}
        self._ws = None
        self._task: asyncio.Task | None = None
        self._running = False

    @property
    def name(self) -> str:
        return "KrakenFeed"

    async def connect(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._listen())
        logger.info("[KrakenFeed] Connection started")

    async def disconnect(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[KrakenFeed] Disconnected")

    async def get_price(self, symbol: str) -> float | None:
        return self._prices.get(symbol)

    async def _listen(self) -> None:
        backoff = 2.0
        while self._running:
            try:
                async with websockets.connect(KRAKEN_WS_URL) as ws:
                    self._ws = ws
                    backoff = 2.0
                    logger.info("[KrakenFeed] WebSocket connected")

                    sub_msg = json.dumps({
                        "method": "subscribe",
                        "params": {"channel": "ticker", "symbol": SYMBOLS},
                    })
                    await ws.send(sub_msg)

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        if msg.get("channel") != "ticker":
                            continue

                        for tick in msg.get("data", []):
                            symbol = tick.get("symbol")
                            last = tick.get("last")
                            if symbol and last is not None:
                                self._prices[symbol] = float(last)
                                self._last_success = time.time()

            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("[KrakenFeed] Connection error: %s — retrying in %.0fs", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
