import asyncio
import logging
import time

import aiohttp

from .base import PriceFeed

logger = logging.getLogger(__name__)

EXMO_TICKER_URL = "https://api.exmo.com/v1.1/ticker"

# EXMO uses underscore pairs; we normalize to slash format
PAIRS = {
    "BTC_USD": "BTC/USD",
    "ETH_USD": "ETH/USD",
    "SOL_USD": "SOL/USD",
    "BTC_EUR": "BTC/EUR",
    "ETH_EUR": "ETH/EUR",
}
# SOL/EUR not available on EXMO — omitted intentionally


class ExmoFeed(PriceFeed):
    """EXMO REST polling price feed (15s interval)."""

    def __init__(self):
        super().__init__()
        self._prices: dict[str, float] = {}
        self._session: aiohttp.ClientSession | None = None
        self._task: asyncio.Task | None = None
        self._running = False

    @property
    def name(self) -> str:
        return "ExmoFeed"

    async def connect(self) -> None:
        self._running = True
        self._session = aiohttp.ClientSession()
        self._task = asyncio.create_task(self._poll_loop())
        logger.info("[ExmoFeed] Polling started")

    async def disconnect(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()
        logger.info("[ExmoFeed] Disconnected")

    async def get_price(self, symbol: str) -> float | None:
        return self._prices.get(symbol)

    async def _poll_loop(self) -> None:
        backoff = 2.0
        while self._running:
            try:
                async with self._session.get(EXMO_TICKER_URL, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    resp.raise_for_status()
                    data = await resp.json()

                for exmo_pair, normalized in PAIRS.items():
                    if exmo_pair in data:
                        try:
                            self._prices[normalized] = float(data[exmo_pair]["last_trade"])
                            self._last_success = time.time()
                        except (KeyError, ValueError, TypeError) as e:
                            logger.warning("[ExmoFeed] Parse error for %s: %s", exmo_pair, e)

                backoff = 2.0
                await asyncio.sleep(15)

            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("[ExmoFeed] Poll error: %s — retrying in %.0fs", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
