import asyncio
import logging
import time

import aiohttp

logger = logging.getLogger(__name__)

BYBIT_TICKERS_URL = "https://api.bybit.com/v5/market/tickers"

SYMBOLS = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "SOL": "SOLUSDT",
}


class BybitFundingFeed:
    """Bybit perpetual funding rate poller (60s interval)."""

    def __init__(self):
        self._rates: dict[str, dict] = {}
        self._session: aiohttp.ClientSession | None = None
        self._task: asyncio.Task | None = None
        self._running = False
        self._last_success: float = 0.0

    @property
    def name(self) -> str:
        return "BybitFundingFeed"

    @property
    def is_healthy(self) -> bool:
        return (time.time() - self._last_success) < 120.0

    async def connect(self) -> None:
        self._running = True
        self._session = aiohttp.ClientSession(
            headers={"User-Agent": "EdgeLab/1.0"},
        )
        self._task = asyncio.create_task(self._poll_loop())
        logger.info("[BybitFundingFeed] Polling started")

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
        logger.info("[BybitFundingFeed] Disconnected")

    def get_funding_rate(self, asset: str) -> dict:
        cached = self._rates.get(asset)
        if cached:
            stale = (time.time() - cached["timestamp"]) > 120
            return {**cached, "stale": stale}
        return {"current": 0.0, "timestamp": 0, "stale": True}

    async def _poll_loop(self) -> None:
        backoff = 2.0
        while self._running:
            for asset, symbol in SYMBOLS.items():
                try:
                    params = {"category": "linear", "symbol": symbol}
                    async with self._session.get(
                        BYBIT_TICKERS_URL, params=params,
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        resp.raise_for_status()
                        body = await resp.json()

                    entry = body["result"]["list"][0]
                    self._rates[asset] = {
                        "current": float(entry["fundingRate"]),
                        "timestamp": time.time(),
                    }
                    self._last_success = time.time()
                    backoff = 2.0

                except asyncio.CancelledError:
                    return
                except Exception as e:
                    logger.warning("[BybitFundingFeed] Error fetching %s: %s", asset, e)

            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                return
