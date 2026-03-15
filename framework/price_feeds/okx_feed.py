import asyncio
import logging
import time

import aiohttp

logger = logging.getLogger(__name__)

OKX_FUNDING_URL = "https://www.okx.com/api/v5/public/funding-rate"

INST_IDS = {
    "BTC": "BTC-USDT-SWAP",
    "ETH": "ETH-USDT-SWAP",
    "SOL": "SOL-USDT-SWAP",
}


class OkxFundingFeed:
    """OKX perpetual funding rate poller (60s interval)."""

    def __init__(self):
        self._rates: dict[str, dict] = {}
        self._session: aiohttp.ClientSession | None = None
        self._task: asyncio.Task | None = None
        self._running = False
        self._last_success: float = 0.0

    @property
    def name(self) -> str:
        return "OkxFundingFeed"

    @property
    def is_healthy(self) -> bool:
        return (time.time() - self._last_success) < 120.0

    async def connect(self) -> None:
        self._running = True
        self._session = aiohttp.ClientSession()
        self._task = asyncio.create_task(self._poll_loop())
        logger.info("[OkxFundingFeed] Polling started")

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
        logger.info("[OkxFundingFeed] Disconnected")

    def get_funding_rate(self, asset: str) -> dict:
        cached = self._rates.get(asset)
        if cached:
            stale = (time.time() - cached["timestamp"]) > 120
            return {**cached, "stale": stale}
        return {"current": 0.0, "next": 0.0, "timestamp": 0, "stale": True}

    async def _poll_loop(self) -> None:
        backoff = 2.0
        while self._running:
            for asset, inst_id in INST_IDS.items():
                try:
                    params = {"instId": inst_id}
                    async with self._session.get(
                        OKX_FUNDING_URL, params=params,
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        resp.raise_for_status()
                        body = await resp.json()

                    entry = body["data"][0]
                    next_raw = entry.get("nextFundingRate", "")
                    self._rates[asset] = {
                        "current": float(entry["fundingRate"]),
                        "next": float(next_raw) if next_raw else 0.0,
                        "timestamp": time.time(),
                    }
                    self._last_success = time.time()
                    backoff = 2.0

                except asyncio.CancelledError:
                    return
                except Exception as e:
                    logger.warning("[OkxFundingFeed] Error fetching %s: %s", asset, e)

            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                return
