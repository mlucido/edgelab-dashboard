import asyncio
import json
import logging
import time
from collections import deque
from statistics import mean

from .kraken_feed import KrakenFeed
from .exmo_feed import ExmoFeed
from .okx_feed import OkxFundingFeed
from .bybit_feed import BybitFundingFeed

logger = logging.getLogger(__name__)

ASSETS = ["BTC", "ETH", "SOL"]
USD_SYMBOLS = {"BTC": "BTC/USD", "ETH": "ETH/USD", "SOL": "SOL/USD"}
EUR_SYMBOLS = {"BTC": "BTC/EUR", "ETH": "ETH/EUR", "SOL": "SOL/EUR"}


class MultiVenuePriceAggregator:
    """Aggregates prices from multiple venues and writes signals to Redis."""

    def __init__(self, redis_client):
        self.feeds = [KrakenFeed(), ExmoFeed()]
        self.okx = OkxFundingFeed()
        self.bybit = BybitFundingFeed()
        self.redis = redis_client
        self._price_cache: dict[str, dict[str, float]] = {}
        self._eur_history: dict[str, deque] = {a: deque(maxlen=3) for a in ASSETS}
        self._usd_history: dict[str, deque] = {a: deque(maxlen=3) for a in ASSETS}
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        # Connect all feeds concurrently
        connect_tasks = [f.connect() for f in self.feeds]
        connect_tasks.append(self.okx.connect())
        connect_tasks.append(self.bybit.connect())
        await asyncio.gather(*connect_tasks, return_exceptions=True)

        self._tasks.append(asyncio.create_task(self._update_loop()))
        self._tasks.append(asyncio.create_task(self._funding_loop()))
        logger.info("[Aggregator] Started — %d price feeds, 2 funding feeds", len(self.feeds))

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass

        disconnect_tasks = [f.disconnect() for f in self.feeds]
        disconnect_tasks.append(self.okx.disconnect())
        disconnect_tasks.append(self.bybit.disconnect())
        await asyncio.gather(*disconnect_tasks, return_exceptions=True)
        logger.info("[Aggregator] Stopped")

    async def _update_loop(self) -> None:
        while True:
            try:
                await self._update_prices()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("[Aggregator] Update loop error: %s", e)
            await asyncio.sleep(10)

    async def _update_prices(self) -> None:
        now = time.time()
        pipe = self.redis.pipeline()

        for asset in ASSETS:
            usd_sym = USD_SYMBOLS[asset]
            eur_sym = EUR_SYMBOLS[asset]

            # Collect USD prices from healthy feeds
            usd_prices: dict[str, float] = {}
            for feed in self.feeds:
                if not feed.is_healthy:
                    continue
                price = await feed.get_price(usd_sym)
                if price is not None and price > 0:
                    usd_prices[feed.name.lower().replace("feed", "")] = price

            # Write per-venue price keys
            for venue, price in usd_prices.items():
                key = f"edgelab:prices:{venue}:{asset}"
                pipe.set(key, str(price), ex=60)

            # Divergence score
            if len(usd_prices) >= 2:
                vals = list(usd_prices.values())
                mn, mx = min(vals), max(vals)
                div_score = (mx - mn) / mn if mn > 0 else 0.0
            else:
                div_score = 0.0

            pipe.set(
                f"edgelab:signals:divergence:{asset}",
                json.dumps({"score": div_score, "ts": now, "prices": usd_prices}),
                ex=120,
            )

            # EUR confirmation logic
            eur_prices_collected: dict[str, float] = {}
            for feed in self.feeds:
                if not feed.is_healthy:
                    continue
                price = await feed.get_price(eur_sym)
                if price is not None and price > 0:
                    eur_prices_collected[feed.name] = price

            # Convert EUR prices to USD via implied rate from Kraken
            kraken_btc_usd = await self.feeds[0].get_price("BTC/USD")
            kraken_btc_eur = await self.feeds[0].get_price("BTC/EUR")

            if eur_prices_collected and kraken_btc_usd and kraken_btc_eur and kraken_btc_eur > 0:
                eur_usd_implied = kraken_btc_usd / kraken_btc_eur
                avg_eur = mean(eur_prices_collected.values())
                eur_as_usd = avg_eur * eur_usd_implied
                self._eur_history[asset].append(eur_as_usd)

            if usd_prices:
                avg_usd = mean(usd_prices.values())
                self._usd_history[asset].append(avg_usd)

            eur_confirmed = True  # fail open
            eur_momentum = 0.0
            usd_momentum = 0.0

            eur_hist = self._eur_history[asset]
            usd_hist = self._usd_history[asset]

            if len(eur_hist) >= 2 and len(usd_hist) >= 2:
                eur_momentum = (eur_hist[-1] - eur_hist[0]) / eur_hist[0] if eur_hist[0] > 0 else 0.0
                usd_momentum = (usd_hist[-1] - usd_hist[0]) / usd_hist[0] if usd_hist[0] > 0 else 0.0

                same_sign = (eur_momentum >= 0) == (usd_momentum >= 0)
                eur_confirmed = same_sign and abs(eur_momentum) > 0.0005

            pipe.set(
                f"edgelab:signals:eur_confirm:{asset}",
                json.dumps({
                    "confirmed": eur_confirmed,
                    "eur_momentum": round(eur_momentum, 8),
                    "usd_momentum": round(usd_momentum, 8),
                    "ts": now,
                }),
                ex=120,
            )

        await pipe.execute()

    async def _funding_loop(self) -> None:
        while True:
            try:
                await self._update_funding()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("[Aggregator] Funding loop error: %s", e)
            await asyncio.sleep(60)

    async def _update_funding(self) -> None:
        now = time.time()
        pipe = self.redis.pipeline()

        for asset in ASSETS:
            okx_data = self.okx.get_funding_rate(asset)
            bybit_data = self.bybit.get_funding_rate(asset)

            rates = []
            if not okx_data.get("stale", True):
                rates.append(okx_data["current"])
            if not bybit_data.get("stale", True):
                rates.append(bybit_data["current"])

            avg_funding = mean(rates) if rates else 0.0

            if avg_funding < -0.0001:
                sentiment = "bullish"
            elif avg_funding > 0.0001:
                sentiment = "bearish"
            else:
                sentiment = "neutral"

            pipe.set(
                f"edgelab:signals:funding:{asset}",
                json.dumps({
                    "okx": okx_data["current"],
                    "bybit": bybit_data["current"],
                    "avg": round(avg_funding, 8),
                    "sentiment": sentiment,
                    "next_okx": okx_data.get("next", 0.0),
                    "ts": now,
                }),
                ex=600,
            )

        await pipe.execute()

    async def get_divergence(self, asset: str) -> float:
        raw = await self.redis.get(f"edgelab:signals:divergence:{asset}")
        if raw:
            return json.loads(raw).get("score", 0.0)
        return 0.0

    async def get_eur_confirmation(self, asset: str) -> bool:
        raw = await self.redis.get(f"edgelab:signals:eur_confirm:{asset}")
        if raw:
            return json.loads(raw).get("confirmed", True)
        return True  # fail open

    async def get_funding_sentiment(self, asset: str) -> str:
        raw = await self.redis.get(f"edgelab:signals:funding:{asset}")
        if raw:
            return json.loads(raw).get("sentiment", "neutral")
        return "neutral"

    async def health_report(self) -> dict:
        now = time.time()
        report = {}
        for feed in self.feeds:
            report[feed.name] = {
                "is_healthy": feed.is_healthy,
                "last_price_age_seconds": round(now - feed._last_success, 1) if feed._last_success > 0 else None,
            }
        for funding_feed in [self.okx, self.bybit]:
            report[funding_feed.name] = {
                "is_healthy": funding_feed.is_healthy,
                "last_price_age_seconds": round(now - funding_feed._last_success, 1) if funding_feed._last_success > 0 else None,
            }
        return report
