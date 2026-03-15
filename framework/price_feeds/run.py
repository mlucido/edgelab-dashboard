import asyncio
import json
import logging
import os
import signal
import sys

import redis.asyncio as aioredis
from dotenv import load_dotenv

# Ensure parent packages are importable when run as a script
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from framework.price_feeds.aggregator import MultiVenuePriceAggregator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def _health_reporter(aggregator: MultiVenuePriceAggregator) -> None:
    """Log a health summary every 60s."""
    while True:
        await asyncio.sleep(60)
        try:
            report = await aggregator.health_report()
            logger.info("[HealthReport] Feed status: %s", json.dumps(report, indent=2))

            for asset in ["BTC", "ETH", "SOL"]:
                div = await aggregator.get_divergence(asset)
                eur = await aggregator.get_eur_confirmation(asset)
                funding = await aggregator.get_funding_sentiment(asset)
                logger.info(
                    "[HealthReport] %s — divergence=%.6f  eur_confirmed=%s  funding=%s",
                    asset, div, eur, funding,
                )
        except Exception as e:
            logger.warning("[HealthReport] Error: %s", e)


async def _run() -> None:
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

    redis_client = aioredis.Redis(host="localhost", port=6379, decode_responses=True)
    aggregator = MultiVenuePriceAggregator(redis_client)

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _signal_handler():
        logger.info("[Runner] Shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    await aggregator.start()
    health_task = asyncio.create_task(_health_reporter(aggregator))

    logger.info("[Runner] Price feed aggregator running — Ctrl+C to stop")

    await shutdown_event.wait()

    health_task.cancel()
    try:
        await health_task
    except asyncio.CancelledError:
        pass

    # Print final health report before exit
    report = await aggregator.health_report()
    print("\n=== Final Health Report ===")
    print(json.dumps(report, indent=2))

    await aggregator.stop()
    await redis_client.aclose()
    logger.info("[Runner] Clean shutdown complete")


def run_aggregator() -> None:
    """Entry point for running the aggregator."""
    asyncio.run(_run())


if __name__ == "__main__":
    run_aggregator()
