"""
EdgeLab BaseStrategy
--------------------
Abstract base class for all EdgeLab trading strategies.
"""

import asyncio
import logging
import os
import uuid
from abc import ABC, abstractmethod
from logging.handlers import RotatingFileHandler
from pathlib import Path

from framework.risk_engine import BankrollManager, KellyCalculator, RiskEngine

# ─── Constants ───────────────────────────────────────────────────────────────

LOG_DIR = Path(__file__).resolve().parent.parent / "strategies" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

STRATEGY_MODE = os.getenv("STRATEGY_MODE", "paper").lower()  # "paper" | "live"


# ─── BaseStrategy ─────────────────────────────────────────────────────────────

class BaseStrategy(ABC):
    """
    Abstract base for all EdgeLab strategies.

    Subclasses must implement:
        scan()    — find opportunities
        score()   — return edge score 0.0-1.0
        execute() — place trade, return trade record dict
    """

    MIN_SCORE_THRESHOLD: float = 0.08

    def __init__(
        self,
        strategy_name: str,
        platform: str,
        interval_seconds: int = 60,
    ):
        self.strategy_name = strategy_name
        self.platform = platform
        self.interval_seconds = interval_seconds
        self.mode = STRATEGY_MODE

        self._risk = RiskEngine()
        self._kelly = KellyCalculator()
        self._bankroll_mgr = BankrollManager(self._risk)

        self.logger = self._build_logger(strategy_name)
        self.logger.info(
            "Initialized strategy=%s platform=%s mode=%s",
            strategy_name, platform, self.mode,
        )

    # ── Logger ───────────────────────────────────────────────────────────────

    @staticmethod
    def _build_logger(name: str) -> logging.Logger:
        log_path = LOG_DIR / f"{name}.log"
        logger = logging.getLogger(f"edgelab.{name}")
        logger.setLevel(logging.DEBUG)

        if not logger.handlers:
            handler = RotatingFileHandler(
                log_path, maxBytes=10 * 1024 * 1024, backupCount=3
            )
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s %(levelname)s [%(name)s] %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S",
                )
            )
            logger.addHandler(handler)

            # Also surface to stdout in paper mode
            if STRATEGY_MODE == "paper":
                console = logging.StreamHandler()
                console.setLevel(logging.INFO)
                console.setFormatter(logging.Formatter(
                    "%(asctime)s %(levelname)s [%(name)s] %(message)s",
                    datefmt="%H:%M:%S",
                ))
                logger.addHandler(console)

        return logger

    # ── Abstract interface ───────────────────────────────────────────────────

    @abstractmethod
    async def scan(self) -> list[dict]:
        """Discover tradeable opportunities. Returns list of opportunity dicts."""
        ...

    @abstractmethod
    async def score(self, opp: dict) -> float:
        """Score an opportunity. Returns edge score in [0.0, 1.0]."""
        ...

    @abstractmethod
    async def execute(self, opp: dict, size: float) -> dict:
        """
        Execute a trade.

        In paper mode, subclasses should log instead of sending live orders and
        return a mock fill dict. In live mode, send the actual order.

        Returns a trade record dict (must contain at least 'trade_id').
        """
        ...

    # ── Concrete cycle logic ─────────────────────────────────────────────────

    async def run_cycle(self):
        """Single scan-score-execute cycle."""
        opps = await self.scan()
        self.logger.debug("scan() returned %d opportunities", len(opps))

        for opp in opps:
            score = await self.score(opp)

            if score < self.MIN_SCORE_THRESHOLD:
                self.logger.debug(
                    "Skipping opp=%s score=%.4f below threshold=%.2f",
                    opp.get("market_id", "?"), score, self.MIN_SCORE_THRESHOLD,
                )
                continue

            # Get current allocation for position sizing
            allocation = await self._bankroll_mgr.get_allocation(self.strategy_name)
            size = self._kelly.fractional_kelly(
                edge=score, odds=1.0, bankroll=allocation
            )

            allowed, reason = await self._risk.check_trade_allowed(self.strategy_name, size)
            if not allowed:
                self.logger.warning(
                    "Trade blocked strategy=%s reason=%s", self.strategy_name, reason
                )
                continue

            # Paper mode: wrap execute() to always mock if mode == paper
            if self.mode == "paper":
                trade = await self._paper_execute(opp, size)
            else:
                trade = await self.execute(opp, size)

            trade_id = trade.get("trade_id", str(uuid.uuid4()))
            market_id = opp.get("market_id", "unknown")
            entry_price = trade.get("entry_price", opp.get("price", 0.0))

            await self._risk.record_trade_open(
                strategy_name=self.strategy_name,
                trade_id=trade_id,
                size=size,
                entry_price=entry_price,
                market_id=market_id,
            )
            self.logger.info(
                "Trade opened trade_id=%s market_id=%s size=%.2f entry=%.4f score=%.4f",
                trade_id, market_id, size, entry_price, score,
            )

    async def _paper_execute(self, opp: dict, size: float) -> dict:
        """Mock fill for paper mode — calls execute() but wraps with logging."""
        self.logger.info(
            "[PAPER] Simulated trade opp=%s size=%.2f",
            opp.get("market_id", "?"), size,
        )
        try:
            return await self.execute(opp, size)
        except NotImplementedError:
            return {
                "trade_id": str(uuid.uuid4()),
                "entry_price": opp.get("price", 0.5),
                "paper": True,
            }

    async def run(self):
        """Main loop: run cycles indefinitely, sleeping interval_seconds between."""
        self.logger.info(
            "Starting run loop strategy=%s interval=%ds mode=%s",
            self.strategy_name, self.interval_seconds, self.mode,
        )
        while True:
            try:
                await self.run_cycle()
            except Exception as exc:
                self.logger.error(
                    "Error in run_cycle strategy=%s: %s",
                    self.strategy_name, exc,
                    exc_info=True,
                )
            await asyncio.sleep(self.interval_seconds)
