"""
Risk Guard — pre-quote risk checks for liquidity provision.
Cross-strategy awareness: checks news_arb and resolution_lag signals.
"""
import logging
import os
import sys
import time
from collections import defaultdict
from typing import Optional

from . import config
from .inventory_manager import InventoryManager

# Add EdgeLab root to path for framework imports
sys.path.insert(0, os.path.expanduser("~/Dropbox/EdgeLab"))
sys.path.insert(0, os.path.expanduser("~/Dropbox (Personal)/EdgeLab"))

logger = logging.getLogger("liquidity_provision.risk_guard")

VOLATILITY_PAUSE_SECS = 15 * 60   # 15 minutes


class RiskGuard:
    """
    Runs all risk checks before the quote engine posts a quote.
    All checks return (allowed: bool, reason: str).
    """

    def __init__(self, inventory_manager: InventoryManager):
        self.inventory = inventory_manager
        # {market_id: (last_prices deque, pause_until)}
        self._price_history: dict = defaultdict(list)    # market_id → [(ts, mid), ...]
        self._volatility_pause: dict = {}                # market_id → pause_until timestamp
        # Cache for cross-strategy active signals
        self._news_arb_markets: set = set()
        self._resolution_lag_markets: set = set()
        self._signals_last_refresh = 0.0
        self._signals_ttl = 60.0   # refresh cross-strategy cache every 60 seconds

    def _refresh_cross_strategy_signals(self):
        """Pull recent trades from news_arb and resolution_lag to build exclusion sets."""
        now = time.time()
        if now - self._signals_last_refresh < self._signals_ttl:
            return

        try:
            from framework.trade_journal import get_recent_trades
            # news_arb: any trade in last 30 minutes = active signal
            news_trades = get_recent_trades(strategy_name="news_arb", limit=50)
            self._news_arb_markets = {t["market_id"] for t in news_trades[:20]}
        except Exception as e:
            logger.debug(f"Could not refresh news_arb signals: {e}")

        try:
            from framework.trade_journal import get_recent_trades
            lag_trades = get_recent_trades(strategy_name="resolution_lag", limit=50)
            self._resolution_lag_markets = {t["market_id"] for t in lag_trades[:20]}
        except Exception as e:
            logger.debug(f"Could not refresh resolution_lag signals: {e}")

        self._signals_last_refresh = now

    def record_price(self, market_id: str, mid: float):
        """Record a mid-price observation for volatility tracking."""
        now = time.time()
        history = self._price_history[market_id]
        history.append((now, mid))
        # Keep only last 15 minutes of observations
        cutoff = now - 15 * 60
        self._price_history[market_id] = [(t, p) for t, p in history if t >= cutoff]

    def _check_inventory(self, additional_usd: float) -> tuple:
        """Check if adding this position would exceed inventory limit."""
        if not self.inventory.check_inventory_limit(additional_usd):
            current = self.inventory.get_inventory_risk()
            return False, (f"Inventory limit: current=${current:.2f}, "
                           f"limit=${config.MAX_INVENTORY_USD:.2f}")
        return True, "ok"

    def _check_time_to_resolution(self, hours_remaining: float) -> tuple:
        """Refuse to quote if market resolves within 24 hours."""
        if hours_remaining < config.MIN_TIME_TO_RESOLUTION_HOURS:
            return False, f"Too close to resolution: {hours_remaining:.1f}h remaining"
        return True, "ok"

    def _check_volatility(self, market_id: str) -> tuple:
        """Pause quoting if market moved > 5 cents in last 10 minutes."""
        pause_until = self._volatility_pause.get(market_id, 0)
        if time.time() < pause_until:
            remaining = int(pause_until - time.time())
            return False, f"Volatility pause active: {remaining}s remaining"

        history = self._price_history.get(market_id, [])
        ten_min_ago = time.time() - 600
        recent = [p for t, p in history if t >= ten_min_ago]
        if len(recent) >= 2:
            price_range = max(recent) - min(recent)
            if price_range > 0.05:
                self._volatility_pause[market_id] = time.time() + VOLATILITY_PAUSE_SECS
                return False, f"Volatility triggered: {price_range:.3f} move in 10 min — pausing 15 min"

        return True, "ok"

    def _check_news_arb(self, market_id: str) -> tuple:
        """Don't quote a market where news_arb has an active signal."""
        self._refresh_cross_strategy_signals()
        if market_id in self._news_arb_markets:
            return False, f"news_arb has active signal on {market_id}"
        return True, "ok"

    def _check_resolution_lag(self, market_id: str) -> tuple:
        """Don't quote a market where resolution_lag has an active signal."""
        self._refresh_cross_strategy_signals()
        if market_id in self._resolution_lag_markets:
            return False, f"resolution_lag has active signal on {market_id}"
        return True, "ok"

    def pre_quote_check(
        self,
        market_id: str,
        current_mid: float,
        hours_remaining: float,
        proposed_size_usd: float,
    ) -> tuple:
        """
        Run all checks before posting a quote.
        Returns (allowed: bool, reason: str).
        """
        self.record_price(market_id, current_mid)

        checks = [
            self._check_inventory(proposed_size_usd),
            self._check_time_to_resolution(hours_remaining),
            self._check_volatility(market_id),
            self._check_news_arb(market_id),
            self._check_resolution_lag(market_id),
        ]

        for allowed, reason in checks:
            if not allowed:
                logger.debug(f"Risk check failed [{market_id}]: {reason}")
                return False, reason

        return True, "ok"
