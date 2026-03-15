"""
Inventory Manager — tracks open LP positions and inventory risk.
Thread-safe state management for active quotes and fills.
"""
import logging
import threading
from typing import Dict, Optional

from . import config

logger = logging.getLogger("liquidity_provision.inventory_manager")


class InventoryManager:
    """
    Tracks per-market inventory: yes_inventory, no_inventory, avg_entry, unrealized_pnl.
    All dollar amounts in USD.
    """

    def __init__(self):
        self._lock = threading.Lock()
        # {market_id: {yes_inventory, no_inventory, avg_entry_yes, avg_entry_no, unrealized_pnl}}
        self._state: Dict[str, dict] = {}

    def _ensure_market(self, market_id: str):
        if market_id not in self._state:
            self._state[market_id] = {
                "yes_inventory": 0.0,    # USD size in YES contracts
                "no_inventory": 0.0,     # USD size in NO contracts
                "avg_entry_yes": 0.0,
                "avg_entry_no": 0.0,
                "unrealized_pnl": 0.0,
            }

    def add_fill(self, market_id: str, side: str, price: float, size: float):
        """
        Record a fill. side must be 'YES' or 'NO'.
        Updates inventory and recalculates average entry.
        """
        with self._lock:
            self._ensure_market(market_id)
            pos = self._state[market_id]
            side = side.upper()

            if side == "YES":
                prev_size = pos["yes_inventory"]
                prev_avg = pos["avg_entry_yes"]
                new_size = prev_size + size
                if new_size > 0:
                    pos["avg_entry_yes"] = (prev_avg * prev_size + price * size) / new_size
                pos["yes_inventory"] = new_size
            elif side == "NO":
                prev_size = pos["no_inventory"]
                prev_avg = pos["avg_entry_no"]
                new_size = prev_size + size
                if new_size > 0:
                    pos["avg_entry_no"] = (prev_avg * prev_size + price * size) / new_size
                pos["no_inventory"] = new_size
            else:
                logger.warning(f"Unknown side '{side}' for fill on {market_id}")
                return

            logger.debug(f"Fill recorded: {market_id} {side} ${size:.2f} @ {price:.3f} | "
                         f"yes_inv={pos['yes_inventory']:.2f} no_inv={pos['no_inventory']:.2f}")

    def update_unrealized_pnl(self, market_id: str, current_mid: float):
        """Recalculate unrealized PnL given current mid price."""
        with self._lock:
            if market_id not in self._state:
                return
            pos = self._state[market_id]
            yes_pnl = pos["yes_inventory"] * (current_mid - pos["avg_entry_yes"])
            no_pnl = pos["no_inventory"] * ((1.0 - current_mid) - pos["avg_entry_no"])
            pos["unrealized_pnl"] = round(yes_pnl + no_pnl, 4)

    def get_inventory_risk(self) -> float:
        """
        Total USD at risk = sum of one-sided exposure across all markets.
        Returns the larger of (yes_inventory, no_inventory) per market summed.
        """
        with self._lock:
            total = sum(
                max(pos["yes_inventory"], pos["no_inventory"])
                for pos in self._state.values()
            )
            return round(total, 2)

    def get_position(self, market_id: str) -> Optional[dict]:
        """Return current inventory snapshot for a market, or None if not tracked."""
        with self._lock:
            return dict(self._state[market_id]) if market_id in self._state else None

    def check_inventory_limit(self, additional_usd: float = 0.0) -> bool:
        """
        Returns True if there is headroom to add more inventory.
        Optional additional_usd: check if adding this amount would still be within limit.
        """
        current_risk = self.get_inventory_risk()
        return (current_risk + additional_usd) <= config.MAX_INVENTORY_USD

    def get_headroom_usd(self) -> float:
        """How much more inventory we can add before hitting MAX_INVENTORY_USD."""
        return max(0.0, config.MAX_INVENTORY_USD - self.get_inventory_risk())

    def get_all_positions(self) -> dict:
        """Return a copy of all current positions."""
        with self._lock:
            return {k: dict(v) for k, v in self._state.items()}

    def clear_market(self, market_id: str):
        """Remove a market's inventory record (e.g. after resolution)."""
        with self._lock:
            self._state.pop(market_id, None)
