"""
kalshi_executor.py — Live order execution for Kalshi crypto markets.

Handles order placement, status checks, cancellations, positions, and balance.
Auth: RSA-PSS key-pair signing (same as kalshi_client.py).
DRY_RUN mode prevents real orders by default.
"""

import asyncio
import base64
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

import aiohttp
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

import config
from signal_engine import Signal

log = logging.getLogger(__name__)

DRY_RUN = os.environ.get("DRY_RUN", "true").lower() == "true"


class KalshiExecutor:
    """Async Kalshi order executor with RSA-PSS auth and dry-run safety."""

    def __init__(self):
        self.api_key = os.environ.get("KALSHI_API_KEY", "")
        self.base_url = config.KALSHI_API_URL
        self._private_key = None

        key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "kalshi.pem")
        p = Path(key_path)
        if not p.is_absolute():
            p = Path(__file__).parent / p
        with open(p, "rb") as f:
            self._private_key = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )

    # ── RSA-PSS auth (identical to kalshi_client.py) ─────────────────────────

    def _sign(self, text: str) -> str:
        sig = self._private_key.sign(
            text.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode("utf-8")

    def _auth_headers(self, method: str, path: str) -> dict:
        ts = str(int(time.time() * 1000))
        msg = ts + method.upper() + path.split("?")[0]
        return {
            "KALSHI-ACCESS-KEY": self.api_key,
            "KALSHI-ACCESS-SIGNATURE": self._sign(msg),
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "Content-Type": "application/json",
        }

    # ── HTTP helpers ─────────────────────────────────────────────────────────

    async def _request(
        self,
        method: str,
        path: str,
        body: Optional[dict] = None,
        timeout_secs: float = 10,
    ) -> Optional[dict]:
        url = f"{self.base_url}{path}"
        full_path = f"/trade-api/v2{path}"
        headers = self._auth_headers(method, full_path)
        try:
            async with aiohttp.ClientSession() as session:
                kwargs = {
                    "headers": headers,
                    "timeout": aiohttp.ClientTimeout(total=timeout_secs),
                }
                if body is not None:
                    kwargs["json"] = body

                async with session.request(method, url, **kwargs) as resp:
                    text = await resp.text()
                    if resp.status in (200, 201):
                        return json.loads(text) if text else {}
                    log.warning(
                        "Kalshi %s %s → %d: %s", method, path, resp.status, text
                    )
                    return {"_error": True, "_status": resp.status, "_body": text}
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.error("Kalshi connection error %s %s: %s", method, path, e)
            return {"_error": True, "_status": 0, "_body": str(e)}

    # ── Place order ──────────────────────────────────────────────────────────

    async def place_order(self, signal: Signal, size_usd: float) -> str:
        """Place an order and return the order_id string.

        Raises on failure so callers can catch and handle cleanly.
        """
        price = signal.market_price
        if price <= 0:
            raise ValueError(f"Invalid market price <= 0: {price}")

        contracts = max(1, int(size_usd / price))
        side = "yes" if signal.contract_side == "YES" else "no"
        client_order_id = f"bot_{int(time.time())}_{signal.asset}"

        order_body = {
            "ticker": signal.best_market.condition_id,
            "client_order_id": client_order_id,
            "type": "market",
            "action": "buy",
            "side": side,
            "count": contracts,
            "yes_price": int(signal.market_price * 100),
        }

        if DRY_RUN:
            log.info("DRY RUN — would place order: %s", json.dumps(order_body))
            return f"dry_{client_order_id}"

        resp = await self._request("POST", "/portfolio/orders", body=order_body)
        if resp is None or resp.get("_error"):
            error_msg = resp.get("_body", "Unknown error") if resp else "No response"
            log.error("Order failed: %s", error_msg)
            raise RuntimeError(f"Order placement failed: {error_msg}")

        order = resp.get("order", resp)
        order_id = order.get("order_id") if isinstance(order, dict) else None
        if not order_id or not isinstance(order_id, str):
            log.error("Response missing valid order_id: %s", resp)
            raise RuntimeError(f"Kalshi response missing order_id: {resp}")

        log.info("Order placed: %s (%d contracts)", order_id, contracts)
        return order_id

    # ── Order status ─────────────────────────────────────────────────────────

    async def get_order_status(self, order_id: str) -> dict:
        resp = await self._request("GET", f"/portfolio/orders/{order_id}")
        if resp is None or resp.get("_error"):
            return {"status": "error"}
        order = resp.get("order", resp)
        return {
            "status": order.get("status", "unknown"),
            "filled_count": order.get("filled_count", 0),
            "remaining_count": order.get("remaining_count", 0),
        }

    # ── Cancel order ─────────────────────────────────────────────────────────

    async def cancel_order(self, order_id: str) -> bool:
        resp = await self._request("DELETE", f"/portfolio/orders/{order_id}")
        if resp is None or resp.get("_error"):
            log.error("Cancel failed for %s", order_id)
            return False
        log.info("Cancelled order %s", order_id)
        return True

    # ── Positions ────────────────────────────────────────────────────────────

    async def get_positions(self) -> list:
        resp = await self._request("GET", "/portfolio/positions")
        if resp is None or resp.get("_error"):
            log.error("Failed to fetch positions")
            return []

        positions = resp.get("market_positions", resp.get("positions", []))
        results = []
        for p in positions:
            resting = p.get("resting_orders_count", p.get("resting_contracts_count", 0))
            total = p.get("total_traded", p.get("position", 0))
            if total == 0 and resting == 0:
                continue
            results.append({
                "ticker": p.get("ticker", p.get("market_ticker", "?")),
                "contracts": total,
                "avg_price": p.get("average_price", 0),
                "current_value": p.get("market_value", 0),
                "unrealized_pnl": p.get("unrealized_pnl", 0),
            })
        return results

    # ── Best ask (exit price fetch) ───────────────────────────────────────────

    async def get_best_ask(self, market_id: str) -> Optional[float]:
        """
        Fetch the best ask price for a market from the Kalshi orderbook.

        Returns the best ask as a float in [0, 1], or None on failure.
        Falls back to current_bid * 0.98 if no asks are present.
        """
        resp = await self._request("GET", f"/markets/{market_id}/orderbook", timeout_secs=5)
        if resp is None or resp.get("_error"):
            log.warning("get_best_ask: failed to fetch orderbook for %s", market_id)
            return None

        orderbook = resp.get("orderbook", resp)
        asks = orderbook.get("yes", [])   # yes-side asks (price ascending)
        bids = orderbook.get("no", [])    # no-side bids used as fallback

        if asks:
            # Kalshi orderbook prices are in cents (0-100); normalise to [0,1]
            best = asks[0] if isinstance(asks[0], (int, float)) else asks[0][0]
            price = float(best) / 100.0 if float(best) > 1.0 else float(best)
            return round(price, 4)

        # No asks — derive from best bid with a conservative haircut
        if bids:
            best_bid = bids[0] if isinstance(bids[0], (int, float)) else bids[0][0]
            bid_price = float(best_bid) / 100.0 if float(best_bid) > 1.0 else float(best_bid)
            fallback = round(bid_price * 0.98, 4)
            log.warning(
                "get_best_ask: no asks for %s — using bid * 0.98 fallback: %.4f",
                market_id, fallback,
            )
            return fallback

        log.warning("get_best_ask: empty orderbook for %s", market_id)
        return None

    # ── Balance ──────────────────────────────────────────────────────────────

    async def get_balance(self) -> float:
        resp = await self._request("GET", "/portfolio/balance")
        if resp is None or resp.get("_error"):
            log.error("Failed to fetch balance")
            return -1.0
        balance_cents = resp.get("balance", 0)
        return balance_cents / 100.0


# ── Standalone test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()

    async def _test():
        logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
        print(f"\nDRY_RUN mode: {DRY_RUN}")

        if not os.environ.get("KALSHI_API_KEY"):
            print("KALSHI_API_KEY not set — aborting.")
            return

        executor = KalshiExecutor()

        print("\n--- Balance ---")
        balance = await executor.get_balance()
        print(f"  Available: ${balance:.2f}")

        print("\n--- Positions ---")
        positions = await executor.get_positions()
        if positions:
            for p in positions:
                print(f"  {p['ticker']}: {p['contracts']} contracts, "
                      f"avg=${p['avg_price']:.2f}, pnl=${p['unrealized_pnl']:.2f}")
        else:
            print("  No open positions")

        print()

    asyncio.run(_test())
