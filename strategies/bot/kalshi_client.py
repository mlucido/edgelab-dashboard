"""
kalshi_client.py — Read-only Kalshi API client.

Drop-in replacement for polymarket_client.py using Kalshi REST API.
Auth: RSA key-pair signing (KALSHI_API_KEY + KALSHI_PRIVATE_KEY_PATH in .env).

Endpoints:
  - GET /markets?series_ticker=KXBTC  → BTC markets
  - GET /markets?series_ticker=KXETH  → ETH markets
  - GET /markets?series_ticker=KXSOL  → SOL markets
  - GET /markets/{ticker}/orderbook   → live YES/NO prices, liquidity
"""

import asyncio
import base64
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import aiohttp
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

import config

log = logging.getLogger(__name__)

# Series tickers → asset label
CRYPTO_SERIES = {
    "KXBTC": "BTC",
    "KXETH": "ETH",
    "KXSOL": "SOL",
}


# ── RSA signature auth ──────────────────────────────────────────────────────

_private_key_cache = None


def _load_private_key():
    global _private_key_cache
    if _private_key_cache is not None:
        return _private_key_cache

    key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "kalshi.pem")
    p = Path(key_path)
    if not p.is_absolute():
        p = Path(__file__).parent / p
    with open(p, "rb") as f:
        _private_key_cache = serialization.load_pem_private_key(
            f.read(), password=None, backend=default_backend()
        )
    return _private_key_cache


def _sign(text: str) -> str:
    pk = _load_private_key()
    sig = pk.sign(
        text.encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(sig).decode("utf-8")


def _auth_headers(method: str, path: str) -> dict:
    ts = str(int(time.time() * 1000))
    msg = ts + method.upper() + path.split("?")[0]
    return {
        "KALSHI-ACCESS-KEY": os.environ.get("KALSHI_API_KEY", ""),
        "KALSHI-ACCESS-SIGNATURE": _sign(msg),
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "Content-Type": "application/json",
    }


# ── Data model ───────────────────────────────────────────────────────────────

@dataclass
class KalshiMarket:
    condition_id:    str
    question:        str
    asset:           str        # BTC / ETH / SOL
    direction:       str        # UP / DOWN
    yes_token_id:    str
    no_token_id:     str
    yes_price:       float      # 0–1
    no_price:        float      # 0–1
    spread:          float      # yes_price + no_price - 1 (ideally ~0)
    liquidity:       float      # USD
    end_date:        datetime
    hours_remaining: float
    strike_price:    float = 0.0  # parsed from ticker (e.g. B73375 → 73375)

    @property
    def has_real_prices(self) -> bool:
        """True if both sides have real orderbook quotes (not placeholders)."""
        return self.yes_price != 0.50 and self.no_price != 0.50

    @property
    def is_tradeable(self) -> bool:
        if self.liquidity < config.MIN_MARKET_LIQUIDITY:
            return False
        if 0 >= self.hours_remaining or self.hours_remaining > config.MAX_MARKET_HOURS_TO_CLOSE:
            return False
        # Only enforce spread check when both sides have real quotes
        if self.has_real_prices and abs(self.spread) > config.MAX_BID_ASK_SPREAD:
            return False
        return True


# ── Direction detection ──────────────────────────────────────────────────────

# Regex to detect range markets like "$69,000-$70,000" or "$69k-$70k"
_RANGE_RE = re.compile(r"\$[\d,.]+k?\s*[-–]\s*\$[\d,.]+k?", re.IGNORECASE)


def _parse_strike(ticker: str) -> float:
    """
    Extract the strike price from a Kalshi ticker.
    E.g. KXBTC-26MAR1312-B73375 → 73375.0, KXETH-26MAR1312-B2230 → 2230.0
    """
    parts = ticker.split("-")
    if len(parts) >= 3:
        strike_part = parts[-1]
        # Strip the B/T prefix
        if strike_part and strike_part[0] in ("B", "T"):
            try:
                return float(strike_part[1:])
            except ValueError:
                pass
    return 0.0


def _detect_direction(ticker: str, title: str) -> Optional[str]:
    """
    Determine UP or DOWN from Kalshi ticker + title.
    Returns None for range markets or ambiguous titles.

    Kalshi ticker conventions:
      - B{strike} = "above/at-or-above" that strike → UP
      - T{strike} = "top" boundary / ceiling         → DOWN (price stays below)
    Title keywords override ticker prefix when present.
    """
    combined = f"{ticker} {title}".lower()

    # Skip range markets (e.g. "Bitcoin $69,000-$70,000")
    if _RANGE_RE.search(combined):
        return None

    # Keyword-based detection (strongest signal)
    up_words = ["above", "higher", "over", "at or above", "at least"]
    down_words = ["below", "lower", "under", "at or below", "at most"]

    for w in up_words:
        if w in combined:
            return "UP"
    for w in down_words:
        if w in combined:
            return "DOWN"

    # Fallback: ticker prefix convention
    # Extract the suffix after the date portion (e.g. KXBTC-26MAR1217-B78250 → B78250)
    parts = ticker.split("-")
    if len(parts) >= 3:
        strike_part = parts[-1]
        if strike_part.startswith("B"):
            return "UP"
        if strike_part.startswith("T"):
            return "DOWN"

    return None


# ── API helpers ──────────────────────────────────────────────────────────────

async def _api_get(
    session: aiohttp.ClientSession,
    path: str,
    params: Optional[dict] = None,
    timeout_secs: float = 10,
) -> Optional[dict]:
    url = f"{config.KALSHI_API_URL}{path}"
    full_path = f"/trade-api/v2{path}"
    try:
        async with session.get(
            url,
            headers=_auth_headers("GET", full_path),
            params=params,
            timeout=aiohttp.ClientTimeout(total=timeout_secs),
        ) as resp:
            if resp.status == 401:
                log.error("Kalshi auth failed (401) — check KALSHI_API_KEY and private key")
                return None
            if resp.status == 429:
                log.warning("Kalshi rate limited, backing off 5s...")
                await asyncio.sleep(5)
                async with session.get(
                    url,
                    headers=_auth_headers("GET", full_path),
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=timeout_secs),
                ) as retry_resp:
                    if retry_resp.status != 200:
                        log.warning("Kalshi retry failed with %d", retry_resp.status)
                        return None
                    return await retry_resp.json()
            if resp.status != 200:
                log.warning("Kalshi API %s returned %d", path, resp.status)
                return None
            return await resp.json()
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.error("Kalshi connection error on %s: %s", path, e)
        return None


def _parse_price(raw) -> float:
    """Normalize a price value to 0–1 float, handling strings and cents."""
    val = float(raw)
    return val / 100.0 if val > 1 else val


def _extract_orderbook_prices(
    orderbook: dict,
    market_obj: Optional[dict] = None,
) -> tuple[float, float, float]:
    """
    Extract (yes_price, no_price, liquidity) from an orderbook response.
    Handles both legacy format (yes/no arrays) and current orderbook_fp
    format (yes_dollars/no_dollars with string prices).
    Falls back to market-level fields or 0.50 placeholder if orderbook is empty.
    """
    # Support both legacy and orderbook_fp formats
    fp = orderbook.get("orderbook_fp", orderbook)
    yes_asks = (
        fp.get("yes_dollars")
        or fp.get("yes")
        or orderbook.get("yes")
        or orderbook.get("asks", [])
    )
    no_asks = (
        fp.get("no_dollars")
        or fp.get("no")
        or orderbook.get("no", [])
    )

    yes_price = 0.0
    no_price = 0.0

    if yes_asks:
        sorted_yes = sorted(
            yes_asks,
            key=lambda x: float(x[0]) if isinstance(x, list) else float(x.get("price", 100)),
        )
        first = sorted_yes[0]
        raw = float(first[0]) if isinstance(first, list) else float(first.get("price", 0))
        yes_price = _parse_price(raw)

    if no_asks:
        sorted_no = sorted(
            no_asks,
            key=lambda x: float(x[0]) if isinstance(x, list) else float(x.get("price", 100)),
        )
        first_no = sorted_no[0]
        raw_no = float(first_no[0]) if isinstance(first_no, list) else float(first_no.get("price", 0))
        no_price = _parse_price(raw_no)

    # Fallback: market-level fields
    if yes_price == 0.0 and market_obj:
        for field in ["yes_ask", "yes_bid", "last_price"]:
            val = market_obj.get(field)
            if val:
                yes_price = _parse_price(val)
                break

    if no_price == 0.0 and market_obj:
        for field in ["no_ask", "no_bid"]:
            val = market_obj.get(field)
            if val:
                no_price = _parse_price(val)
                break

    # Last resort: placeholder so market still appears in scan
    if yes_price == 0.0:
        yes_price = 0.50
    if no_price == 0.0:
        no_price = round(1.0 - yes_price, 4)

    # Liquidity from all sides
    liquidity = 0.0
    for side in [yes_asks, no_asks]:
        for entry in side:
            if isinstance(entry, list) and len(entry) >= 2:
                p = _parse_price(entry[0])
                liquidity += p * float(entry[1])
            elif isinstance(entry, dict):
                p = _parse_price(entry.get("price", 0))
                size = float(entry.get("quantity", entry.get("size", 0)))
                liquidity += p * size

    # Fallback: use volume or open_interest from market object as liquidity proxy
    if liquidity == 0.0 and market_obj:
        liquidity = float(market_obj.get("volume", 0) or 0)
        if liquidity == 0.0:
            liquidity = float(market_obj.get("open_interest", 0) or 0)

    return yes_price, no_price, liquidity


# ── Core fetch ───────────────────────────────────────────────────────────────

async def _fetch_series(
    session: aiohttp.ClientSession,
    series_ticker: str,
    asset: str,
) -> List[KalshiMarket]:
    """Fetch all open markets for a single series ticker (e.g. KXBTC)."""
    data = await _api_get(
        session, "/markets",
        params={"status": "open", "series_ticker": series_ticker, "limit": 200},
    )
    if not data:
        return []

    raw_markets = data.get("markets", [])
    now_utc = datetime.now(timezone.utc)
    markets: List[KalshiMarket] = []

    for m in raw_markets:
        try:
            ticker = m.get("ticker", "")
            title = m.get("title", "")

            # Parse close time
            close_time = m.get("close_time") or m.get("expiration_time", "")
            if not close_time:
                continue
            if close_time.endswith("Z"):
                close_time = close_time[:-1] + "+00:00"
            try:
                end_dt = datetime.fromisoformat(close_time)
                if end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

            hours_left = (end_dt - now_utc).total_seconds() / 3600
            if hours_left <= 0 or hours_left > config.MAX_MARKET_HOURS_TO_CLOSE:
                continue

            # Direction detection
            direction = _detect_direction(ticker, title)
            if not direction:
                continue

            # Fetch orderbook
            book_data = await _api_get(
                session, f"/markets/{ticker}/orderbook", timeout_secs=5,
            )
            orderbook = (
                book_data.get("orderbook_fp")
                or book_data.get("orderbook")
                or book_data
            ) if book_data else {}

            yes_price, no_price, liquidity = _extract_orderbook_prices(orderbook, m)
            spread = abs((yes_price + no_price) - 1.0)

            market = KalshiMarket(
                condition_id=ticker,
                question=title,
                asset=asset,
                direction=direction,
                yes_token_id=f"{ticker}_yes",
                no_token_id=f"{ticker}_no",
                yes_price=round(yes_price, 4),
                no_price=round(no_price, 4),
                spread=round(spread, 4),
                liquidity=round(liquidity, 2),
                end_date=end_dt,
                hours_remaining=round(hours_left, 2),
                strike_price=_parse_strike(ticker),
            )
            markets.append(market)

        except Exception as e:
            log.debug("Skipping Kalshi market %s: %s", m.get("ticker", "?"), e)
            continue

    return markets


async def fetch_crypto_markets(session: aiohttp.ClientSession) -> List[KalshiMarket]:
    """
    Fetch all active short-duration crypto markets from Kalshi.
    Queries KXBTC, KXETH, KXSOL in parallel via asyncio.gather().
    """
    results = await asyncio.gather(
        *[
            _fetch_series(session, series, asset)
            for series, asset in CRYPTO_SERIES.items()
        ],
        return_exceptions=True,
    )

    all_markets: List[KalshiMarket] = []
    for series, result in zip(CRYPTO_SERIES.keys(), results):
        if isinstance(result, Exception):
            log.error("Error fetching %s: %s", series, result)
            continue
        log.info("Kalshi %s: %d markets within %dh window",
                 series, len(result), config.MAX_MARKET_HOURS_TO_CLOSE)
        all_markets.extend(result)

    tradeable = [m for m in all_markets if m.is_tradeable]
    log.info("Found %d total / %d tradeable crypto markets on Kalshi",
             len(all_markets), len(tradeable))
    return all_markets


async def get_market_snapshot(
    session: aiohttp.ClientSession,
    ticker: str,
) -> Optional[tuple[float, float]]:
    """
    Refresh the YES/NO price for a specific market.
    Falls back to market-level data if orderbook is empty.
    """
    # Try orderbook first
    book_data = await _api_get(session, f"/markets/{ticker}/orderbook", timeout_secs=5)
    orderbook = book_data.get("orderbook", book_data) if book_data else {}

    # Fetch market object for fallback fields
    market_data = await _api_get(session, f"/markets/{ticker}", timeout_secs=5)
    market_obj = market_data.get("market", market_data) if market_data else None

    yes_price, no_price, _ = _extract_orderbook_prices(orderbook, market_obj)
    return round(yes_price, 4), round(no_price, 4)


# ── Standalone test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    async def _test():
        logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

        if not os.environ.get("KALSHI_API_KEY"):
            print("KALSHI_API_KEY not set — aborting.")
            return

        async with aiohttp.ClientSession() as session:
            markets = await fetch_crypto_markets(session)

            if not markets:
                print("\nNo crypto markets found within time window.")
                return

            tradeable = [m for m in markets if m.is_tradeable]

            print("\n" + "=" * 90)
            print("  KALSHI CRYPTO MARKET SCAN")
            print("=" * 90)

            # Per-series counts
            from collections import Counter
            by_asset = Counter(m.asset for m in markets)
            by_asset_t = Counter(m.asset for m in tradeable)
            for asset in ["BTC", "ETH", "SOL"]:
                total = by_asset.get(asset, 0)
                trd = by_asset_t.get(asset, 0)
                print(f"  {asset}: {total} markets found, {trd} tradeable")
            print()

            # Sample of first 5
            print("  SAMPLE MARKETS:")
            print("  %-35s %-6s %6s %6s %8s %6s" % (
                "TICKER", "DIR", "YES", "NO", "LIQ", "HRS"))
            print("  " + "-" * 75)
            for m in markets[:5]:
                print("  %-35s %-6s %6.2f %6.2f %8.0f %6.1f" % (
                    m.condition_id, m.direction,
                    m.yes_price, m.no_price,
                    m.liquidity, m.hours_remaining))

            if len(markets) > 5:
                print("  ... and %d more" % (len(markets) - 5))

            print()
            print("  TOTAL: %d markets, %d tradeable (liq >= $%d)" % (
                len(markets), len(tradeable), config.MIN_MARKET_LIQUIDITY))
            print("=" * 90 + "\n")

    asyncio.run(_test())
