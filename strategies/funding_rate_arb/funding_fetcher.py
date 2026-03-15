"""
Fetches current and historical funding rates from multiple exchanges.
All endpoints are public — no auth required.
Tries Binance first; falls back to Bybit, then OKX on 451 (US IP block).
"""
import logging
import time
import threading
from datetime import datetime, timezone
from typing import Optional

try:
    import httpx
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx", "-q"])
    import httpx

from . import config

logger = logging.getLogger("funding_arb.fetcher")

# ── Error log throttle ────────────────────────────────────────────────────────
_last_source_error_log: float = 0  # monotonic time of last "all sources failed" log
_ERROR_LOG_INTERVAL = 3600  # only log once per hour

# ── Cache ──────────────────────────────────────────────────────────────────────
_cache: dict = {}          # asset → (FundingRate dict, fetch_time)
_CACHE_TTL_SECS = 30
_STALE_ERROR_SECS = 600    # 10 min


def _now_ts() -> float:
    return time.monotonic()


def _cached(asset: str) -> Optional[dict]:
    entry = _cache.get(asset)
    if entry:
        rate, ts = entry
        if _now_ts() - ts < _CACHE_TTL_SECS:
            return rate
    return None


def _store(asset: str, rate: dict):
    _cache[asset] = (rate, _now_ts())


def _cache_age(asset: str) -> float:
    entry = _cache.get(asset)
    if entry:
        _, ts = entry
        return _now_ts() - ts
    return float("inf")


# ── Per-exchange fetch helpers ─────────────────────────────────────────────────

def _from_binance(asset: str) -> Optional[dict]:
    symbol = config.ASSETS[asset]["binance_symbol"]
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(
                config.FUNDING_SOURCES["binance"],
                params={"symbol": symbol, "limit": 1},
            )
            if resp.status_code == 451:
                logger.debug(f"Binance 451 (US IP block) for {asset}")
                return None
            resp.raise_for_status()
            data = resp.json()
            # Returns a list; most recent first
            if isinstance(data, list) and data:
                item = data[0]
            elif isinstance(data, dict):
                item = data
            else:
                return None
            return {
                "asset": asset,
                "rate": float(item.get("fundingRate", 0)),
                "next_funding_time": item.get("fundingTime", ""),
                "source": "binance",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
    except Exception as e:
        logger.debug(f"Binance fetch failed for {asset}: {e}")
        return None


def _from_bybit(asset: str) -> Optional[dict]:
    symbol = config.ASSETS[asset]["bybit_symbol"]
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(
                config.FUNDING_SOURCES["bybit"],
                params={"category": "linear", "symbol": symbol, "limit": 1},
            )
            resp.raise_for_status()
            rows = resp.json().get("result", {}).get("list", [])
            if not rows:
                return None
            item = rows[0]
            return {
                "asset": asset,
                "rate": float(item.get("fundingRate", 0)),
                "next_funding_time": item.get("fundingRateTimestamp", ""),
                "source": "bybit",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
    except Exception as e:
        logger.debug(f"Bybit fetch failed for {asset}: {e}")
        return None


def _from_okx(asset: str) -> Optional[dict]:
    symbol = config.ASSETS[asset]["binance_symbol"].replace("USDT", "-USDT-SWAP")
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(
                config.FUNDING_SOURCES["okx"],
                params={"instId": symbol},
            )
            resp.raise_for_status()
            data_list = resp.json().get("data", [])
            if not data_list:
                return None
            item = data_list[0]
            return {
                "asset": asset,
                "rate": float(item.get("fundingRate", 0)),
                "next_funding_time": item.get("nextFundingTime", ""),
                "source": "okx",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
    except Exception as e:
        logger.warning(f"OKX fetch failed for {asset}: {e}")
        return None


# ── Public API ─────────────────────────────────────────────────────────────────

def get_current_funding_rate(asset: str) -> Optional[dict]:
    """
    Fetch current funding rate for asset.
    Returns FundingRate dict: {asset, rate, next_funding_time, source, timestamp}
    Tries Binance → Bybit → OKX. Caches result 30 s.
    Falls back to cached data with WARNING if all sources fail.
    """
    cached = _cached(asset)
    if cached:
        return cached

    for fetcher in (_from_binance, _from_bybit, _from_okx):
        result = fetcher(asset)
        if result:
            _store(asset, result)
            return result

    # All sources failed — check stale cache
    global _last_source_error_log
    entry = _cache.get(asset)
    if entry:
        rate, ts = entry
        age = _now_ts() - ts
        if age > _STALE_ERROR_SECS:
            if _now_ts() - _last_source_error_log > _ERROR_LOG_INTERVAL:
                logger.warning(f"Funding data stale for {asset} ({age/60:.0f} min) — Binance/Bybit blocked, using OKX fallback")
                _last_source_error_log = _now_ts()
        return rate

    if _now_ts() - _last_source_error_log > _ERROR_LOG_INTERVAL:
        logger.warning(f"No funding data available for {asset} — all sources blocked from this IP")
        _last_source_error_log = _now_ts()
    return None


def get_funding_history(asset: str, limit: int = 10) -> list:
    """
    Returns last N funding periods for asset (8-hour periods at 00/08/16 UTC).
    Tries Bybit first (reliable history endpoint), then Binance, then OKX.
    """
    symbol = config.ASSETS[asset]["bybit_symbol"]
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.get(
                config.FUNDING_SOURCES["bybit"],
                params={"category": "linear", "symbol": symbol, "limit": limit},
            )
            resp.raise_for_status()
            rows = resp.json().get("result", {}).get("list", [])
            history = []
            for item in rows:
                history.append({
                    "asset": asset,
                    "rate": float(item.get("fundingRate", 0)),
                    "next_funding_time": item.get("fundingRateTimestamp", ""),
                    "source": "bybit",
                    "timestamp": item.get("fundingRateTimestamp", ""),
                })
            return history
    except Exception as e:
        logger.debug(f"Bybit history fetch failed for {asset}: {e}")

    # Fallback: OKX history (US-accessible, no auth)
    okx_symbol = config.ASSETS[asset]["binance_symbol"].replace("USDT", "-USDT-SWAP")
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.get(
                "https://www.okx.com/api/v5/public/funding-rate-history",
                params={"instId": okx_symbol, "limit": str(limit)},
            )
            resp.raise_for_status()
            data_list = resp.json().get("data", [])
            history = []
            for item in data_list:
                history.append({
                    "asset": asset,
                    "rate": float(item.get("fundingRate", 0)),
                    "next_funding_time": item.get("fundingTime", ""),
                    "source": "okx",
                    "timestamp": item.get("fundingTime", ""),
                })
            if history:
                return history
    except Exception as e:
        logger.debug(f"OKX history fallback failed for {asset}: {e}")

    # Last resort: Binance (likely blocked)
    symbol_b = config.ASSETS[asset]["binance_symbol"]
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.get(
                config.FUNDING_SOURCES["binance"],
                params={"symbol": symbol_b, "limit": limit},
            )
            if resp.status_code != 451:
                resp.raise_for_status()
                rows = resp.json() if isinstance(resp.json(), list) else []
                history = []
                for item in rows:
                    history.append({
                        "asset": asset,
                        "rate": float(item.get("fundingRate", 0)),
                        "next_funding_time": item.get("fundingTime", ""),
                        "source": "binance",
                        "timestamp": item.get("fundingTime", ""),
                    })
                return history
    except Exception as e:
        logger.debug(f"Binance history fallback failed for {asset}: {e}")

    return []


def get_all_funding_rates() -> dict:
    """
    Fetch current funding rates for all configured assets concurrently.
    Returns dict of {asset: FundingRate}.
    """
    results = {}
    threads = []
    lock = threading.Lock()

    def _fetch(asset):
        rate = get_current_funding_rate(asset)
        if rate:
            with lock:
                results[asset] = rate

    for asset in config.ASSETS:
        t = threading.Thread(target=_fetch, args=(asset,), daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join(timeout=15)

    return results
