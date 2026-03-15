#!/usr/bin/env python3
"""
Carry Trade Strategy — Paper mode cash-and-carry basis trade on BTC/ETH futures.

Reads recommended parameters from carry_analysis_report.json.
Tracks positions and scans in carry_trades.db (SQLite, WAL mode).
"""

import asyncio
import json
import logging
import os
import signal
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import httpx

log = logging.getLogger("carry_trade")

# ── Paths ────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent.parent  # strategies/edgelab/
REPORT_PATH = BASE_DIR / "analysis" / "carry_analysis_report.json"
DB_PATH = BASE_DIR / "carry_trades.db"

# ── Load report defaults ─────────────────────────────────────────────────────

def _load_report_defaults() -> dict:
    """Load recommended values from backtest report."""
    defaults = {
        "entry_threshold": 12.0,
        "max_leverage": 1.5,
        "margin_buffer": 35.0,
    }
    if REPORT_PATH.exists():
        try:
            with open(REPORT_PATH) as f:
                report = json.load(f)
            defaults["entry_threshold"] = report.get("recommended_entry_threshold_apr", 12.0)
            defaults["max_leverage"] = report.get("recommended_max_leverage", 1.5)
            defaults["margin_buffer"] = report.get("recommended_margin_buffer_pct", 35.0)
        except Exception as e:
            log.warning("Failed to load report: %s", e)
    return defaults

_REPORT = _load_report_defaults()

# ── Config (env overrides) ───────────────────────────────────────────────────

CARRY_ENABLED = os.environ.get("CARRY_ENABLED", "true").lower() == "true"
CARRY_ENTRY_BASIS_APR = float(os.environ.get("CARRY_ENTRY_BASIS_APR", _REPORT["entry_threshold"]))
CARRY_EXIT_BASIS_APR = float(os.environ.get("CARRY_EXIT_BASIS_APR", "3.0"))
CARRY_POSITION_SIZE_USD = float(os.environ.get("CARRY_POSITION_SIZE_USD", "1000.0"))
CARRY_MAX_LEVERAGE = float(os.environ.get("CARRY_MAX_LEVERAGE", _REPORT["max_leverage"]))
CARRY_MARGIN_BUFFER_PCT = float(os.environ.get("CARRY_MARGIN_BUFFER_PCT", _REPORT["margin_buffer"]))
CARRY_ASSETS = os.environ.get("CARRY_ASSETS", "BTC,ETH").split(",")
CARRY_SCAN_INTERVAL_SECS = int(os.environ.get("CARRY_SCAN_INTERVAL_SECS", "60"))
COOLDOWN_SECS = 1800  # 30 min cooldown after close


# ── Database ─────────────────────────────────────────────────────────────────

def init_db() -> sqlite3.Connection:
    """Initialize carry trades database with WAL mode."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS carry_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset TEXT NOT NULL,
            entry_time TEXT NOT NULL,
            entry_spot REAL,
            entry_futures REAL,
            entry_basis_apr REAL,
            days_to_expiry INTEGER,
            spot_size_usd REAL,
            futures_notional_usd REAL,
            leverage REAL,
            margin_call_price REAL,
            status TEXT DEFAULT 'open',
            exit_time TEXT,
            exit_spot REAL,
            exit_futures REAL,
            exit_basis_apr REAL,
            pnl_usd REAL,
            exit_reason TEXT,
            notes TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS carry_scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_time TEXT NOT NULL,
            asset TEXT NOT NULL,
            spot_price REAL,
            futures_price REAL,
            basis_apr REAL,
            days_to_expiry INTEGER,
            action_taken TEXT,
            signal TEXT
        )
    """)
    conn.commit()
    return conn


# ── CarryTradeStrategy ───────────────────────────────────────────────────────

class CarryTradeStrategy:
    """Async carry trade strategy — paper mode only."""

    def __init__(self):
        self.db = init_db()
        self.positions: Dict[str, dict] = {}  # asset -> position dict
        self.last_close_time: Dict[str, float] = {}  # asset -> timestamp
        self._running = True
        self._client = httpx.AsyncClient(timeout=15)

        # Load any existing open positions from DB
        self._load_open_positions()

    def _load_open_positions(self):
        """Load open positions from DB on restart."""
        rows = self.db.execute(
            "SELECT * FROM carry_positions WHERE status='open'"
        ).fetchall()
        for row in rows:
            self.positions[row["asset"]] = dict(row)
        if self.positions:
            log.info("Loaded %d open positions from DB", len(self.positions))

    async def fetch_live_basis(self, asset: str) -> Optional[dict]:
        """Fetch live basis data from Binance APIs."""
        symbol = f"{asset}USDT"
        try:
            # Fetch spot price
            spot_resp = await self._client.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": symbol}
            )
            spot_resp.raise_for_status()
            spot_price = float(spot_resp.json()["price"])

            # Fetch futures premium index
            prem_resp = await self._client.get(
                "https://fapi.binance.com/fapi/v1/premiumIndex",
                params={"symbol": symbol}
            )
            prem_resp.raise_for_status()
            prem_data = prem_resp.json()
            mark_price = float(prem_data["markPrice"])

            # Try to find quarterly contract for days_to_expiry
            days_to_expiry = 90  # default
            contract_symbol = f"{symbol}_QUARTERLY"
            try:
                info_resp = await self._client.get(
                    "https://fapi.binance.com/fapi/v1/exchangeInfo"
                )
                info_resp.raise_for_status()
                symbols_info = info_resp.json().get("symbols", [])
                quarterly_contracts = [
                    s for s in symbols_info
                    if s.get("baseAsset") == asset
                    and s.get("contractType") == "CURRENT_QUARTER"
                    and s.get("status") == "TRADING"
                ]
                if quarterly_contracts:
                    qc = quarterly_contracts[0]
                    contract_symbol = qc["symbol"]
                    expiry_ms = qc.get("deliveryDate", 0)
                    if expiry_ms:
                        expiry_dt = datetime.fromtimestamp(expiry_ms / 1000, tz=timezone.utc)
                        days_to_expiry = max(1, (expiry_dt - datetime.now(timezone.utc)).days)
            except Exception:
                pass  # Use defaults

            # Annualize basis
            basis_apr = ((mark_price - spot_price) / spot_price) * (365 / max(days_to_expiry, 1)) * 100

            return {
                "asset": asset,
                "spot": spot_price,
                "futures_mark": mark_price,
                "basis_apr": round(basis_apr, 4),
                "days_to_expiry": days_to_expiry,
                "contract_symbol": contract_symbol,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 451:
                # Geo-blocked — generate simulated data for paper mode
                return self._simulate_basis(asset)
            log.warning("HTTP error fetching %s basis: %s", asset, e)
            return self._simulate_basis(asset)
        except Exception as e:
            log.warning("Error fetching %s basis: %s", asset, e)
            return self._simulate_basis(asset)

    def _simulate_basis(self, asset: str) -> dict:
        """Generate simulated basis data for paper mode when API unavailable."""
        import random
        base_prices = {"BTC": 84000, "ETH": 2100}
        base_basis = {"BTC": 11.5, "ETH": 14.2}

        spot = base_prices.get(asset, 50000) * (1 + random.uniform(-0.02, 0.02))
        basis_apr = base_basis.get(asset, 10) + random.uniform(-3, 5)
        futures = spot * (1 + basis_apr / 100 * 90 / 365)

        return {
            "asset": asset,
            "spot": round(spot, 2),
            "futures_mark": round(futures, 2),
            "basis_apr": round(basis_apr, 2),
            "days_to_expiry": 90,
            "contract_symbol": f"{asset}USDT_SIM",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "simulated": True,
        }

    def should_enter(self, basis_data: dict) -> Tuple[bool, str]:
        """Check if we should enter a carry trade."""
        asset = basis_data["asset"]
        basis_apr = basis_data["basis_apr"]

        # Already have a position?
        if asset in self.positions:
            return False, f"Already have open {asset} position"

        # In cooldown?
        last_close = self.last_close_time.get(asset, 0)
        if time.time() - last_close < COOLDOWN_SECS:
            remaining = int(COOLDOWN_SECS - (time.time() - last_close))
            return False, f"Cooldown: {remaining}s remaining after last close"

        # Basis check
        if basis_apr < CARRY_ENTRY_BASIS_APR:
            return False, f"Basis {basis_apr:.1f}% < threshold {CARRY_ENTRY_BASIS_APR:.1f}%"

        return True, f"Basis {basis_apr:.1f}% >= threshold {CARRY_ENTRY_BASIS_APR:.1f}%"

    def should_exit(self, position: dict, current_basis: dict) -> Tuple[bool, str]:
        """Check if we should exit a carry trade."""
        # Basis collapsed
        if current_basis["basis_apr"] <= CARRY_EXIT_BASIS_APR:
            return True, f"Basis collapsed to {current_basis['basis_apr']:.1f}% <= {CARRY_EXIT_BASIS_APR:.1f}%"

        # Days to expiry < 7 (contract approaching settlement)
        if current_basis["days_to_expiry"] < 7:
            return True, "Contract near expiry (<7 days)"

        # Margin health critical
        health = self.check_margin_health(position, current_basis["spot"])
        if health["action"] == "close":
            return True, f"Margin health critical: {health['health_pct']:.0f}%"

        return False, "Hold"

    def calculate_position(self, basis_data: dict) -> dict:
        """Calculate full position sizing."""
        spot = basis_data["spot"]
        spot_size_usd = CARRY_POSITION_SIZE_USD
        futures_notional = spot_size_usd * CARRY_MAX_LEVERAGE
        required_margin = futures_notional / CARRY_MAX_LEVERAGE
        buffer_margin = required_margin * (1 + CARRY_MARGIN_BUFFER_PCT / 100)
        total_capital = spot_size_usd + buffer_margin

        # Margin call price (no cross-margin)
        margin_call_price = spot * (1 - 0.5 / CARRY_MAX_LEVERAGE)

        return {
            "asset": basis_data["asset"],
            "entry_time": datetime.now(timezone.utc).isoformat(),
            "entry_spot": spot,
            "entry_futures": basis_data["futures_mark"],
            "entry_basis_apr": basis_data["basis_apr"],
            "days_to_expiry": basis_data["days_to_expiry"],
            "contract_symbol": basis_data["contract_symbol"],
            "spot_size_usd": round(spot_size_usd, 2),
            "spot_units": round(spot_size_usd / spot, 8),
            "futures_notional_usd": round(futures_notional, 2),
            "futures_units": round(futures_notional / basis_data["futures_mark"], 8),
            "leverage": CARRY_MAX_LEVERAGE,
            "required_margin_usd": round(required_margin, 2),
            "buffer_margin_usd": round(buffer_margin, 2),
            "total_capital_usd": round(total_capital, 2),
            "margin_call_price": round(margin_call_price, 2),
            "status": "open",
        }

    def check_margin_health(self, position: dict, current_spot: float) -> dict:
        """Check margin health of an open position."""
        entry_spot = position["entry_spot"]
        leverage = position["leverage"]

        price_drop_pct = (entry_spot - current_spot) / entry_spot
        margin_threshold = 0.5 / leverage
        margin_consumed_pct = max(0, price_drop_pct / margin_threshold) if margin_threshold > 0 else 0
        health_pct = max(0, 100 - margin_consumed_pct * 100)

        if health_pct >= 75:
            action = "ok"
        elif health_pct >= 50:
            action = "watch"
        elif health_pct >= 25:
            action = "add_margin"
        else:
            action = "close"

        return {
            "health_pct": round(health_pct, 1),
            "margin_call_price": position["margin_call_price"],
            "current_spot": current_spot,
            "price_drop_pct": round(price_drop_pct * 100, 2),
            "action": action,
        }

    def calculate_pnl(self, position: dict, current_basis: dict) -> dict:
        """Calculate P&L breakdown for an open position."""
        entry_spot = position["entry_spot"]
        entry_futures = position["entry_futures"]
        current_spot = current_basis["spot"]
        current_futures = current_basis["futures_mark"]

        spot_units = position.get("spot_units", position["spot_size_usd"] / entry_spot)
        futures_units = position.get("futures_units", position["futures_notional_usd"] / entry_futures)

        # Spot P&L: long spot, profit if price goes up
        spot_pnl = (current_spot - entry_spot) * spot_units
        # Futures P&L: short futures, profit if price goes down
        futures_pnl = (entry_futures - current_futures) * futures_units

        total_notional = position["spot_size_usd"] + position["futures_notional_usd"]
        fees = total_notional * 0.004  # 0.4% round trip

        # Basis captured
        basis_captured = position["entry_basis_apr"] - current_basis["basis_apr"]

        net_pnl = spot_pnl + futures_pnl - fees

        return {
            "spot_pnl": round(spot_pnl, 2),
            "futures_pnl": round(futures_pnl, 2),
            "gross_pnl": round(spot_pnl + futures_pnl, 2),
            "fees": round(fees, 2),
            "net_pnl": round(net_pnl, 2),
            "basis_captured_apr": round(basis_captured, 2),
            "entry_basis_apr": position["entry_basis_apr"],
            "current_basis_apr": current_basis["basis_apr"],
        }

    def _open_position(self, position: dict):
        """Record a new paper position in DB."""
        asset = position["asset"]
        self.positions[asset] = position

        self.db.execute("""
            INSERT INTO carry_positions
            (asset, entry_time, entry_spot, entry_futures, entry_basis_apr,
             days_to_expiry, spot_size_usd, futures_notional_usd, leverage,
             margin_call_price, status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)
        """, (
            asset, position["entry_time"], position["entry_spot"],
            position["entry_futures"], position["entry_basis_apr"],
            position["days_to_expiry"], position["spot_size_usd"],
            position["futures_notional_usd"], position["leverage"],
            position["margin_call_price"],
            "paper_mode" + (" | simulated_data" if position.get("simulated") else ""),
        ))
        self.db.commit()
        log.info("OPENED %s carry position: basis=%.1f%%, size=$%.0f",
                 asset, position["entry_basis_apr"], position["spot_size_usd"])

    def _close_position(self, asset: str, current_basis: dict, reason: str):
        """Close a paper position and record in DB."""
        position = self.positions.pop(asset, None)
        if not position:
            return

        pnl = self.calculate_pnl(position, current_basis)
        now = datetime.now(timezone.utc).isoformat()

        self.db.execute("""
            UPDATE carry_positions SET
                status='closed', exit_time=?, exit_spot=?, exit_futures=?,
                exit_basis_apr=?, pnl_usd=?, exit_reason=?
            WHERE asset=? AND status='open'
        """, (
            now, current_basis["spot"], current_basis["futures_mark"],
            current_basis["basis_apr"], pnl["net_pnl"], reason,
            asset,
        ))
        self.db.commit()
        self.last_close_time[asset] = time.time()

        log.info("CLOSED %s carry position: pnl=$%.2f reason=%s",
                 asset, pnl["net_pnl"], reason)

    def _log_scan(self, basis_data: dict, action: str, signal: str):
        """Log a scan result to DB."""
        self.db.execute("""
            INSERT INTO carry_scans
            (scan_time, asset, spot_price, futures_price, basis_apr,
             days_to_expiry, action_taken, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            basis_data["timestamp"], basis_data["asset"],
            basis_data["spot"], basis_data["futures_mark"],
            basis_data["basis_apr"], basis_data["days_to_expiry"],
            action, signal,
        ))
        self.db.commit()

    async def run_scan(self) -> List[dict]:
        """Fetch basis for all assets concurrently."""
        tasks = [self.fetch_live_basis(asset) for asset in CARRY_ASSETS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        opportunities = []
        for r in results:
            if isinstance(r, Exception):
                log.warning("Scan error: %s", r)
                continue
            if r is not None:
                opportunities.append(r)

        # Sort by basis_apr descending
        opportunities.sort(key=lambda x: x["basis_apr"], reverse=True)
        return opportunities

    async def paper_trade_loop(self):
        """Main paper trading loop."""
        log.info("Starting carry trade paper loop (interval=%ds)", CARRY_SCAN_INTERVAL_SECS)

        while self._running:
            try:
                opportunities = await self.run_scan()

                for basis_data in opportunities:
                    asset = basis_data["asset"]

                    # Determine signal
                    if basis_data["basis_apr"] >= CARRY_ENTRY_BASIS_APR:
                        signal = "ENTER"
                    elif basis_data["basis_apr"] >= 5.0:
                        signal = "WATCH"
                    else:
                        signal = "PASS"

                    # Check entry
                    should, reason = self.should_enter(basis_data)
                    if should:
                        position = self.calculate_position(basis_data)
                        if basis_data.get("simulated"):
                            position["simulated"] = True
                        self._open_position(position)
                        self._log_scan(basis_data, "ENTER", signal)
                    else:
                        self._log_scan(basis_data, reason, signal)

                    # Check existing positions
                    if asset in self.positions:
                        pos = self.positions[asset]
                        should_exit, exit_reason = self.should_exit(pos, basis_data)
                        if should_exit:
                            self._close_position(asset, basis_data, exit_reason)

                # Yield status
                yield {
                    "scan_time": datetime.now(timezone.utc).isoformat(),
                    "opportunities": opportunities,
                    "open_positions": dict(self.positions),
                }

            except Exception as e:
                log.error("Scan cycle error: %s", e, exc_info=True)
                yield {"error": str(e)}

            await asyncio.sleep(CARRY_SCAN_INTERVAL_SECS)

    def stop(self):
        """Signal the loop to stop."""
        self._running = False

    async def close(self):
        """Clean up resources."""
        self._running = False
        await self._client.aclose()
        self.db.close()
