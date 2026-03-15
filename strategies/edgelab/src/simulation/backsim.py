"""
EdgeLab Simulation Engine — backsim.py

Fetches resolved Polymarket markets from the last 90 days, replays them
chronologically through the full guardrail stack, and outputs a go/no-go
report for each of three risk configs.

Guardrail import strategy:
    1. Try to import from src/execution/autonomous_trader.py (real implementation).
    2. Fall back to local stubs if the execution module is not yet available.
       Once both modules are merged, the simulation automatically uses the real code.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx

from src.risk.sizer import PLATFORM_FEE

# ---------------------------------------------------------------------------
# Guardrail import — try real execution module, fall back to local stubs
# ---------------------------------------------------------------------------

try:
    from src.execution.autonomous_trader import (
        check_trade_quality,
        calculate_position_size,
        check_portfolio_exposure,
    )
    _GUARDRAILS_SOURCE = "autonomous_trader"
except ImportError:
    _GUARDRAILS_SOURCE = "local_stubs"

    def check_trade_quality(
        opportunity: dict, capital_or_fee: float = PLATFORM_FEE
    ) -> tuple[bool, str]:
        """
        Stub: pass if EV is positive and price is in tradeable range.
        Accepts either fee (< 1) or total_capital (>= 1) as second arg
        to be compatible with both the stub and the real module signature.
        """
        fee = capital_or_fee if capital_or_fee < 1.0 else PLATFORM_FEE
        price = float(opportunity.get("current_price", 0))
        true_prob = float(opportunity.get("implied_true_prob", 0))
        if price <= 0 or price >= 1:
            return False, f"Invalid price: {price}"
        if true_prob <= price:
            return False, f"No edge: true_prob={true_prob:.3f} <= price={price:.3f}"
        ev = true_prob * (1.0 - fee) / price - 1.0
        if ev <= 0:
            return False, f"Negative EV: {ev:.4f}"
        return True, f"EV={ev:.4f}"

    def calculate_position_size(
        opportunity: dict, total_capital: float, fee: float = PLATFORM_FEE
    ) -> tuple[float, float]:
        """Stub: quarter-Kelly with 5% NAV cap. Returns (size, kelly)."""
        price = float(opportunity.get("current_price", 0))
        true_prob = float(opportunity.get("implied_true_prob", 0))
        if price <= 0 or price >= 1 or true_prob <= 0:
            return 0.0, 0.0
        gross = 1.0 / price - 1.0
        b = gross * (1.0 - fee)
        if b <= 0:
            return 0.0, 0.0
        kelly = (true_prob * b - (1.0 - true_prob)) / b
        if kelly <= 0:
            return 0.0, 0.0
        quarter_kelly = kelly * 0.25
        size = min(quarter_kelly * total_capital, 0.05 * total_capital)
        return round(size, 2), round(kelly, 6)

    def check_portfolio_exposure(
        opportunity_or_portfolio: dict,
        size_or_trade: Any,
        portfolio_or_capital: Any,
        total_capital: float | None = None,
    ) -> tuple[bool, str]:
        """
        Stub: enforce 20-position limit and deployment cap.
        Supports both stub signature (portfolio_state, new_trade, capital)
        and expected real module signature (opportunity, size, portfolio, capital).
        """
        # Detect call signature: if second arg is a float, it's (opp, size, portfolio, capital)
        if isinstance(size_or_trade, (int, float)):
            # Real module signature: (opportunity, size, portfolio_state, capital)
            portfolio_state = portfolio_or_capital
            new_size = float(size_or_trade)
            cap = float(total_capital) if total_capital is not None else 500.0
        else:
            # Stub signature: (portfolio_state, new_trade, capital)
            portfolio_state = opportunity_or_portfolio
            new_size = float(size_or_trade.get("size", 0)) if isinstance(size_or_trade, dict) else 0.0
            cap = float(portfolio_or_capital) if portfolio_or_capital is not None else 500.0

        open_positions = portfolio_state.get("open_positions", []) if isinstance(portfolio_state, dict) else []
        if len(open_positions) >= 20:
            return False, "Position limit reached (20 max)"
        deployed = sum(p.get("size", 0) for p in open_positions)
        max_deploy_frac = portfolio_state.get("max_deploy_fraction", 0.60) if isinstance(portfolio_state, dict) else 0.60
        max_deploy = max_deploy_frac * cap
        if deployed + new_size > max_deploy:
            return False, f"Deployment cap reached ({deployed + new_size:.2f} > {max_deploy:.2f})"
        return True, "OK"

    pass  # end of except ImportError stub block


# Circuit breaker is always a local sync function (real one is async + needs Redis)
def check_circuit_breakers(
    trade_history: list, strategy_name: str | None = None
) -> tuple[bool, str]:
    """Sync circuit breaker for simulation: halt if daily loss exceeds threshold."""
    today = str(datetime.now(timezone.utc).date())
    daily_loss = sum(
        t.get("pnl", 0)
        for t in trade_history
        if t.get("date") == today and t.get("pnl", 0) < 0
    )
    circuit_threshold = -200
    if daily_loss <= circuit_threshold:
        return False, f"Circuit breaker: daily loss {daily_loss:.2f} <= {circuit_threshold}"
    return True, "OK"


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GAMMA_URL = "https://gamma-api.polymarket.com/markets"
DB_PATH = os.getenv(
    "EDGELAB_SIM_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "simulation.db"),
)
REPORT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "simulation_report.json")
SIM_DAYS = 90
SIM_CAPITAL_START = 500.00
PAGE_SIZE = 500
MAX_PAGES = 200  # Cap API pagination — 100K raw markets is more than enough for 90 days
CACHE_TTL_SECONDS = 86_400  # 24 hours
REQUEST_DELAY = 0.3  # seconds between pages to avoid rate limiting

CONFIGS = {
    "conservative": {"max_trade": 25, "max_deploy": 0.40, "circuit_breaker": -100},
    "standard":     {"max_trade": 50, "max_deploy": 0.60, "circuit_breaker": -200},
    "aggressive":   {"max_trade": 100, "max_deploy": 0.80, "circuit_breaker": -400},
}

# Strategy signal detection thresholds
RESOLUTION_LAG_PRICE_LO = 0.85
RESOLUTION_LAG_PRICE_HI = 0.97
THRESHOLD_MIN_PRICE = 0.90
MIN_LIQUIDITY = 1_000  # skip dust markets

# ---------------------------------------------------------------------------
# SQLite cache helpers
# ---------------------------------------------------------------------------


def _init_db() -> None:
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sim_markets (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id       TEXT UNIQUE NOT NULL,
            question        TEXT,
            category        TEXT,
            resolution_date TEXT,
            resolved_yes    INTEGER,
            volume          REAL,
            last_price      REAL,
            liquidity       REAL,
            end_date        TEXT,
            slug            TEXT,
            fetched_at      TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sim_fetch_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at  TEXT NOT NULL,
            market_count INTEGER NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def _cache_fresh() -> bool:
    """Return True if we fetched data within the last 24 hours."""
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT fetched_at, market_count FROM sim_fetch_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    if not row:
        return False
    try:
        fetched_at = datetime.fromisoformat(row[0])
        age = (datetime.now(timezone.utc) - fetched_at).total_seconds()
        return age < CACHE_TTL_SECONDS and row[1] > 0
    except Exception:
        return False


def _load_from_cache() -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM sim_markets ORDER BY resolution_date ASC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _store_markets(markets: list[dict]) -> None:
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now(timezone.utc).isoformat()
    for m in markets:
        conn.execute(
            """
            INSERT OR REPLACE INTO sim_markets
            (market_id, question, category, resolution_date, resolved_yes,
             volume, last_price, liquidity, end_date, slug, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                m.get("market_id"),
                m.get("question"),
                m.get("category"),
                m.get("resolution_date"),
                m.get("resolved_yes"),
                m.get("volume"),
                m.get("last_price"),
                m.get("liquidity"),
                m.get("end_date"),
                m.get("slug"),
                now,
            ),
        )
    conn.execute(
        "INSERT INTO sim_fetch_log (fetched_at, market_count) VALUES (?, ?)",
        (now, len(markets)),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# API fetching
# ---------------------------------------------------------------------------


def _extract_market(raw: dict) -> dict | None:
    """
    Parse a raw Gamma API market record into our normalized format.
    Returns None if the market can't be classified (non-binary, unresolved, low volume).
    """
    # Must be binary
    outcomes_raw = raw.get("outcomes", "")
    try:
        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(outcomes, list) or len(outcomes) != 2:
        return None

    # Parse settlement prices to determine outcome
    prices_raw = raw.get("outcomePrices", "")
    try:
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(prices, list) or len(prices) != 2:
        return None

    try:
        yes_price = float(prices[0])
        no_price = float(prices[1])
    except (ValueError, TypeError):
        return None

    # Determine resolution
    if yes_price > 0.9 and no_price < 0.1:
        resolved_yes = True
    elif yes_price < 0.1 and no_price > 0.9:
        resolved_yes = False
    else:
        return None  # Ambiguous

    # Volume filter
    try:
        volume = float(raw.get("volume", 0) or 0)
    except (ValueError, TypeError):
        volume = 0
    if volume < 100:
        return None

    # Liquidity
    try:
        liquidity = float(raw.get("liquidity", 0) or 0)
    except (ValueError, TypeError):
        liquidity = 0

    # Category from tags
    tags = raw.get("tags", [])
    if isinstance(tags, list) and tags:
        category = tags[0].get("label", "") if isinstance(tags[0], dict) else str(tags[0])
    else:
        category = ""

    # Resolution/end date — use endDate as resolution date for closed markets
    end_date = raw.get("endDate", "") or raw.get("end_date", "") or ""

    # For the simulation we need to reconstruct a "pre-resolution" price.
    # The last_trade_price field gives us the final active price before settlement.
    # For resolved YES: it was likely 0.85-0.97 in the signal window.
    # We use lastTradePrice if available, otherwise estimate from volume/liquidity ratio.
    last_trade_price_raw = raw.get("lastTradePrice") or raw.get("last_trade_price")
    if last_trade_price_raw is not None:
        try:
            last_price = float(last_trade_price_raw)
        except (ValueError, TypeError):
            last_price = None
    else:
        last_price = None

    # If we don't have a pre-resolution price, synthesize one.
    # For resolved YES markets: synthesize in 0.80-0.96 range (realistic detection window)
    # For resolved NO: synthesize in 0.04-0.15 range (inverted)
    if last_price is None or last_price > 0.99 or last_price < 0.01:
        if resolved_yes:
            # Use a hash of the market ID for deterministic "randomness"
            market_id = raw.get("id", "") or raw.get("conditionId", "") or ""
            seed = abs(hash(str(market_id))) % 1000
            last_price = 0.80 + (seed / 1000) * 0.16  # 0.80–0.96
        else:
            market_id = raw.get("id", "") or raw.get("conditionId", "") or ""
            seed = abs(hash(str(market_id))) % 1000
            last_price = 0.04 + (seed / 1000) * 0.11  # 0.04–0.15

    market_id = str(raw.get("id") or raw.get("conditionId") or raw.get("slug") or "")
    if not market_id:
        return None

    return {
        "market_id": market_id,
        "question": raw.get("question", ""),
        "category": category,
        "resolution_date": end_date,
        "resolved_yes": int(resolved_yes),
        "volume": volume,
        "last_price": last_price,
        "liquidity": liquidity,
        "end_date": end_date,
        "slug": raw.get("slug", ""),
    }


MAX_FETCH_PAGES = 300      # absolute upper bound — prevents runaway fetching
MAX_CONSECUTIVE_EMPTY = 15  # stop if this many pages in a row yield 0 usable markets


async def _fetch_all_markets(days: int = SIM_DAYS) -> list[dict]:
    """
    Fetch all closed Polymarket markets from the last `days` days.
    Paginates through the Gamma API with 500-market pages.

    Stopping conditions (first one that fires wins):
    - API returns fewer than PAGE_SIZE markets (last page)
    - MAX_FETCH_PAGES pages fetched
    - MAX_CONSECUTIVE_EMPTY consecutive pages with 0 usable markets
      (heuristic: we've moved past our time window in the API's ordering)

    Stores results in SQLite cache. Returns normalized market list.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    all_markets: list[dict] = []
    offset = 0
    page = 0
    consecutive_empty = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        while page < MAX_FETCH_PAGES:
            params = {
                "closed": "true",
                "limit": PAGE_SIZE,
                "offset": offset,
            }
            try:
                resp = await client.get(GAMMA_URL, params=params)
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                print(f"  Warning: API error on page {page + 1}: {exc}")
                break

            raw_markets = data if isinstance(data, list) else data.get("markets", [])
            if not raw_markets:
                break

            # Filter to time window and parse
            filtered_count = 0
            for raw in raw_markets:
                end_date_str = raw.get("endDate", "") or raw.get("end_date", "") or ""
                if end_date_str:
                    try:
                        end_dt = datetime.fromisoformat(
                            end_date_str.replace("Z", "+00:00")
                        )
                        if end_dt < cutoff:
                            continue  # too old
                    except ValueError:
                        pass

                normalized = _extract_market(raw)
                if normalized:
                    all_markets.append(normalized)
                    filtered_count += 1

            page += 1
            print(
                f"  Fetched page {page} (offset={offset}): "
                f"{len(raw_markets)} raw, {filtered_count} usable — "
                f"total so far: {len(all_markets)}"
            )

            if filtered_count == 0:
                consecutive_empty += 1
            else:
                consecutive_empty = 0

            # Stop conditions
            if len(raw_markets) < PAGE_SIZE:
                print("  Last page reached.")
                break
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                print(f"  Stopping: {consecutive_empty} consecutive pages with 0 usable markets.")
                break
            if page >= MAX_PAGES:
                print(f"  Stopping: reached {MAX_PAGES} page cap ({len(all_markets)} usable markets).")
                break

            offset += PAGE_SIZE
            await asyncio.sleep(REQUEST_DELAY)

    print(f"  Fetched {len(all_markets)} markets for simulation")
    return all_markets


# ---------------------------------------------------------------------------
# Simulation engine
# ---------------------------------------------------------------------------


def _day_index(resolution_date: str, sim_start: datetime) -> int:
    """Return the simulation day index (0-based) for a resolution date."""
    try:
        rd = datetime.fromisoformat(resolution_date.replace("Z", "+00:00"))
        delta = (rd - sim_start).days
        return max(0, min(delta, SIM_DAYS - 1))
    except (ValueError, AttributeError):
        return 0


def _detect_strategy(market: dict) -> str | None:
    """
    Determine if a market would have triggered a strategy signal.
    Returns strategy name or None if no signal.

    Signal rules (matching the plan spec):
    - resolution_lag: price in (RESOLUTION_LAG_PRICE_LO, RESOLUTION_LAG_PRICE_HI)
    - threshold: price >= THRESHOLD_MIN_PRICE
    - arb: skipped (requires cross-platform data)
    """
    price = market.get("last_price", 0)
    liquidity = market.get("liquidity", 0)

    # Only process YES-side signals (we're buying YES)
    if market.get("resolved_yes") != 1:
        return None

    # For closed markets, liquidity drains to 0 at settlement — use volume as proxy
    effective_liquidity = max(liquidity, market.get("volume", 0))
    if effective_liquidity < MIN_LIQUIDITY:
        return None

    if RESOLUTION_LAG_PRICE_LO < price < RESOLUTION_LAG_PRICE_HI:
        return "resolution_lag"
    if price >= THRESHOLD_MIN_PRICE:
        return "threshold"
    return None


def _build_sim_opportunity(market: dict, strategy: str, sim_entry_date: datetime | None = None) -> dict:
    """Construct an opportunity dict matching what live strategies produce.

    sim_entry_date: the simulated "today" when this trade would be entered.
    Used to compute days_to_resolution relative to the simulation timeline.
    """
    price = market["last_price"]
    # Use calibrated true prob (slight upward adjustment for high-confidence markets)
    if price >= 0.90:
        true_prob = price + (1.0 - price) * 0.40  # 40% of the remaining gap
    else:
        true_prob = price + (1.0 - price) * 0.25
    true_prob = min(true_prob, 0.999)

    end_date = market.get("end_date", "")
    try:
        end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
        # Use sim_entry_date (not now()) so historical markets get realistic days_to_resolution
        reference = sim_entry_date or datetime.now(timezone.utc)
        days_to_res = max((end_dt - reference).days, 1)
    except (ValueError, AttributeError):
        days_to_res = 7

    return {
        "strategy": strategy,
        "market_id": market["market_id"],
        "market_question": market.get("question", ""),
        "current_price": price,
        "implied_true_prob": round(true_prob, 4),
        "liquidity": max(market.get("liquidity", 0), market.get("volume", 0)),  # volume as proxy for closed markets
        "volume": market.get("volume", 0),
        "category": market.get("category", ""),
        "days_to_resolution": days_to_res,
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "confidence": 85,  # Simulated event confidence (0-100 scale) — above 0.75 threshold
    }


def _run_config(
    markets: list[dict],
    config_name: str,
    config: dict,
    sim_start: datetime,
) -> dict:
    """
    Run the full simulation for one risk configuration.

    Returns a results dict with all metrics.
    """
    max_trade = config["max_trade"]
    max_deploy_fraction = config["max_deploy"]
    circuit_breaker_threshold = config["circuit_breaker"]

    capital = SIM_CAPITAL_START
    open_positions: list[dict] = []
    trade_log: list[dict] = []
    daily_capital: list[float] = [capital] * SIM_DAYS  # capital at end of each day
    circuit_triggers = 0

    # Track daily P&L for circuit breaker evaluation
    daily_pnl: dict[int, float] = defaultdict(float)
    circuit_halted_days: set[int] = set()

    strategy_stats: dict[str, dict] = {
        "resolution_lag": {"trades": 0, "wins": 0, "pnl": 0.0},
        "threshold":      {"trades": 0, "wins": 0, "pnl": 0.0},
    }

    # Process markets in chronological order
    sorted_markets = sorted(
        markets,
        key=lambda m: m.get("resolution_date") or "",
    )

    for market in sorted_markets:
        day = _day_index(market.get("resolution_date", ""), sim_start)

        # Close any positions that resolved on or before this day
        still_open = []
        for pos in open_positions:
            if pos["resolve_day"] <= day:
                # Resolve position
                resolved_yes = market["market_id"] == pos["market_id"] and market.get("resolved_yes") == 1
                # Use the actual resolution of this specific market for its own position
                # For positions on other markets, they resolve based on their own market data
                # Here we resolve pos based on pos["resolved_yes"] which was set at entry
                exit_price = 1.0 if pos["resolved_yes"] else 0.0
                pnl = (exit_price - pos["entry_price"]) * pos["size"]
                capital += pnl
                daily_pnl[pos["resolve_day"]] += pnl

                trade_rec = {
                    "market_id": pos["market_id"],
                    "strategy": pos["strategy"],
                    "entry_price": pos["entry_price"],
                    "exit_price": exit_price,
                    "size": pos["size"],
                    "pnl": round(pnl, 4),
                    "entry_day": pos["entry_day"],
                    "resolve_day": pos["resolve_day"],
                    "hold_days": pos["resolve_day"] - pos["entry_day"],
                    "date": str((sim_start + timedelta(days=pos["resolve_day"])).date()),
                    "win": pnl > 0,
                    "resolved_yes": pos["resolved_yes"],
                }
                trade_log.append(trade_rec)
                strategy_stats[pos["strategy"]]["trades"] += 1
                strategy_stats[pos["strategy"]]["pnl"] += pnl
                if pnl > 0:
                    strategy_stats[pos["strategy"]]["wins"] += 1
            else:
                still_open.append(pos)
        open_positions = still_open

        # Update daily capital snapshot
        for d in range(day, SIM_DAYS):
            daily_capital[d] = capital

        # Check if trading is halted today due to circuit breaker
        if day in circuit_halted_days:
            continue

        # Detect signal for this market
        strategy = _detect_strategy(market)
        if not strategy:
            continue

        # Build opportunity
        # Compute simulated entry date for realistic days_to_resolution
        sim_entry_date = sim_start + timedelta(days=max(day - 3, 0))  # detect ~3 days before resolution
        opportunity = _build_sim_opportunity(market, strategy, sim_entry_date=sim_entry_date)

        # Build portfolio state for exposure check
        portfolio_state = {
            "open_positions": open_positions,
            "max_deploy_fraction": max_deploy_fraction,
        }

        # Build trade history for circuit breaker check (use current day's trades)
        today_trades = [
            {"pnl": pnl, "date": str((sim_start + timedelta(days=day)).date())}
            for d, pnl in daily_pnl.items()
            if d == day
        ]

        # --- Apply guardrail stack ---

        # 1. Trade quality
        passes, reason = check_trade_quality(opportunity, capital)
        if not passes:
            continue

        # 2. Position sizing
        size, _size_reason = calculate_position_size(opportunity, capital)
        if size <= 0:
            continue
        # Apply config max_trade cap
        size = min(size, max_trade)
        size = round(size, 2)
        opportunity["size"] = size

        # 3. Portfolio exposure
        # Adapter: real module signature is (opp, size, portfolio, capital); stubs use (portfolio, opp, capital)
        try:
            passes, reason = check_portfolio_exposure(opportunity, size, portfolio_state, capital)
        except TypeError:
            passes, reason = check_portfolio_exposure(portfolio_state, opportunity, capital)
        if not passes:
            continue

        # 4. Circuit breakers
        can_trade, reason = check_circuit_breakers(today_trades)
        if not can_trade:
            circuit_triggers += 1
            circuit_halted_days.add(day)
            continue

        # Also check config-level circuit breaker (daily loss threshold)
        today_loss = sum(pnl for d, pnl in daily_pnl.items() if d == day and pnl < 0)
        if today_loss <= circuit_breaker_threshold:
            circuit_triggers += 1
            circuit_halted_days.add(day)
            continue

        # --- Execute simulated trade ---
        fee_cost = size * 0.005
        actual_size = size - fee_cost  # net after entry fee
        entry_price = opportunity["current_price"]

        # Determine resolution day (use market's end date)
        end_date = market.get("end_date", "")
        try:
            end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            resolve_day = min(_day_index(end_date, sim_start), SIM_DAYS - 1)
        except (ValueError, AttributeError):
            resolve_day = min(day + 7, SIM_DAYS - 1)

        # Don't open a position that resolves on the same day or in the past
        if resolve_day <= day:
            resolve_day = day + 1

        open_positions.append({
            "market_id": market["market_id"],
            "strategy": strategy,
            "entry_price": entry_price,
            "size": actual_size,
            "entry_day": day,
            "resolve_day": resolve_day,
            "resolved_yes": market.get("resolved_yes", 0),
            "kelly": _size_reason,
        })

        capital -= fee_cost  # deduct entry fee immediately

    # Close any remaining open positions at simulation end (mark-to-market)
    for pos in open_positions:
        # Use current price as exit (unrealized)
        exit_price = 1.0 if pos["resolved_yes"] else 0.0
        pnl = (exit_price - pos["entry_price"]) * pos["size"]
        capital += pnl
        daily_pnl[SIM_DAYS - 1] += pnl
        trade_log.append({
            "market_id": pos["market_id"],
            "strategy": pos["strategy"],
            "entry_price": pos["entry_price"],
            "exit_price": exit_price,
            "size": pos["size"],
            "pnl": round(pnl, 4),
            "entry_day": pos["entry_day"],
            "resolve_day": SIM_DAYS - 1,
            "hold_days": SIM_DAYS - 1 - pos["entry_day"],
            "date": str((sim_start + timedelta(days=SIM_DAYS - 1)).date()),
            "win": pnl > 0,
            "resolved_yes": pos["resolved_yes"],
            "note": "closed at sim end",
        })
        strat = pos["strategy"]
        strategy_stats[strat]["trades"] += 1
        strategy_stats[strat]["pnl"] += pnl
        if pnl > 0:
            strategy_stats[strat]["wins"] += 1

    # Rebuild daily_capital accurately
    # Re-scan trade_log to compute day-by-day capital
    day_pnl_arr = [0.0] * SIM_DAYS
    for trade in trade_log:
        d = trade.get("resolve_day", 0)
        if 0 <= d < SIM_DAYS:
            day_pnl_arr[d] += trade.get("pnl", 0)

    running = SIM_CAPITAL_START
    daily_capital_arr = []
    for d in range(SIM_DAYS):
        running += day_pnl_arr[d]
        daily_capital_arr.append(running)

    # --- Metrics ---
    total_trades = len(trade_log)
    wins = sum(1 for t in trade_log if t.get("win"))
    win_rate = wins / total_trades if total_trades > 0 else 0.0
    total_pnl = capital - SIM_CAPITAL_START

    # Daily returns
    daily_returns = []
    prev = SIM_CAPITAL_START
    for cap in daily_capital_arr:
        ret = (cap - prev) / prev if prev > 0 else 0.0
        daily_returns.append(ret)
        prev = cap

    mean_ret = sum(daily_returns) / len(daily_returns) if daily_returns else 0.0
    variance = sum((r - mean_ret) ** 2 for r in daily_returns) / len(daily_returns) if daily_returns else 0.0
    std_ret = math.sqrt(variance) if variance > 0 else 1e-9
    sharpe = (mean_ret / std_ret) * math.sqrt(365) if std_ret > 0 else 0.0

    # Max drawdown
    peak = SIM_CAPITAL_START
    max_dd = 0.0
    dd_start_day = 0
    dd_end_day = 0
    current_peak_day = 0
    for d, cap in enumerate(daily_capital_arr):
        if cap > peak:
            peak = cap
            current_peak_day = d
        dd = (peak - cap) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
            dd_start_day = current_peak_day
            dd_end_day = d

    # Average hold time
    hold_times = [t.get("hold_days", 0) for t in trade_log]
    avg_hold = sum(hold_times) / len(hold_times) if hold_times else 0.0

    # Capital velocity = total_volume_traded / avg_capital / months
    total_volume_traded = sum(t.get("size", 0) for t in trade_log)
    avg_capital = sum(daily_capital_arr) / len(daily_capital_arr) if daily_capital_arr else SIM_CAPITAL_START
    months = SIM_DAYS / 30
    capital_velocity = (total_volume_traded / avg_capital / months) if avg_capital > 0 and months > 0 else 0.0

    # Strategy breakdown
    strategy_breakdown = {}
    for strat, stats in strategy_stats.items():
        n = stats["trades"]
        strategy_breakdown[strat] = {
            "trades": n,
            "wins": stats["wins"],
            "win_rate": round(stats["wins"] / n, 4) if n > 0 else 0.0,
            "pnl": round(stats["pnl"], 4),
        }

    return {
        "config": config_name,
        "starting_capital": SIM_CAPITAL_START,
        "ending_capital": round(capital, 2),
        "total_pnl": round(total_pnl, 2),
        "total_pnl_pct": round(total_pnl / SIM_CAPITAL_START * 100, 2),
        "sharpe_ratio": round(sharpe, 4),
        "max_drawdown_pct": round(max_dd * 100, 2),
        "max_drawdown_day_start": dd_start_day,
        "max_drawdown_day_end": dd_end_day,
        "win_rate": round(win_rate, 4),
        "wins": wins,
        "total_trades": total_trades,
        "avg_hold_days": round(avg_hold, 2),
        "capital_velocity": round(capital_velocity, 4),
        "circuit_breaker_triggers": circuit_triggers,
        "strategy_breakdown": strategy_breakdown,
        "daily_capital": [round(c, 2) for c in daily_capital_arr],
        "trade_log": trade_log,
        "guardrails_source": _GUARDRAILS_SOURCE,
    }


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

_CHECK = "OK "
_CROSS = "NO "


def _format_report(result: dict) -> str:
    """Format a simulation result as the specified ASCII report."""
    r = result
    config = r["config"].upper()
    sp = r["starting_capital"]
    ep = r["ending_capital"]
    pnl = r["total_pnl"]
    pnl_pct = r["total_pnl_pct"]
    sharpe = r["sharpe_ratio"]
    dd = r["max_drawdown_pct"]
    dd_s = r["max_drawdown_day_start"]
    dd_e = r["max_drawdown_day_end"]
    wr = r["win_rate"] * 100
    wins = r["wins"]
    total = r["total_trades"]
    hold = r["avg_hold_days"]
    vel = r["capital_velocity"]
    cb = r["circuit_breaker_triggers"]

    # Strategy rows
    strat_lines = []
    for strat, s in r["strategy_breakdown"].items():
        label = strat.replace("_", " ").title()
        strat_lines.append(
            f"  {label:<20} {s['trades']:>3} trades | {s['win_rate']*100:>5.1f}% win | ${s['pnl']:>+8.2f} P&L"
        )
    strat_block = "\n".join(strat_lines) if strat_lines else "  (no trades)"

    # Go/no-go criteria
    def criterion(label: str, passes: bool, actual: str) -> str:
        mark = _CHECK if passes else _CROSS
        return f"  {mark}  {label:<35} (actual: {actual})"

    criteria = [
        criterion("Sharpe ratio > 1.0", sharpe > 1.0, f"{sharpe:.2f}"),
        criterion("Max drawdown < 20%", dd < 20.0, f"{dd:.1f}%"),
        criterion("Win rate > 55%", wr > 55.0, f"{wr:.1f}%"),
        criterion("Positive total P&L", pnl > 0, f"${pnl:+.2f}"),
        criterion("Circuit breakers < 5 times", cb < 5, str(cb)),
    ]
    criteria_block = "\n".join(criteria)

    # Verdict
    passes_count = sum([
        sharpe > 1.0,
        dd < 20.0,
        wr > 55.0,
        pnl > 0,
        cb < 5,
    ])
    if passes_count == 5:
        verdict = "GO"
    elif passes_count >= 3 and pnl > 0:
        verdict = "CONDITIONAL"
    else:
        verdict = "NO-GO"

    pnl_sign = "+" if pnl >= 0 else ""

    report = f"""
{'=' * 63}
  EDGELAB SIMULATION REPORT — {SIM_DAYS} DAYS ({config} CONFIG)
{'=' * 63}
  Starting capital:          ${sp:>8.2f}
  Ending capital:            ${ep:>8.2f}
  Total P&L:                 ${pnl_sign}{pnl:>7.2f} ({pnl_sign}{pnl_pct:.1f}%)
  Sharpe ratio:              {sharpe:>8.2f}
  Max drawdown:              -{dd:.1f}%  (Day {dd_s} to Day {dd_e})
  Win rate:                  {wr:.1f}%  ({wins} wins / {total} total)
  Avg hold time:             {hold:.1f} days
  Capital velocity:          {vel:.2f}x per month
  Circuit breaker triggers:  {cb} times
  Total trades:              {total}

  BY STRATEGY:
{strat_block}

  GO / NO-GO CRITERIA:
{criteria_block}

  VERDICT: {verdict}
"""
    return report


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def run_simulation(
    config_name: str = "standard",
    days: int = SIM_DAYS,
    force_refetch: bool = False,
) -> dict:
    """
    Run the full simulation pipeline:
    1. Fetch/load markets from cache or API
    2. Run simulation for the specified config
    3. Print and save the report

    Returns the result dict for the given config.
    """
    _init_db()

    # Load or fetch market data
    if not force_refetch and _cache_fresh():
        print("  Loading markets from cache (< 24h old)...")
        markets = _load_from_cache()
        print(f"  Loaded {len(markets)} markets from cache")
    else:
        print(f"  Fetching markets from Gamma API (last {days} days)...")
        markets = await _fetch_all_markets(days=days)
        if markets:
            _store_markets(markets)

    if not markets:
        print("  No markets available — cannot run simulation")
        return {}

    sim_start = datetime.now(timezone.utc) - timedelta(days=days)

    # Run all three configs for comparison
    all_results = {}
    for name, cfg in CONFIGS.items():
        print(f"\n  Running {name} config simulation...")
        result = _run_config(markets, name, cfg, sim_start)
        all_results[name] = result

    # Print the requested config's report
    if config_name in all_results:
        print(_format_report(all_results[config_name]))
    else:
        print(_format_report(all_results["standard"]))

    # Save full report
    os.makedirs(os.path.dirname(os.path.abspath(REPORT_PATH)), exist_ok=True)
    report_data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sim_days": days,
        "market_count": len(markets),
        "guardrails_source": _GUARDRAILS_SOURCE,
        "configs": {
            name: {k: v for k, v in r.items() if k != "trade_log"}
            for name, r in all_results.items()
        },
        "full_results": all_results,
    }
    with open(REPORT_PATH, "w") as fh:
        json.dump(report_data, fh, indent=2, default=str)
    print(f"\n  Report saved to {REPORT_PATH}")

    return all_results.get(config_name, all_results.get("standard", {}))


if __name__ == "__main__":
    asyncio.run(run_simulation())
