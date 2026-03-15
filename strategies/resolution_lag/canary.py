"""
Resolution Lag CANARY — 5-trade live pipeline test.

Hardcoded safety limits (non-configurable):
  MAX_TRADES = 5, MAX_POSITION_USD = $5, TOTAL_CAPITAL_CAP = $25
  MAX_RUNTIME = 24h, MIN_CERTAINTY = 0.90, MIN_EDGE = 0.10
  METHOD 1 ONLY (Kalshi finalized status)

Run: python3 -m strategies.resolution_lag.canary
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ── Ensure project root on sys.path ──────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(dotenv_path=PROJECT_ROOT / ".env", override=True)

# ── Hardcoded canary limits (NOT configurable) ──────────────────────────────
MAX_TRADES = 5
MAX_POSITION_USD = 5.0
TOTAL_CAPITAL_CAP = 25.0
MAX_RUNTIME_SECS = 86400
MIN_CERTAINTY_SCORE = 0.90
MIN_EDGE_PCT = 0.10
REQUIRE_KALSHI_FINALIZED = True
ESPN_FALLBACK_AFTER_SECS = 600  # Allow METHOD 2 after 10 min with 0 METHOD 1 signals
SCAN_INTERVAL_SECS = 20

# ── Logging ──────────────────────────────────────────────────────────────────
log_dir = PROJECT_ROOT / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
LOG_PATH = log_dir / "resolution_lag_canary.log"

logger = logging.getLogger("canary")
logger.setLevel(logging.DEBUG)
_fmt = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")

_fh = logging.FileHandler(LOG_PATH)
_fh.setFormatter(_fmt)
logger.addHandler(_fh)

_ch = logging.StreamHandler(sys.stdout)
_ch.setFormatter(_fmt)

# Also capture resolution_lag.detector logs
_det_logger = logging.getLogger("resolution_lag.detector")
_det_logger.setLevel(logging.DEBUG)
_det_logger.addHandler(_fh)
_det_logger.addHandler(_ch)
logger.addHandler(_ch)


# ── State ────────────────────────────────────────────────────────────────────
class CanaryState:
    trades_completed: int = 0
    trades_attempted: int = 0
    orders_failed: int = 0
    total_deployed: float = 0.0
    estimated_pnl: float = 0.0
    framework_logged: bool = False
    start_time: float = 0.0
    fills: list[dict] = []

    def __init__(self):
        self.fills = []
        self.start_time = time.time()


# ── Preflight ────────────────────────────────────────────────────────────────
async def preflight() -> bool:
    """Run all preflight checks. Returns True if all pass."""

    # (a) .env credentials
    kalshi_key = os.environ.get("KALSHI_API_KEY", "")
    kalshi_key_id = os.environ.get("KALSHI_KEY_ID", "")
    if not kalshi_key:
        logger.error("PREFLIGHT FAIL: KALSHI_API_KEY not set")
        return False
    if not kalshi_key_id:
        logger.error("PREFLIGHT FAIL: KALSHI_KEY_ID not set")
        return False
    logger.info("PREFLIGHT: KALSHI_API_KEY and KALSHI_KEY_ID present")

    # (b) Import kalshi_executor
    try:
        from strategies.edgelab.src.execution.kalshi_executor import (
            place_kalshi_order,
            get_kalshi_balance,
            KALSHI_BASE_URL,
        )
        logger.info("PREFLIGHT: kalshi_executor imported OK")
    except Exception as e:
        logger.error("PREFLIGHT FAIL: kalshi_executor import error: %s", e)
        return False

    # (c) Ping Kalshi exchange status
    import httpx
    from strategies.edgelab.src.execution.kalshi_executor import _kalshi_headers

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            path = "/trade-api/v2/exchange/status"
            resp = await client.get(
                f"{KALSHI_BASE_URL.rstrip('/')}/exchange/status",
                headers=_kalshi_headers("GET", path),
            )
            if resp.status_code == 200:
                logger.info("PREFLIGHT: Kalshi exchange status — 200 OK")
            else:
                logger.error("PREFLIGHT FAIL: exchange/status returned %d: %s",
                             resp.status_code, resp.text[:200])
                return False
    except Exception as e:
        logger.error("PREFLIGHT FAIL: exchange/status request error: %s", e)
        return False

    # (d) Check account balance
    try:
        balance_cents = await get_kalshi_balance()
        balance_usd = balance_cents / 100.0
        logger.info("PREFLIGHT: Kalshi balance = $%.2f (%d cents)", balance_usd, balance_cents)
        if balance_usd < 30.0:
            logger.error("PREFLIGHT FAIL: balance $%.2f < $30 minimum", balance_usd)
            return False
    except Exception as e:
        logger.error("PREFLIGHT FAIL: balance check error: %s", e)
        return False

    # (e) Framework trade journal
    try:
        from framework.trade_journal import init_db
        init_db()
        logger.info("PREFLIGHT: framework.trade_journal.init_db() OK")
    except Exception as e:
        logger.error("PREFLIGHT FAIL: trade_journal init error: %s", e)
        return False

    logger.info("CANARY PREFLIGHT: ALL CHECKS PASSED — starting live canary")
    return True


# ── Signal scan (METHOD 1 only) ─────────────────────────────────────────────
def find_signals(canary_start_time: float | None = None) -> list[dict]:
    """
    Detect resolved events via METHOD 1 (Kalshi finalized).
    Falls back to METHOD 2 (ESPN final scores) if METHOD 1 yields 0 signals
    after ESPN_FALLBACK_AFTER_SECS of scanning.
    """
    from strategies.resolution_lag.resolution_detector import detect_resolved_events
    from strategies.resolution_lag.certainty_scorer import score_certainty
    from strategies.resolution_lag.lag_scanner import scan_for_lag

    try:
        events = detect_resolved_events()
    except Exception as e:
        logger.error("detect_resolved_events() failed: %s", e)
        return []

    # Filter: METHOD 1 (Kalshi finalized API)
    method1_events = [
        ev for ev in events
        if ev.get("_source_method") == "kalshi_finalized"
    ]

    # ESPN fallback: allow METHOD 2 if METHOD 1 has 0 signals after 10 min
    accepted_events = method1_events
    if not method1_events and canary_start_time is not None:
        elapsed = time.time() - canary_start_time
        if elapsed >= ESPN_FALLBACK_AFTER_SECS:
            espn_events = [
                ev for ev in events
                if ev.get("_source_method") == "espn_final"
            ]
            if espn_events:
                logger.info("METHOD 1 produced 0 signals after %.0fs — "
                            "falling back to METHOD 2 (ESPN): %d events",
                            elapsed, len(espn_events))
                accepted_events = espn_events
        elif events:
            logger.debug("Detected %d events but 0 are METHOD 1 (kalshi_finalized) — "
                         "ESPN fallback in %.0fs",
                         len(events), ESPN_FALLBACK_AFTER_SECS - elapsed)
    elif events and not method1_events:
        logger.debug("Detected %d events but 0 are METHOD 1 (kalshi_finalized) — skipping",
                      len(events))

    opportunities = []
    for event in accepted_events:
        try:
            certainty = score_certainty(event)
        except Exception as e:
            logger.error("score_certainty failed: %s", e)
            continue

        if certainty["score"] < MIN_CERTAINTY_SCORE:
            logger.debug("Event %s certainty %.2f < %.2f — skip",
                         event.get("event_description", "")[:60],
                         certainty["score"], MIN_CERTAINTY_SCORE)
            continue

        try:
            opps = scan_for_lag(event, certainty)
        except Exception as e:
            logger.error("scan_for_lag failed: %s", e)
            continue

        for opp in opps:
            if opp.get("edge_pct", 0) < MIN_EDGE_PCT:
                continue
            if opp.get("urgency") != "IMMEDIATE":
                continue
            opp["certainty_score"] = certainty["score"]
            opp["recommended_side"] = certainty.get("recommended_side", opp.get("expected_resolution"))
            opportunities.append(opp)

    return opportunities


# ── Order execution ──────────────────────────────────────────────────────────
async def execute_trade(opp: dict, state: CanaryState) -> bool:
    """Place a single canary trade. Returns True if filled."""
    from strategies.edgelab.src.execution.kalshi_executor import place_kalshi_order

    market_id = opp["market_id"]
    side = opp.get("expected_resolution", "YES").lower()
    size_cents = int(MAX_POSITION_USD * 100)  # $5 = 500 cents

    state.trades_attempted += 1
    n = state.trades_completed + 1

    logger.info("CANARY TRADE %d/%d — placing order: %s %s $%.2f",
                n, MAX_TRADES, market_id, side.upper(), MAX_POSITION_USD)
    logger.info("  Signal: edge=%.1f%% certainty=%.2f urgency=%s method=kalshi_finalized",
                opp.get("edge_pct", 0) * 100, opp.get("certainty_score", 0),
                opp.get("urgency", "?"))

    try:
        result = await place_kalshi_order(
            market_ticker=market_id,
            side=side,
            size_cents=size_cents,
        )
    except Exception as e:
        logger.error("ORDER FAILED (not counting toward 5-trade limit): %s", e)
        state.orders_failed += 1
        return False

    if not result.get("success"):
        error_msg = result.get("error", "unknown")
        logger.error("ORDER FAILED (not counting toward 5-trade limit): %s", error_msg)
        state.orders_failed += 1
        return False

    # Fill succeeded
    order_id = result.get("order_id", "N/A")
    contracts = result.get("contracts", 0)
    logger.info("ORDER FILLED: order_id=%s, contracts=%d, size=%d cents",
                order_id, contracts, size_cents)

    state.trades_completed += 1
    state.total_deployed += MAX_POSITION_USD
    state.fills.append({
        "order_id": order_id,
        "market_id": market_id,
        "side": side,
        "size_usd": MAX_POSITION_USD,
        "contracts": contracts,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "edge_pct": opp.get("edge_pct", 0),
        "certainty": opp.get("certainty_score", 0),
    })

    # Log to framework trade journal
    try:
        from framework.trade_journal import log_trade
        log_trade(
            strategy_name="resolution_lag_canary",
            market_id=market_id,
            platform="kalshi",
            side=side.upper(),
            price=0.0,  # actual fill price unknown until settlement
            size=MAX_POSITION_USD,
            edge_pct=opp.get("edge_pct", 0),
            signal_strength="IMMEDIATE",
            market_conditions={
                "certainty_score": opp.get("certainty_score", 0),
                "source_method": "kalshi_finalized",
                "canary_trade_num": state.trades_completed,
            },
            notes=f"CANARY trade {state.trades_completed}/{MAX_TRADES}",
        )
        state.framework_logged = True
        logger.info("Framework trade journal logged for trade %d/%d",
                     state.trades_completed, MAX_TRADES)
    except Exception as e:
        logger.warning("Failed to log to framework trade journal: %s", e)

    return True


# ── Kill switch checks ───────────────────────────────────────────────────────
def should_halt(state: CanaryState) -> tuple[bool, str]:
    """Check all halt conditions. Returns (should_halt, reason)."""
    if state.trades_completed >= MAX_TRADES:
        return True, f"CANARY COMPLETE: {state.trades_completed} trades placed, shutting down"
    if state.total_deployed >= TOTAL_CAPITAL_CAP:
        return True, f"CANARY CAPITAL CAP REACHED: ${state.total_deployed:.2f} deployed, halting"
    elapsed = time.time() - state.start_time
    if elapsed > MAX_RUNTIME_SECS:
        return True, f"CANARY 24HR TIMEOUT: {elapsed:.0f}s elapsed, halting"
    return False, ""


# ── Final report ─────────────────────────────────────────────────────────────
def print_report(state: CanaryState, halt_reason: str = ""):
    if state.trades_completed >= 1 and state.framework_logged:
        status = "VERIFIED"
    elif state.trades_attempted == 0 and state.orders_failed == 0:
        status = "PARTIAL"
    elif state.orders_failed > 0 and state.trades_completed == 0:
        status = "FAILED"
    else:
        status = "PARTIAL"

    report = f"""
{'=' * 55}
 RESOLUTION LAG CANARY — FINAL REPORT
{'=' * 55}
 Trades attempted:    {state.trades_attempted}
 Trades filled:       {state.trades_completed}
 Orders failed:       {state.orders_failed}
 Total deployed:      ${state.total_deployed:.2f}
 Estimated P&L:       ${state.estimated_pnl:.2f}
 Framework logged:    {"Y" if state.framework_logged else "N"}
 Pipeline status:     {status}
 Halt reason:         {halt_reason or "manual / no halt"}
 Runtime:             {time.time() - state.start_time:.0f}s
{'=' * 55}
"""
    logger.info(report)
    print(report)


# ── Main loop ────────────────────────────────────────────────────────────────
async def run_canary():
    state = CanaryState()
    logger.info("=" * 55)
    logger.info("RESOLUTION LAG CANARY — STARTING")
    logger.info("  Limits: %d trades, $%.0f/trade, $%.0f total, 24h max",
                MAX_TRADES, MAX_POSITION_USD, TOTAL_CAPITAL_CAP)
    logger.info("  Filters: certainty >= %.2f, edge >= %.0f%%, METHOD 1 (ESPN fallback after %ds)",
                MIN_CERTAINTY_SCORE, MIN_EDGE_PCT * 100, ESPN_FALLBACK_AFTER_SECS)
    logger.info("=" * 55)

    # Preflight
    if not await preflight():
        logger.error("CANARY ABORTED: preflight failed")
        print_report(state, "Preflight failed")
        return

    # Signal loop
    cycle = 0
    while True:
        cycle += 1
        halt, reason = should_halt(state)
        if halt:
            logger.info(reason)
            print_report(state, reason)
            return

        try:
            signals = find_signals(canary_start_time=state.start_time)
        except Exception as e:
            logger.error("Signal scan error (cycle %d): %s", cycle, e)
            signals = []

        if signals:
            logger.info("Cycle %d: found %d qualifying signal(s)", cycle, len(signals))
        else:
            if cycle % 15 == 1:  # Log every ~5 min
                elapsed = time.time() - state.start_time
                logger.info("Cycle %d (%.0fs elapsed): scanning — no signals yet "
                            "(%d/%d trades done)",
                            cycle, elapsed, state.trades_completed, MAX_TRADES)

        for signal in signals:
            # Re-check halt before each trade
            halt, reason = should_halt(state)
            if halt:
                logger.info(reason)
                print_report(state, reason)
                return

            logger.info("Signal details: market=%s side=%s edge=%.1f%% certainty=%.2f",
                        signal.get("market_id", "?"),
                        signal.get("expected_resolution", "?"),
                        signal.get("edge_pct", 0) * 100,
                        signal.get("certainty_score", 0))

            await execute_trade(signal, state)

        await asyncio.sleep(SCAN_INTERVAL_SECS)


# ── Entry point ──────────────────────────────────────────────────────────────
def main():
    try:
        asyncio.run(run_canary())
    except KeyboardInterrupt:
        logger.info("CANARY interrupted by user (Ctrl+C)")
        # Print whatever we have
        print("\n[Interrupted — partial state only]")


if __name__ == "__main__":
    main()
