# EdgeLab Live Trading Prep — $50-100 Test

## Live Trading Readiness Checklist

| # | Item | Status | Notes |
|---|------|--------|-------|
| 1 | TRADING_MODE flag | YES | `.env` has `TRADING_MODE=LIVE` — executor checks this |
| 2 | Kalshi executor wired | YES | `kalshi_executor.py` works with Bearer token auth |
| 3 | KALSHI_API_KEY set | **NO** | Missing from `strategies/edgelab/.env` — exists in `strategies/bot/.env` |
| 4 | Max position size limit | YES | `KALSHI_MAX_TRADE_CENTS=1000` ($10 default) |
| 5 | Daily loss limit | YES | Circuit breaker: -$200 daily → halt |
| 6 | Max concurrent positions | YES | 20 max open positions, 20% max deployed |
| 7 | Paper-only blocks | **BLOCKER** | No strategy emits `platform: "kalshi"` — see below |
| 8 | Kalshi feed (real data) | **NO** | Falls back to mock when `KALSHI_API_KEY` is missing |
| 9 | Order pricing | **BLOCKER** | Hardcoded ~50¢ midpoint — no order book fetch |
| 10 | Mode manager vs env var | WARN | `edgelab:mode` Redis key and `TRADING_MODE` env are independent |

## Blockers Requiring Code Changes

### Blocker 1: No strategy emits `platform: "kalshi"` opportunities

**Problem:** `autonomous_trader._execute_live_trade()` routes to Kalshi executor only when
`opp.get("platform") == "kalshi"`. No current strategy (arb_detector, resolution_lag,
threshold_scanner, whale_tracker) sets this field in published opportunities.

**Fix needed:** In each strategy's Redis publish call, add `"platform": "kalshi"` to the
opportunity payload when the market source is Kalshi. Estimated ~5-10 lines per strategy.

Files to modify:
- `src/strategies/arb_detector.py` — publish to `opportunities:arb`
- `src/strategies/resolution_lag.py` — publish to `opportunities:resolution_lag`
- `src/strategies/whale_tracker.py` — publish to `opportunities:whale`

### Blocker 2: Order pricing is hardcoded

**Problem:** `kalshi_executor.py` calculates order price as `size_cents // contracts`,
assuming ~50¢ midpoint. No live order book is fetched. This means live orders would be
placed at incorrect prices, potentially buying at terrible fills.

**Fix needed:** Add order book fetch before placing orders. Use Kalshi's
`GET /markets/{ticker}/orderbook` endpoint to get best ask/bid, then place limit order
at a reasonable price.

File: `src/execution/kalshi_executor.py` — ~20 lines to add order book fetch logic.

### Blocker 3: Kalshi feed uses mock data without API key

**Problem:** `src/feeds/kalshi.py` falls back to generating 20 mock markets when
`KALSHI_API_KEY` is not set. Strategies can't detect real Kalshi opportunities from
fake data.

**Fix:** Add `KALSHI_API_KEY` to `strategies/edgelab/.env` (same key from `strategies/bot/.env`).

## Config Changes for $50-100 Live Test

The following values should be set in `strategies/edgelab/.env`:

```env
# Live trading config — conservative test
TOTAL_CAPITAL=100
KALSHI_MAX_TRADE_CENTS=1000    # $10 max per trade
TRADING_MODE=LIVE              # Already set
KALSHI_ENABLED=true            # Already set
KALSHI_API_KEY=<copy from strategies/bot/.env>
```

And in `src/execution/autonomous_trader.py`, these constants apply:
- `MAX_TRADE_SIZE = 50` → will be capped to $10 by KALSHI_MAX_TRADE_CENTS
- `MAX_DEPLOYED_FRACTION = 0.20` → $20 max deployed (20% of $100)
- Circuit breaker daily loss: -$200 → should be lowered to -$20 for test

## Manual Actions Required

1. **Copy Kalshi API key**: `strategies/bot/.env` → `strategies/edgelab/.env`
2. **Fix order pricing** (Blocker 2) before going live
3. **Add platform field** to strategy opportunity payloads (Blocker 1)
4. **Verify Kalshi account balance** has at least $100
5. **Lower daily loss limit** in circuit_breaker.py for the test ($20 instead of $200)
6. **Do NOT set DRY_RUN=false** — there is no DRY_RUN flag. Trading is controlled by
   `TRADING_MODE` env var. It is ALREADY set to `LIVE` in `.env`, but execution falls
   back to paper because `KALSHI_API_KEY` is missing (Gate 4 in autonomous_trader).
