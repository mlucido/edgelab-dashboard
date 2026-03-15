# Resolution Lag — Viability Finding

**Date:** 2026-03-15
**Status: EDGE DOES NOT EXIST ON KALSHI ALONE**

---

## What Was Tested

- Fetched 50 settled Kalshi markets via `GET /trade-api/v2/markets?status=settled`
- Queried the orderbook for each of the first 30 markets via `GET /markets/{ticker}/orderbook`
- Checked for stale buy-side orders on the winning outcome (would indicate a lag window to front-run)
- Also scanned 200 recently settled markets (closed within last 4 hours) for any transitional windows
- Checked all valid market status values: `settled` and `closed` are the only two that return data (`finalized` and `determined` return HTTP 400)

---

## Findings

| Metric | Result |
|--------|--------|
| Markets fetched | 50 |
| Orderbooks checked | 30 |
| Empty orderbooks (fully cleared) | 30 |
| Stale orders / lag opportunities found | **0** |

**All 30 orderbooks were completely empty by the time a market reached `status=settled`.**
No stale resting orders existed on the winning side.

---

## Conclusion

**Kalshi CLOBs clear atomically at settlement.** When a market settles, Kalshi cancels all open resting orders before the market transitions to `settled` status. There is no observable lag window between the resolution event and orderbook clearing.

The resolution lag strategy premise — that market participants leave stale orders at sub-\$1.00 prices after a YES/NO outcome is known — does not hold on Kalshi's infrastructure.

---

## Why the Edge Was Theorized

The strategy was designed around the hypothesis that Kalshi resolves markets asynchronously: the result gets posted, but resting orders aren't cancelled for some seconds/minutes, allowing a fast bot to fill at, say, \$0.72 on a known winner.

**That window either does not exist or is sub-second** (faster than any API-based strategy can exploit).

---

## Viable Path Forward

The edge may exist in **cross-platform latency** between Polymarket and Kalshi:
- Polymarket resolves first (on-chain UMA oracle) → Kalshi book hasn't updated yet
- Or vice versa
- Arbitrage the delta between the two platforms during the resolution window

**This requires Polymarket US deposits to reopen.** As of 2026-03-15, US users cannot deposit on Polymarket.

**Recommendation:** Keep strategy in `PAPER` mode (default). Revisit when:
1. Polymarket reopens US deposits, OR
2. A new prediction market platform with slower settlement is identified

---

## Current Config

`MODE` defaults to `"PAPER"` in `config.py` (reads env var `RESOLUTION_LAG_MODE`, falls back to `"PAPER"`). No code changes were needed — the strategy was already in paper mode.
