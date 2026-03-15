# BUG REPORT: invalid_UUID errors in live trading

**Date:** 2026-03-13
**Severity:** High — prevents order status checks for all open positions
**Status:** Diagnosed, not yet fixed

---

## Symptom

Every position monitoring cycle produces a 400 error from Kalshi:

```
Kalshi GET /portfolio/orders/{'success': True, 'order_id': '9ab9e949-df2b-4710-aa43-9ab493d54048', 'contracts': 60, 'cost_usd': 30.0}
→ 400: {"error":{"code":"invalid_UUID","message":"invalid UUID","service":"query-exchange"}}
```

The full Python dict is being interpolated into the URL path instead of a bare UUID string.

## Root Cause

**`live_engine.py:241`** stores whatever `place_order()` returns into `self.open_orders[trade_id]`.

**`kalshi_executor.py:150-151`** then uses that value directly in the URL:
```python
async def get_order_status(self, order_id: str) -> dict:
    resp = await self._request("GET", f"/portfolio/orders/{order_id}")
```

The current on-disk code at `kalshi_executor.py:139-146` correctly extracts the UUID string from the API response and returns it. However, the **running process** has an older version of this code that returns the full response dict instead of the extracted UUID.

### Evidence

The executor's own log confirms it extracts the UUID correctly at log time:
```
[INFO] kalshi_executor — Order placed: 9ab9e949-df2b-4710-aa43-9ab493d54048 (60 contracts)
```

But `live_engine` receives and stores the full dict:
```
[INFO] live_engine — LIVE ORDER placed: ... | order_id={'success': True, 'order_id': '9ab9e949-...', ...}
```

This discrepancy means the on-disk fix at lines 139-146 was applied **after** the bot process started. The running process still has the old return path.

## Expected Format

`order_id` should be a bare UUID string: `9ab9e949-df2b-4710-aa43-9ab493d54048`

## Actual Value Being Stored

`order_id` is a dict: `{'success': True, 'order_id': '9ab9e949-df2b-4710-aa43-9ab493d54048', 'contracts': 60, 'cost_usd': 30.0}`

## Affected Code Path

1. `kalshi_executor.py:133` — API response received as dict
2. `kalshi_executor.py:139-146` — Should extract UUID string (fixed on disk, not in running process)
3. `live_engine.py:211` — `order_id = await self.executor.place_order(signal, size)`
4. `live_engine.py:241` — `self.open_orders[trade_id] = order_id` (stores full dict)
5. `live_engine.py:275` — `get_order_status(order_id)` passes dict to URL
6. `kalshi_executor.py:151` — `f"/portfolio/orders/{order_id}"` interpolates dict repr into URL

## Impact

- All 4 open positions cannot be status-checked
- Exit logic (resolution detection, stop-loss, time-based exits) is non-functional
- Positions will remain open indefinitely until manually closed

## Recommended Fix

1. **Restart the bot process** so the on-disk fix at `kalshi_executor.py:139-146` takes effect
2. **Add a defensive guard** in `live_engine.py:241` to extract UUID if a dict is received:
   ```python
   if isinstance(order_id, dict):
       order_id = order_id.get("order_id", str(order_id))
   self.open_orders[trade_id] = order_id
   ```
3. **Manually close** the 4 orphaned positions via Kalshi dashboard or API
