# Polymarket 401 Auth Diagnosis

## Status: FAILING — all CLOB /trades queries return 401 Unauthorized

## Failing Endpoint

```
GET https://clob.polymarket.com/trades?maker_address={wallet}&limit=20
→ HTTP/1.1 401 Unauthorized
```

This endpoint is called every 30 seconds by the **whale_tracker** strategy
(`src/strategies/whale_tracker.py:194`) for each of the 5 tracked whale wallets.

## Affected Whale Addresses (from `data/whale_registry_v2.json`)

| # | Address |
|---|---------|
| 1 | `0xD91A176De78D4297AA3553C7B42402384f0bA200` |
| 2 | `0x1B7a916090B932DC515e8741E70633C302940bFd` |
| 3 | `0x52D29E69e64ed2e5b2E0697F423cBC19b0E1E52C` |
| 4 | `0x3F04cc827e9E1d22ca9D3F4baB3D3e38aE89DE54` |
| 5 | `0xEBfA258FcC5Ba54CC72C7d7249992b3CB28b7A8F` |

## Root Cause

The Polymarket CLOB API (`clob.polymarket.com`) requires authentication for the
`/trades` endpoint. The EdgeLab `.env` file is **missing** the required credentials:

- `POLYMARKET_API_KEY` — **not present** in `.env`
- `POLYGON_PRIVATE_KEY` — **not present** in `.env`

The `.env.example` lists both keys but the live `.env` has neither set.

The CLOB API uses a **signature-based auth** scheme where requests must be signed
with the Polygon wallet private key. The `POLYMARKET_API_KEY` is a separate API key
obtained from the Polymarket developer portal.

## Files That Need Credentials

| File | Credential Used | Purpose |
|------|----------------|---------|
| `strategies/edgelab/.env` | `POLYMARKET_API_KEY` | CLOB order placement, trade queries |
| `strategies/edgelab/.env` | `POLYGON_PRIVATE_KEY` | Wallet signing, USDC balance checks |
| `src/strategies/whale_tracker.py` | (none — uses unauthenticated requests) | Whale trade monitoring |
| `src/execution/live_trader.py` | `POLYMARKET_API_KEY` | Live order execution |
| `src/execution/autonomous_trader.py` | `POLYMARKET_API_KEY` | Autonomous trade execution |

## Other Feeds (NOT affected)

- **Polymarket REST feed** (`src/feeds/polymarket.py`): Uses the **Gamma API**
  (`gamma-api.polymarket.com/markets`) which is public/unauthenticated — working fine
  (logs show "REST poll complete: 500 binary markets published")
- **Polymarket WebSocket** (`wss://ws-subscriptions-clob.polymarket.com/ws/market`):
  Public WebSocket — working fine
- **Kalshi feed**: Uses separate RSA key auth — working independently

## Step-by-Step Fix

### 1. Get a Polymarket CLOB API Key

1. Go to https://polymarket.com and sign in with your wallet
2. Navigate to Settings > API Keys (or https://polymarket.com/settings/api)
3. Generate a new API key
4. Copy the key value

### 2. Get your Polygon Private Key

If you already have a funded Polygon wallet:
1. Export the private key from MetaMask or your wallet provider
2. Ensure the wallet has USDC on Polygon mainnet

### 3. Update `.env`

Add to `strategies/edgelab/.env`:
```
POLYMARKET_API_KEY=<your-api-key>
POLYGON_PRIVATE_KEY=<your-private-key-hex>
POLYGON_RPC_URL=https://polygon-rpc.com
```

### 4. Fix Whale Tracker Auth

The whale tracker (`whale_tracker.py:194`) calls the CLOB `/trades` endpoint
**without any auth headers**. After adding the API key to `.env`, the whale tracker
code needs to be updated to include auth headers in its httpx requests:

```python
headers = {"Authorization": f"Bearer {os.getenv('POLYMARKET_API_KEY', '')}"}
resp = await client.get(
    f"{CLOB_BASE}/trades",
    params={"maker_address": wallet, "limit": 20},
    headers=headers,
)
```

### 5. Alternative: Use py-clob-client

Polymarket's official Python client (`py-clob-client`) handles auth automatically
with L1/L2 signing. Consider replacing raw httpx calls with:

```
pip install py-clob-client
```

This would handle the EIP-712 signature scheme that CLOB API requires.

### 6. Restart EdgeLab

```bash
# After updating .env, restart the EdgeLab process
```

## Impact While Broken

- Whale tracking signals are **completely offline** (all 5 wallets returning 401)
- Live Polymarket order execution will fail (no API key)
- Market data feeds are unaffected (public APIs)
- Kalshi execution is unaffected (separate auth)
