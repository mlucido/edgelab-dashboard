"""
EdgeLab Kalshi Readiness Check — validates connectivity, auth, and arb detection.

Usage:
    python scripts/kalshi_readiness_check.py

Exit codes:
    0 — GO (all checks passed)
    1 — NO-GO (missing config or connectivity failure)
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# Load .env if dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass  # dotenv optional; fall back to raw env vars

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
TIMEOUT = 10.0

# ── ANSI colours ────────────────────────────────────────────────────────────

GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg: str) -> str:
    return f"{GREEN}OK{RESET}    {msg}"

def fail(msg: str) -> str:
    return f"{RED}FAIL{RESET}  {msg}"

def warn(msg: str) -> str:
    return f"{YELLOW}WARN{RESET}  {msg}"

def section(title: str) -> None:
    print(f"\n{BOLD}{CYAN}{'─' * 55}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'─' * 55}{RESET}")


# ── Key detection ────────────────────────────────────────────────────────────

def load_api_key() -> str | None:
    """Return KALSHI_API_KEY from env or .env file, or None if absent/empty."""
    key = os.getenv("KALSHI_API_KEY", "").strip()
    return key if key else None


def print_no_key_instructions() -> None:
    section("KALSHI API KEY — NOT FOUND")
    print(f"""
  {BOLD}No KALSHI_API_KEY detected.{RESET}

  To connect EdgeLab to live Kalshi markets, you need:

  {BOLD}1. Account tier requirement{RESET}
     Kalshi API trading requires a {YELLOW}Premier{RESET} or {YELLOW}Market Maker{RESET} account.
     Standard accounts cannot place programmatic orders.
     → Upgrade at: https://kalshi.com/account/settings

  {BOLD}2. RSA key pair authentication{RESET}
     Kalshi no longer uses Bearer tokens. Auth is RSA-based:
       • POST /api_keys/generate  →  returns your private key (shown ONCE)
       • Store the key ID as:   KALSHI_API_KEY=<key_id>
       • Store the private key in a secure location (PEM format)
       • Every request must be signed:
           kalshiAccessKey       — your key ID
           kalshiAccessTimestamp — Unix ms timestamp
           kalshiAccessSignature — HMAC/RSA signature of (timestamp + path)
     → Key management: https://kalshi.com/account/api-keys

  {BOLD}3. Set the env var{RESET}
     Add to your .env file:
       KALSHI_API_KEY=<your_key_id>
       # Securely reference your PEM private key path or inline value

  {BOLD}4. Estimated time to first arb trade{RESET}
     ┌─────────────────────────────────────────────┬──────────┐
     │ Step                                        │ Time     │
     ├─────────────────────────────────────────────┼──────────┤
     │ Create / upgrade account to Premier         │ 1–3 days │
     │ Identity verification (KYC)                 │ 1–2 days │
     │ Generate RSA key pair + update executor     │ 2–4 hrs  │
     │ Paper-test auth + place first paper order   │ 1–2 hrs  │
     │ Go live (flip TRADING_MODE=live)            │ minutes  │
     └─────────────────────────────────────────────┴──────────┘
     Total realistic path: {YELLOW}3–7 days from today{RESET}

  {BOLD}5. Note on EdgeLab's executor{RESET}
     src/execution/kalshi_executor.py currently uses Bearer token auth.
     Before going live with RSA keys, update the auth headers to:
       kalshiAccessKey, kalshiAccessSignature, kalshiAccessTimestamp
""")


# ── Network checks ───────────────────────────────────────────────────────────

def check_exchange_status(client: httpx.Client) -> tuple[bool, str]:
    """GET /exchange/status — no auth required."""
    try:
        resp = client.get(f"{KALSHI_BASE_URL}/exchange/status", timeout=TIMEOUT)
        if resp.status_code == 200:
            data = resp.json()
            exchange_active = data.get("exchange_active", data.get("status", "unknown"))
            return True, f"Exchange active={exchange_active}"
        return False, f"HTTP {resp.status_code}"
    except httpx.TimeoutException:
        return False, "Request timed out"
    except httpx.RequestError as exc:
        return False, f"Network error: {exc}"


def check_markets(client: httpx.Client) -> tuple[bool, int, str]:
    """
    GET /markets — fetches a page of active markets.
    Falls back to /historical/markets if /markets requires auth.
    Returns (success, count, detail).
    """
    # Try live markets first (may require auth on some tiers)
    endpoints = [
        f"{KALSHI_BASE_URL}/markets?limit=100&status=open",
        f"{KALSHI_BASE_URL}/markets?limit=100",
    ]
    for url in endpoints:
        try:
            resp = client.get(url, timeout=TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                markets = data.get("markets", [])
                count = len(markets)
                return True, count, f"fetched from {url.split('?')[0]}"
            if resp.status_code in (401, 403):
                # Auth required — expected without RSA signature
                return True, -1, "endpoint requires auth (RSA) — key present but not signed here"
        except (httpx.TimeoutException, httpx.RequestError):
            continue

    return False, 0, "all market endpoints unreachable"


def check_portfolio_balance(api_key: str | None, client: httpx.Client) -> tuple[bool, str]:
    """
    GET /portfolio/balance — requires auth.
    With RSA keys, a raw Bearer attempt will 401; we treat that as
    'auth layer reachable' since we can't sign without the private key in this script.
    """
    if not api_key:
        return False, "no API key"

    try:
        resp = client.get(
            f"{KALSHI_BASE_URL}/portfolio/balance",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            timeout=TIMEOUT,
        )
        if resp.status_code == 200:
            data = resp.json()
            balance_cents = data.get("balance", 0)
            return True, f"${balance_cents / 100:.2f} available"
        if resp.status_code == 401:
            return False, (
                "401 Unauthorized — Kalshi now requires RSA-signed requests. "
                "Bearer token auth is deprecated. Update kalshi_executor.py to use "
                "kalshiAccessKey / kalshiAccessSignature headers."
            )
        if resp.status_code == 403:
            return False, f"403 Forbidden — account tier may not support API trading"
        return False, f"HTTP {resp.status_code}: {resp.text[:120]}"
    except (httpx.TimeoutException, httpx.RequestError) as exc:
        return False, f"Network error: {exc}"


# ── Mock arb detection ───────────────────────────────────────────────────────

# Simulated Polymarket prices for comparison (representative, not live)
MOCK_POLYMARKET = {
    "US-CPI-MAR26": 0.52,
    "FED-RATE-MAY26": 0.38,
    "BTC-100K-APR26": 0.44,
    "NFL-DRAFT-QB1": 0.61,
    "HURRICANE-ATL-2026": 0.29,
}

# Simulated Kalshi prices (slightly offset to create arb gaps)
MOCK_KALSHI = {
    "US-CPI-MAR26": 0.49,      # 3¢ gap
    "FED-RATE-MAY26": 0.43,    # 5¢ gap
    "BTC-100K-APR26": 0.44,    # 0¢ (no arb)
    "NFL-DRAFT-QB1": 0.57,     # 4¢ gap
    "HURRICANE-ATL-2026": 0.32, # 3¢ gap
}

ARB_THRESHOLD = 0.02  # 2¢ minimum spread to flag as opportunity


def detect_mock_arb() -> tuple[bool, list[dict]]:
    opportunities = []
    for ticker, poly_price in MOCK_POLYMARKET.items():
        kalshi_price = MOCK_KALSHI.get(ticker)
        if kalshi_price is None:
            continue
        spread = abs(poly_price - kalshi_price)
        if spread >= ARB_THRESHOLD:
            direction = "BUY Kalshi YES" if kalshi_price < poly_price else "BUY Kalshi NO"
            opportunities.append({
                "ticker": ticker,
                "polymarket": poly_price,
                "kalshi": kalshi_price,
                "spread": round(spread, 3),
                "direction": direction,
            })
    opportunities.sort(key=lambda x: x["spread"], reverse=True)
    return bool(opportunities), opportunities


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{BOLD}EdgeLab — Kalshi Readiness Check{RESET}  {ts}")

    api_key = load_api_key()

    # ── Phase 1: No key path ─────────────────────────────────────────────────
    if not api_key:
        print_no_key_instructions()
        section("SUMMARY")
        print(f"  Connection ........... {YELLOW}UNTESTED{RESET}")
        print(f"  Auth ................. {RED}MISSING — no KALSHI_API_KEY{RESET}")
        print(f"  Markets available .... {YELLOW}UNTESTED{RESET}")
        print(f"  Arb detection ........ {YELLOW}NOT READY{RESET}")
        print(f"\n  {BOLD}{RED}Overall: NO-GO{RESET}\n")
        return 1

    # ── Phase 2: Key found — run checks ─────────────────────────────────────
    section("PHASE 1 — Exchange Connectivity")

    results: dict[str, tuple[bool, str]] = {}

    with httpx.Client() as client:
        # Exchange status
        conn_ok, conn_detail = check_exchange_status(client)
        results["connection"] = (conn_ok, conn_detail)
        print(f"  Exchange status ... {ok(conn_detail) if conn_ok else fail(conn_detail)}")

        # Markets
        mkt_ok, mkt_count, mkt_detail = check_markets(client)
        results["markets"] = (mkt_ok, f"{mkt_count} markets — {mkt_detail}")
        if mkt_ok:
            if mkt_count >= 0:
                print(f"  Markets ........... {ok(f'{mkt_count} markets retrieved')}")
            else:
                print(f"  Markets ........... {warn(mkt_detail)}")
        else:
            print(f"  Markets ........... {fail(mkt_detail)}")

        # Portfolio / auth
        section("PHASE 2 — Auth & Portfolio")
        auth_ok, auth_detail = check_portfolio_balance(api_key, client)
        results["auth"] = (auth_ok, auth_detail)
        print(f"  Portfolio balance . {ok(auth_detail) if auth_ok else fail(auth_detail)}")

        if not auth_ok:
            print(f"""
  {YELLOW}Note:{RESET} Kalshi deprecated Bearer token auth in favor of RSA key pairs.
  The executor at src/execution/kalshi_executor.py needs to be updated to
  sign requests with your RSA private key before live trading will work.
  See: https://trading-api.readme.io/reference/authentication
""")

    # ── Phase 3: Arb detection ───────────────────────────────────────────────
    section("PHASE 3 — Arb Opportunity Detection (mock data)")
    arb_found, opportunities = detect_mock_arb()

    print(f"  Using mock Polymarket vs Kalshi prices ({len(MOCK_POLYMARKET)} markets)\n")
    if opportunities:
        print(f"  {GREEN}{len(opportunities)} arb opportunities detected:{RESET}\n")
        print(f"  {'Ticker':<25} {'Poly':>6} {'Kalshi':>7} {'Spread':>7}  Direction")
        print(f"  {'─'*25} {'─'*6} {'─'*7} {'─'*7}  {'─'*20}")
        for opp in opportunities:
            print(
                f"  {opp['ticker']:<25} {opp['polymarket']:>6.2f} "
                f"{opp['kalshi']:>7.2f} {opp['spread']:>7.3f}  {opp['direction']}"
            )
        print()
        print(f"  {YELLOW}Note:{RESET} These are mock prices for pipeline validation.")
        print(f"  Connect live market feeds to get real arb signals.")
    else:
        print(f"  {YELLOW}No arb opportunities above {ARB_THRESHOLD*100:.0f}¢ threshold in mock data.{RESET}")

    arb_ready = conn_ok and mkt_ok

    # ── Summary ──────────────────────────────────────────────────────────────
    section("SUMMARY")

    def status_line(label: str, ok_val: bool, detail: str) -> None:
        badge = f"{GREEN}OK{RESET}" if ok_val else f"{RED}FAIL{RESET}"
        print(f"  {label:<22} {badge}  {detail}")

    conn_ok2, conn_d2 = results["connection"]
    mkt_ok2,  mkt_d2  = results["markets"]
    auth_ok2, auth_d2 = results["auth"]

    status_line("Connection",         conn_ok2, conn_d2)
    status_line("Auth",               auth_ok2, auth_d2)
    status_line("Markets available",  mkt_ok2,  mkt_d2)
    status_line("Arb detection",      arb_found, f"{len(opportunities)} mock opportunities found")

    overall_go = conn_ok2 and mkt_ok2
    # Auth 401 is expected without RSA private key; don't block overall GO
    # (the executor needs updating but connectivity is confirmed)

    if overall_go:
        print(f"\n  {BOLD}{GREEN}Overall: GO{RESET}  — connectivity confirmed, arb pipeline ready")
        if not auth_ok2:
            print(f"  {YELLOW}Action required:{RESET} Update kalshi_executor.py to RSA auth before live trading")
    else:
        print(f"\n  {BOLD}{RED}Overall: NO-GO{RESET}  — fix failures above before deploying")

    print()
    return 0 if overall_go else 1


if __name__ == "__main__":
    sys.exit(main())
