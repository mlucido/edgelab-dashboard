#!/usr/bin/env python3
"""
EdgeLab Dashboard Exporter
Proxies all localhost:8050 API endpoints into a single data.json,
then pushes to GitHub Pages. The static index.html shims fetch()
to read from this bundle instead of hitting live API.

Run: while true; do python3 scripts/export_dashboard.py; sleep 300; done
"""
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = ROOT / "dashboard_export"
DASHBOARD_URL = "http://localhost:8050"

# Keys that map to /api/{key} on the dashboard server
API_KEYS = [
    "data",
    "framework",
    "news_arb",
    "sports_momentum",
    "resolution_lag",
    "funding_rate_arb",
    "liquidity_provision",
]

# Fields to strip from export (never send logs/sensitive data to public repo)
STRIP_FIELDS = {"live_log", "edge_log"}


def fetch_all():
    """Fetch all API endpoints from the local dashboard and bundle them."""
    bundle = {}
    for key in API_KEYS:
        try:
            r = requests.get(f"{DASHBOARD_URL}/api/{key}", timeout=5)
            if r.status_code == 200:
                payload = r.json()
                # Strip raw log lines from the main data endpoint
                if key == "data" and isinstance(payload, dict):
                    for field in STRIP_FIELDS:
                        payload.pop(field, None)
                bundle[key] = payload
        except Exception as e:
            print(f"  Warning: /api/{key} unavailable: {e}")
    bundle["_exported_at"] = datetime.now(timezone.utc).isoformat()
    return bundle


if __name__ == "__main__":
    bundle = fetch_all()
    if not bundle.get("data"):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Dashboard not responding — skipping export")
        sys.exit(0)

    out = EXPORT_DIR / "data.json"
    out.write_text(json.dumps(bundle, indent=2))

    bot = bundle.get("data", {}).get("bot", {})
    live_strats = sum(
        1 for k in ["news_arb", "sports_momentum", "resolution_lag", "funding_rate_arb", "liquidity_provision"]
        if bundle.get(k, {}).get("mode") == "LIVE"
    )
    # Count bot as live
    if bot.get("available") and bot.get("process_running"):
        live_strats += 1

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] Exported {len(bundle)} endpoints, "
        f"bankroll=${bot.get('bankroll', 0):.2f}, "
        f"{live_strats} live"
    )

    # Git push
    os.chdir(EXPORT_DIR)
    subprocess.run(["git", "add", "-A"], capture_output=True)
    diff = subprocess.run(["git", "diff", "--cached", "--quiet"], capture_output=True)
    if diff.returncode != 0:
        subprocess.run(
            ["git", "commit", "-m", f"update {datetime.now().strftime('%H:%M')}"],
            capture_output=True,
        )
        push = subprocess.run(
            ["git", "push", "origin", "main"],
            capture_output=True, text=True,
        )
        if push.returncode == 0:
            print("  Pushed to GitHub Pages")
        else:
            print(f"  Push failed: {push.stderr[:200]}")
    else:
        print("  No changes")
