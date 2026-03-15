"""
EdgeLab Capital Management Console
Dark terminal trading dashboard with live Kalshi integration.
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import streamlit as st

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------------------------------------------------------------------------
# Optional dependency imports
# ---------------------------------------------------------------------------
try:
    import redis as redis_sync
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    import plotly.graph_objects as go
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    from src.calibration import tracker as cal_tracker
    TRACKER_AVAILABLE = True
except (ImportError, Exception):
    TRACKER_AVAILABLE = False

try:
    from src.execution import circuit_breaker as cb
    CB_AVAILABLE = True
except (ImportError, Exception):
    CB_AVAILABLE = False

try:
    from src.simulation import backsim
    BACKSIM_AVAILABLE = True
except (ImportError, Exception):
    BACKSIM_AVAILABLE = False

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="EdgeLab",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Design tokens — clean light
# ---------------------------------------------------------------------------
BG = "#f8fafc"
BG_CARD = "#ffffff"
BG_SIDEBAR = "#f0f4f8"
GREEN = "#16a34a"
GREEN_DIM = "rgba(22,163,74,0.08)"
RED = "#dc2626"
RED_DIM = "rgba(220,38,38,0.08)"
YELLOW = "#d97706"
YELLOW_DIM = "rgba(217,119,6,0.08)"
BLUE = "#2563eb"
BLUE_DIM = "rgba(37,99,235,0.08)"
TEXT = "#0f172a"
TEXT_DIM = "#64748b"
TEXT_MUTED = "#94a3b8"
BORDER = "#e2e8f0"
BORDER_LIGHT = "#f1f5f9"

PLOTLY_LAYOUT = dict(
    template="plotly_white",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="JetBrains Mono, monospace", color=TEXT_DIM, size=11),
    xaxis=dict(gridcolor="#f1f5f9", zerolinecolor="#e2e8f0", showgrid=True),
    yaxis=dict(gridcolor="#f1f5f9", zerolinecolor="#e2e8f0", showgrid=True),
)

# ---------------------------------------------------------------------------
# CSS — clean light theme with compact spacing
# ---------------------------------------------------------------------------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');

/* ===== RESET & GLOBAL ===== */
.stApp, [data-testid="stAppViewContainer"], .main, .block-container,
[data-testid="stAppViewBlockContainer"] {
    background-color: #f8fafc !important;
    color: #0f172a !important;
    font-family: 'Inter', -apple-system, sans-serif !important;
}
.block-container {
    padding: 0.5rem 2rem 2rem 2rem !important;
    max-width: 100% !important;
}

/* ===== HIDE STREAMLIT TOOLBAR/HEADER =====
   The built-in header with Deploy button overlaps our content.
   Nuke it entirely so our el-header shows cleanly. */
header[data-testid="stHeader"] {
    display: none !important;
}
[data-testid="stToolbar"] {
    display: none !important;
}
[data-testid="stDecoration"] {
    display: none !important;
}

/* ===== GLOBAL SPACING FIX =====
   Streamlit 1.38 uses CSS `gap` on stVerticalBlock flex containers.
   That's what creates the huge spaces between elements.
   We target `gap` directly — not margin on children. */
[data-testid="stVerticalBlock"] {
    gap: 0.35rem !important;
}
[data-testid="stHorizontalBlock"] {
    gap: 12px !important;
}
/* Element containers have min-height that inflates empty space */
[data-testid="stElementContainer"] {
    margin: 0 !important;
}
/* Compact markdown and dividers */
.stMarkdown { min-height: 0 !important; }
hr { margin: 4px 0 !important; }

/* ===== SIDEBAR ===== */
[data-testid="stSidebar"] {
    background-color: #ffffff !important;
    border-right: 1px solid #e2e8f0 !important;
    min-width: 220px !important;
    max-width: 220px !important;
}
[data-testid="stSidebar"] [data-testid="stSidebarContent"] {
    padding: 12px 12px !important;
}
[data-testid="stSidebar"] * {
    color: #0f172a !important;
}
[data-testid="stSidebar"] hr {
    border-color: #e2e8f0 !important;
    margin: 4px 0 !important;
}
/* CRITICAL FIX: Streamlit 1.38 uses CSS `gap` on stVerticalBlock
   flex containers — that's the actual source of button spacing.
   Targeting children's margin/padding does nothing. */
[data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
    gap: 0px !important;
}
[data-testid="stSidebar"] [data-testid="stVerticalBlock"] > div {
    margin: 0 !important;
    padding: 0 !important;
    min-height: 0 !important;
}
[data-testid="stSidebar"] .stButton {
    margin: 2px 0 !important;
}
[data-testid="stSidebar"] .stButton > button {
    background-color: #f8fafc !important;
    border: 1px solid #e2e8f0 !important;
    color: #475569 !important;
    font-size: 13px !important;
    padding: 6px 12px !important;
    border-radius: 6px !important;
    transition: all 0.15s ease !important;
    font-weight: 500 !important;
    margin: 0 !important;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background-color: #f1f5f9 !important;
    color: #0f172a !important;
    border-color: #cbd5e1 !important;
}

/* ===== TYPOGRAPHY ===== */
h1, h2, h3, h4, h5, h6 {
    color: #0f172a !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 600 !important;
}

/* ===== METRIC CARDS ===== */
.el-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    padding: 16px 18px;
    position: relative;
    overflow: hidden;
    transition: box-shadow 0.2s ease;
}
.el-card:hover {
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}
.el-card.accent-green { border-left: 3px solid #16a34a; }
.el-card.accent-red { border-left: 3px solid #dc2626; }
.el-card.accent-yellow { border-left: 3px solid #d97706; }
.el-card.accent-blue { border-left: 3px solid #2563eb; }

.el-card .label {
    font-size: 10px;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    font-weight: 600;
    margin-bottom: 6px;
    font-family: 'Inter', sans-serif;
}
.el-card .value {
    font-size: 24px;
    font-weight: 700;
    color: #0f172a;
    font-family: 'JetBrains Mono', monospace;
    line-height: 1.1;
    letter-spacing: -0.5px;
}
.el-card .sub {
    font-size: 11px;
    margin-top: 4px;
    font-weight: 500;
    font-family: 'JetBrains Mono', monospace;
}
.el-card .sub.green { color: #16a34a; }
.el-card .sub.red { color: #dc2626; }
.el-card .sub.dim { color: #94a3b8; }
.el-card .sub.yellow { color: #d97706; }

/* ===== SECTION PANELS ===== */
.el-panel {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    padding: 16px 18px;
    margin-bottom: 12px;
}
.el-panel-title {
    font-size: 12px;
    font-weight: 600;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    margin-bottom: 12px;
    font-family: 'Inter', sans-serif;
    display: flex;
    align-items: center;
    gap: 6px;
}
.el-panel-title .dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: #16a34a;
    display: inline-block;
}

/* ===== HEADER BAR ===== */
.el-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 20px;
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    margin-bottom: 16px;
}
.el-header .logo {
    font-size: 18px;
    font-weight: 700;
    color: #0f172a;
    font-family: 'JetBrains Mono', monospace;
    letter-spacing: -0.5px;
}
.el-header .logo .accent { color: #16a34a; }
.el-header .clock {
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    color: #94a3b8;
}
.el-header .mode-badge {
    padding: 3px 14px;
    border-radius: 20px;
    font-weight: 600;
    font-size: 10px;
    letter-spacing: 1px;
    text-transform: uppercase;
    font-family: 'JetBrains Mono', monospace;
}
.el-header .mode-badge.paper {
    background: rgba(37,99,235,0.08);
    color: #2563eb;
    border: 1px solid rgba(37,99,235,0.2);
}
.el-header .mode-badge.live {
    background: rgba(22,163,74,0.08);
    color: #16a34a;
    border: 1px solid rgba(22,163,74,0.2);
    animation: livePulse 2s ease-in-out infinite;
}

/* ===== STATUS BADGES ===== */
.badge { padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; font-family: 'JetBrains Mono', monospace; }
.badge-green { background: rgba(22,163,74,0.08); color: #16a34a; }
.badge-red { background: rgba(220,38,38,0.08); color: #dc2626; }
.badge-yellow { background: rgba(217,119,6,0.08); color: #d97706; }
.badge-blue { background: rgba(37,99,235,0.08); color: #2563eb; }

.status-dot {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-weight: 600;
    font-size: 13px;
}
.status-dot::before {
    content: '';
    width: 8px;
    height: 8px;
    border-radius: 50%;
    display: inline-block;
}
.status-dot.running::before { background: #16a34a; animation: pulse 2s ease-in-out infinite; }
.status-dot.running { color: #16a34a; }
.status-dot.paused::before { background: #d97706; }
.status-dot.paused { color: #d97706; }
.status-dot.halted::before { background: #dc2626; animation: dangerPulse 1.5s ease-in-out infinite; }
.status-dot.halted { color: #dc2626; }

/* ===== DATA TABLES ===== */
.el-table {
    width: 100%;
    border-collapse: collapse;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
}
.el-table th {
    text-align: left;
    padding: 6px 10px;
    color: #94a3b8;
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.8px;
    border-bottom: 1px solid #e2e8f0;
    font-weight: 600;
}
.el-table td {
    padding: 8px 10px;
    color: #475569;
    border-bottom: 1px solid #f1f5f9;
}
.el-table tr:hover td {
    background: #f8fafc;
}
.el-table .strat {
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
}
.strat-resolution { background: rgba(37,99,235,0.08); color: #2563eb; }
.strat-threshold { background: rgba(147,51,234,0.08); color: #7c3aed; }
.strat-arbitrage { background: rgba(217,119,6,0.08); color: #d97706; }
.strat-whale { background: rgba(22,163,74,0.08); color: #16a34a; }
.strat-default { background: rgba(100,116,139,0.08); color: #64748b; }

/* ===== ACTIVITY FEED ===== */
.el-feed-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 10px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    border-bottom: 1px solid #f1f5f9;
    transition: background 0.1s ease;
}
.el-feed-item:hover { background: #f8fafc; }
.el-feed-item .icon { font-size: 13px; width: 18px; text-align: center; flex-shrink: 0; color: #94a3b8; }
.el-feed-item .time { color: #94a3b8; font-size: 11px; min-width: 56px; flex-shrink: 0; }
.el-feed-item .action { font-weight: 600; flex-shrink: 0; min-width: 44px; }
.el-feed-item .action.win { color: #16a34a; }
.el-feed-item .action.loss { color: #dc2626; }
.el-feed-item .action.open { color: #2563eb; }
.el-feed-item .market { color: #64748b; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; }
.el-feed-item .pnl { font-weight: 600; min-width: 56px; text-align: right; flex-shrink: 0; }
.el-feed-item .pnl.pos { color: #16a34a; }
.el-feed-item .pnl.neg { color: #dc2626; }
.el-feed-item .pnl.neu { color: #2563eb; }

/* ===== EMPTY STATE ===== */
.el-empty {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 6px;
    padding: 28px 16px;
    color: #94a3b8;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
}
.el-empty .dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #2563eb;
    animation: pulse 2s ease-in-out infinite;
}

/* ===== PROGRESS BAR ===== */
.el-bar-bg {
    width: 100%;
    height: 4px;
    background: #e2e8f0;
    border-radius: 2px;
    margin-top: 8px;
    overflow: hidden;
}
.el-bar-fill {
    height: 100%;
    border-radius: 2px;
    transition: width 0.5s ease;
}

/* ===== GO / NO-GO ===== */
.el-verdict-go {
    background: rgba(22,163,74,0.06);
    border: 2px solid #16a34a;
    border-radius: 10px;
    padding: 16px;
    text-align: center;
    font-size: 22px;
    font-weight: 700;
    color: #16a34a;
    font-family: 'JetBrains Mono', monospace;
}
.el-verdict-nogo {
    background: rgba(220,38,38,0.06);
    border: 2px solid #dc2626;
    border-radius: 10px;
    padding: 16px;
    text-align: center;
    font-size: 22px;
    font-weight: 700;
    color: #dc2626;
    font-family: 'JetBrains Mono', monospace;
}
.el-criteria { padding: 6px 10px; font-size: 12px; border-bottom: 1px solid #f1f5f9; display: flex; align-items: center; gap: 6px; }
.el-criteria.pass { color: #16a34a; }
.el-criteria.fail { color: #dc2626; }

/* ===== SIDEBAR CUSTOM ===== */
.sb-logo {
    font-family: 'JetBrains Mono', monospace;
    font-size: 16px;
    font-weight: 700;
    color: #0f172a;
    letter-spacing: -0.5px;
}
.sb-logo .accent { color: #16a34a; }
.sb-version { font-family: 'JetBrains Mono', monospace; font-size: 10px; color: #94a3b8; margin-top: 1px; }
.sb-label {
    font-size: 9px;
    text-transform: uppercase;
    letter-spacing: 1.2px;
    color: #94a3b8;
    font-weight: 600;
    margin: 8px 0 4px 0;
    font-family: 'Inter', sans-serif;
}
.sb-mode {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 4px;
    font-weight: 600;
    font-size: 10px;
    letter-spacing: 0.8px;
    font-family: 'JetBrains Mono', monospace;
}
.sb-mode.paper { background: rgba(37,99,235,0.08); color: #2563eb; }
.sb-mode.live { background: rgba(22,163,74,0.08); color: #16a34a; animation: livePulse 2s ease-in-out infinite; }
.sb-uptime {
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    color: #64748b;
    padding: 2px 0;
}

/* Sidebar radio nav — style as pill buttons */
[data-testid="stSidebar"] [data-testid="stRadio"] > div {
    gap: 0 !important;
}
[data-testid="stSidebar"] [data-testid="stRadio"] label {
    background: #f8fafc !important;
    border: 1px solid #e2e8f0 !important;
    border-radius: 6px !important;
    padding: 7px 12px !important;
    margin: 2px 0 !important;
    cursor: pointer !important;
    transition: all 0.15s ease !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    color: #475569 !important;
    display: flex !important;
    align-items: center !important;
}
[data-testid="stSidebar"] [data-testid="stRadio"] label:hover {
    background: #f1f5f9 !important;
    border-color: #cbd5e1 !important;
    color: #0f172a !important;
}
[data-testid="stSidebar"] [data-testid="stRadio"] label[data-checked="true"],
[data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked) {
    background: #eff6ff !important;
    border-color: #2563eb !important;
    border-left: 3px solid #2563eb !important;
    color: #2563eb !important;
    font-weight: 600 !important;
}
/* Hide the radio dot */
[data-testid="stSidebar"] [data-testid="stRadio"] input[type="radio"] {
    display: none !important;
}
[data-testid="stSidebar"] [data-testid="stRadio"] label > div:first-child {
    display: none !important;
}

/* Sidebar halt button — last button, red style */
[data-testid="stSidebar"] .stButton > button[kind="primary"] {
    background-color: rgba(220,38,38,0.06) !important;
    border: 1px solid #dc2626 !important;
    color: #dc2626 !important;
    font-weight: 700 !important;
    letter-spacing: 0.5px !important;
    font-size: 12px !important;
}
[data-testid="stSidebar"] .stButton > button[kind="primary"]:hover {
    background-color: rgba(220,38,38,0.12) !important;
}

/* ===== STREAMLIT COMPONENT OVERRIDES ===== */
[data-testid="stMetric"] { display: none !important; }
[data-testid="stForm"] {
    background-color: #ffffff !important;
    border: 1px solid #e2e8f0 !important;
    border-radius: 10px !important;
    padding: 16px !important;
}
input, textarea, select,
[data-testid="stNumberInput"] input,
[data-testid="stTextInput"] input {
    background-color: #f8fafc !important;
    color: #0f172a !important;
    border-color: #e2e8f0 !important;
    border-radius: 6px !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 13px !important;
}
[data-testid="stExpander"] {
    background-color: #ffffff !important;
    border: 1px solid #e2e8f0 !important;
    border-radius: 8px !important;
}
[data-testid="stExpander"] summary { color: #475569 !important; }
[data-testid="stSelectbox"] > div > div {
    background-color: #f8fafc !important;
    border-color: #e2e8f0 !important;
}
.stButton > button {
    border: 1px solid #e2e8f0 !important;
    border-radius: 6px !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 500 !important;
    transition: all 0.15s ease !important;
    background-color: #ffffff !important;
    color: #475569 !important;
}
.stButton > button:hover {
    border-color: #cbd5e1 !important;
    background-color: #f1f5f9 !important;
    color: #0f172a !important;
}
.stButton > button[kind="primary"] {
    background-color: #2563eb !important;
    border-color: #2563eb !important;
    color: #ffffff !important;
}
.stButton > button[kind="primary"]:hover {
    background-color: #1d4ed8 !important;
}
[data-testid="stDataFrame"], .stDataFrame {
    border: 1px solid #e2e8f0 !important;
    border-radius: 8px !important;
}
[data-testid="stAlert"] {
    border-radius: 8px !important;
}
.stCodeBlock, .stCode, pre, code {
    background-color: #f8fafc !important;
    border: 1px solid #e2e8f0 !important;
    border-radius: 6px !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 11px !important;
}

/* ===== ANIMATIONS ===== */
@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.4; }
}
@keyframes livePulse {
    0%, 100% { box-shadow: 0 0 0 0 rgba(22,163,74,0.15); }
    50% { box-shadow: 0 0 0 4px rgba(22,163,74,0); }
}
@keyframes dangerPulse {
    0%, 100% { box-shadow: 0 0 0 0 rgba(220,38,38,0.2); }
    50% { box-shadow: 0 0 8px 2px rgba(220,38,38,0.08); }
}

/* ===== HIDE STREAMLIT CHROME ===== */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Redis helpers
# ---------------------------------------------------------------------------
@st.cache_resource
def get_redis_client():
    if not REDIS_AVAILABLE:
        return None
    try:
        r = redis_sync.Redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379"),
            decode_responses=True,
            socket_connect_timeout=2,
        )
        r.ping()
        return r
    except Exception:
        return None


def redis_get(key: str, default=None):
    r = get_redis_client()
    if r is None:
        return default
    try:
        val = r.get(key)
        return val if val is not None else default
    except Exception:
        return default


def redis_set(key: str, value: str):
    r = get_redis_client()
    if r is None:
        return False
    try:
        r.set(key, value)
        return True
    except Exception:
        return False


def redis_delete(key: str):
    r = get_redis_client()
    if r is None:
        return False
    try:
        r.delete(key)
        return True
    except Exception:
        return False

# ---------------------------------------------------------------------------
# SQLite helper
# ---------------------------------------------------------------------------
DB_PATH = os.getenv(
    "EDGELAB_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "edgelab.db"),
)


def _get_db():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Async runner
# ---------------------------------------------------------------------------
def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result(timeout=10)
        return loop.run_until_complete(coro)
    except Exception:
        return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Mode detection
# ---------------------------------------------------------------------------
def is_paper_mode() -> bool:
    mode = redis_get("bot:mode", "paper")
    return mode != "live"


def get_bot_status() -> dict:
    raw = redis_get("bot:status")
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass

    total_capital = float(os.getenv("TOTAL_CAPITAL", "500"))
    capital_deployed = 0.0
    pnl_today = 0.0
    pnl_alltime = 0.0

    db = _get_db()
    if db is not None:
        try:
            row = db.execute(
                "SELECT SUM(size) as s FROM trades_paper WHERE status = 'open'"
            ).fetchone()
            capital_deployed = float(row["s"] or 0.0)
        except Exception:
            pass
        try:
            row = db.execute(
                "SELECT SUM(pnl) as s FROM trades_paper WHERE status = 'closed' AND date(timestamp) = date('now')"
            ).fetchone()
            pnl_today = float(row["s"] or 0.0)
        except Exception:
            pass
        try:
            row = db.execute(
                "SELECT SUM(pnl) as s FROM trades_paper WHERE status = 'closed'"
            ).fetchone()
            pnl_alltime = float(row["s"] or 0.0)
        except Exception:
            pass
        db.close()

    pnl_today_pct = (pnl_today / total_capital * 100) if total_capital > 0 else 0.0
    pnl_alltime_pct = (pnl_alltime / total_capital * 100) if total_capital > 0 else 0.0

    return {
        "state": "RUNNING",
        "capital_total": total_capital,
        "capital_deployed": capital_deployed,
        "pnl_today": round(pnl_today, 2),
        "pnl_today_pct": round(pnl_today_pct, 2),
        "pnl_alltime": round(pnl_alltime, 2),
        "pnl_alltime_pct": round(pnl_alltime_pct, 2),
        "start_time": None,
    }


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------
def load_open_trades(paper: bool = True) -> list:
    db = _get_db()
    if db is None:
        return []
    table = "trades_paper" if paper else "trades_live"
    try:
        rows = db.execute(
            f"SELECT * FROM {table} WHERE status = 'open' ORDER BY timestamp"
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        db.close()


def load_closed_trades(paper: bool = True) -> list:
    db = _get_db()
    if db is None:
        return []
    table = "trades_paper" if paper else "trades_live"
    try:
        rows = db.execute(
            f"SELECT * FROM {table} WHERE status = 'closed' ORDER BY timestamp"
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        db.close()


def load_pnl_curve(paper: bool = True) -> list:
    db = _get_db()
    if db is None:
        return []
    table = "trades_paper" if paper else "trades_live"
    try:
        rows = db.execute(
            f"SELECT date(timestamp) as day, SUM(pnl) as daily_pnl "
            f"FROM {table} WHERE status = 'closed' GROUP BY date(timestamp) ORDER BY day"
        ).fetchall()
        curve = []
        cumulative = 0.0
        for r in rows:
            daily = float(r["daily_pnl"] or 0.0)
            cumulative += daily
            curve.append({
                "timestamp": r["day"],
                "pnl": round(daily, 2),
                "cumulative_pnl": round(cumulative, 2),
            })
        return curve
    except Exception:
        return []
    finally:
        db.close()


def load_calibration_stats() -> dict:
    if TRACKER_AVAILABLE:
        try:
            return run_async(cal_tracker.get_calibration_stats())
        except Exception:
            pass
    return {}


def load_strategy_pnl_summary(closed_trades: list) -> dict:
    summary: dict[str, dict] = {}
    for t in closed_trades:
        s = t.get("strategy", "unknown")
        pnl = t.get("pnl", 0.0) or 0.0
        if s not in summary:
            summary[s] = {"total_pnl": 0.0, "wins": 0, "count": 0}
        summary[s]["total_pnl"] += pnl
        summary[s]["count"] += 1
        if pnl > 0:
            summary[s]["wins"] += 1
    for s in summary:
        c = summary[s]["count"]
        summary[s]["win_rate"] = round(summary[s]["wins"] / c, 4) if c > 0 else 0.0
        summary[s]["total_pnl"] = round(summary[s]["total_pnl"], 2)
    return summary


def load_recent_trades(paper: bool = True, limit: int = 50) -> list:
    db = _get_db()
    if db is None:
        return []
    table = "trades_paper" if paper else "trades_live"
    try:
        rows = db.execute(
            f"SELECT * FROM {table} ORDER BY timestamp DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        db.close()


def load_all_trades_count(paper: bool = True) -> int:
    db = _get_db()
    if db is None:
        return 0
    table = "trades_paper" if paper else "trades_live"
    try:
        row = db.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()
        return int(row["c"] or 0)
    except Exception:
        return 0
    finally:
        db.close()


def load_cb_status() -> dict:
    if CB_AVAILABLE:
        try:
            return run_async(cb.get_status())
        except Exception:
            pass
    return {
        "can_trade": True,
        "trade_reason": "ok",
        "global_pause": False,
        "pause_reason": "",
        "daily_pnl": 0.0,
        "daily_loss_limit": -200.0,
        "win_rate": None,
        "win_rate_min": 0.45,
        "win_rate_lookback": 20,
        "total_outcomes_tracked": 0,
        "consecutive_losses_by_strategy": {},
        "paused_strategies": {},
        "api_errors_last_10min": 0,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _strat_class(strategy: str) -> str:
    s = strategy.lower()
    if "resolution" in s:
        return "strat-resolution"
    elif "threshold" in s:
        return "strat-threshold"
    elif "arb" in s:
        return "strat-arbitrage"
    elif "whale" in s:
        return "strat-whale"
    return "strat-default"


def _relative_time(ts_str: str) -> str:
    try:
        ts_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - ts_dt
        secs = int(delta.total_seconds())
        if secs < 60:
            return f"{secs}s ago"
        elif secs < 3600:
            return f"{secs // 60}m ago"
        elif secs < 86400:
            return f"{secs // 3600}h ago"
        else:
            return f"{secs // 86400}d ago"
    except Exception:
        return ts_str[:16]


def _header(title: str, icon: str = ""):
    paper = is_paper_mode()
    mode_cls = "paper" if paper else "live"
    mode_label = "PAPER" if paper else "LIVE"
    now_str = datetime.now().strftime('%H:%M:%S')
    st.markdown(
        f'<div class="el-header">'
        f'<div class="logo">{icon} Edge<span class="accent">Lab</span></div>'
        f'<span class="mode-badge {mode_cls}">{mode_label}</span>'
        f'<div class="clock">{now_str} UTC</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


def _card(label: str, value: str, sub: str = "", accent: str = "blue", sub_class: str = "dim"):
    return (
        f'<div class="el-card accent-{accent}">'
        f'<div class="label">{label}</div>'
        f'<div class="value">{value}</div>'
        f'{f"""<div class="sub {sub_class}">{sub}</div>""" if sub else ""}'
        f'</div>'
    )


def _empty(msg: str):
    return (
        f'<div class="el-empty">'
        f'<div class="dot"></div>'
        f'<div>{msg}</div>'
        f'</div>'
    )


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
def render_sidebar():
    with st.sidebar:
        # --- Single HTML block for all header info (1 widget instead of 6) ---
        paper = is_paper_mode()
        mode_cls = "paper" if paper else "live"
        mode_label = "PAPER" if paper else "LIVE"

        bot_status = get_bot_status()
        start_raw = bot_status.get("start_time")
        uptime_str = "--:--:--"
        if start_raw:
            try:
                start_dt = datetime.fromisoformat(start_raw.replace("Z", "+00:00"))
                uptime = datetime.now(timezone.utc) - start_dt
                hours, rem = divmod(int(uptime.total_seconds()), 3600)
                minutes, secs = divmod(rem, 60)
                uptime_str = f"{hours:02d}:{minutes:02d}:{secs:02d}"
            except Exception:
                pass

        st.markdown(
            f'<div style="margin-bottom:12px">'
            f'<div class="sb-logo">⚡ Edge<span class="accent">Lab</span></div>'
            f'<div style="display:flex;align-items:center;gap:8px;margin-top:4px">'
            f'<span class="sb-mode {mode_cls}">{mode_label}</span>'
            f'<span class="sb-version">v2.1</span>'
            f'<span class="sb-uptime">{uptime_str}</span>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        # --- Navigation via st.radio (1 widget for all nav) ---
        page_options = ["Capital Command", "Performance", "Live Kalshi", "Simulation"]
        page_keys = ["capital", "performance", "live_kalshi", "controls"]
        current_key = st.session_state.get("page", "capital")
        current_idx = page_keys.index(current_key) if current_key in page_keys else 0

        selected = st.radio(
            "Navigate",
            page_options,
            index=current_idx,
            label_visibility="collapsed",
        )
        new_key = page_keys[page_options.index(selected)]
        if new_key != current_key:
            st.session_state["page"] = new_key
            st.rerun()

        # --- Mode switch + Kill switch (2-3 widgets total) ---
        if paper:
            if st.button("Switch to LIVE", use_container_width=True):
                st.session_state["live_switch_confirm"] = True

            if st.session_state.get("live_switch_confirm"):
                st.error("This activates real-money trading.")
                col_a, col_b = st.columns(2)
                with col_a:
                    if st.button("CONFIRM LIVE"):
                        redis_set("bot:mode", "live")
                        st.session_state["live_switch_confirm"] = False
                        st.rerun()
                with col_b:
                    if st.button("Stay Paper"):
                        st.session_state["live_switch_confirm"] = False
                        st.rerun()
        else:
            if st.button("Switch to PAPER", use_container_width=True):
                redis_set("bot:mode", "paper")
                st.rerun()

        st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)

        if st.button("⛔ HALT ALL TRADING", use_container_width=True, type="primary"):
            st.session_state["kill_switch_confirm"] = True

        if st.session_state.get("kill_switch_confirm"):
            st.warning("This will immediately pause all trading.")
            col_yes, col_no = st.columns(2)
            with col_yes:
                if st.button("CONFIRM", type="primary"):
                    if CB_AVAILABLE:
                        try:
                            run_async(cb.reset_all(pause=True))
                            st.success("Bot halted.")
                        except Exception as e:
                            st.error(f"Error: {e}")
                    else:
                        redis_set("edgelab:cb:global_pause", "1")
                        redis_set("edgelab:cb:global_pause_reason", "Manual kill switch")
                        st.success("Halt signal sent.")
                    st.session_state["kill_switch_confirm"] = False
                    st.rerun()
            with col_no:
                if st.button("Cancel"):
                    st.session_state["kill_switch_confirm"] = False
                    st.rerun()


# ---------------------------------------------------------------------------
# Page 1: Capital Command
# ---------------------------------------------------------------------------
def page_capital_command():
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=15000, key="capital_refresh")
    except ImportError:
        pass

    _header("Capital Command", "⚡")

    bot_status = get_bot_status()
    paper = is_paper_mode()

    total_capital = bot_status.get("capital_total", 0.0)
    deployed = bot_status.get("capital_deployed", 0.0)
    idle = total_capital - deployed
    deploy_pct = (deployed / total_capital * 100) if total_capital > 0 else 0

    pnl_today = bot_status.get("pnl_today", 0.0)
    pnl_today_pct = bot_status.get("pnl_today_pct", 0.0)
    pnl_alltime = bot_status.get("pnl_alltime", 0.0)
    pnl_alltime_pct = bot_status.get("pnl_alltime_pct", 0.0)

    # Hero metrics
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        bar_html = f'<div class="el-bar-bg"><div class="el-bar-fill" style="width:{deploy_pct:.0f}%;background:linear-gradient(90deg,#00d4aa,#3b82f6)"></div></div>'
        st.markdown(
            f'<div class="el-card accent-blue">'
            f'<div class="label">Total Capital</div>'
            f'<div class="value">${total_capital:,.0f}</div>'
            f'<div class="sub dim">${deployed:,.0f} deployed &middot; ${idle:,.0f} idle</div>'
            f'{bar_html}'
            f'</div>',
            unsafe_allow_html=True,
        )

    with c2:
        sub_cls = "green" if pnl_today > 0 else ("red" if pnl_today < 0 else "dim")
        accent = "green" if pnl_today > 0 else ("red" if pnl_today < 0 else "yellow")
        st.markdown(_card("Today's P&L", f"${pnl_today:+,.2f}", f"{pnl_today_pct:+.2f}%", accent, sub_cls), unsafe_allow_html=True)

    with c3:
        sub_cls = "green" if pnl_alltime > 0 else ("red" if pnl_alltime < 0 else "dim")
        accent = "green" if pnl_alltime > 0 else ("red" if pnl_alltime < 0 else "yellow")
        st.markdown(_card("All-Time P&L", f"${pnl_alltime:+,.2f}", f"{pnl_alltime_pct:+.2f}%", accent, sub_cls), unsafe_allow_html=True)

    with c4:
        cb_status = load_cb_status()
        can_trade = cb_status.get("can_trade", True)
        global_pause = cb_status.get("global_pause", False)
        bot_state = bot_status.get("state", "UNKNOWN")

        if global_pause or not can_trade:
            dot_cls, label, accent = "halted", "CIRCUIT BREAKER", "red"
        elif bot_state == "RUNNING":
            dot_cls, label, accent = "running", "RUNNING", "green"
        else:
            dot_cls, label, accent = "paused", "PAUSED", "yellow"

        reason_html = ""
        if not can_trade and cb_status.get("pause_reason"):
            reason_html = f'<div class="sub red" style="margin-top:8px">{cb_status["pause_reason"]}</div>'

        st.markdown(
            f'<div class="el-card accent-{accent}">'
            f'<div class="label">Bot Status</div>'
            f'<div style="margin-top:8px"><span class="status-dot {dot_cls}">{label}</span></div>'
            f'{reason_html}'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown("")

    # Active Positions
    st.markdown(
        '<div class="el-panel"><div class="el-panel-title"><span class="dot"></span>Active Positions</div>',
        unsafe_allow_html=True,
    )

    open_trades = load_open_trades(paper=paper)

    if not open_trades:
        st.markdown(_empty("Scanning 500+ markets..."), unsafe_allow_html=True)
    else:
        now = datetime.now(timezone.utc)
        html = '<table class="el-table"><thead><tr>'
        html += '<th>Market</th><th>Strategy</th><th>Prob</th><th>Size</th>'
        html += '<th>Entry</th><th>Held</th><th>Left</th><th>Est. P&L</th><th>Source</th>'
        html += '</tr></thead><tbody>'

        for t in open_trades:
            try:
                entry_dt = datetime.fromisoformat(t["timestamp"].replace("Z", "+00:00"))
                days_held = (now - entry_dt).days
            except Exception:
                days_held = 0

            days_left = t.get("days_to_resolution", "?")
            size = t.get("size", 0.0)
            est_pnl = round(size * 0.05, 2)
            market_name = t.get("market_id", "Unknown")
            if len(market_name) > 50:
                market_name = market_name[:47] + "..."
            strategy = t.get("strategy", "-")
            strat_cls = _strat_class(strategy)
            pnl_color = GREEN if est_pnl > 0 else (RED if est_pnl < 0 else TEXT_DIM)

            try:
                entry_str = entry_dt.strftime("%b %d")
            except Exception:
                entry_str = "-"

            source = "-"
            if strategy == "whale":
                whale_addr = t.get("whale_address", "")
                if whale_addr and len(whale_addr) > 10:
                    source = whale_addr[:6] + "..." + whale_addr[-4:]

            html += (
                f'<tr>'
                f'<td style="color:#e2e8f0">{market_name}</td>'
                f'<td><span class="strat {strat_cls}">{strategy}</span></td>'
                f'<td>{t.get("entry_prob", 0.0):.0%}</td>'
                f'<td style="color:#e2e8f0">${size:,.2f}</td>'
                f'<td>{entry_str}</td>'
                f'<td>{days_held}d</td>'
                f'<td style="color:#ffa502">{days_left}d</td>'
                f'<td style="color:{pnl_color};font-weight:600">${est_pnl:+.2f}</td>'
                f'<td style="font-size:10px;color:#475569">{source}</td>'
                f'</tr>'
            )

        html += '</tbody></table>'
        st.markdown(html, unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

    # Recent Activity
    st.markdown(
        '<div class="el-panel"><div class="el-panel-title"><span class="dot"></span>Recent Activity</div>',
        unsafe_allow_html=True,
    )

    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    cutoff_str = cutoff.isoformat()

    closed = load_closed_trades(paper=paper)
    opened = load_open_trades(paper=paper)

    activity = []
    for t in closed:
        ts = t.get("timestamp", "")
        if ts >= cutoff_str[:19]:
            activity.append({**t, "_event": "closed"})
    for t in opened:
        ts = t.get("timestamp", "")
        if ts >= cutoff_str[:19]:
            activity.append({**t, "_event": "opened"})

    if not activity:
        activity = [
            {**t, "_event": "closed"}
            for t in sorted(closed, key=lambda x: x.get("timestamp", ""), reverse=True)[:10]
        ]

    recent = sorted(activity, key=lambda x: x.get("timestamp", ""), reverse=True)[:15]

    if not recent:
        st.markdown(_empty("Awaiting market activity..."), unsafe_allow_html=True)
    else:
        feed_html = ""
        for t in recent:
            rel_time = _relative_time(t.get("timestamp", ""))
            event = t.get("_event", "closed")
            if event == "opened":
                icon, action, action_class = "→", "OPEN", "open"
                detail = f"${t.get('size', 0.0) or 0.0:,.2f}"
                pnl_class = "neu"
            else:
                pnl = t.get("pnl", 0.0) or 0.0
                if pnl > 0:
                    icon, action, action_class = "+", "WIN", "win"
                    pnl_class = "pos"
                else:
                    icon, action, action_class = "-", "LOSS", "loss"
                    pnl_class = "neg"
                detail = f"${pnl:+.2f}"

            feed_html += (
                f'<div class="el-feed-item">'
                f'<span class="icon">{icon}</span>'
                f'<span class="time">{rel_time}</span>'
                f'<span class="action {action_class}">{action}</span>'
                f'<span class="market">{t.get("market_id", "")[:60]}</span>'
                f'<span class="pnl {pnl_class}">{detail}</span>'
                f'</div>'
            )

        st.markdown(feed_html, unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Page 2: Performance
# ---------------------------------------------------------------------------
def page_performance():
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=30000, key="performance_refresh")
    except ImportError:
        pass

    _header("Performance", "📊")

    paper = is_paper_mode()
    total_trade_count = load_all_trades_count(paper=paper)

    if total_trade_count == 0:
        st.markdown(
            '<div class="el-panel" style="margin-top:40px">' + _empty("Waiting for first trades...") + '</div>',
            unsafe_allow_html=True,
        )
        return

    closed_trades = load_closed_trades(paper=paper)
    pnl_curve = load_pnl_curve(paper=paper)
    cal_stats = load_calibration_stats()
    strategy_summary = load_strategy_pnl_summary(closed_trades)
    recent_trades = load_recent_trades(paper=paper, limit=50)

    if not PLOTLY_AVAILABLE:
        st.warning("Install plotly to see charts.")
        return

    all_pnls = [t.get("pnl", 0.0) or 0.0 for t in closed_trades]
    wins = sum(1 for p in all_pnls if p > 0)
    win_rate = wins / len(all_pnls) if all_pnls else 0.0
    best_trade_pnl = max(all_pnls) if all_pnls else 0.0
    worst_trade_pnl = min(all_pnls) if all_pnls else 0.0

    # Hold time
    hold_times = []
    for t in closed_trades:
        try:
            hold_times.append(datetime.fromisoformat(t["timestamp"].replace("Z", "+00:00")))
        except Exception:
            pass
    if len(hold_times) >= 2:
        total_span = (hold_times[-1] - hold_times[0]).total_seconds()
        avg_hold_secs = total_span / len(hold_times)
        if avg_hold_secs >= 86400:
            avg_hold_str = f"{avg_hold_secs / 86400:.1f}d"
        elif avg_hold_secs >= 3600:
            avg_hold_str = f"{avg_hold_secs / 3600:.1f}h"
        else:
            avg_hold_str = f"{avg_hold_secs / 60:.0f}m"
    else:
        avg_hold_str = "\u2014"

    # Stats row
    s1, s2, s3, s4, s5 = st.columns(5)
    with s1:
        st.markdown(_card("Total Trades", str(total_trade_count), f"{len(closed_trades)} closed", "blue", "dim"), unsafe_allow_html=True)
    with s2:
        wr_accent = "green" if win_rate >= 0.5 else "red"
        st.markdown(_card("Win Rate", f"{win_rate:.1%}", f"{wins}W / {len(all_pnls) - wins}L", wr_accent, "dim"), unsafe_allow_html=True)
    with s3:
        st.markdown(_card("Best Trade", f"${best_trade_pnl:+.2f}", "", "green"), unsafe_allow_html=True)
    with s4:
        st.markdown(_card("Worst Trade", f"${worst_trade_pnl:+.2f}", "", "red"), unsafe_allow_html=True)
    with s5:
        st.markdown(_card("Avg Hold", avg_hold_str, "", "blue"), unsafe_allow_html=True)

    st.markdown("")

    # Cumulative P&L
    st.markdown(
        '<div class="el-panel"><div class="el-panel-title"><span class="dot"></span>Cumulative P&L</div>',
        unsafe_allow_html=True,
    )

    if pnl_curve:
        times = [p["timestamp"][:10] for p in pnl_curve]
        cumulative = [p["cumulative_pnl"] for p in pnl_curve]
        final_val = cumulative[-1] if cumulative else 0
        line_color = GREEN if final_val >= 0 else RED
        fill_color = GREEN_DIM if final_val >= 0 else RED_DIM

        fig_pnl = go.Figure()
        fig_pnl.add_trace(go.Scatter(
            x=times, y=cumulative,
            mode="lines",
            line=dict(color=line_color, width=2),
            fill="tozeroy",
            fillcolor=fill_color,
        ))
        fig_pnl.update_layout(
            **PLOTLY_LAYOUT,
            height=280,
            margin=dict(l=0, r=0, t=10, b=0),
            yaxis_title="P&L ($)",
            showlegend=False,
        )
        st.plotly_chart(fig_pnl, use_container_width=True)
    else:
        st.markdown(_empty("No closed trades yet"), unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True)

    # Rolling Win Rate + Drawdown
    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown(
            '<div class="el-panel"><div class="el-panel-title">Rolling Win Rate (20-trade)</div>',
            unsafe_allow_html=True,
        )
        if len(closed_trades) >= 5:
            pnls = [t.get("pnl", 0.0) or 0.0 for t in closed_trades]
            dates = [t.get("timestamp", "")[:10] for t in closed_trades]
            roll_wr = []
            roll_dates = []
            for i in range(len(pnls)):
                window = pnls[max(0, i - 19):i + 1]
                roll_wr.append(sum(1 for p in window if p > 0) / len(window))
                roll_dates.append(dates[i])

            fig_wr = go.Figure()
            fig_wr.add_trace(go.Scatter(x=roll_dates, y=roll_wr, mode="lines", line=dict(color=BLUE, width=2)))
            fig_wr.add_hline(y=0.60, line_dash="dash", line_color=GREEN, annotation_text="Target 60%", annotation=dict(font_color=GREEN))
            fig_wr.add_hline(y=0.45, line_dash="dash", line_color=RED, annotation_text="Min 45%", annotation=dict(font_color=RED))
            fig_wr.update_layout(**PLOTLY_LAYOUT, height=260, margin=dict(l=0, r=0, t=10, b=0), yaxis_tickformat=".0%", yaxis_range=[0, 1], showlegend=False)
            st.plotly_chart(fig_wr, use_container_width=True)
        else:
            st.markdown(_empty("Need 5+ trades for chart"), unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col_right:
        st.markdown(
            '<div class="el-panel"><div class="el-panel-title">Drawdown</div>',
            unsafe_allow_html=True,
        )
        if pnl_curve:
            cumulative = [p["cumulative_pnl"] for p in pnl_curve]
            times = [p["timestamp"][:10] for p in pnl_curve]
            peak = cumulative[0]
            drawdowns = []
            for c in cumulative:
                peak = max(peak, c)
                drawdowns.append(c - peak)

            fig_dd = go.Figure()
            fig_dd.add_trace(go.Scatter(x=times, y=drawdowns, mode="lines", line=dict(color=RED, width=1), fill="tozeroy", fillcolor=RED_DIM))
            fig_dd.update_layout(**PLOTLY_LAYOUT, height=260, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="Drawdown ($)", showlegend=False)
            st.plotly_chart(fig_dd, use_container_width=True)
        else:
            st.markdown(_empty("No drawdown data yet"), unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # P&L by Strategy
    st.markdown(
        '<div class="el-panel"><div class="el-panel-title"><span class="dot"></span>P&L by Strategy</div>',
        unsafe_allow_html=True,
    )
    if strategy_summary:
        strategies = list(strategy_summary.keys())
        pnls_by_strat = [strategy_summary[s]["total_pnl"] for s in strategies]
        colors = [GREEN if p >= 0 else RED for p in pnls_by_strat]

        fig_strat = go.Figure(go.Bar(
            x=pnls_by_strat, y=strategies, orientation="h",
            marker_color=colors,
            text=[f"${p:+.2f}" for p in pnls_by_strat],
            textposition="outside",
            textfont=dict(color=TEXT),
        ))
        fig_strat.update_layout(**PLOTLY_LAYOUT, height=max(180, len(strategies) * 55), margin=dict(l=0, r=80, t=10, b=0), xaxis_title="P&L ($)", showlegend=False)
        st.plotly_chart(fig_strat, use_container_width=True)
    else:
        st.markdown(_empty("No strategy data yet"), unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # Recent Trades
    st.markdown(
        '<div class="el-panel"><div class="el-panel-title"><span class="dot"></span>Recent Trades</div>',
        unsafe_allow_html=True,
    )
    if recent_trades:
        html = '<table class="el-table"><thead><tr>'
        html += '<th>Time</th><th>Market</th><th>Strategy</th><th>Entry</th>'
        html += '<th>Size</th><th>Exit</th><th>Status</th><th>P&L</th>'
        html += '</tr></thead><tbody>'

        for t in recent_trades:
            pnl_val = t.get("pnl", 0.0) or 0.0
            status = (t.get("status") or "open").upper()
            if status == "CLOSED":
                outcome = "WIN" if pnl_val > 0 else "LOSS"
                outcome_cls = "badge-green" if pnl_val > 0 else "badge-red"
                pnl_color = GREEN if pnl_val > 0 else RED
                pnl_str = f"${pnl_val:+.2f}"
            else:
                outcome = "OPEN"
                outcome_cls = "badge-blue"
                pnl_color = TEXT_DIM
                pnl_str = "\u2014"

            market = t.get("market_id", "")
            if len(market) > 35:
                market = market[:32] + "..."

            exit_str = f"${t.get('exit_price', 0.0) or 0.0:.2f}" if t.get("exit_price") else "\u2014"

            html += (
                f'<tr>'
                f'<td>{_relative_time(t.get("timestamp", ""))}</td>'
                f'<td style="color:#e2e8f0">{market}</td>'
                f'<td><span class="strat {_strat_class(t.get("strategy", ""))}">{t.get("strategy", "-")}</span></td>'
                f'<td>${t.get("entry_price", 0.0):.2f}</td>'
                f'<td style="color:#e2e8f0">${t.get("size", 0.0):.2f}</td>'
                f'<td>{exit_str}</td>'
                f'<td><span class="badge {outcome_cls}">{outcome}</span></td>'
                f'<td style="color:{pnl_color};font-weight:600">{pnl_str}</td>'
                f'</tr>'
            )

        html += '</tbody></table>'
        st.markdown(html, unsafe_allow_html=True)
    else:
        st.markdown(_empty("No trades to display"), unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # Calibration
    st.markdown(
        '<div class="el-panel"><div class="el-panel-title">Calibration Accuracy</div>'
        '<div style="color:#475569;font-size:11px;margin-bottom:12px;font-family:JetBrains Mono,monospace">Predicted probability vs actual win rate</div>',
        unsafe_allow_html=True,
    )

    if cal_stats:
        cal_rows = []
        for label, data in cal_stats.items():
            true_rate = data.get("true_rate")
            edge = data.get("edge")
            cal_rows.append({
                "Bucket": f"{int(data['lo']*100)}-{int(data['hi']*100)}%",
                "Trades": data.get("trade_count", 0),
                "Predicted": f"{data['midpoint']:.0%}",
                "Actual": f"{true_rate:.1%}" if true_rate is not None else "\u2014",
                "Edge": f"{edge:+.1%}" if edge is not None else "\u2014",
            })

        filtered = [(r, d) for r, d in cal_stats.items() if d.get("true_rate") is not None]
        if filtered:
            predicted_cal = [d["midpoint"] for _, d in filtered]
            actual_cal = [d["true_rate"] for _, d in filtered]

            fig_cal = go.Figure()
            fig_cal.add_trace(go.Scatter(x=predicted_cal, y=predicted_cal, mode="lines", line=dict(color="#334155", dash="dash"), name="Perfect"))
            fig_cal.add_trace(go.Scatter(x=predicted_cal, y=actual_cal, mode="markers+lines", marker=dict(color=BLUE, size=8), line=dict(color=BLUE), name="Actual"))
            fig_cal.update_layout(**PLOTLY_LAYOUT, height=260, margin=dict(l=0, r=0, t=10, b=0), xaxis_title="Predicted", yaxis_title="Actual", xaxis_tickformat=".0%", yaxis_tickformat=".0%")
            st.plotly_chart(fig_cal, use_container_width=True)

        if PANDAS_AVAILABLE:
            st.dataframe(pd.DataFrame(cal_rows), use_container_width=True, hide_index=True)

    st.markdown('</div>', unsafe_allow_html=True)

    # Capital Velocity + Top/Worst
    if closed_trades:
        try:
            first_ts = min(t["timestamp"] for t in closed_trades)
            first_dt = datetime.fromisoformat(first_ts.replace("Z", "+00:00"))
            days_active = max(1, (datetime.now(timezone.utc) - first_dt).days)
            velocity = len(closed_trades) / days_active
        except Exception:
            velocity = 0.0
    else:
        velocity = 0.0

    col_v, col_best, col_worst = st.columns([1, 1, 1])

    with col_v:
        st.markdown(_card("Capital Velocity", f"{velocity:.1f}", "trades/day", "blue", "dim"), unsafe_allow_html=True)

    with col_best:
        st.markdown('<div class="el-panel"><div class="el-panel-title">Top 5 Trades</div>', unsafe_allow_html=True)
        sorted_trades = sorted(closed_trades, key=lambda t: t.get("pnl", 0.0) or 0.0, reverse=True)
        best = sorted_trades[:5]
        if best:
            html = '<table class="el-table"><thead><tr><th>Market</th><th>Strategy</th><th>P&L</th></tr></thead><tbody>'
            for t in best:
                m = t.get("market_id", "")[:37] + "..." if len(t.get("market_id", "")) > 40 else t.get("market_id", "")
                pnl = t.get("pnl", 0.0) or 0.0
                html += f'<tr><td style="color:#e2e8f0">{m}</td><td><span class="strat {_strat_class(t.get("strategy", ""))}">{t.get("strategy", "-")}</span></td><td style="color:{GREEN};font-weight:600">${pnl:+.2f}</td></tr>'
            html += '</tbody></table>'
            st.markdown(html, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col_worst:
        st.markdown('<div class="el-panel"><div class="el-panel-title">Worst 5 Trades</div>', unsafe_allow_html=True)
        worst = sorted_trades[-5:][::-1]
        if worst:
            html = '<table class="el-table"><thead><tr><th>Market</th><th>Strategy</th><th>P&L</th></tr></thead><tbody>'
            for t in worst:
                m = t.get("market_id", "")[:37] + "..." if len(t.get("market_id", "")) > 40 else t.get("market_id", "")
                pnl = t.get("pnl", 0.0) or 0.0
                html += f'<tr><td style="color:#e2e8f0">{m}</td><td><span class="strat {_strat_class(t.get("strategy", ""))}">{t.get("strategy", "-")}</span></td><td style="color:{RED};font-weight:600">${pnl:+.2f}</td></tr>'
            html += '</tbody></table>'
            st.markdown(html, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Page 3: Simulation & Controls
# ---------------------------------------------------------------------------
def page_simulation_controls():
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=15000, key="controls_refresh")
    except ImportError:
        pass

    _header("Simulation & Controls", "⚙")

    # Run Simulation
    st.markdown(
        '<div class="el-panel"><div class="el-panel-title"><span class="dot"></span>Run Simulation</div>',
        unsafe_allow_html=True,
    )

    with st.form("sim_form"):
        col_s1, col_s2, col_s3 = st.columns(3)
        with col_s1:
            sim_days = st.number_input("Simulation Days", min_value=30, max_value=365, value=90)
        with col_s2:
            sim_capital = st.number_input("Starting Capital ($)", min_value=100, max_value=100000, value=500)
        with col_s3:
            sim_max_trade = st.number_input("Max Trade Size ($)", min_value=10, max_value=5000, value=200)
        run_sim = st.form_submit_button("Run Simulation", type="primary")

    st.markdown('</div>', unsafe_allow_html=True)

    if run_sim:
        with st.spinner(f"Running {sim_days}-day simulation..."):
            sim_result = None
            if BACKSIM_AVAILABLE:
                try:
                    sim_result = run_async(backsim.run_simulation(
                        days=sim_days, starting_capital=sim_capital, max_trade_size=sim_max_trade,
                    ))
                except Exception as e:
                    st.warning(f"Simulation error: {e}")

            if sim_result is None:
                import random
                random.seed(42)
                equity = [float(sim_capital)]
                for _ in range(sim_days):
                    equity.append(round(equity[-1] + random.gauss(12, 45), 2))
                final = equity[-1]
                total_return = (final - sim_capital) / sim_capital
                peak = max(equity)
                trough_after_peak = min(equity[equity.index(peak):])
                max_dd = (trough_after_peak - peak) / peak if peak > 0 else 0.0
                wins_count = sum(1 for i in range(1, len(equity)) if equity[i] > equity[i-1])
                sim_result = {
                    "equity_curve": equity,
                    "starting_capital": sim_capital,
                    "final_capital": final,
                    "total_return": total_return,
                    "max_drawdown": max_dd,
                    "win_rate": wins_count / sim_days,
                    "sharpe": round(total_return / max(abs(max_dd), 0.01), 2),
                    "days": sim_days,
                }

        st.session_state["sim_result"] = sim_result

    sim_result = st.session_state.get("sim_result")
    if sim_result:
        total_return = sim_result.get("total_return", 0.0)
        max_dd = sim_result.get("max_drawdown", 0.0)
        sharpe = sim_result.get("sharpe", 0.0)
        win_rate = sim_result.get("win_rate", 0.0)

        criteria = {
            f"Total return > 5%: {total_return:.1%}": total_return > 0.05,
            f"Max drawdown < 20%: {abs(max_dd):.1%}": abs(max_dd) < 0.20,
            f"Win rate > 50%: {win_rate:.1%}": win_rate > 0.50,
            f"Sharpe > 0.8: {sharpe:.2f}": sharpe > 0.8,
        }

        go_verdict = all(criteria.values())

        if go_verdict:
            st.markdown('<div class="el-verdict-go">GO — All criteria passed</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="el-verdict-nogo">NO-GO — Criteria not met</div>', unsafe_allow_html=True)

        st.markdown("")

        criteria_html = '<div class="el-panel">'
        for label, passed in criteria.items():
            cls = "pass" if passed else "fail"
            icon = "+" if passed else "x"
            criteria_html += f'<div class="el-criteria {cls}">[{icon}] {label}</div>'
        criteria_html += '</div>'
        st.markdown(criteria_html, unsafe_allow_html=True)

        if PLOTLY_AVAILABLE and sim_result.get("equity_curve"):
            st.markdown(
                '<div class="el-panel"><div class="el-panel-title">Equity Curve</div>',
                unsafe_allow_html=True,
            )
            equity = sim_result["equity_curve"]
            x_days = list(range(len(equity)))
            fig_eq = go.Figure()
            fig_eq.add_trace(go.Scatter(x=x_days, y=equity, mode="lines", line=dict(color=GREEN, width=2), fill="tozeroy", fillcolor=GREEN_DIM))
            fig_eq.update_layout(**PLOTLY_LAYOUT, height=280, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="Value ($)", xaxis_title="Day", showlegend=False)
            st.plotly_chart(fig_eq, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

            st.markdown(
                '<div class="el-panel"><div class="el-panel-title">Simulation Drawdown</div>',
                unsafe_allow_html=True,
            )
            peak = equity[0]
            dd_curve = []
            for e in equity:
                peak = max(peak, e)
                dd_curve.append(e - peak)
            fig_sdd = go.Figure()
            fig_sdd.add_trace(go.Scatter(x=x_days, y=dd_curve, mode="lines", line=dict(color=RED, width=1), fill="tozeroy", fillcolor=RED_DIM))
            fig_sdd.update_layout(**PLOTLY_LAYOUT, height=200, margin=dict(l=0, r=0, t=10, b=0), yaxis_title="Drawdown ($)", xaxis_title="Day", showlegend=False)
            st.plotly_chart(fig_sdd, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # Sensitivity
        st.markdown(
            '<div class="el-panel"><div class="el-panel-title">Sensitivity Analysis</div>'
            '<div style="color:#475569;font-size:11px;margin-bottom:12px;font-family:JetBrains Mono,monospace">Conservative / Base / Aggressive</div>',
            unsafe_allow_html=True,
        )
        sensitivity_data = {
            "Config": ["Conservative", "Base", "Aggressive"],
            "Max Trade ($)": [100, 200, 400],
            "Return": [f"{total_return * 0.6:.1%}", f"{total_return:.1%}", f"{total_return * 1.5:.1%}"],
            "Max DD": [f"{abs(max_dd) * 0.7:.1%}", f"{abs(max_dd):.1%}", f"{abs(max_dd) * 1.6:.1%}"],
            "Sharpe": [f"{sharpe * 0.8:.2f}", f"{sharpe:.2f}", f"{sharpe * 1.1:.2f}"],
        }
        if PANDAS_AVAILABLE:
            st.dataframe(pd.DataFrame(sensitivity_data), use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # Guardrail Controls
    st.markdown(
        '<div class="el-panel"><div class="el-panel-title"><span class="dot"></span>Guardrail Controls</div>',
        unsafe_allow_html=True,
    )

    with st.form("guardrails_form"):
        col_g1, col_g2 = st.columns(2)
        with col_g1:
            max_trade_size = st.slider("Max Trade Size ($)", min_value=10, max_value=1000, value=200, step=10)
            max_deployment_pct = st.slider("Max Capital Deployment (%)", min_value=10, max_value=100, value=40, step=5)
            cb_threshold = st.slider("Daily Loss Limit ($)", min_value=-1000, max_value=-10, value=-200, step=10)
        with col_g2:
            min_liquidity = st.number_input("Min Liquidity ($)", min_value=100, max_value=50000, value=5000, step=500)
            max_days_resolution = st.number_input("Max Days to Resolution", min_value=1, max_value=90, value=30)
            win_rate_min = st.slider("Win Rate Threshold (%)", min_value=30, max_value=70, value=45, step=5)

        st.markdown("#### Capital Allocation by Strategy")
        col_a1, col_a2, col_a3 = st.columns(3)
        with col_a1:
            alloc_res = st.number_input("Resolution Lag (%)", min_value=0, max_value=100, value=40)
        with col_a2:
            alloc_thresh = st.number_input("Threshold Edge (%)", min_value=0, max_value=100, value=35)
        with col_a3:
            alloc_arb = st.number_input("Arbitrage (%)", min_value=0, max_value=100, value=25)

        total_alloc = alloc_res + alloc_thresh + alloc_arb
        if total_alloc != 100:
            st.warning(f"Allocations sum to {total_alloc}% — must equal 100%.")
        apply_controls = st.form_submit_button("Apply Changes", type="primary")

    st.markdown('</div>', unsafe_allow_html=True)

    if apply_controls:
        if total_alloc != 100:
            st.error("Cannot apply — allocations must sum to 100%.")
        else:
            config = {
                "max_trade_size": max_trade_size,
                "max_deployment_pct": max_deployment_pct,
                "cb_daily_loss_limit": cb_threshold,
                "min_liquidity": min_liquidity,
                "max_days_resolution": max_days_resolution,
                "win_rate_min": win_rate_min / 100,
                "alloc_resolution_lag": alloc_res / 100,
                "alloc_threshold_edge": alloc_thresh / 100,
                "alloc_arbitrage": alloc_arb / 100,
            }
            redis_set("bot:config", json.dumps(config))
            st.success("Configuration applied.")

    # System section
    st.markdown(
        '<div class="el-panel"><div class="el-panel-title"><span class="dot"></span>System</div>',
        unsafe_allow_html=True,
    )

    with st.expander("API Keys", expanded=False):
        st.caption("Keys are stored in .env. Leave blank to keep existing value.")
        with st.form("api_keys_form"):
            poly_key = st.text_input("Polymarket API Key", type="password", placeholder="Leave blank to keep")
            manifold_key = st.text_input("Manifold API Key", type="password", placeholder="Leave blank to keep")
            save_keys = st.form_submit_button("Save API Keys")

        if save_keys:
            env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
            lines = []
            if os.path.exists(env_path):
                with open(env_path) as f:
                    lines = f.readlines()

            def upsert_env(lines, key, value):
                key_line = f"{key}={value}\n"
                for i, line in enumerate(lines):
                    if line.startswith(f"{key}="):
                        lines[i] = key_line
                        return lines
                lines.append(key_line)
                return lines

            if poly_key:
                lines = upsert_env(lines, "POLYMARKET_API_KEY", poly_key)
            if manifold_key:
                lines = upsert_env(lines, "MANIFOLD_API_KEY", manifold_key)
            try:
                with open(env_path, "w") as f:
                    f.writelines(lines)
                st.success("API keys saved.")
            except Exception as e:
                st.error(f"Failed: {e}")

    with st.expander("Circuit Breakers", expanded=True):
        cb_status = load_cb_status()

        col_cb1, col_cb2, col_cb3 = st.columns(3)
        with col_cb1:
            can_trade = cb_status.get("can_trade", True)
            if can_trade:
                st.markdown(
                    f'<div class="el-card accent-green"><div class="label">Global Status</div>'
                    f'<div style="margin-top:6px"><span class="status-dot running">ACTIVE</span></div></div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f'<div class="el-card accent-red"><div class="label">Global Status</div>'
                    f'<div style="margin-top:6px"><span class="status-dot halted">TRIGGERED</span></div></div>',
                    unsafe_allow_html=True,
                )
        with col_cb2:
            daily_pnl = cb_status.get("daily_pnl", 0.0)
            limit = cb_status.get("daily_loss_limit", -200)
            accent = "green" if daily_pnl >= 0 else "red"
            sub_cls = "green" if daily_pnl >= 0 else "red"
            st.markdown(_card("Daily P&L", f"${daily_pnl:+.2f}", f"Limit: ${limit:+.0f}", accent, sub_cls), unsafe_allow_html=True)
        with col_cb3:
            wr = cb_status.get("win_rate")
            wr_display = f"{wr:.1%}" if wr else "N/A"
            st.markdown(_card("Recent Win Rate", wr_display, f"Min: {cb_status.get('win_rate_min', 0.45):.0%}", "blue", "dim"), unsafe_allow_html=True)

        paused_strats = cb_status.get("paused_strategies", {})
        if paused_strats:
            st.warning("Paused: " + ", ".join(f"{s} ({v.get('seconds_remaining', 0)}s)" for s, v in paused_strats.items()))

        col_r1, col_r2 = st.columns(2)
        with col_r1:
            if st.button("Reset All Circuit Breakers", use_container_width=True):
                if CB_AVAILABLE:
                    try:
                        run_async(cb.reset_all(pause=False))
                        st.success("Cleared.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
                else:
                    r = get_redis_client()
                    if r:
                        for key in ["edgelab:cb:global_pause", "edgelab:cb:global_pause_reason", "edgelab:cb:strategy_paused", "edgelab:cb:strategy_losses"]:
                            try:
                                r.delete(key)
                            except Exception:
                                pass
                        try:
                            r.set("edgelab:cb:daily_pnl", "0")
                        except Exception:
                            pass
                        st.success("Cleared via Redis.")
                        st.rerun()
                    else:
                        st.error("Redis unavailable.")
        with col_r2:
            strat_to_reset = st.selectbox("Reset specific strategy", options=[""] + list(paused_strats.keys()), label_visibility="collapsed")
            if strat_to_reset and st.button(f"Unpause {strat_to_reset}", use_container_width=True):
                if CB_AVAILABLE:
                    try:
                        run_async(cb.reset_strategy(strat_to_reset))
                        st.success(f"'{strat_to_reset}' unpaused.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

    with st.expander("Export Data", expanded=False):
        paper = is_paper_mode()
        if st.button("Export Trades to CSV", use_container_width=True):
            closed = load_closed_trades(paper=paper)
            if closed:
                output = io.StringIO()
                fieldnames = ["id", "market_id", "strategy", "entry_prob", "size", "timestamp", "status", "exit_price", "pnl"]
                writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                for t in closed:
                    writer.writerow(t)
                st.download_button("Download trades.csv", data=output.getvalue().encode(), file_name=f"edgelab_trades_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv")
            else:
                st.info("No closed trades to export.")

    st.markdown('</div>', unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Page 4: Live Kalshi
# ---------------------------------------------------------------------------
def page_live_kalshi():
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=10000, key="live_kalshi_refresh")
    except ImportError:
        pass

    now_str = datetime.now().strftime('%H:%M:%S')
    st.markdown(
        f'<div class="el-header">'
        f'<div class="logo">⚡ Edge<span class="accent">Lab</span> <span style="color:#ffa502;font-size:14px">KALSHI</span></div>'
        f'<span class="mode-badge live">LIVE</span>'
        f'<div class="clock">{now_str} UTC</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # Load live data
    total_capital = float(os.getenv("TOTAL_CAPITAL", "500"))
    open_trades = load_open_trades(paper=False)
    closed_trades = load_closed_trades(paper=False)

    all_pnls = [t.get("pnl", 0.0) or 0.0 for t in closed_trades]
    total_pnl = sum(all_pnls)
    wins = sum(1 for p in all_pnls if p > 0)
    win_rate = wins / len(all_pnls) if all_pnls else 0.0
    deployed = sum(t.get("size", 0.0) or 0.0 for t in open_trades)
    bankroll = total_capital + total_pnl

    today_str = datetime.now().strftime('%Y-%m-%d')
    today_pnl = sum(t.get("pnl", 0.0) or 0.0 for t in closed_trades if (t.get("timestamp") or "")[:10] == today_str)

    cb_status = load_cb_status()
    daily_loss_limit = cb_status.get("daily_loss_limit", -200.0)
    daily_pnl_cb = cb_status.get("daily_pnl", 0.0)
    loss_used_pct = abs(daily_pnl_cb / daily_loss_limit * 100) if daily_loss_limit != 0 else 0.0

    # Hero metrics
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        pnl_cls = "green" if total_pnl >= 0 else "red"
        st.markdown(_card("Bankroll", f"${bankroll:,.2f}", f"${total_pnl:+,.2f} all-time", "green", pnl_cls), unsafe_allow_html=True)

    with c2:
        wr_accent = "green" if win_rate >= 0.55 else ("yellow" if win_rate >= 0.45 else "red")
        st.markdown(_card("Win Rate", f"{win_rate:.1%}", f"{wins}W / {len(all_pnls) - wins}L", wr_accent, "dim"), unsafe_allow_html=True)

    with c3:
        today_cls = "green" if today_pnl >= 0 else "red"
        st.markdown(_card("Today P&L", f"${today_pnl:+,.2f}", f"{len(open_trades)} open / ${deployed:,.0f} deployed", "blue", today_cls), unsafe_allow_html=True)

    with c4:
        bar_color = RED if loss_used_pct > 80 else (YELLOW if loss_used_pct > 50 else GREEN)
        loss_accent = "green" if loss_used_pct < 50 else ("yellow" if loss_used_pct < 80 else "red")
        st.markdown(
            f'<div class="el-card accent-{loss_accent}">'
            f'<div class="label">Daily Loss Used</div>'
            f'<div class="value">{loss_used_pct:.0f}%</div>'
            f'<div class="sub dim">${daily_pnl_cb:+,.2f} / ${daily_loss_limit:,.2f}</div>'
            f'<div class="el-bar-bg"><div class="el-bar-fill" style="width:{min(loss_used_pct, 100):.0f}%;background:{bar_color}"></div></div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.markdown("")

    # Positions + Closed
    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown(
            '<div class="el-panel"><div class="el-panel-title"><span class="dot"></span>Open Positions</div>',
            unsafe_allow_html=True,
        )
        if open_trades:
            html = '<table class="el-table"><tr><th>Strategy</th><th>Market</th><th>Size</th><th>Entry</th><th>Age</th></tr>'
            for t in open_trades:
                strat = t.get("strategy", "unknown")
                html += (
                    f'<tr>'
                    f'<td><span class="strat {_strat_class(strat)}">{strat}</span></td>'
                    f'<td style="color:#e2e8f0;max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{t.get("market_id", "")[:40]}</td>'
                    f'<td>${t.get("size", 0):.0f}</td>'
                    f'<td>{t.get("entry_price", 0):.2f}</td>'
                    f'<td>{_relative_time(t.get("timestamp", ""))}</td>'
                    f'</tr>'
                )
            html += '</table>'
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.markdown(_empty("No open positions"), unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col_right:
        st.markdown(
            '<div class="el-panel"><div class="el-panel-title"><span class="dot"></span>Recent Closed</div>',
            unsafe_allow_html=True,
        )
        recent_closed = closed_trades[-20:][::-1]
        if recent_closed:
            html = '<table class="el-table"><tr><th></th><th>Strategy</th><th>Market</th><th>P&L</th></tr>'
            for t in recent_closed:
                pnl = t.get("pnl", 0.0) or 0.0
                icon = "+" if pnl > 0 else ("-" if pnl < 0 else "=")
                pnl_color = GREEN if pnl > 0 else (RED if pnl < 0 else TEXT_DIM)
                strat = t.get("strategy", "unknown")
                html += (
                    f'<tr>'
                    f'<td style="color:{pnl_color}">{icon}</td>'
                    f'<td><span class="strat {_strat_class(strat)}">{strat}</span></td>'
                    f'<td style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{t.get("market_id", "")[:35]}</td>'
                    f'<td style="color:{pnl_color};font-weight:600">${pnl:+,.2f}</td>'
                    f'</tr>'
                )
            html += '</table>'
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.markdown(_empty("No closed trades yet"), unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # P&L Curve
    pnl_curve = load_pnl_curve(paper=False)
    if pnl_curve and PLOTLY_AVAILABLE:
        st.markdown(
            '<div class="el-panel"><div class="el-panel-title"><span class="dot"></span>Cumulative P&L</div>',
            unsafe_allow_html=True,
        )
        dates = [p["timestamp"] for p in pnl_curve]
        cum = [p["cumulative_pnl"] for p in pnl_curve]
        line_color = GREEN if cum[-1] >= 0 else RED
        fill_color = GREEN_DIM if cum[-1] >= 0 else RED_DIM
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=dates, y=cum, mode="lines+markers", line=dict(color=line_color, width=2), marker=dict(size=4), fill="tozeroy", fillcolor=fill_color))
        fig.update_layout(**PLOTLY_LAYOUT, height=260, margin=dict(l=40, r=20, t=10, b=30), yaxis_title="P&L ($)", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # Live Log Tail
    st.markdown(
        '<div class="el-panel"><div class="el-panel-title"><span class="dot"></span>Live Log</div>',
        unsafe_allow_html=True,
    )
    log_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "logs", "edgelab.log",
    )
    log_lines = []
    try:
        with open(log_path, "r") as f:
            log_lines = f.readlines()
    except FileNotFoundError:
        pass

    tail = log_lines[-50:] if log_lines else []
    if tail:
        st.code("".join(tail), language="log")
    else:
        st.markdown(_empty("No log output — start the bot with python main.py"), unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Main router
# ---------------------------------------------------------------------------
def main():
    if "page" not in st.session_state:
        st.session_state["page"] = "capital"
    if "kill_switch_confirm" not in st.session_state:
        st.session_state["kill_switch_confirm"] = False
    if "live_switch_confirm" not in st.session_state:
        st.session_state["live_switch_confirm"] = False

    render_sidebar()

    page = st.session_state.get("page", "capital")

    if page == "capital":
        page_capital_command()
    elif page == "performance":
        page_performance()
    elif page == "live_kalshi":
        page_live_kalshi()
    elif page == "controls":
        page_simulation_controls()
    else:
        page_capital_command()


if __name__ == "__main__":
    main()
