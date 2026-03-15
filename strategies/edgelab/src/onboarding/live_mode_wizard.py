"""
EdgeLab Live Mode Wizard — Step-by-step onboarding to live trading.

Guides the user through:
  Step 1 — wallet_check()         Verify POLYGON_PRIVATE_KEY + derive address
  Step 2 — balance_check()        Confirm USDC wallet has >= $50
  Step 3 — simulation_check()     Confirm simulation verdict is GO
  Step 4 — configure_capital()    Set starting capital + per-trade cap
  Step 5 — activate()             Flip to LIVE mode, send alert, store Redis state

Supports:
  can_deactivate()   Check 24h cooldown before switching back to PAPER
  deactivate()       Return to PAPER mode if cooldown has passed
  get_status()       Summary of current wizard state
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Simulation report paths (priority order)
# ---------------------------------------------------------------------------

_REPORT_PATHS = [
    Path(__file__).parent.parent.parent / "data" / "simulation_report_optimized.json",
    Path(__file__).parent.parent.parent / "data" / "simulation_report.json",
    Path(__file__).parent.parent.parent / "data" / "sim_50k_report.json",
]

_REDIS_KEY_ACTIVATED_AT = "edgelab:live_activated_at"
_REDIS_KEY_COOLDOWN = "edgelab:can_deactivate_after"


def _get_redis_url() -> str:
    return os.getenv("REDIS_URL", "redis://localhost:6379")


def _get_sync_redis():
    """Return a synchronous Redis client. Raises ImportError if redis not installed."""
    import redis  # type: ignore
    return redis.from_url(_get_redis_url(), decode_responses=True)


class LiveModeWizard:
    """
    Step-by-step guide for activating EdgeLab live trading.

    Each step is a regular (synchronous) method returning a dict with at
    minimum ``{step: int, status: str}``.  Steps that require async work
    (mode_manager, Redis) run the coroutine via a private helper so callers
    don't need an event loop.
    """

    # ------------------------------------------------------------------
    # Step 1 — wallet check
    # ------------------------------------------------------------------

    def wallet_check(self) -> dict[str, Any]:
        """
        Read POLYGON_PRIVATE_KEY from .env, derive the wallet address.

        The private key is NEVER returned — only the derived public address.
        """
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()

        private_key = os.getenv("POLYGON_PRIVATE_KEY", "").strip()
        if not private_key:
            return {
                "step": 1,
                "status": "missing_key",
                "message": "Add POLYGON_PRIVATE_KEY to .env first",
            }

        try:
            from web3 import Web3  # type: ignore
            account = Web3().eth.account.from_key(private_key)
            return {
                "step": 1,
                "status": "ok",
                "address": account.address,
            }
        except Exception as exc:
            logger.error("wallet_check: failed to derive address: %s", exc)
            return {
                "step": 1,
                "status": "error",
                "message": f"Could not derive address from key: {exc}",
            }

    # ------------------------------------------------------------------
    # Step 2 — balance check
    # ------------------------------------------------------------------

    def balance_check(self, address: str) -> dict[str, Any]:
        """Verify the wallet has at least $50 USDC on Polygon."""
        try:
            from src.execution.live_trader import get_usdc_balance  # type: ignore
        except ImportError as exc:
            logger.error("balance_check: could not import live_trader: %s", exc)
            return {
                "step": 2,
                "status": "error",
                "message": f"Import error: {exc}",
            }

        try:
            balance = get_usdc_balance(address)
        except Exception as exc:
            logger.error("balance_check: RPC call failed: %s", exc)
            return {
                "step": 2,
                "status": "error",
                "message": f"Balance lookup failed: {exc}",
            }

        if balance < 50:
            return {
                "step": 2,
                "status": "insufficient_funds",
                "balance": balance,
                "message": "Fund wallet with at least $50 USDC on Polygon",
            }

        return {"step": 2, "status": "ok", "balance": balance}

    # ------------------------------------------------------------------
    # Step 3 — simulation check
    # ------------------------------------------------------------------

    def simulation_check(self) -> dict[str, Any]:
        """Verify that the latest simulation report carries a GO verdict."""
        report_path: Path | None = None
        for path in _REPORT_PATHS:
            if path.exists():
                report_path = path
                break

        if report_path is None:
            return {
                "step": 3,
                "status": "error",
                "message": (
                    "No simulation report found. Run backsim.py first. "
                    f"Expected one of: {[str(p.name) for p in _REPORT_PATHS]}"
                ),
            }

        try:
            raw = json.loads(report_path.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            return {"step": 3, "status": "error", "message": f"Could not read report: {exc}"}

        # Report may be a list (multiple configs) or a dict (single run)
        if isinstance(raw, list):
            # Pick the best config — prefer one where verdict contains "GO" without "NO"
            go_entries = [
                r for r in raw
                if "GO" in str(r.get("verdict", "")).upper()
                and "NO" not in str(r.get("verdict", "")).upper()
            ]
            entry = go_entries[0] if go_entries else raw[0]
        else:
            entry = raw

        verdict = str(entry.get("verdict", "")).upper()
        metrics = {
            "sharpe": entry.get("sharpe"),
            "win_rate": entry.get("win_rate"),
            "max_drawdown": entry.get("max_dd"),
            "verdict": entry.get("verdict"),
        }

        is_go = "GO" in verdict and "NO" not in verdict

        if not is_go:
            return {
                "step": 3,
                "status": "blocked",
                "message": "Simulation verdict is not GO. Cannot activate live mode.",
                "metrics": metrics,
            }

        return {"step": 3, "status": "ok", "metrics": metrics}

    # ------------------------------------------------------------------
    # Step 4 — configure capital
    # ------------------------------------------------------------------

    def configure_capital(
        self,
        starting_capital: float,
        max_trade_override: float = 10,
    ) -> dict[str, Any]:
        """Validate and confirm capital configuration before activation."""
        if not isinstance(starting_capital, (int, float)) or starting_capital <= 0:
            return {
                "step": 4,
                "status": "invalid",
                "message": "starting_capital must be a positive number",
            }

        if not isinstance(max_trade_override, (int, float)) or not (1 <= max_trade_override <= 100):
            return {
                "step": 4,
                "status": "invalid",
                "message": "max_trade_override must be between 1 and 100",
            }

        summary = (
            f"Starting capital: ${starting_capital:.2f}. "
            f"Max single trade: ${max_trade_override:.2f} "
            f"({max_trade_override / starting_capital * 100:.1f}% of capital)."
        )

        return {
            "step": 4,
            "status": "ok",
            "starting_capital": starting_capital,
            "max_trade_override": max_trade_override,
            "summary": summary,
        }

    # ------------------------------------------------------------------
    # Step 5 — activate
    # ------------------------------------------------------------------

    def activate(
        self,
        confirmed: bool,
        starting_capital: float,
        max_trade_override: float = 10,
    ) -> dict[str, Any]:
        """
        Flip to LIVE mode.

        Side effects (on confirmed=True):
          - Calls mode_manager.set_mode("LIVE", "user_wizard")
          - Sends LIVE_MODE_ACTIVATED Telegram alert
          - Writes Redis keys: edgelab:live_activated_at, edgelab:can_deactivate_after
        """
        if not confirmed:
            return {
                "step": 5,
                "status": "cancelled",
                "message": "Live mode activation cancelled",
            }

        ts_now = datetime.now(timezone.utc)
        ts_iso = ts_now.isoformat()
        cooldown_ts = (ts_now + timedelta(hours=24)).isoformat()

        # Flip trading mode via mode_manager
        try:
            self._run_async(self._set_live_mode())
        except Exception as exc:
            logger.error("activate: mode_manager.set_mode failed: %s", exc)
            return {
                "step": 5,
                "status": "error",
                "message": f"Failed to set LIVE mode: {exc}",
            }

        # Send Telegram alert
        try:
            from src.alerts.telegram_bot import send_alert  # type: ignore
            send_alert(
                "LIVE_MODE_ACTIVATED",
                (
                    f"Live trading activated. Capital: ${starting_capital:.2f}. "
                    f"Max trade: ${max_trade_override:.2f}."
                ),
                {
                    "starting_capital": starting_capital,
                    "max_trade_override": max_trade_override,
                    "timestamp": ts_iso,
                },
            )
        except Exception as exc:
            logger.warning("activate: Telegram alert failed (non-fatal): %s", exc)

        # Store Redis state
        try:
            r = _get_sync_redis()
            r.set(_REDIS_KEY_ACTIVATED_AT, ts_iso)
            r.set(_REDIS_KEY_COOLDOWN, cooldown_ts)
        except Exception as exc:
            logger.warning("activate: Redis state write failed (non-fatal): %s", exc)

        return {
            "step": 5,
            "status": "activated",
            "message": (
                f"Live trading is now active. Max trade: ${max_trade_override:.2f}. "
                "Monitor via health endpoint."
            ),
            "activated_at": ts_iso,
            "can_deactivate_after": cooldown_ts,
        }

    # ------------------------------------------------------------------
    # Deactivation + status helpers
    # ------------------------------------------------------------------

    def can_deactivate(self) -> bool:
        """
        Return True if the 24-hour post-activation cooldown has passed.

        Returns True (can always deactivate) if no cooldown key is found in Redis.
        """
        try:
            r = _get_sync_redis()
            raw = r.get(_REDIS_KEY_COOLDOWN)
            if raw is None:
                return True
            cooldown_dt = datetime.fromisoformat(raw)
            return datetime.now(timezone.utc) >= cooldown_dt
        except Exception as exc:
            logger.error("can_deactivate: Redis error: %s", exc)
            # Fail open — allow deactivation if we can't check
            return True

    def deactivate(self) -> dict[str, Any]:
        """Switch back to PAPER mode if the 24h cooldown has passed."""
        if not self.can_deactivate():
            try:
                r = _get_sync_redis()
                cooldown_raw = r.get(_REDIS_KEY_COOLDOWN)
            except Exception:
                cooldown_raw = None

            return {
                "status": "cooldown_active",
                "message": "Cannot deactivate yet — 24h cooldown is still active.",
                "can_deactivate_after": cooldown_raw,
            }

        try:
            self._run_async(self._set_paper_mode())
        except Exception as exc:
            return {"status": "error", "message": f"Failed to set PAPER mode: {exc}"}

        return {"status": "deactivated", "message": "Switched back to PAPER trading mode."}

    def get_status(self) -> dict[str, Any]:
        """Return a summary of current wizard / live-mode state."""
        try:
            r = _get_sync_redis()
            activated_at = r.get(_REDIS_KEY_ACTIVATED_AT)
            cooldown = r.get(_REDIS_KEY_COOLDOWN)
        except Exception:
            activated_at = None
            cooldown = None

        return {
            "activated_at": activated_at,
            "can_deactivate_after": cooldown,
            "can_deactivate_now": self.can_deactivate(),
            "trading_mode": os.getenv("TRADING_MODE", "PAPER"),
        }

    # ------------------------------------------------------------------
    # Internal async helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _run_async(coro) -> Any:
        """Run a coroutine from a synchronous context."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # In an already-running loop (e.g. Jupyter / FastAPI), create a task
                import concurrent.futures
                future = concurrent.futures.Future()

                async def _wrapper():
                    try:
                        result = await coro
                        future.set_result(result)
                    except Exception as exc:
                        future.set_exception(exc)

                asyncio.ensure_future(_wrapper())
                return future.result(timeout=15)
            else:
                return loop.run_until_complete(coro)
        except RuntimeError:
            return asyncio.run(coro)

    @staticmethod
    async def _set_live_mode() -> None:
        from src.execution.mode_manager import set_mode  # type: ignore
        await set_mode("LIVE", "user_wizard")

    @staticmethod
    async def _set_paper_mode() -> None:
        from src.execution.mode_manager import set_mode  # type: ignore
        await set_mode("PAPER", "user_wizard")
