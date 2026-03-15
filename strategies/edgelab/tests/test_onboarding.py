"""
Tests for:
  - src/alerts/telegram_bot.send_alert  (synchronous)
  - src/onboarding/live_mode_wizard.LiveModeWizard

All external I/O (httpx, Redis, web3, mode_manager, filesystem) is mocked.
Run with: pytest tests/test_onboarding.py -v
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_last_log_line(tmp_path: Path) -> str:
    log_file = tmp_path / "logs" / "alerts.log"
    assert log_file.exists(), f"Expected log file at {log_file}"
    lines = log_file.read_text().strip().splitlines()
    return lines[-1]


# ===========================================================================
# send_alert — synchronous Telegram function
# ===========================================================================

class TestSendAlertNoTelegram:
    """When TELEGRAM_TOKEN/CHAT_ID are absent, alerts must be written to log."""

    def test_logs_to_file_when_no_credentials(self, tmp_path, monkeypatch):
        monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

        # Redirect the log directory to tmp_path
        import src.alerts.telegram_bot as bot_module
        monkeypatch.setattr(bot_module, "_LOG_DIR", tmp_path / "logs")
        monkeypatch.setattr(bot_module, "_ALERT_LOG", tmp_path / "logs" / "alerts.log")

        from src.alerts.telegram_bot import send_alert
        result = send_alert("TEST", "hello from test", {})

        assert result is True
        last = _read_last_log_line(tmp_path)
        assert "TEST" in last
        assert "hello from test" in last

    def test_returns_true_when_logged(self, tmp_path, monkeypatch):
        monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

        import src.alerts.telegram_bot as bot_module
        monkeypatch.setattr(bot_module, "_LOG_DIR", tmp_path / "logs")
        monkeypatch.setattr(bot_module, "_ALERT_LOG", tmp_path / "logs" / "alerts.log")

        from src.alerts.telegram_bot import send_alert
        assert send_alert("BOT_DOWN", "crashed", {}) is True


class TestSendAlertWithTelegram:
    """When credentials are present, alert should POST to Telegram."""

    def test_sends_to_telegram_with_mock(self, monkeypatch):
        monkeypatch.setenv("TELEGRAM_TOKEN", "fake-token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")

        mock_response = MagicMock()
        mock_response.status_code = 200

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post = MagicMock(return_value=mock_response)

        with patch("httpx.Client", return_value=mock_client):
            from src.alerts.telegram_bot import send_alert
            result = send_alert("TEST", "integration test alert", {})

        assert result is True
        mock_client.post.assert_called_once()
        call_kwargs = mock_client.post.call_args
        payload = call_kwargs[1]["json"] if "json" in call_kwargs[1] else call_kwargs[0][1]
        assert "EdgeLab" in payload["text"]
        assert "integration test alert" in payload["text"]

    def test_falls_back_to_log_on_telegram_error(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TELEGRAM_TOKEN", "bad-token")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "99999")

        import src.alerts.telegram_bot as bot_module
        monkeypatch.setattr(bot_module, "_LOG_DIR", tmp_path / "logs")
        monkeypatch.setattr(bot_module, "_ALERT_LOG", tmp_path / "logs" / "alerts.log")

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post = MagicMock(return_value=mock_response)

        with patch("httpx.Client", return_value=mock_client):
            from src.alerts.telegram_bot import send_alert
            result = send_alert("TEST", "auth failure test", {})

        assert result is False  # Telegram 401 → False
        # Should still have written to log
        last = _read_last_log_line(tmp_path)
        assert "TEST" in last


class TestSendAlertEmojiMapping:
    """Each event type should produce the correct emoji prefix."""

    @pytest.fixture(autouse=True)
    def no_telegram(self, monkeypatch, tmp_path):
        monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
        import src.alerts.telegram_bot as bot_module
        monkeypatch.setattr(bot_module, "_LOG_DIR", tmp_path / "logs")
        monkeypatch.setattr(bot_module, "_ALERT_LOG", tmp_path / "logs" / "alerts.log")
        self.tmp_path = tmp_path

    def _get_formatted(self, event_type: str) -> str:
        from src.alerts.telegram_bot import send_alert
        send_alert(event_type, "test", {})
        return _read_last_log_line(self.tmp_path)

    def test_circuit_breaker_emoji(self):
        line = self._get_formatted("CIRCUIT_BREAKER")
        assert "\u26a0\ufe0f" in line or "⚠" in line

    def test_strategy_paused_emoji(self):
        line = self._get_formatted("STRATEGY_PAUSED")
        assert "\U0001f534" in line or "🔴" in line

    def test_bot_restarted_emoji(self):
        line = self._get_formatted("BOT_RESTARTED")
        assert "\U0001f504" in line or "🔄" in line

    def test_daily_milestone_emoji(self):
        line = self._get_formatted("DAILY_MILESTONE")
        assert "\U0001f4b0" in line or "💰" in line

    def test_live_mode_activated_emoji(self):
        line = self._get_formatted("LIVE_MODE_ACTIVATED")
        assert "\U0001f6a8" in line or "🚨" in line

    def test_trade_placed_suppressed_by_default(self, monkeypatch):
        monkeypatch.delenv("TRADE_ALERTS_ENABLED", raising=False)
        log_file = self.tmp_path / "logs" / "alerts.log"
        size_before = log_file.stat().st_size if log_file.exists() else 0
        from src.alerts.telegram_bot import send_alert
        result = send_alert("TRADE_PLACED", "some trade", {})
        size_after = log_file.stat().st_size if log_file.exists() else 0
        assert result is False
        assert size_after == size_before  # nothing written

    def test_trade_placed_emoji_when_enabled(self, monkeypatch):
        monkeypatch.setenv("TRADE_ALERTS_ENABLED", "1")
        line = self._get_formatted("TRADE_PLACED")
        assert "\U0001f4ca" in line or "📊" in line

    def test_bot_down_emoji(self):
        line = self._get_formatted("BOT_DOWN")
        assert "\U0001f6a8" in line or "🚨" in line

    def test_test_emoji(self):
        line = self._get_formatted("TEST")
        assert "\U0001f9ea" in line or "🧪" in line


class TestSendAlertTradePlacedFormat:
    """TRADE_PLACED should use the special market|size@prob|strategy format."""

    def test_trade_placed_formatting(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TRADE_ALERTS_ENABLED", "1")
        monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

        import src.alerts.telegram_bot as bot_module
        monkeypatch.setattr(bot_module, "_LOG_DIR", tmp_path / "logs")
        monkeypatch.setattr(bot_module, "_ALERT_LOG", tmp_path / "logs" / "alerts.log")

        from src.alerts.telegram_bot import send_alert
        send_alert("TRADE_PLACED", "formatted trade", {
            "market_question": "Will X happen before year end?",
            "size": 9.5,
            "prob": 0.85,
            "strategy": "resolution_lag",
        })

        last = _read_last_log_line(tmp_path)
        assert "Will X happen" in last
        assert "9.5" in last
        assert "85" in last
        assert "resolution_lag" in last


# ===========================================================================
# LiveModeWizard
# ===========================================================================

class TestWizardStep1WalletCheck:
    """Step 1: POLYGON_PRIVATE_KEY presence and address derivation."""

    def test_missing_key_returns_missing_status(self, monkeypatch):
        monkeypatch.delenv("POLYGON_PRIVATE_KEY", raising=False)

        with patch("dotenv.load_dotenv"):
            from src.onboarding.live_mode_wizard import LiveModeWizard
            result = LiveModeWizard().wallet_check()

        assert result["step"] == 1
        assert result["status"] == "missing_key"
        assert "POLYGON_PRIVATE_KEY" in result["message"]

    def test_present_key_returns_address(self, monkeypatch):
        monkeypatch.setenv("POLYGON_PRIVATE_KEY", "0xdeadbeef" + "0" * 56)

        mock_account = MagicMock()
        mock_account.address = "0xABCDEF1234567890" * 2 + "00"

        mock_web3 = MagicMock()
        mock_web3.return_value.eth.account.from_key.return_value = mock_account

        with patch("dotenv.load_dotenv"), patch("web3.Web3", mock_web3):
            from src.onboarding.live_mode_wizard import LiveModeWizard
            result = LiveModeWizard().wallet_check()

        assert result["step"] == 1
        assert result["status"] == "ok"
        assert "address" in result
        # Private key must not be leaked
        assert "POLYGON_PRIVATE_KEY" not in str(result.get("address", ""))
        assert "deadbeef" not in str(result)


class TestWizardStep2BalanceCheck:
    """Step 2: USDC balance must be >= $50."""

    def test_insufficient_balance(self):
        with patch("src.execution.live_trader.get_usdc_balance", return_value=25.0):
            from src.onboarding.live_mode_wizard import LiveModeWizard
            result = LiveModeWizard().balance_check("0xABC")

        assert result["step"] == 2
        assert result["status"] == "insufficient_funds"
        assert result["balance"] == 25.0
        assert "$50" in result["message"]

    def test_sufficient_balance(self):
        with patch("src.execution.live_trader.get_usdc_balance", return_value=200.0):
            from src.onboarding.live_mode_wizard import LiveModeWizard
            result = LiveModeWizard().balance_check("0xABC")

        assert result["step"] == 2
        assert result["status"] == "ok"
        assert result["balance"] == 200.0

    def test_import_error_handled(self):
        with patch.dict("sys.modules", {"src.execution.live_trader": None}):
            from src.onboarding.live_mode_wizard import LiveModeWizard
            result = LiveModeWizard().balance_check("0xABC")

        assert result["step"] == 2
        assert result["status"] == "error"


class TestWizardStep3SimulationCheck:
    """Step 3: Simulation report verdict must be GO."""

    def _make_go_report(self) -> dict:
        return {
            "sharpe": 1.5,
            "win_rate": 0.65,
            "max_dd": 0.08,
            "verdict": "GO",
        }

    def _make_nogo_report(self) -> dict:
        return {
            "sharpe": 0.3,
            "win_rate": 0.45,
            "max_dd": 0.35,
            "verdict": "\u274c NO-GO",
        }

    def test_go_verdict_passes(self, tmp_path):
        report_file = tmp_path / "simulation_report_optimized.json"
        report_file.write_text(json.dumps(self._make_go_report()))

        import src.onboarding.live_mode_wizard as wiz_module
        with patch.object(wiz_module, "_REPORT_PATHS", [report_file]):
            from src.onboarding.live_mode_wizard import LiveModeWizard
            result = LiveModeWizard().simulation_check()

        assert result["step"] == 3
        assert result["status"] == "ok"
        assert result["metrics"]["sharpe"] == 1.5

    def test_nogo_verdict_blocks(self, tmp_path):
        report_file = tmp_path / "simulation_report_optimized.json"
        report_file.write_text(json.dumps(self._make_nogo_report()))

        import src.onboarding.live_mode_wizard as wiz_module
        with patch.object(wiz_module, "_REPORT_PATHS", [report_file]):
            from src.onboarding.live_mode_wizard import LiveModeWizard
            result = LiveModeWizard().simulation_check()

        assert result["step"] == 3
        assert result["status"] == "blocked"
        assert "GO" in result["message"]

    def test_list_report_selects_go_entry(self, tmp_path):
        report_file = tmp_path / "sim.json"
        entries = [self._make_nogo_report(), {**self._make_go_report(), "config": "aggressive"}]
        report_file.write_text(json.dumps(entries))

        import src.onboarding.live_mode_wizard as wiz_module
        with patch.object(wiz_module, "_REPORT_PATHS", [report_file]):
            from src.onboarding.live_mode_wizard import LiveModeWizard
            result = LiveModeWizard().simulation_check()

        assert result["status"] == "ok"

    def test_missing_report_returns_error(self, tmp_path):
        import src.onboarding.live_mode_wizard as wiz_module
        nonexistent = tmp_path / "does_not_exist.json"
        with patch.object(wiz_module, "_REPORT_PATHS", [nonexistent]):
            from src.onboarding.live_mode_wizard import LiveModeWizard
            result = LiveModeWizard().simulation_check()

        assert result["step"] == 3
        assert result["status"] == "error"


class TestWizardStep4ConfigureCapital:
    """Step 4: Capital configuration validation."""

    def test_valid_config(self):
        from src.onboarding.live_mode_wizard import LiveModeWizard
        result = LiveModeWizard().configure_capital(500.0, 10.0)

        assert result["step"] == 4
        assert result["status"] == "ok"
        assert result["starting_capital"] == 500.0
        assert result["max_trade_override"] == 10.0
        assert "summary" in result

    def test_zero_capital_invalid(self):
        from src.onboarding.live_mode_wizard import LiveModeWizard
        result = LiveModeWizard().configure_capital(0)
        assert result["status"] == "invalid"

    def test_negative_capital_invalid(self):
        from src.onboarding.live_mode_wizard import LiveModeWizard
        result = LiveModeWizard().configure_capital(-100)
        assert result["status"] == "invalid"

    def test_max_trade_below_1_invalid(self):
        from src.onboarding.live_mode_wizard import LiveModeWizard
        result = LiveModeWizard().configure_capital(500.0, 0.5)
        assert result["status"] == "invalid"

    def test_max_trade_above_100_invalid(self):
        from src.onboarding.live_mode_wizard import LiveModeWizard
        result = LiveModeWizard().configure_capital(500.0, 101)
        assert result["status"] == "invalid"


class TestWizardStep5Activate:
    """Step 5: Activation flow."""

    def test_not_confirmed_cancels(self):
        from src.onboarding.live_mode_wizard import LiveModeWizard
        result = LiveModeWizard().activate(confirmed=False, starting_capital=500.0)

        assert result["step"] == 5
        assert result["status"] == "cancelled"

    def test_confirmed_activates(self, monkeypatch):
        mock_redis = MagicMock()
        mock_redis.set = MagicMock()

        mock_send_alert = MagicMock(return_value=True)

        with (
            patch("src.onboarding.live_mode_wizard._get_sync_redis", return_value=mock_redis),
            patch("src.alerts.telegram_bot.send_alert", mock_send_alert),
            patch("src.onboarding.live_mode_wizard.LiveModeWizard._run_async"),
        ):
            from src.onboarding.live_mode_wizard import LiveModeWizard
            result = LiveModeWizard().activate(
                confirmed=True,
                starting_capital=500.0,
                max_trade_override=10.0,
            )

        assert result["step"] == 5
        assert result["status"] == "activated"
        assert "activated_at" in result
        assert "can_deactivate_after" in result

    def test_confirmed_stores_redis_keys(self, monkeypatch):
        mock_redis = MagicMock()
        stored = {}
        mock_redis.set = lambda k, v: stored.update({k: v})

        with (
            patch("src.onboarding.live_mode_wizard._get_sync_redis", return_value=mock_redis),
            patch("src.alerts.telegram_bot.send_alert", return_value=True),
            patch("src.onboarding.live_mode_wizard.LiveModeWizard._run_async"),
        ):
            from src.onboarding.live_mode_wizard import LiveModeWizard
            LiveModeWizard().activate(confirmed=True, starting_capital=100.0)

        assert "edgelab:live_activated_at" in stored
        assert "edgelab:can_deactivate_after" in stored


class TestWizardCanDeactivate:
    """can_deactivate() must respect the 24h cooldown stored in Redis."""

    def test_no_cooldown_key_allows_deactivation(self):
        mock_redis = MagicMock()
        mock_redis.get = MagicMock(return_value=None)

        with patch("src.onboarding.live_mode_wizard._get_sync_redis", return_value=mock_redis):
            from src.onboarding.live_mode_wizard import LiveModeWizard
            assert LiveModeWizard().can_deactivate() is True

    def test_active_cooldown_blocks_deactivation(self):
        future_ts = (datetime.now(timezone.utc) + timedelta(hours=12)).isoformat()

        mock_redis = MagicMock()
        mock_redis.get = MagicMock(return_value=future_ts)

        with patch("src.onboarding.live_mode_wizard._get_sync_redis", return_value=mock_redis):
            from src.onboarding.live_mode_wizard import LiveModeWizard
            assert LiveModeWizard().can_deactivate() is False

    def test_expired_cooldown_allows_deactivation(self):
        past_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()

        mock_redis = MagicMock()
        mock_redis.get = MagicMock(return_value=past_ts)

        with patch("src.onboarding.live_mode_wizard._get_sync_redis", return_value=mock_redis):
            from src.onboarding.live_mode_wizard import LiveModeWizard
            assert LiveModeWizard().can_deactivate() is True

    def test_redis_error_fails_open(self):
        mock_redis = MagicMock()
        mock_redis.get = MagicMock(side_effect=Exception("Redis unavailable"))

        with patch("src.onboarding.live_mode_wizard._get_sync_redis", return_value=mock_redis):
            from src.onboarding.live_mode_wizard import LiveModeWizard
            # Fail open — allow deactivation when Redis is unavailable
            assert LiveModeWizard().can_deactivate() is True
