"""
Tests for src/execution/live_trader.py and src/execution/mode_manager.py.

All web3 and Redis calls are mocked — no real network required.
Run with: pytest tests/test_live_trader.py -v
"""
from __future__ import annotations

import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Import the modules under test at collection time so patches resolve correctly.
import src.execution.live_trader as lt
import src.execution.mode_manager as mm

from src.execution.live_trader import TradingModeError, InsufficientBalanceError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Reset relevant env vars before each test."""
    monkeypatch.delenv("TRADING_MODE", raising=False)
    monkeypatch.delenv("POLYGON_PRIVATE_KEY", raising=False)
    monkeypatch.delenv("POLYGON_RPC_URL", raising=False)
    monkeypatch.delenv("LIVE_MAX_TRADE_OVERRIDE", raising=False)
    monkeypatch.delenv("POLYMARKET_API_KEY", raising=False)


@pytest.fixture
def live_env(monkeypatch):
    """Set env to LIVE mode with dummy credentials."""
    monkeypatch.setenv("TRADING_MODE", "LIVE")
    monkeypatch.setenv("POLYGON_PRIVATE_KEY", "0x" + "a" * 64)
    monkeypatch.setenv("POLYGON_RPC_URL", "https://polygon-rpc.com")
    monkeypatch.setenv("POLYMARKET_API_KEY", "test-api-key")
    monkeypatch.setenv("LIVE_MAX_TRADE_OVERRIDE", "10")


# ---------------------------------------------------------------------------
# Helper: build a mock CLOB HTTP response
# ---------------------------------------------------------------------------

def _make_clob_mock(status_code: int = 201, order_id: str = "order-123", tx: str = "0xabc"):
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.text = "Bad Request" if status_code >= 400 else "OK"
    mock_resp.json.return_value = {"id": order_id, "transactionHash": tx}

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    return mock_client, mock_resp


# ===========================================================================
# Section 1: TradingModeError gate
# ===========================================================================

@pytest.mark.asyncio
async def test_place_order_raises_trading_mode_error_when_paper(monkeypatch):
    """place_order must raise TradingModeError when TRADING_MODE is not LIVE."""
    monkeypatch.setenv("TRADING_MODE", "PAPER")
    with pytest.raises(TradingModeError, match="TRADING_MODE"):
        await lt.place_order("market-1", "YES", 5.0)


@pytest.mark.asyncio
async def test_place_order_raises_trading_mode_error_when_unset():
    """place_order must raise TradingModeError when TRADING_MODE is absent (defaults PAPER)."""
    # clean_env fixture already removed TRADING_MODE
    with pytest.raises(TradingModeError):
        await lt.place_order("market-1", "YES", 5.0)


@pytest.mark.asyncio
async def test_place_order_raises_trading_mode_error_case_insensitive(monkeypatch):
    """TRADING_MODE=paper (lowercase) must still raise TradingModeError."""
    monkeypatch.setenv("TRADING_MODE", "paper")
    with pytest.raises(TradingModeError):
        await lt.place_order("market-1", "YES", 5.0)


# ===========================================================================
# Section 2: Size capping
# ===========================================================================

@pytest.mark.asyncio
async def test_place_order_caps_size_at_live_max(live_env):
    """When size_usdc > LIVE_MAX_TRADE_OVERRIDE, it is capped before execution."""
    mock_client, _ = _make_clob_mock(status_code=201, order_id="cap-order", tx="0xcap")

    with patch.object(lt, "_load_account", return_value=MagicMock(address="0xAddr")), \
         patch.object(lt, "get_usdc_balance", return_value=1000.0), \
         patch.object(lt, "_log_live_trade", new_callable=AsyncMock), \
         patch("src.execution.live_trader.httpx.AsyncClient", return_value=mock_client):

        result = await lt.place_order("market-cap", "YES", size_usdc=500.0, entry_prob=0.7)

    assert result["success"] is True
    assert result["size_usdc"] == 10.0  # capped to LIVE_MAX_TRADE_OVERRIDE


# ===========================================================================
# Section 3: Insufficient balance
# ===========================================================================

@pytest.mark.asyncio
async def test_place_order_returns_error_on_insufficient_balance(live_env):
    """Returns an error dict (does not raise) when USDC balance < size_usdc."""
    with patch.object(lt, "_load_account", return_value=MagicMock(address="0xBroke")), \
         patch.object(lt, "get_usdc_balance", return_value=3.0):

        result = await lt.place_order("market-broke", "YES", size_usdc=5.0, entry_prob=0.6)

    assert result["success"] is False
    assert "Insufficient" in result["error"]
    assert result["balance"] == 3.0


# ===========================================================================
# Section 4: Successful order placement
# ===========================================================================

@pytest.mark.asyncio
async def test_place_order_success(live_env):
    """Full happy path: mode=LIVE, balance ok, CLOB returns 201."""
    mock_client, _ = _make_clob_mock(
        status_code=201, order_id="order-xyz-123", tx="0xdeadbeef"
    )

    with patch.object(lt, "_load_account", return_value=MagicMock(address="0xAddr")), \
         patch.object(lt, "get_usdc_balance", return_value=100.0), \
         patch.object(lt, "_log_live_trade", new_callable=AsyncMock), \
         patch("src.execution.live_trader.httpx.AsyncClient", return_value=mock_client):

        result = await lt.place_order("market-success", "YES", size_usdc=8.0, entry_prob=0.72)

    assert result["success"] is True
    assert result["order_id"] == "order-xyz-123"
    assert result["tx_hash"] == "0xdeadbeef"
    assert result["market_id"] == "market-success"
    assert result["outcome"] == "YES"
    assert result["size_usdc"] == 8.0
    assert result["entry_prob"] == 0.72


@pytest.mark.asyncio
async def test_place_order_clob_api_error_returns_failure(live_env):
    """CLOB API non-2xx response returns error dict, does not raise."""
    mock_client, _ = _make_clob_mock(status_code=400)

    with patch.object(lt, "_load_account", return_value=MagicMock(address="0xAddr")), \
         patch.object(lt, "get_usdc_balance", return_value=100.0), \
         patch("src.execution.live_trader.httpx.AsyncClient", return_value=mock_client):

        result = await lt.place_order("market-fail", "NO", size_usdc=5.0)

    assert result["success"] is False
    assert "400" in result["error"]


# ===========================================================================
# Section 5: close_position
# ===========================================================================

@pytest.mark.asyncio
async def test_close_position_logs_correctly():
    """close_position returns closed trade IDs for the given market."""
    mock_open_trades = [
        {"id": 42, "market_id": "market-close-me", "size": 8.0},
        {"id": 99, "market_id": "other-market", "size": 5.0},
    ]

    with patch.object(lt, "get_open_trades", new_callable=AsyncMock, return_value=mock_open_trades), \
         patch.object(lt, "close_trade", new_callable=AsyncMock, return_value=-2.0):

        result = await lt.close_position("market-close-me")

    assert result["market_id"] == "market-close-me"
    assert 42 in result["closed_trade_ids"]
    assert 99 not in result["closed_trade_ids"]


@pytest.mark.asyncio
async def test_close_position_no_open_trades():
    """close_position with no matching open trades returns empty list."""
    with patch.object(lt, "get_open_trades", new_callable=AsyncMock, return_value=[]):
        result = await lt.close_position("market-empty")

    assert result["closed_trade_ids"] == []


# ===========================================================================
# Section 6: get_usdc_balance
# ===========================================================================

def test_get_usdc_balance_returns_correct_value():
    """get_usdc_balance converts raw uint256 to float USDC correctly (6 decimals)."""
    raw_balance = 123_456_789  # = 123.456789 USDC

    mock_contract = MagicMock()
    mock_contract.functions.balanceOf.return_value.call.return_value = raw_balance

    mock_w3 = MagicMock()
    mock_w3.eth.contract.return_value = mock_contract
    mock_w3.to_checksum_address = lambda addr: addr

    with patch.object(lt, "_load_web3", return_value=mock_w3):
        balance = lt.get_usdc_balance("0xTestAddress")

    assert balance == pytest.approx(123.456789, rel=1e-6)


def test_get_usdc_balance_zero():
    """get_usdc_balance returns 0.0 when raw balance is 0."""
    mock_contract = MagicMock()
    mock_contract.functions.balanceOf.return_value.call.return_value = 0

    mock_w3 = MagicMock()
    mock_w3.eth.contract.return_value = mock_contract
    mock_w3.to_checksum_address = lambda addr: addr

    with patch.object(lt, "_load_web3", return_value=mock_w3):
        balance = lt.get_usdc_balance("0xEmpty")

    assert balance == 0.0


# ===========================================================================
# Section 7: mode_manager tests
# ===========================================================================

def _make_mock_redis(mode_value: str | None = None, history: list | None = None):
    """Build a mock Redis instance for mode_manager tests."""
    mock_r = AsyncMock()

    async def fake_get(key):
        if key == "edgelab:mode":
            return mode_value
        return None

    mock_r.get = fake_get
    mock_r.set = AsyncMock()
    mock_r.rpush = AsyncMock()
    mock_r.ltrim = AsyncMock()
    mock_r.publish = AsyncMock()
    mock_r.aclose = AsyncMock()

    entries = [json.dumps(e) for e in (history or [])]
    mock_r.lrange = AsyncMock(return_value=entries)

    return mock_r


@pytest.mark.asyncio
async def test_get_mode_defaults_to_paper():
    """get_mode returns 'PAPER' when Redis has no stored mode."""
    mock_r = _make_mock_redis(mode_value=None)
    result = await mm.get_mode(redis=mock_r)
    assert result == "PAPER"


@pytest.mark.asyncio
async def test_get_mode_returns_live_when_set():
    """get_mode returns 'LIVE' when Redis has edgelab:mode = 'LIVE'."""
    mock_r = _make_mock_redis(mode_value="LIVE")
    result = await mm.get_mode(redis=mock_r)
    assert result == "LIVE"


@pytest.mark.asyncio
async def test_set_mode_writes_to_redis():
    """set_mode stores the new mode in Redis."""
    mock_r = _make_mock_redis(mode_value="PAPER")
    await mm.set_mode("LIVE", confirmed_by="test-user", redis=mock_r)
    mock_r.set.assert_awaited_with("edgelab:mode", "LIVE")


@pytest.mark.asyncio
async def test_set_mode_appends_history():
    """set_mode pushes a history record to edgelab:mode_history."""
    mock_r = _make_mock_redis(mode_value="PAPER")
    await mm.set_mode("LIVE", confirmed_by="admin", redis=mock_r)

    assert mock_r.rpush.await_count == 1
    key_used = mock_r.rpush.call_args[0][0]
    assert key_used == "edgelab:mode_history"

    pushed_json = mock_r.rpush.call_args[0][1]
    record = json.loads(pushed_json)
    assert record["mode"] == "LIVE"
    assert record["previous_mode"] == "PAPER"
    assert record["confirmed_by"] == "admin"
    assert "timestamp" in record


@pytest.mark.asyncio
async def test_set_mode_publishes_alert():
    """set_mode publishes a TRADING_MODE_CHANGED alert to the alerts channel."""
    mock_r = _make_mock_redis(mode_value="PAPER")
    await mm.set_mode("LIVE", confirmed_by="ops", redis=mock_r)

    mock_r.publish.assert_awaited_once()
    channel = mock_r.publish.call_args[0][0]
    payload = json.loads(mock_r.publish.call_args[0][1])
    assert channel == "alerts"
    assert payload["type"] == "TRADING_MODE_CHANGED"
    assert payload["details"]["new_mode"] == "LIVE"


@pytest.mark.asyncio
async def test_set_mode_raises_on_invalid_mode():
    """set_mode raises ValueError for unknown modes."""
    mock_r = _make_mock_redis(mode_value="PAPER")
    with pytest.raises(ValueError, match="Invalid mode"):
        await mm.set_mode("TURBO", confirmed_by="test", redis=mock_r)


@pytest.mark.asyncio
async def test_is_live_returns_false_in_paper_mode():
    """is_live() returns False when mode is PAPER."""
    mock_r = _make_mock_redis(mode_value="PAPER")
    result = await mm.is_live(redis=mock_r)
    assert result is False


@pytest.mark.asyncio
async def test_is_live_returns_true_in_live_mode():
    """is_live() returns True when mode is LIVE."""
    mock_r = _make_mock_redis(mode_value="LIVE")
    result = await mm.is_live(redis=mock_r)
    assert result is True


@pytest.mark.asyncio
async def test_is_live_returns_false_when_no_mode_set():
    """is_live() returns False when no mode is stored (defaults to PAPER)."""
    mock_r = _make_mock_redis(mode_value=None)
    result = await mm.is_live(redis=mock_r)
    assert result is False


@pytest.mark.asyncio
async def test_get_mode_history_returns_records():
    """get_mode_history returns parsed history records."""
    history = [
        {"mode": "LIVE", "previous_mode": "PAPER", "confirmed_by": "user1", "timestamp": "2026-01-01T00:00:00+00:00"},
        {"mode": "PAPER", "previous_mode": "LIVE", "confirmed_by": "watchdog", "timestamp": "2026-01-02T00:00:00+00:00"},
    ]
    mock_r = _make_mock_redis(mode_value="PAPER", history=history)
    result = await mm.get_mode_history(redis=mock_r)

    assert len(result) == 2
    assert result[0]["mode"] == "LIVE"
    assert result[1]["confirmed_by"] == "watchdog"


@pytest.mark.asyncio
async def test_get_mode_history_empty():
    """get_mode_history returns empty list when no history exists."""
    mock_r = _make_mock_redis(mode_value=None, history=[])
    result = await mm.get_mode_history(redis=mock_r)
    assert result == []
