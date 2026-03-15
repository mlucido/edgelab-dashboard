"""
Tests for Kalshi live feed, arb detector, and executor.
"""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio


# ---------------------------------------------------------------------------
# kalshi_live.py tests
# ---------------------------------------------------------------------------

class TestKalshiLiveMockGeneration:
    def test_mock_market_count(self):
        from src.feeds.kalshi_live import _MOCK_MARKETS
        assert len(_MOCK_MARKETS) >= 20

    def test_mock_markets_have_required_fields(self):
        from src.feeds.kalshi_live import _MOCK_MARKETS
        required = {"ticker", "title", "yes_bid", "yes_ask"}
        for m in _MOCK_MARKETS:
            assert required <= set(m.keys()), f"Market {m.get('ticker')} missing fields"

    def test_mock_markets_cover_categories(self):
        from src.feeds.kalshi_live import _MOCK_MARKETS
        all_tags = set()
        for m in _MOCK_MARKETS:
            all_tags.update(m.get("tags", []))
        # Should cover at least 4 distinct categories
        assert len(all_tags) >= 4

    @pytest.mark.asyncio
    async def test_poll_mock_publishes_all_markets(self):
        from src.feeds.kalshi_live import _poll_mock, _MOCK_MARKETS

        published = []
        mock_redis = AsyncMock()
        mock_redis.publish = AsyncMock(side_effect=lambda ch, data: published.append((ch, data)))

        count = await _poll_mock(mock_redis)
        assert count == len(_MOCK_MARKETS)
        assert mock_redis.publish.call_count == len(_MOCK_MARKETS)

    @pytest.mark.asyncio
    async def test_poll_mock_publishes_to_correct_channel(self):
        from src.feeds.kalshi_live import _poll_mock, REDIS_CHANNEL

        mock_redis = AsyncMock()
        await _poll_mock(mock_redis)

        for call in mock_redis.publish.call_args_list:
            channel = call[0][0]
            assert channel == REDIS_CHANNEL


class TestKalshiLiveNormalization:
    def test_normalize_standard_market(self):
        from src.feeds.kalshi_live import normalize_market

        raw = {
            "ticker": "TEST-MARKET",
            "title": "Will X happen?",
            "yes_bid": 60,
            "yes_ask": 64,
            "volume": 100000,
            "open_interest": 50000,
            "close_time": "2025-12-31T23:59:00Z",
            "tags": ["test"],
        }
        result = normalize_market(raw)
        assert result is not None
        assert result["id"] == "TEST-MARKET"
        assert result["market_id"] == "TEST-MARKET"
        assert result["question"] == "Will X happen?"
        # yes_bid=60, yes_ask=64 → midpoint=62 → 0.62
        assert result["prob_yes"] == pytest.approx(0.62, abs=0.001)
        assert result["prob_no"] == pytest.approx(0.38, abs=0.001)
        assert result["volume"] == 100000.0
        assert result["liquidity"] == 50000.0
        assert result["source"] == "kalshi"

    def test_normalize_cents_conversion(self):
        """Prices in cents (0-100) should be converted to 0-1 probability."""
        from src.feeds.kalshi_live import normalize_market

        raw = {
            "ticker": "PRICE-TEST",
            "title": "Test question",
            "yes_bid": 30,
            "yes_ask": 30,
        }
        result = normalize_market(raw)
        assert result is not None
        assert result["prob_yes"] == pytest.approx(0.30, abs=0.001)
        assert result["prob_no"] == pytest.approx(0.70, abs=0.001)

    def test_normalize_missing_ticker_returns_none(self):
        from src.feeds.kalshi_live import normalize_market

        raw = {"title": "No ticker market", "yes_bid": 50}
        result = normalize_market(raw)
        assert result is None

    def test_normalize_missing_title_returns_none(self):
        from src.feeds.kalshi_live import normalize_market

        raw = {"ticker": "NO-TITLE"}
        result = normalize_market(raw)
        assert result is None

    def test_normalize_includes_backward_compat_fields(self):
        """price field should alias prob_yes for strategy compatibility."""
        from src.feeds.kalshi_live import normalize_market

        raw = {
            "ticker": "COMPAT-TEST",
            "title": "Compat test",
            "yes_bid": 55,
            "yes_ask": 55,
        }
        result = normalize_market(raw)
        assert result is not None
        assert result["price"] == result["prob_yes"]

    def test_normalize_mock_market_round_trip(self):
        """Mock market normalized output should be valid EdgeLab schema."""
        from src.feeds.kalshi_live import _MOCK_MARKETS, _normalize_mock

        required_keys = {"id", "market_id", "question", "prob_yes", "prob_no", "source"}
        for raw in _MOCK_MARKETS:
            result = _normalize_mock(raw)
            assert required_keys <= set(result.keys()), f"Missing keys in {raw['ticker']}"
            assert 0 < result["prob_yes"] < 1
            assert 0 < result["prob_no"] < 1
            assert abs(result["prob_yes"] + result["prob_no"] - 1.0) < 0.01


# ---------------------------------------------------------------------------
# arb_detector_live.py tests
# ---------------------------------------------------------------------------

class TestArbDetectorLiveCalculation:
    def test_arb_detected_when_spread_exceeds_threshold(self):
        from src.strategies.arb_detector_live import compute_arb

        poly = {"id": "poly-1", "question": "Q", "prob_yes": 0.60, "liquidity": 10000}
        kalshi = {"id": "kal-1", "question": "Q", "prob_yes": 0.40, "liquidity": 10000}

        result = compute_arb(poly, kalshi)
        assert result is not None
        # spread = 0.20, locked = 0.20 - 2*0.02 = 0.16 (PLATFORM_FEE=0.02)
        assert result["locked_return"] == pytest.approx(0.16, abs=0.001)
        assert result["spread"] == pytest.approx(0.20, abs=0.001)

    def test_arb_not_triggered_when_spread_too_small(self):
        from src.strategies.arb_detector_live import compute_arb

        # spread = 0.02, locked = 0.02 - 0.01 = 0.01 < 0.015 threshold
        poly = {"id": "poly-1", "question": "Q", "prob_yes": 0.52, "liquidity": 10000}
        kalshi = {"id": "kal-1", "question": "Q", "prob_yes": 0.50, "liquidity": 10000}

        result = compute_arb(poly, kalshi)
        assert result is None

    def test_arb_sides_assigned_correctly_poly_cheaper(self):
        """When poly_yes < kalshi_yes: buy yes on poly, no on kalshi."""
        from src.strategies.arb_detector_live import compute_arb

        poly = {"id": "poly-1", "question": "Q", "prob_yes": 0.40, "liquidity": 5000}
        kalshi = {"id": "kal-1", "question": "Q", "prob_yes": 0.65, "liquidity": 5000}

        result = compute_arb(poly, kalshi)
        assert result is not None
        assert result["poly_side"] == "yes"
        assert result["kalshi_side"] == "no"

    def test_arb_sides_assigned_correctly_kalshi_cheaper(self):
        """When kalshi_yes < poly_yes: buy yes on kalshi, no on poly."""
        from src.strategies.arb_detector_live import compute_arb

        poly = {"id": "poly-1", "question": "Q", "prob_yes": 0.70, "liquidity": 5000}
        kalshi = {"id": "kal-1", "question": "Q", "prob_yes": 0.45, "liquidity": 5000}

        result = compute_arb(poly, kalshi)
        assert result is not None
        assert result["poly_side"] == "no"
        assert result["kalshi_side"] == "yes"

    def test_arb_returns_none_for_invalid_prices(self):
        from src.strategies.arb_detector_live import compute_arb

        # Zero or out-of-range prices
        poly = {"id": "poly-1", "question": "Q", "prob_yes": 0.0}
        kalshi = {"id": "kal-1", "question": "Q", "prob_yes": 0.70}
        assert compute_arb(poly, kalshi) is None

        poly = {"id": "poly-1", "question": "Q", "prob_yes": 1.0}
        kalshi = {"id": "kal-1", "question": "Q", "prob_yes": 0.70}
        assert compute_arb(poly, kalshi) is None

    def test_arb_result_includes_required_fields(self):
        from src.strategies.arb_detector_live import compute_arb

        poly = {"id": "poly-1", "question": "Q", "prob_yes": 0.60, "liquidity": 5000}
        kalshi = {"id": "kal-1", "question": "Q", "prob_yes": 0.35, "liquidity": 5000}

        result = compute_arb(poly, kalshi)
        assert result is not None
        required = {"strategy", "poly_market", "kalshi_market", "poly_side", "kalshi_side",
                    "locked_return", "size", "timestamp"}
        assert required <= set(result.keys())

    def test_arb_fee_deduction(self):
        """locked_return = spread - 2 * 0.005 = spread - 0.01"""
        from src.strategies.arb_detector_live import compute_arb, PLATFORM_FEE

        spread = 0.08
        poly_yes = 0.50
        kalshi_yes = poly_yes + spread

        poly = {"id": "p", "question": "Q", "prob_yes": poly_yes, "liquidity": 10000}
        kalshi = {"id": "k", "question": "Q", "prob_yes": kalshi_yes, "liquidity": 10000}

        result = compute_arb(poly, kalshi)
        assert result is not None
        expected = spread - 2 * PLATFORM_FEE
        assert result["locked_return"] == pytest.approx(expected, abs=0.001)


class TestArbDetectorLiveClaudeMatching:
    @pytest.mark.asyncio
    async def test_claude_api_called_with_correct_payload(self):
        """Mock the httpx call to Claude API and verify request structure."""
        from src.strategies.arb_detector_live import _claude_match

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "content": [
                {"text": '{"match": true, "confidence": 0.95, "reasoning": "same event"}'}
            ]
        }

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        result = await _claude_match(mock_client, "Will Biden win 2024?", "Will Biden win the 2024 election?")

        assert result["match"] is True
        assert result["confidence"] == pytest.approx(0.95)
        assert result["reasoning"] == "same event"

        call_kwargs = mock_client.post.call_args
        payload = call_kwargs[1]["json"] if "json" in call_kwargs[1] else call_kwargs[0][1]
        assert payload["model"] == "claude-haiku-4-5-20251001"
        assert "Biden" in payload["messages"][0]["content"]

    @pytest.mark.asyncio
    async def test_claude_api_returns_no_match(self):
        from src.strategies.arb_detector_live import _claude_match

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "content": [
                {"text": '{"match": false, "confidence": 0.10, "reasoning": "different events"}'}
            ]
        }

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        result = await _claude_match(mock_client, "Will it rain tomorrow?", "Will SpaceX land on Mars?")
        assert result["match"] is False
        assert result["confidence"] < 0.5

    @pytest.mark.asyncio
    async def test_claude_api_handles_markdown_fenced_json(self):
        """Claude sometimes wraps JSON in markdown code fences."""
        from src.strategies.arb_detector_live import _claude_match

        fenced = '```json\n{"match": true, "confidence": 0.92, "reasoning": "same"}\n```'
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"content": [{"text": fenced}]}

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        result = await _claude_match(mock_client, "Q1", "Q2")
        assert result["match"] is True
        assert result["confidence"] == pytest.approx(0.92)


class TestArbDetectorLiveRapidfuzzFallback:
    def test_rapidfuzz_matches_similar_questions(self):
        from src.strategies.arb_detector_live import _rapidfuzz_match

        result = _rapidfuzz_match(
            "Will Biden win the 2024 presidential election?",
            "Will Biden win the 2024 US presidential race?",
        )
        assert result["match"] is True
        assert result["confidence"] > 0.70

    def test_rapidfuzz_no_match_for_unrelated_questions(self):
        from src.strategies.arb_detector_live import _rapidfuzz_match

        result = _rapidfuzz_match(
            "Will it snow in New York in January?",
            "Will Elon Musk buy Twitter again?",
        )
        assert result["match"] is False

    def test_rapidfuzz_result_structure(self):
        from src.strategies.arb_detector_live import _rapidfuzz_match

        result = _rapidfuzz_match("Q1", "Q2")
        assert "match" in result
        assert "confidence" in result
        assert "reasoning" in result
        assert isinstance(result["match"], bool)
        assert 0.0 <= result["confidence"] <= 1.0

    def test_rapidfuzz_used_when_no_anthropic_key(self):
        """ArbDetectorLive should use rapidfuzz when ANTHROPIC_API_KEY is not set."""
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False):
            from importlib import reload
            import src.strategies.arb_detector_live as module
            reload(module)
            detector = module.ArbDetectorLive()
            assert detector._use_claude is False


# ---------------------------------------------------------------------------
# kalshi_executor.py tests
# ---------------------------------------------------------------------------

class TestKalshiExecutorSafetyGates:
    @pytest.mark.asyncio
    async def test_raises_mode_error_in_paper_mode(self):
        from src.execution.kalshi_executor import place_kalshi_order, KalshiTradingModeError

        with patch.dict("os.environ", {"TRADING_MODE": "paper"}):
            with pytest.raises(KalshiTradingModeError) as exc_info:
                await place_kalshi_order("TEST-MARKET", "yes", 500)
            assert "TRADING_MODE" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_raises_size_limit_error_when_exceeds_cap(self):
        from src.execution.kalshi_executor import place_kalshi_order, KalshiSizeLimitError

        with patch.dict("os.environ", {"TRADING_MODE": "live", "KALSHI_MAX_TRADE_CENTS": "500"}):
            with pytest.raises(KalshiSizeLimitError) as exc_info:
                await place_kalshi_order("TEST-MARKET", "yes", 1000)
            assert "500" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_raises_insufficient_balance_error(self):
        from src.execution.kalshi_executor import (
            place_kalshi_order,
            KalshiInsufficientBalanceError,
        )

        with patch.dict("os.environ", {"TRADING_MODE": "live", "KALSHI_MAX_TRADE_CENTS": "10000"}):
            with patch(
                "src.execution.kalshi_executor.get_kalshi_balance",
                new_callable=AsyncMock,
                return_value=100,  # only 100 cents available
            ):
                with pytest.raises(KalshiInsufficientBalanceError):
                    await place_kalshi_order("TEST-MARKET", "yes", 500)

    @pytest.mark.asyncio
    async def test_mode_check_fires_before_network_call(self):
        """Mode gate should prevent any HTTP calls from being made in paper mode."""
        from src.execution.kalshi_executor import place_kalshi_order, KalshiTradingModeError

        with patch.dict("os.environ", {"TRADING_MODE": "paper"}):
            with patch("httpx.AsyncClient") as mock_client:
                with pytest.raises(KalshiTradingModeError):
                    await place_kalshi_order("TEST-MARKET", "yes", 500)
                mock_client.assert_not_called()

    @pytest.mark.asyncio
    async def test_size_cap_default_is_1000_cents(self):
        """Default KALSHI_MAX_TRADE_CENTS should be 1000 (=$10)."""
        from src.execution.kalshi_executor import KALSHI_MAX_TRADE_CENTS
        assert KALSHI_MAX_TRADE_CENTS == 1000

    @pytest.mark.asyncio
    async def test_successful_order_placement(self):
        """Full happy-path: live mode, size within cap, sufficient balance."""
        from src.execution.kalshi_executor import place_kalshi_order

        mock_order_response = MagicMock()
        mock_order_response.status_code = 201
        mock_order_response.json.return_value = {
            "order": {"order_id": "ORDER-123"}
        }

        with patch.dict("os.environ", {"TRADING_MODE": "live", "KALSHI_MAX_TRADE_CENTS": "2000", "KALSHI_API_KEY": "test-key"}):
            with patch(
                "src.execution.kalshi_executor.get_kalshi_balance",
                new_callable=AsyncMock,
                return_value=5000,
            ):
                with patch(
                    "src.execution.kalshi_executor._log_kalshi_trade",
                    new_callable=AsyncMock,
                ):
                    with patch("httpx.AsyncClient") as mock_client_cls:
                        mock_client = AsyncMock()
                        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
                        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
                        mock_client.post = AsyncMock(return_value=mock_order_response)

                        result = await place_kalshi_order("TEST-MARKET", "yes", 500)

        assert result["success"] is True
        assert result["order_id"] == "ORDER-123"
        assert result["market_ticker"] == "TEST-MARKET"
        assert result["side"] == "yes"
        assert result["size_cents"] == 500

    @pytest.mark.asyncio
    async def test_get_kalshi_balance_raises_without_api_key(self):
        from src.execution.kalshi_executor import get_kalshi_balance

        with patch.dict("os.environ", {"KALSHI_API_KEY": ""}, clear=False):
            with patch("src.execution.kalshi_executor.KALSHI_API_KEY", ""):
                with pytest.raises(EnvironmentError, match="KALSHI_API_KEY"):
                    await get_kalshi_balance()
