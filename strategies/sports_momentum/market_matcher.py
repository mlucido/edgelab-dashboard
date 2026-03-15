"""
Market matcher: maps live games to open Kalshi markets.
Uses fuzzy team name matching.
"""
import time
import logging
import httpx
from typing import Optional

from . import config

logger = logging.getLogger("sports_momentum.market_matcher")

# Cache: (timestamp, markets_list)
_market_cache: tuple[float, list] = (0.0, [])


def _normalize(s: str) -> str:
    """Lowercase, strip punctuation for comparison."""
    return s.lower().replace(".", "").replace("-", " ").strip()


def _team_match_score(team_name: str, market_title: str) -> float:
    """
    Fuzzy match a team name against a market title.
    Returns score: exact=1.0, city match=0.8, nickname=0.6, partial=0.4, no match=0.0
    """
    norm_team = _normalize(team_name)
    norm_market = _normalize(market_title)

    # Exact team name in market title
    if norm_team in norm_market:
        return 1.0

    # Split into city + nickname (e.g., "Los Angeles Lakers" -> ["Los Angeles", "Lakers"])
    parts = norm_team.split()
    if len(parts) >= 2:
        city = " ".join(parts[:-1])
        nickname = parts[-1]

        if city in norm_market:
            return 0.8
        if nickname in norm_market:
            return 0.6

        # Partial word match
        for part in parts:
            if len(part) > 3 and part in norm_market:
                return 0.4

    # Single token partial match
    if len(norm_team) > 3 and norm_team[:4] in norm_market:
        return 0.3

    return 0.0


def _fetch_kalshi_markets() -> list[dict]:
    """
    Fetch open markets from Kalshi API.
    Returns list of market dicts with ticker, title, yes_price, no_price, volume.
    """
    url = "https://api.elections.kalshi.com/trade-api/v2/markets"
    all_markets = []
    cursor = None

    try:
        for page in range(5):  # cap at 500 markets
            params = {"status": "open", "limit": 100}
            if cursor:
                params["cursor"] = cursor

            headers = {}
            if config.KALSHI_API_KEY:
                headers["Authorization"] = f"Bearer {config.KALSHI_API_KEY}"

            resp = httpx.get(url, params=params, headers=headers, timeout=10.0)
            resp.raise_for_status()
            data = resp.json()

            markets = data.get("markets", [])
            all_markets.extend(markets)

            cursor = data.get("cursor")
            if not cursor or not markets:
                break

    except httpx.TimeoutException:
        logger.warning("Kalshi API timeout — using cached markets")
    except Exception as e:
        logger.warning(f"Kalshi market fetch error: {e}")

    return all_markets


def _get_markets_cached() -> list[dict]:
    """Return cached Kalshi markets, refreshing every 5 minutes."""
    global _market_cache
    ts, markets = _market_cache
    if time.time() - ts > config.LAG_WINDOW_SECS * 2 or not markets:
        try:
            fresh = _fetch_kalshi_markets()
            if fresh:
                _market_cache = (time.time(), fresh)
                logger.info(f"Refreshed Kalshi market cache: {len(fresh)} markets")
                return fresh
        except Exception as e:
            logger.warning(f"Failed to refresh market cache: {e}")
    return markets


def _classify_market_type(title: str) -> str:
    """Classify a market as game_winner, first_half, total_points, or other."""
    norm = title.lower()
    if any(kw in norm for kw in ["winner", "win", "moneyline"]):
        return "game_winner"
    if any(kw in norm for kw in ["first half", "1st half", "halftime"]):
        return "first_half"
    if any(kw in norm for kw in ["total", "over", "under", "points"]):
        return "total_points"
    return "other"


def match_game_to_markets(game: dict, open_markets: Optional[list[dict]] = None) -> list[dict]:
    """
    Match a live game to relevant open Kalshi markets.
    Returns list of matched market dicts with added match_score and market_type fields.
    Min match score: 0.6
    """
    if open_markets is None:
        open_markets = _get_markets_cached()

    if not open_markets:
        logger.debug("No open markets available for matching")
        return []

    home_team = game.get("home_team", "")
    away_team = game.get("away_team", "")
    sport = game.get("sport", "")

    # Sport keywords to filter relevant markets
    sport_keywords = {
        "nba": ["nba", "basketball"],
        "nfl": ["nfl", "football"],
        "mlb": ["mlb", "baseball"],
        "ncaab": ["ncaa", "college basketball", "ncaab"],
    }
    sport_kws = sport_keywords.get(sport, [])

    matched = []
    for market in open_markets:
        title = market.get("title", "") or market.get("subtitle", "") or ""
        ticker = market.get("ticker", "")

        # Quick sport filter
        norm_title = title.lower()
        if sport_kws and not any(kw in norm_title or kw in ticker.lower() for kw in sport_kws):
            # Also check for team name match even without sport keyword
            home_score = _team_match_score(home_team, title)
            away_score = _team_match_score(away_team, title)
            best = max(home_score, away_score)
            if best < 0.6:
                continue

        home_ms = _team_match_score(home_team, title)
        away_ms = _team_match_score(away_team, title)
        best_match = max(home_ms, away_ms)

        if best_match < 0.6:
            continue

        # Both teams present is ideal
        combined = (home_ms + away_ms) / 2.0 if (home_ms > 0 and away_ms > 0) else best_match

        market_type = _classify_market_type(title)
        if market_type not in ("game_winner", "first_half", "total_points"):
            continue

        yes_ask = market.get("yes_ask", 50) or 50
        no_ask = market.get("no_ask", 50) or 50
        volume = market.get("volume", 0) or 0

        matched.append({
            "ticker": ticker,
            "title": title,
            "market_type": market_type,
            "match_score": round(combined, 2),
            "yes_ask": yes_ask / 100.0 if yes_ask > 1 else yes_ask,
            "no_ask": no_ask / 100.0 if no_ask > 1 else no_ask,
            "volume": volume,
            "home_match": home_ms,
            "away_match": away_ms,
        })

    # Sort by match score descending, limit to top 5
    matched.sort(key=lambda x: x["match_score"], reverse=True)
    return matched[:5]
