"""
ESPN live data feed for Sports Momentum strategy.
All endpoints are public/unauthenticated.
"""
import time
import logging
import httpx
from datetime import datetime, timezone
from typing import Optional

from . import config

logger = logging.getLogger("sports_momentum.espn_feed")

# Module-level cache: {cache_key: (timestamp, data)}
_cache: dict = {}
CACHE_TTL_SECS = 10
MARKET_CACHE_TTL_SECS = 300  # 5 min for Kalshi market list


def _cached_get(url: str, ttl: int = CACHE_TTL_SECS, params: dict = None) -> Optional[dict]:
    """GET with TTL cache and graceful fallback on timeout/error."""
    cache_key = url + str(params or {})
    now = time.time()
    if cache_key in _cache:
        ts, data = _cache[cache_key]
        if now - ts < ttl:
            return data

    try:
        resp = httpx.get(url, params=params, timeout=5.0)
        resp.raise_for_status()
        data = resp.json()
        _cache[cache_key] = (now, data)
        return data
    except httpx.TimeoutException:
        logger.warning(f"Timeout fetching {url} — returning cached result if available")
        if cache_key in _cache:
            return _cache[cache_key][1]
        return None
    except Exception as e:
        logger.warning(f"Error fetching {url}: {e}")
        if cache_key in _cache:
            return _cache[cache_key][1]
        return None


def get_live_games(sport: str) -> list[dict]:
    """
    Fetch live (in-progress) games for a given sport.
    Returns list of game dicts with standardized fields.
    """
    endpoint = config.SPORT_ENDPOINTS.get(sport)
    if not endpoint:
        logger.warning(f"Unknown sport: {sport}")
        return []

    url = f"{config.ESPN_BASE_URL}/{endpoint}/scoreboard"
    data = _cached_get(url)
    if not data:
        return []

    games = []
    try:
        for event in data.get("events", []):
            status_type = event.get("status", {}).get("type", {})
            status_name = status_type.get("name", "")
            status_desc = status_type.get("description", "")

            # Only include active/in-progress games
            if status_name not in ("STATUS_IN_PROGRESS", "STATUS_HALFTIME"):
                continue

            competitions = event.get("competitions", [{}])
            comp = competitions[0] if competitions else {}
            competitors = comp.get("competitors", [])

            home = next((c for c in competitors if c.get("homeAway") == "home"), {})
            away = next((c for c in competitors if c.get("homeAway") == "away"), {})

            situation = comp.get("situation", {})
            last_play_text = situation.get("lastPlay", {}).get("text", "") if isinstance(situation.get("lastPlay"), dict) else ""

            # Win probabilities (ESPN provides these for some sports)
            home_wp = None
            away_wp = None
            for predictor in comp.get("predictor", {}).get("homeTeam", []) if isinstance(comp.get("predictor", {}), dict) else []:
                home_wp = predictor.get("gameProjection")
                break
            if isinstance(comp.get("predictor"), dict):
                home_wp = comp["predictor"].get("homeTeam", {}).get("gameProjection")
                away_wp = comp["predictor"].get("awayTeam", {}).get("gameProjection")

            # Period/clock
            status_detail = event.get("status", {})
            period = status_detail.get("period", 0)
            clock = status_detail.get("displayClock", "0:00")

            game = {
                "game_id": event.get("id", ""),
                "sport": sport,
                "home_team": home.get("team", {}).get("displayName", ""),
                "away_team": away.get("team", {}).get("displayName", ""),
                "home_team_abbr": home.get("team", {}).get("abbreviation", ""),
                "away_team_abbr": away.get("team", {}).get("abbreviation", ""),
                "home_score": int(home.get("score", 0) or 0),
                "away_score": int(away.get("score", 0) or 0),
                "period": period,
                "clock": clock,
                "status": status_desc,
                "last_play": last_play_text,
                "home_win_prob": float(home_wp) / 100.0 if home_wp is not None else None,
                "away_win_prob": float(away_wp) / 100.0 if away_wp is not None else None,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
            games.append(game)
    except Exception as e:
        logger.error(f"Error parsing scoreboard for {sport}: {e}")

    return games


def get_game_detail(sport: str, game_id: str) -> dict:
    """
    Fetch full play-by-play and box score for a specific game.
    Returns dict with plays list and team stats.
    """
    endpoint = config.SPORT_ENDPOINTS.get(sport)
    if not endpoint:
        return {}

    url = f"{config.ESPN_BASE_URL}/{endpoint}/summary"
    data = _cached_get(url, ttl=CACHE_TTL_SECS, params={"event": game_id})
    if not data:
        return {}

    result = {
        "game_id": game_id,
        "sport": sport,
        "plays": [],
        "home_stats": {},
        "away_stats": {},
        "scoring_plays": [],
    }

    try:
        # Extract play-by-play
        plays_data = data.get("plays", [])
        for play in plays_data[-50:]:  # last 50 plays
            result["plays"].append({
                "text": play.get("text", ""),
                "period": play.get("period", {}).get("number", 0),
                "clock": play.get("clock", {}).get("displayValue", ""),
                "score_value": play.get("scoreValue", 0),
                "scoring_play": play.get("scoringPlay", False),
                "home_score": play.get("homeScore", 0),
                "away_score": play.get("awayScore", 0),
            })

        # Scoring plays
        scoring_plays = data.get("scoringPlays", [])
        for sp in scoring_plays:
            result["scoring_plays"].append({
                "text": sp.get("text", ""),
                "period": sp.get("period", {}).get("number", 0),
                "home_score": sp.get("homeScore", 0),
                "away_score": sp.get("awayScore", 0),
            })

        # Box score stats
        boxscore = data.get("boxscore", {})
        teams = boxscore.get("teams", [])
        for team_data in teams:
            home_away = team_data.get("homeAway", "home")
            stats = {}
            for stat_group in team_data.get("statistics", []):
                stats[stat_group.get("name", "")] = stat_group.get("displayValue", "")
            if home_away == "home":
                result["home_stats"] = stats
            else:
                result["away_stats"] = stats

    except Exception as e:
        logger.error(f"Error parsing game detail for {game_id}: {e}")

    return result


def get_all_live_scores() -> dict[str, list[dict]]:
    """
    Fetch live scores for all configured sports.
    Returns dict of {sport: [game_dicts]}.
    Adds 1s delay between polls to be respectful.
    """
    results = {}
    for sport in config.SPORT_ENDPOINTS:
        try:
            games = get_live_games(sport)
            results[sport] = games
            if games:
                logger.debug(f"{sport}: {len(games)} live games")
        except Exception as e:
            logger.warning(f"Failed to get live scores for {sport}: {e}")
            results[sport] = []
        time.sleep(1)
    return results
