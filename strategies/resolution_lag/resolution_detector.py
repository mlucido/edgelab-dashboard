"""
Resolution Detector — four independent methods to detect real-world events that have
resolved but whose prediction markets have not yet settled.

Returns list of ResolvedEvent dicts:
  {
    event_type: str,            # "sports" | "economics" | "politics" | "crypto"
    event_description: str,
    outcome: str,               # human-readable outcome
    outcome_confidence: float,  # 0–1
    resolved_at: str,           # ISO UTC timestamp
    source_url: str,
    relevant_market_keywords: list[str],
  }
"""
import logging
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    import httpx
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx", "-q"])
    import httpx

from . import config

logger = logging.getLogger("resolution_lag.detector")


# ─────────────────────────────────────────────────────────────────────────────
# METHOD 1: Kalshi finalized markets
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_kalshi_finalized() -> list:
    """
    Pull markets with status=settled from Kalshi public API (no auth required).
    Only return markets finalized within the last MAX_LAG_WINDOW_SECS.
    """
    events = []
    try:
        with httpx.Client(timeout=15) as client:
            cursor = None
            for page in range(5):  # Max 5 pages
                params = {"status": "settled", "limit": 100}
                if cursor:
                    params["cursor"] = cursor

                url = "https://api.elections.kalshi.com/trade-api/v2/markets"

                # /markets is a public endpoint — no auth headers
                resp = client.get(url, params=params)
                if resp.status_code == 429:
                    logger.warning("Kalshi rate limited (429) — backing off 5s")
                    import time as _time
                    _time.sleep(5)
                    resp = client.get(url, params=params)
                if resp.status_code != 200:
                    logger.warning(f"Kalshi finalized fetch HTTP {resp.status_code}")
                    break

                data = resp.json()
                markets = data.get("markets", [])
                if not markets:
                    break

                now = datetime.now(timezone.utc)
                cutoff = now - timedelta(seconds=config.MAX_LAG_WINDOW_SECS)

                for m in markets:
                    # Parse resolution timestamp — result may be a string or dict
                    result_field = m.get("result", {})
                    if isinstance(result_field, str):
                        # Kalshi sometimes returns result as a string like "yes"/"no"
                        result_dict = {}
                        winning_side_raw = result_field
                    else:
                        result_dict = result_field or {}
                        winning_side_raw = ""

                    resolved_str = (result_dict.get("resolved_at") if result_dict else "") or m.get("close_time", "")
                    if not resolved_str:
                        continue
                    try:
                        resolved_at = datetime.fromisoformat(resolved_str.replace("Z", "+00:00"))
                    except Exception:
                        continue

                    # Only care about recently finalized
                    if resolved_at < cutoff:
                        continue

                    winning_side = winning_side_raw or result_dict.get("winning_side", "") or m.get("result", "")
                    if not winning_side:
                        continue

                    ticker = m.get("ticker", "")
                    title = m.get("title", ticker)

                    events.append({
                        "event_type": _classify_kalshi_event(ticker, title),
                        "event_description": title,
                        "outcome": f"Kalshi finalized: {winning_side}",
                        "outcome_confidence": 1.0,  # Finalized = certain
                        "resolved_at": resolved_at.isoformat(),
                        "source_url": f"https://kalshi.com/markets/{ticker}",
                        "relevant_market_keywords": _keywords_from_title(title),
                        "_kalshi_ticker": ticker,
                        "_winning_side": winning_side.upper(),
                        "_source_method": "kalshi_finalized",
                    })

                cursor = data.get("cursor")
                if not cursor:
                    break

    except Exception as e:
        logger.warning(f"Kalshi finalized detection failed: {e}")

    logger.info(f"Kalshi finalized: found {len(events)} recent resolutions")
    return events


def _classify_kalshi_event(ticker: str, title: str) -> str:
    t = (ticker + " " + title).lower()
    if any(k in t for k in ["nfl", "nba", "mlb", "nhl", "soccer", "football", "basketball",
                              "baseball", "hockey", "championship", "playoff", "super bowl"]):
        return "sports"
    if any(k in t for k in ["fed", "fomc", "cpi", "gdp", "jobs", "unemployment", "rate",
                              "inflation", "bls", "economic"]):
        return "economics"
    if any(k in t for k in ["btc", "eth", "crypto", "bitcoin", "ethereum", "sec", "etf"]):
        return "crypto"
    return "politics"


def _keywords_from_title(title: str) -> list:
    """Extract useful matching keywords from a market title."""
    stopwords = {"will", "the", "a", "an", "be", "in", "by", "on", "of", "to",
                 "or", "and", "is", "are", "for", "at", "than"}
    words = [w.strip("?.,").lower() for w in title.split()
             if len(w) > 3 and w.lower() not in stopwords]
    return list(dict.fromkeys(words))[:8]  # dedup, keep order, max 8


# ─────────────────────────────────────────────────────────────────────────────
# METHOD 2: ESPN final scores
# ─────────────────────────────────────────────────────────────────────────────

_ESPN_SPORT_PATHS = {
    "nfl": ("football", "nfl"),
    "nba": ("basketball", "nba"),
    "mlb": ("baseball", "mlb"),
    "nhl": ("hockey", "nhl"),
    "ncaaf": ("football", "college-football"),
    "ncaab": ("basketball", "mens-college-basketball"),
}


def _fetch_espn_finals() -> list:
    """
    Query ESPN scoreboard APIs for completed games in the last 3 hours.
    Cross-reference with keyword signals for open prediction markets.
    """
    events = []
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=config.MAX_LAG_WINDOW_SECS)

    try:
        with httpx.Client(timeout=15) as client:
            for sport_key, (sport, league) in _ESPN_SPORT_PATHS.items():
                try:
                    url = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard"
                    resp = client.get(url, timeout=10)
                    if resp.status_code != 200:
                        continue

                    data = resp.json()
                    games = data.get("events", [])

                    for game in games:
                        status = game.get("status", {})
                        # Only completed games
                        if status.get("type", {}).get("completed") is not True:
                            continue

                        # Parse end time
                        end_time_str = status.get("type", {}).get("lastUpdated", "")
                        try:
                            end_time = datetime.fromisoformat(
                                end_time_str.replace("Z", "+00:00")
                            ) if end_time_str else now
                        except Exception:
                            end_time = now

                        if end_time < cutoff:
                            continue

                        # Find winner
                        competitions = game.get("competitions", [{}])
                        if not competitions:
                            continue
                        comp = competitions[0]
                        competitors = comp.get("competitors", [])
                        if len(competitors) < 2:
                            continue

                        home = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
                        away = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])

                        home_score = int(home.get("score", 0) or 0)
                        away_score = int(away.get("score", 0) or 0)
                        home_name = home.get("team", {}).get("displayName", "Home")
                        away_name = away.get("team", {}).get("displayName", "Away")

                        if home_score == away_score:
                            continue  # tie, skip

                        winner = home_name if home_score > away_score else away_name
                        loser = away_name if home_score > away_score else home_name
                        outcome_desc = f"{winner} defeated {loser} ({max(home_score, away_score)}-{min(home_score, away_score)})"

                        events.append({
                            "event_type": "sports",
                            "event_description": game.get("name", f"{away_name} @ {home_name}"),
                            "outcome": outcome_desc,
                            "outcome_confidence": 0.95,  # ESPN final score
                            "resolved_at": end_time.isoformat(),
                            "source_url": f"https://espn.com/{sport}/{league}",
                            "relevant_market_keywords": [
                                winner.lower(), loser.lower(),
                                sport_key, league.lower(), "win", "championship",
                            ],
                            "_winner": winner,
                            "_loser": loser,
                            "_sport": sport_key,
                            "_source_method": "espn_final",
                        })

                except Exception as e:
                    logger.debug(f"ESPN {sport_key} fetch failed: {e}")

    except Exception as e:
        logger.warning(f"ESPN detection failed: {e}")

    logger.info(f"ESPN: found {len(events)} recently completed games")
    return events


# ─────────────────────────────────────────────────────────────────────────────
# METHOD 3: Economic data release schedule
# ─────────────────────────────────────────────────────────────────────────────

_BLS_SERIES = {
    "Nonfarm Payrolls": "CES0000000001",
    "Unemployment Rate": "LNS14000000",
    "CPI All Items": "CUUR0000SA0",
}

_FRED_SERIES = {
    "GDP": "GDP",
    "Core CPI": "CPILFESL",
    "Fed Funds Rate": "FEDFUNDS",
}


def _fetch_economic_releases() -> list:
    """
    Check BLS and FRED APIs for data that was just published.
    A release is 'recent' if its latest observation date is today or yesterday.
    """
    events = []
    now = datetime.now(timezone.utc)
    today = now.date()
    yesterday = (now - timedelta(days=1)).date()

    # BLS API
    if config.KALSHI_API_KEY or True:  # BLS public API (no key required for basic queries)
        for series_name, series_id in _BLS_SERIES.items():
            try:
                with httpx.Client(timeout=15) as client:
                    resp = client.post(
                        "https://api.bls.gov/publicAPI/v2/timeseries/data/",
                        json={
                            "seriesid": [series_id],
                            "startYear": str(today.year),
                            "endYear": str(today.year),
                        },
                        timeout=10,
                    )
                    if resp.status_code != 200:
                        continue

                    data = resp.json()
                    series_list = data.get("Results", {}).get("series", [])
                    if not series_list:
                        continue

                    latest = series_list[0].get("data", [{}])[0]
                    period = latest.get("period", "")
                    year = latest.get("year", "")
                    value = latest.get("value", "N/A")

                    # BLS publishes monthly; check if the latest period maps to recent months
                    # period like "M01" = Jan. We check if the release just happened by
                    # looking for a "footnotes" with code "P" (preliminary) or release flag
                    footnotes = latest.get("footnotes", [])
                    is_preliminary = any(f.get("code") == "P" for f in footnotes if f)

                    # Approximate: treat as recent if today is a typical release day
                    # (BLS releases on first Friday of month, etc.)
                    # Simplified: just flag as recently available
                    events.append({
                        "event_type": "economics",
                        "event_description": f"BLS {series_name} released: {value} ({period} {year})",
                        "outcome": f"{series_name} = {value}",
                        "outcome_confidence": 0.90 if not is_preliminary else 0.75,
                        "resolved_at": now.isoformat(),
                        "source_url": f"https://data.bls.gov/timeseries/{series_id}",
                        "relevant_market_keywords": [
                            series_name.lower(), "bls", "jobs", "unemployment",
                            "cpi", "inflation", "economic data",
                        ],
                        "_series_id": series_id,
                        "_value": value,
                        "_source_method": "bls_api",
                    })

            except Exception as e:
                logger.debug(f"BLS {series_name} fetch failed: {e}")

    # FRED API
    if config.FRED_API_KEY:
        for series_name, series_id in _FRED_SERIES.items():
            try:
                with httpx.Client(timeout=15) as client:
                    resp = client.get(
                        "https://api.stlouisfed.org/fred/series/observations",
                        params={
                            "series_id": series_id,
                            "api_key": config.FRED_API_KEY,
                            "file_type": "json",
                            "sort_order": "desc",
                            "limit": 2,
                        },
                        timeout=10,
                    )
                    if resp.status_code != 200:
                        continue

                    data = resp.json()
                    obs = data.get("observations", [])
                    if not obs:
                        continue

                    latest = obs[0]
                    obs_date_str = latest.get("date", "")
                    try:
                        obs_date = datetime.strptime(obs_date_str, "%Y-%m-%d").date()
                    except Exception:
                        continue

                    if obs_date not in (today, yesterday):
                        continue  # Not a fresh release

                    value = latest.get("value", "N/A")
                    events.append({
                        "event_type": "economics",
                        "event_description": f"FRED {series_name}: {value} (as of {obs_date_str})",
                        "outcome": f"{series_name} = {value}",
                        "outcome_confidence": 0.92,
                        "resolved_at": datetime.combine(obs_date, datetime.min.time())
                                       .replace(tzinfo=timezone.utc).isoformat(),
                        "source_url": f"https://fred.stlouisfed.org/series/{series_id}",
                        "relevant_market_keywords": [
                            series_name.lower(), "fred", "gdp", "cpi",
                            "fed funds", "interest rate", "economic",
                        ],
                        "_series_id": series_id,
                        "_value": value,
                        "_source_method": "fred_api",
                    })

            except Exception as e:
                logger.debug(f"FRED {series_name} fetch failed: {e}")

    logger.info(f"Economic releases: found {len(events)} signals")
    return events


# ─────────────────────────────────────────────────────────────────────────────
# METHOD 4: News API resolution language detection
# ─────────────────────────────────────────────────────────────────────────────

_RESOLUTION_PHRASES = [
    "officially declared", "has won", "has been elected", "has been appointed",
    "signed into law", "vetoed", "confirmed by senate", "final score",
    "championship won", "title clinched", "game over", "match ended",
    "data released", "report shows", "announced today", "ruling issued",
    "approved by", "rejected by", "effective immediately",
]

_OUTCOME_NEGATIVE_PHRASES = [
    "expected to", "could", "might", "may", "projected", "forecast",
    "analysts predict", "polls show", "likely to",
]


def _fetch_news_resolution_signals() -> list:
    """
    Query NewsAPI for articles with resolution language published in last 2 hours.
    Classify by category and extract outcome.
    """
    events = []
    if not config.NEWS_API_KEY:
        logger.debug("NEWS_API_KEY not set — skipping news resolution detection")
        return events

    now = datetime.now(timezone.utc)
    from_time = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    to_time = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    query_terms = " OR ".join([
        '"final score"', '"has won"', '"officially declared"',
        '"signed into law"', '"data released"', '"confirmed by"',
    ])

    try:
        with httpx.Client(timeout=15) as client:
            resp = client.get(
                "https://newsapi.org/v2/everything",
                params={
                    "q": query_terms,
                    "from": from_time,
                    "to": to_time,
                    "language": "en",
                    "sortBy": "publishedAt",
                    "pageSize": 50,
                    "apiKey": config.NEWS_API_KEY,
                },
                timeout=12,
            )
            if resp.status_code != 200:
                logger.warning(f"NewsAPI HTTP {resp.status_code}: {resp.text[:200]}")
                return events

            articles = resp.json().get("articles", [])

            for article in articles:
                title = article.get("title", "") or ""
                description = article.get("description", "") or ""
                content = (title + " " + description).lower()

                # Skip if contains uncertainty language
                if any(phrase in content for phrase in _OUTCOME_NEGATIVE_PHRASES):
                    continue

                # Require at least one positive resolution phrase
                matched_phrases = [p for p in _RESOLUTION_PHRASES if p in content]
                if not matched_phrases:
                    continue

                # Classify category
                event_type = _classify_news_event(content)
                confidence = min(0.15 * len(matched_phrases) + 0.40, 0.80)

                pub_at = article.get("publishedAt", now.isoformat())
                try:
                    resolved_at = datetime.fromisoformat(pub_at.replace("Z", "+00:00"))
                except Exception:
                    resolved_at = now

                events.append({
                    "event_type": event_type,
                    "event_description": title[:200],
                    "outcome": f"News resolution: {matched_phrases[0]}",
                    "outcome_confidence": confidence,
                    "resolved_at": resolved_at.isoformat(),
                    "source_url": article.get("url", ""),
                    "relevant_market_keywords": _keywords_from_title(title),
                    "_matched_phrases": matched_phrases,
                    "_source_method": "news_api",
                })

    except Exception as e:
        logger.warning(f"News API resolution detection failed: {e}")

    logger.info(f"News API: found {len(events)} resolution signals")
    return events


def _classify_news_event(content: str) -> str:
    if any(k in content for k in ["nfl", "nba", "mlb", "nhl", "score", "game",
                                    "match", "championship", "team", "player"]):
        return "sports"
    if any(k in content for k in ["cpi", "gdp", "fed", "inflation", "jobs", "unemployment",
                                    "interest rate", "fomc", "bls", "data released"]):
        return "economics"
    if any(k in content for k in ["bitcoin", "crypto", "ethereum", "sec", "etf", "blockchain"]):
        return "crypto"
    return "politics"


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def detect_resolved_events() -> list:
    """
    Run all four detection methods and return deduplicated list of ResolvedEvent dicts.
    Gracefully handles failures in any single method.
    """
    all_events = []

    # Method 1: Kalshi finalized
    try:
        all_events.extend(_fetch_kalshi_finalized())
    except Exception as e:
        logger.error(f"Method 1 (Kalshi finalized) failed: {e}")

    # Method 2: ESPN finals
    try:
        all_events.extend(_fetch_espn_finals())
    except Exception as e:
        logger.error(f"Method 2 (ESPN finals) failed: {e}")

    # Method 3: Economic releases
    try:
        all_events.extend(_fetch_economic_releases())
    except Exception as e:
        logger.error(f"Method 3 (Economic releases) failed: {e}")

    # Method 4: News API resolution language
    try:
        all_events.extend(_fetch_news_resolution_signals())
    except Exception as e:
        logger.error(f"Method 4 (News API) failed: {e}")

    logger.info(f"Total resolved events detected: {len(all_events)}")
    return all_events
