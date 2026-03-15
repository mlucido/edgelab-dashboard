"""
Real-world event monitor.
Polls NewsAPI (top headlines) and ESPN NBA scoreboard.
Extracts keywords and pushes resolved events to Redis channel `realworld:events`.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import string
import time
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CHANNEL = "realworld:events"
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY", "")

NEWSAPI_URL = "https://newsapi.org/v2/top-headlines"
NEWSAPI_PARAMS = {"language": "en", "pageSize": 100, "country": "us"}
NEWSAPI_POLL_INTERVAL = 900  # seconds (15 min) — ~96 requests/day, under 100/day free tier
NEWSAPI_FREE_TIER_LIMIT = 100  # max requests/day on free plan
NEWSAPI_WARN_THRESHOLD = 0.80  # warn at 80% of daily limit

# NewsAPI daily request tracking
_newsapi_daily_count: int = 0
_newsapi_count_date: str = ""  # YYYY-MM-DD of current count

ESPN_SPORTS: dict[str, str] = {
    "NBA": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
    "NFL": "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
    "MLB": "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
    "NHL": "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
    "MLS": "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/scoreboard",
}
ESPN_POLL_INTERVAL = 30  # seconds

# Crypto price feeds (CoinGecko free API, no key required)
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
COINGECKO_COINS = "bitcoin,ethereum,solana,dogecoin,cardano,ripple"
CRYPTO_POLL_INTERVAL = 15  # seconds — crypto markets move fast
COINGECKO_MIN_REQUEST_GAP = 2.0  # minimum seconds between CoinGecko requests (free tier: 30/min)
COINGECKO_COIN_TTL = 60.0  # minimum seconds between fetches per coin
COINGECKO_MAX_CALLS_PER_HOUR = 50  # hard cap on CoinGecko calls per hour
_coingecko_last_request: float = 0.0  # monotonic timestamp of last request
_coingecko_cache: dict | None = None  # last successful response
# Per-coin price cache: coin -> {"price": float, "fetched_at": monotonic float}
_price_cache: dict[str, dict] = {}
# Hourly rate limit tracking
_coingecko_hourly_calls: int = 0
_coingecko_hour_window_start: float = 0.0  # monotonic timestamp when current hour started
# Track price levels for crossing detection
_CRYPTO_PRICE_LEVELS: dict[str, list[int]] = {
    "bitcoin": list(range(50000, 200001, 5000)),
    "ethereum": list(range(1000, 10001, 500)),
    "solana": list(range(50, 501, 25)),
    "dogecoin": [round(x * 0.05, 2) for x in range(1, 21)],
    "cardano": [round(x * 0.25, 2) for x in range(1, 21)],
    "ripple": [round(x * 0.5, 2) for x in range(1, 21)],
}
_CRYPTO_LAST_PRICES: dict[str, float] = {}


# ---------------------------------------------------------------------------
# Mock news data (used when NEWSAPI_KEY is not set)
# ---------------------------------------------------------------------------

_MOCK_HEADLINES = [
    {"title": "Federal Reserve holds interest rates steady amid inflation concerns", "source": {"name": "Reuters"},
     "url": "https://reuters.com/mock/1", "publishedAt": "2024-11-15T14:00:00Z", "description": "The Fed voted unanimously to keep rates unchanged."},
    {"title": "Joe Biden signs new executive order on AI safety", "source": {"name": "Associated Press"},
     "url": "https://apnews.com/mock/2", "publishedAt": "2024-11-15T13:30:00Z", "description": "President Biden signed an executive order targeting AI risks."},
    {"title": "Kansas City Chiefs defeat Denver Broncos 28-14 in overtime thriller", "source": {"name": "ESPN"},
     "url": "https://espn.com/mock/3", "publishedAt": "2024-11-15T22:00:00Z", "description": "Patrick Mahomes led a late comeback for Kansas City."},
    {"title": "Bitcoin surges past $95,000 as ETF inflows accelerate", "source": {"name": "CoinDesk"},
     "url": "https://coindesk.com/mock/4", "publishedAt": "2024-11-15T10:00:00Z", "description": "Bitcoin hit a new high amid strong ETF demand."},
    {"title": "Ukraine reports major drone strike on Russian oil facility", "source": {"name": "BBC"},
     "url": "https://bbc.com/mock/5", "publishedAt": "2024-11-15T08:00:00Z", "description": "Ukrainian forces struck a key Russian infrastructure target."},
    {"title": "SpaceX Starship completes sixth test flight successfully", "source": {"name": "The Verge"},
     "url": "https://theverge.com/mock/6", "publishedAt": "2024-11-15T20:00:00Z", "description": "Starship completed a full orbital test with booster catch."},
    {"title": "Apple announces new AI features coming to iPhone in spring update", "source": {"name": "9to5Mac"},
     "url": "https://9to5mac.com/mock/7", "publishedAt": "2024-11-15T15:00:00Z", "description": "Apple Intelligence will expand to more languages."},
    {"title": "Taylor Swift Eras Tour surpasses $2 billion in total revenue", "source": {"name": "Billboard"},
     "url": "https://billboard.com/mock/8", "publishedAt": "2024-11-15T12:00:00Z", "description": "The record-setting tour continues to break box office records."},
    {"title": "US economy adds 220,000 jobs in October, unemployment holds at 4.1%", "source": {"name": "Wall Street Journal"},
     "url": "https://wsj.com/mock/9", "publishedAt": "2024-11-01T08:30:00Z", "description": "The labor market shows continued resilience."},
    {"title": "Supreme Court agrees to hear major social media free speech case", "source": {"name": "NPR"},
     "url": "https://npr.org/mock/10", "publishedAt": "2024-11-15T11:00:00Z", "description": "The court will review platform moderation policies."},
]


# ---------------------------------------------------------------------------
# Keyword extraction (lightweight NLP, no heavy deps)
# ---------------------------------------------------------------------------

# Common English stopwords — used to filter out noise
_STOPWORDS = frozenset({
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "as", "is", "was", "are", "were", "be",
    "been", "being", "have", "has", "had", "do", "does", "did", "will",
    "would", "could", "should", "may", "might", "shall", "can", "need",
    "this", "that", "these", "those", "it", "its", "they", "them", "their",
    "he", "she", "his", "her", "we", "our", "you", "your", "i", "my",
    "not", "no", "nor", "so", "yet", "both", "either", "neither", "over",
    "after", "before", "while", "during", "into", "through", "about",
    "against", "between", "into", "through", "up", "down", "out", "off",
    "then", "than", "too", "very", "just", "more", "also", "than",
    "new", "said", "says", "say", "amid", "after", "now",
})

# Known teams, orgs, people — augmented by proper-noun detection
_KNOWN_ENTITIES: dict[str, str] = {
    # NFL teams
    "chiefs": "NFL:KansasCity", "patriots": "NFL:NewEngland", "cowboys": "NFL:Dallas",
    "eagles": "NFL:Philadelphia", "49ers": "NFL:SanFrancisco", "bills": "NFL:Buffalo",
    "ravens": "NFL:Baltimore", "bengals": "NFL:Cincinnati", "broncos": "NFL:Denver",
    # NBA teams
    "celtics": "NBA:Boston", "lakers": "NBA:LosAngeles", "warriors": "NBA:GoldenState",
    "heat": "NBA:Miami", "bucks": "NBA:Milwaukee", "nuggets": "NBA:Denver",
    "suns": "NBA:Phoenix", "clippers": "NBA:LAClippers", "nets": "NBA:Brooklyn",
    # MLB
    "dodgers": "MLB:LosAngeles", "yankees": "MLB:NewYork", "red sox": "MLB:Boston",
    "cubs": "MLB:Chicago", "astros": "MLB:Houston",
    # Orgs
    "fed": "ORG:FederalReserve", "federal reserve": "ORG:FederalReserve",
    "supreme court": "ORG:SCOTUS", "scotus": "ORG:SCOTUS",
    "spacex": "ORG:SpaceX", "tesla": "ORG:Tesla", "apple": "ORG:Apple",
    "google": "ORG:Google", "meta": "ORG:Meta", "microsoft": "ORG:Microsoft",
    "openai": "ORG:OpenAI", "anthropic": "ORG:Anthropic",
    "nato": "ORG:NATO", "un": "ORG:UnitedNations", "sec": "ORG:SEC",
    # People
    "biden": "PERSON:Biden", "trump": "PERSON:Trump", "harris": "PERSON:Harris",
    "powell": "PERSON:Powell", "musk": "PERSON:Musk", "zelenskyy": "PERSON:Zelenskyy",
    "putin": "PERSON:Putin", "mahomes": "PERSON:Mahomes",
    "swift": "PERSON:TaylorSwift", "taylor swift": "PERSON:TaylorSwift",
    # Crypto
    "bitcoin": "CRYPTO:Bitcoin", "btc": "CRYPTO:Bitcoin",
    "ethereum": "CRYPTO:Ethereum", "eth": "CRYPTO:Ethereum",
}


def extract_keywords(text: str) -> list[str]:
    """
    Extract meaningful keywords from text:
    1. Match against known entity dictionary (multi-word phrases first)
    2. Extract capitalized words not at sentence start (likely proper nouns)
    3. Filter stopwords and short tokens
    """
    if not text:
        return []

    lower = text.lower()
    found: list[str] = []
    matched_spans: list[tuple[int, int]] = []

    # Phase 1: known entity matching (longest match first)
    sorted_entities = sorted(_KNOWN_ENTITIES.keys(), key=len, reverse=True)
    for entity in sorted_entities:
        idx = lower.find(entity)
        while idx != -1:
            end = idx + len(entity)
            # Check we're not inside an already-matched span
            overlap = any(s <= idx < e or s < end <= e for s, e in matched_spans)
            if not overlap:
                matched_spans.append((idx, end))
                found.append(_KNOWN_ENTITIES[entity])
            idx = lower.find(entity, idx + 1)

    # Phase 2: proper noun detection — capitalized words not at sentence start
    # Split into sentences, look for interior capital words
    sentences = re.split(r'[.!?]', text)
    for sentence in sentences:
        words = sentence.split()
        for i, word in enumerate(words):
            if i == 0:
                continue  # skip sentence-start capitalization
            clean = word.strip(string.punctuation)
            if (
                clean
                and len(clean) >= 3
                and clean[0].isupper()
                and clean.lower() not in _STOPWORDS
                and not any(c.isdigit() for c in clean)
            ):
                found.append(clean)

    # Phase 3: financial/numeric signals
    # Prices, percentages, rates that might be market-relevant
    numbers = re.findall(r'\$[\d,.]+[BMK]?|\d+(?:\.\d+)?%|\d+,\d+', text)
    found.extend(numbers)

    # Deduplicate preserving order
    seen: set[str] = set()
    result: list[str] = []
    for kw in found:
        if kw not in seen:
            seen.add(kw)
            result.append(kw)

    return result


# ---------------------------------------------------------------------------
# Event classification
# ---------------------------------------------------------------------------

_EVENT_TYPE_RULES: list[tuple[list[str], str]] = [
    (["nba", "nfl", "mlb", "nhl", "nascar", "score", "win", "defeat", "beat", "game",
      "championship", "tournament", "super bowl", "world series", "finals"], "sports"),
    (["fed", "federal reserve", "rate", "inflation", "cpi", "gdp", "jobs", "unemployment",
      "recession", "economy", "economic", "stock", "nasdaq", "s&p", "dow"], "economics"),
    (["election", "vote", "senate", "congress", "president", "white house", "democrat",
      "republican", "supreme court", "scotus", "legislation", "bill", "policy"], "politics"),
    (["bitcoin", "ethereum", "crypto", "blockchain", "etf", "defi", "nft"], "crypto"),
    (["spacex", "nasa", "launch", "rocket", "mars", "moon", "starship", "satellite"], "science"),
    (["ukraine", "russia", "israel", "gaza", "nato", "war", "conflict", "ceasefire",
      "sanctions", "nuclear"], "geopolitics"),
    (["apple", "google", "microsoft", "meta", "openai", "anthropic", "ai", "tech",
      "iphone", "software", "model", "chip"], "technology"),
    (["oscar", "grammy", "emmy", "box office", "album", "tour", "movie", "film",
      "concert", "taylor swift", "beyonce"], "entertainment"),
]


def classify_event_type(text: str) -> str:
    lower = text.lower()
    for keywords, event_type in _EVENT_TYPE_RULES:
        if any(kw in lower for kw in keywords):
            return event_type
    return "general"


def is_likely_resolved(article: dict) -> bool:
    """
    Heuristic: articles about completed sporting events, election results,
    confirmed economic data, or definitive outcomes are 'resolved'.
    """
    text = (article.get("title", "") + " " + (article.get("description") or "")).lower()
    resolved_signals = [
        "wins", "defeats", "beats", "finalizes", "signs", "passed", "approved",
        "confirmed", "reaches", "surpasses", "exceeds", "falls", "drops",
        "announces", "releases", "launches", "completes", "ends", "closes",
        "score", "final", "result", "outcome",
    ]
    return any(sig in text for sig in resolved_signals)


# ---------------------------------------------------------------------------
# Redis publisher
# ---------------------------------------------------------------------------

async def publish(redis: aioredis.Redis, event: dict) -> None:
    await redis.publish(REDIS_CHANNEL, json.dumps(event))


# ---------------------------------------------------------------------------
# News poller
# ---------------------------------------------------------------------------

def _newsapi_track_request() -> None:
    """Increment daily NewsAPI request counter, warn if approaching limit."""
    global _newsapi_daily_count, _newsapi_count_date
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if today != _newsapi_count_date:
        _newsapi_daily_count = 0
        _newsapi_count_date = today
    _newsapi_daily_count += 1
    used_pct = _newsapi_daily_count / NEWSAPI_FREE_TIER_LIMIT
    if used_pct >= NEWSAPI_WARN_THRESHOLD:
        logger.warning(
            "NewsAPI daily usage: %d/%d (%.0f%%) — approaching free tier limit",
            _newsapi_daily_count, NEWSAPI_FREE_TIER_LIMIT, used_pct * 100,
        )
    elif _newsapi_daily_count % 20 == 0:
        logger.info("NewsAPI daily usage: %d/%d", _newsapi_daily_count, NEWSAPI_FREE_TIER_LIMIT)


async def _fetch_news(session: aiohttp.ClientSession) -> list[dict]:
    if not NEWSAPI_KEY:
        return _MOCK_HEADLINES

    _newsapi_track_request()
    params = dict(NEWSAPI_PARAMS, apiKey=NEWSAPI_KEY)
    async with session.get(NEWSAPI_URL, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        resp.raise_for_status()
        data = await resp.json()
    return data.get("articles", [])


def _article_to_event(article: dict) -> dict:
    title = article.get("title") or ""
    description = article.get("description") or ""
    full_text = f"{title}. {description}"
    resolved = is_likely_resolved(article)

    return {
        "event_type": classify_event_type(full_text),
        "headline": title,
        "keywords": extract_keywords(full_text),
        "source": article.get("source", {}).get("name") or "unknown",
        "url": article.get("url"),
        "timestamp": article.get("publishedAt") or datetime.now(timezone.utc).isoformat(),
        "resolved": resolved,
        "resolution_signal": "yes" if resolved else "unknown",
        "event_id": hashlib.md5((title + (article.get("publishedAt") or "")).encode()).hexdigest()[:12],
        "details": {
            "description": description,
        },
        "feed": "newsapi",
    }


async def news_poller(redis: aioredis.Redis) -> None:
    logger.info("Starting NewsAPI poller (interval=%ds, key_set=%s)", NEWSAPI_POLL_INTERVAL, bool(NEWSAPI_KEY))
    if not NEWSAPI_KEY:
        logger.warning("NEWSAPI_KEY not set — using mock headlines")

    consecutive_429s = 0
    poll_interval = NEWSAPI_POLL_INTERVAL

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                articles = await _fetch_news(session)
                count = 0
                for article in articles:
                    if not article.get("title"):
                        continue
                    event = _article_to_event(article)
                    await publish(redis, event)
                    count += 1
                logger.info("NewsAPI poll: %d articles published", count)
                # Reset backoff on success
                consecutive_429s = 0
                poll_interval = NEWSAPI_POLL_INTERVAL

            except aiohttp.ClientResponseError as exc:
                if exc.status == 401:
                    logger.error("NewsAPI auth error — check NEWSAPI_KEY")
                elif exc.status == 429:
                    consecutive_429s += 1
                    if consecutive_429s <= 3:
                        backoff = 2 ** consecutive_429s  # 2s, 4s, 8s
                        logger.warning(
                            "NewsAPI 429 rate limited (attempt %d/3) — backing off %ds",
                            consecutive_429s, backoff,
                        )
                        await asyncio.sleep(backoff)
                        continue  # retry immediately after backoff, skip the normal sleep
                    else:
                        # Repeated 429s — double the poll interval to reduce pressure
                        poll_interval = min(poll_interval * 2, 300)  # cap at 5 min
                        logger.warning(
                            "NewsAPI repeated 429s — increasing poll interval to %ds",
                            poll_interval,
                        )
                else:
                    logger.warning("NewsAPI HTTP %d: %s", exc.status, exc.message)
            except aiohttp.ClientError as exc:
                logger.warning("NewsAPI request error: %s", exc)
            except Exception as exc:
                logger.error("Unexpected NewsAPI error: %s", exc, exc_info=True)

            await asyncio.sleep(poll_interval)


# ---------------------------------------------------------------------------
# ESPN NBA poller
# ---------------------------------------------------------------------------

def _game_to_event(game: dict) -> Optional[dict]:
    """Convert an ESPN scoreboard game object to an event."""
    try:
        status_type = game.get("status", {}).get("type", {})
        state = status_type.get("state", "")  # "pre", "in", "post"
        completed = status_type.get("completed", False)
        status_desc = status_type.get("description", "")

        competitions = game.get("competitions", [])
        if not competitions:
            return None
        competition = competitions[0]

        competitors = competition.get("competitors", [])
        if len(competitors) < 2:
            return None

        home = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
        away = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])

        home_name = home.get("team", {}).get("displayName", "Home")
        away_name = away.get("team", {}).get("displayName", "Away")
        home_score = home.get("score", "0")
        away_score = away.get("score", "0")

        headline = f"{away_name} vs {home_name}: {away_score}-{home_score} ({status_desc})"

        # Determine winner if game is final
        winner = None
        if completed:
            try:
                if int(home_score) > int(away_score):
                    winner = home_name
                elif int(away_score) > int(home_score):
                    winner = away_name
            except (ValueError, TypeError):
                pass

        keywords = extract_keywords(f"{home_name} {away_name}")
        keywords.extend([home_name, away_name])
        keywords = list(dict.fromkeys(keywords))  # deduplicate

        return {
            "event_type": "sports",
            "headline": headline,
            "keywords": keywords,
            "source": "ESPN",
            "url": "https://www.espn.com/nba/",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "resolved": completed,
            "resolution_signal": "yes" if completed else "unknown",
            "details": {
                "sport": "NBA",
                "home_team": home_name,
                "away_team": away_name,
                "home_score": home_score,
                "away_score": away_score,
                "game_state": state,
                "status": status_desc,
                "winner": winner,
                "game_id": game.get("id"),
            },
            "feed": "espn_nba",
        }

    except Exception as exc:
        logger.debug("Error parsing ESPN game: %s", exc)
        return None


async def espn_sports_poller(redis: aioredis.Redis) -> None:
    """Poll all ESPN sports scoreboards for completed games."""
    logger.info("Starting ESPN multi-sport poller (%s, interval=%ds)",
                ", ".join(ESPN_SPORTS.keys()), ESPN_POLL_INTERVAL)
    async with aiohttp.ClientSession() as session:
        while True:
            total_count = 0
            for sport, url in ESPN_SPORTS.items():
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                        resp.raise_for_status()
                        data = await resp.json()

                    events = data.get("events", [])
                    count = 0
                    for game in events:
                        event = _game_to_event(game)
                        if event:
                            # Override sport label
                            event["details"]["sport"] = sport
                            await publish(redis, event)
                            count += 1

                    if count > 0:
                        logger.info("ESPN %s: %d game events published", sport, count)
                    total_count += count

                except aiohttp.ClientError as exc:
                    logger.warning("ESPN %s request error: %s", sport, exc)
                except Exception as exc:
                    logger.error("ESPN %s unexpected error: %s", sport, exc, exc_info=True)

            if total_count > 0:
                logger.info("ESPN total: %d events across %d sports", total_count, len(ESPN_SPORTS))

            await asyncio.sleep(ESPN_POLL_INTERVAL)


def _coingecko_rate_limit_ok() -> bool:
    """Return True if we are under the hourly call cap, False if capped.

    Resets the counter automatically when an hour has elapsed.
    """
    global _coingecko_hourly_calls, _coingecko_hour_window_start
    now = time.monotonic()
    if now - _coingecko_hour_window_start >= 3600:
        _coingecko_hourly_calls = 0
        _coingecko_hour_window_start = now
    return _coingecko_hourly_calls < COINGECKO_MAX_CALLS_PER_HOUR


def _coingecko_all_coins_fresh() -> bool:
    """Return True if every tracked coin has a cached price younger than COINGECKO_COIN_TTL."""
    now = time.monotonic()
    for coin in COINGECKO_COINS.split(","):
        entry = _price_cache.get(coin.strip())
        if entry is None or (now - entry["fetched_at"]) >= COINGECKO_COIN_TTL:
            return False
    return True


async def _fetch_coingecko(session: aiohttp.ClientSession) -> dict | None:
    """Fetch CoinGecko prices with rate limiting, per-coin TTL, backoff, and caching.

    - Returns cached data if all coins are within COINGECKO_COIN_TTL (60s).
    - Logs WARNING and returns cache if hourly call cap (50/hr) is exceeded.
    - Exponential backoff on 429: 2s, 4s, 8s (max).
    """
    global _coingecko_last_request, _coingecko_cache, _coingecko_hourly_calls

    # Per-coin TTL check — if all coins are fresh, skip the network call
    if _coingecko_all_coins_fresh():
        logger.debug("CoinGecko: all coins within TTL — serving cache")
        return _coingecko_cache

    # Hourly rate limit cap
    if not _coingecko_rate_limit_ok():
        logger.warning(
            "CoinGecko: hourly call cap (%d/hr) reached — returning cached data",
            COINGECKO_MAX_CALLS_PER_HOUR,
        )
        return _coingecko_cache  # may be None if never fetched successfully

    # Enforce minimum gap between requests (free tier: 30 calls/min)
    now = time.monotonic()
    elapsed = now - _coingecko_last_request
    if elapsed < COINGECKO_MIN_REQUEST_GAP:
        wait = COINGECKO_MIN_REQUEST_GAP - elapsed
        await asyncio.sleep(wait)

    params = {
        "ids": COINGECKO_COINS,
        "vs_currencies": "usd",
        "include_24hr_change": "true",
    }

    for attempt in range(4):  # attempts give backoff: 2s, 4s, 8s, 16s max
        _coingecko_last_request = time.monotonic()
        _coingecko_hourly_calls += 1
        try:
            async with session.get(COINGECKO_URL, params=params,
                                   timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 429:
                    backoff = min(2 ** (attempt + 1), 16)  # 2s, 4s, 8s, 16s
                    logger.warning(
                        "CoinGecko 429 rate limited (attempt %d/4) — backing off %ds",
                        attempt + 1, backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue
                resp.raise_for_status()
                data = await resp.json()
            _coingecko_cache = data
            # Update per-coin cache with fresh timestamp
            fetched_at = time.monotonic()
            for coin, prices in data.items():
                _price_cache[coin] = {"price": prices.get("usd"), "fetched_at": fetched_at}
            logger.debug("CoinGecko: fresh data fetched")
            return data
        except aiohttp.ClientResponseError as exc:
            if exc.status == 429:
                backoff = min(2 ** (attempt + 1), 16)
                logger.warning(
                    "CoinGecko 429 rate limited (attempt %d/4) — backing off %ds",
                    attempt + 1, backoff,
                )
                await asyncio.sleep(backoff)
                continue
            # Non-429 HTTP errors: log and stop retrying
            logger.warning("CoinGecko HTTP %d: %s", exc.status, exc.message)
            break
        except aiohttp.ClientError as exc:
            logger.warning("CoinGecko request error (attempt %d/4): %s", attempt + 1, exc)
            await asyncio.sleep(min(2 ** (attempt + 1), 16))

    # All retries exhausted or non-retryable error — serve cache if available
    if _coingecko_cache is not None:
        logger.warning("CoinGecko: serving cached data (last fresh fetch succeeded earlier)")
        return _coingecko_cache
    return None


async def crypto_price_poller(redis: aioredis.Redis) -> None:
    """Poll CoinGecko for crypto prices, emit events when price crosses key levels."""
    logger.info("Starting crypto price poller (coins=%s, interval=%ds)",
                COINGECKO_COINS, CRYPTO_POLL_INTERVAL)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                data = await _fetch_coingecko(session)
                if data is None:
                    logger.warning("CoinGecko: no data available (no cache, all retries failed)")
                    await asyncio.sleep(CRYPTO_POLL_INTERVAL)
                    continue

                for coin, prices in data.items():
                    current_price = prices.get("usd")
                    if current_price is None:
                        continue

                    prev_price = _CRYPTO_LAST_PRICES.get(coin)
                    _CRYPTO_LAST_PRICES[coin] = current_price

                    if prev_price is None:
                        continue  # first poll, no crossing to detect

                    # Check for level crossings
                    levels = _CRYPTO_PRICE_LEVELS.get(coin, [])
                    for level in levels:
                        crossed_up = prev_price < level <= current_price
                        crossed_down = prev_price > level >= current_price
                        if crossed_up or crossed_down:
                            direction = "above" if crossed_up else "below"
                            ticker = coin.upper()
                            if coin == "bitcoin":
                                ticker = "BTC"
                            elif coin == "ethereum":
                                ticker = "ETH"
                            elif coin == "solana":
                                ticker = "SOL"
                            elif coin == "dogecoin":
                                ticker = "DOGE"
                            elif coin == "ripple":
                                ticker = "XRP"

                            headline = (
                                f"{ticker} crosses {direction} ${level:,} — "
                                f"now ${current_price:,.2f}"
                            )
                            event = {
                                "event_type": "crypto",
                                "headline": headline,
                                "keywords": [f"CRYPTO:{ticker}", ticker, coin,
                                              str(level), direction],
                                "source": "CoinGecko",
                                "url": f"https://www.coingecko.com/en/coins/{coin}",
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "resolved": True,
                                "resolution_signal": "yes",
                                "event_id": hashlib.md5(
                                    f"{coin}:{level}:{direction}:{datetime.now(timezone.utc).isoformat()[:13]}".encode()
                                ).hexdigest()[:12],
                                "details": {
                                    "coin": coin,
                                    "ticker": ticker,
                                    "level": level,
                                    "direction": direction,
                                    "current_price": current_price,
                                    "prev_price": prev_price,
                                },
                                "feed": "coingecko",
                            }
                            await publish(redis, event)
                            logger.info(
                                "Crypto crossing: %s %s $%s (now $%.2f)",
                                ticker, direction, f"{level:,}", current_price,
                            )

            except aiohttp.ClientError as exc:
                logger.warning("CoinGecko request error: %s", exc)
            except Exception as exc:
                logger.error("CoinGecko unexpected error: %s", exc, exc_info=True)

            await asyncio.sleep(CRYPTO_POLL_INTERVAL)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def run() -> None:
    """Start real-world event monitors: NewsAPI + ESPN (multi-sport) + Crypto prices."""
    redis = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    logger.info("Events feed starting (NewsAPI + ESPN %s + Crypto)", ", ".join(ESPN_SPORTS.keys()))

    try:
        await asyncio.gather(
            news_poller(redis),
            espn_sports_poller(redis),
            crypto_price_poller(redis),
        )
    finally:
        await redis.aclose()
        logger.info("Events feed stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    asyncio.run(run())
