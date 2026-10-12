"""
hackernews/config.py — Hacker News poller ki saari settings.

Is file mein SIRF settings hain: koi network call nahi, koi DB call nahi.
index.py se kuch import NAHI kiya jata (circular import se bachne ke liye).

Do qisam ki cheezein hain:
  1. Live-checked switches (is_*_enabled functions): har call par .env dobara
     padhi jati hai (load_dotenv(override=True)), taake service restart kiye
     baghair switch badla ja sake.
  2. Constants: import ke waqt ek dafa environment se padhe jate hain, defaults
     ke saath.
"""

import os

from dotenv import load_dotenv


# ──────────────────────────────────────────────────────────────────────────────
# Helper
# ──────────────────────────────────────────────────────────────────────────────

def _env_bool(name, default):
    """
    Environment variable ko bool mein padhta hai (case-insensitive).
    Sach maane jane wali values: true / 1 / yes / on
    Jhoot maane jane wali values: false / 0 / no / off
    Variable missing ho ya value pehchani na jaye to `default` wapas hota hai.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in ("true", "1", "yes", "on"):
        return True
    if value in ("false", "0", "no", "off"):
        return False
    return default


# ──────────────────────────────────────────────────────────────────────────────
# Live-checked switches (har call par .env dobara load hoti hai)
# ──────────────────────────────────────────────────────────────────────────────

def is_hackernews_enabled():
    """Poora Hacker News poller on/off (HACKERNEWS_ENABLED, default True)."""
    load_dotenv(override=True)
    return _env_bool("HACKERNEWS_ENABLED", True)


def is_hn_stories_enabled():
    """Stories fetch karni hain ya nahi (HN_FETCH_STORIES, default True)."""
    load_dotenv(override=True)
    return _env_bool("HN_FETCH_STORIES", True)


def is_hn_comments_enabled():
    """Comments fetch karne hain ya nahi (HN_FETCH_COMMENTS, default True)."""
    load_dotenv(override=True)
    return _env_bool("HN_FETCH_COMMENTS", True)


def is_hn_strict_match_enabled():
    """Strict keyword match on/off (HN_STRICT_MATCH, default True)."""
    load_dotenv(override=True)
    return _env_bool("HN_STRICT_MATCH", True)


# ──────────────────────────────────────────────────────────────────────────────
# Constants (import ke waqt ek dafa padhe jate hain)
# ──────────────────────────────────────────────────────────────────────────────

# Algolia Hacker News search endpoint (newest-first, free, API key nahi chahiye).
HN_API_URL = "https://hn.algolia.com/api/v1/search_by_date"

# Ek request mein kitne results (Algolia ka hitsPerPage), clamp 1..1000.
HN_RESULTS_PER_QUERY = max(1, min(1000, int(os.getenv("HN_RESULTS_PER_QUERY", "50"))))

# Ek keyword ke liye max kitne pages. Keywords zyada ho sakte hain, isliye chhota default.
HN_MAX_PAGES = max(1, int(os.getenv("HN_MAX_PAGES", "2")))

# HTTP request ka timeout (seconds).
HN_REQUEST_TIMEOUT = int(os.getenv("HN_REQUEST_TIMEOUT", "15"))

# Do keywords ki requests ke beech wait (seconds), API par load kam rakhne ke liye.
HN_KEYWORD_GAP_SECONDS = float(os.getenv("HN_KEYWORD_GAP_SECONDS", "1.0"))

# Ek cycle mein kitne keywords process hon (rotation), clamp >= 1.
HN_KEYWORDS_PER_CYCLE = max(1, int(os.getenv("HN_KEYWORDS_PER_CYCLE", "100")))

# Do cycles ke beech wait (seconds).
HN_POLL_INTERVAL_SECONDS = int(os.getenv("HN_POLL_INTERVAL_SECONDS", "600"))

# HTTP 429 (rate limit) aane par kitni der ruka jaye (seconds).
HN_RATE_LIMIT_BACKOFF_SECONDS = int(os.getenv("HN_RATE_LIMIT_BACKOFF_SECONDS", "300"))

# Sirf pehli dafa (jab koi cursor na ho) kitne din peeche tak ka data uthana hai.
HN_LOOKBACK_DAYS = int(os.getenv("HN_LOOKBACK_DAYS", os.getenv("LOOKBACK_DAYS", "180")))

# True ho to multi-word keyword exact phrase (quotes mein) bheja jata hai.
HN_EXACT_PHRASE = _env_bool("HN_EXACT_PHRASE", False)

# Algolia ko bheja jane wala User-Agent.
HN_USER_AGENT = os.getenv(
    "HN_USER_AGENT", "flintel-fetch-service/1.0 (contact: ops@example.com)"
)

# `flintel_signals` mein platform ke naam (message_id = f"{platform}_{post['id']}").
HN_PLATFORM_STORY = "hackernews"
HN_PLATFORM_COMMENT = "hackernews_comment"

# _set_service_status ke liye service ka naam.
HN_SERVICE_NAME = "hackernews_poller"

# HN ka state usi MongoDB (index.py ke `db`) ki in collections mein rahega.
HN_STATE_COLLECTION = "flintel_hn_state"
HN_CURSOR_COLLECTION = "flintel_hn_cursor"
