"""
hackernews/fetcher.py — Hacker News (Algolia) search, post-dict conversion, strict match,
pagination, keyword rotation aur always-on loop.

Ye module index.py se kuch import NAHI karta (circular import se bachne ke liye). Jo cheezein
index.py ki chahiye (_save_signal, _get_keywords, _set_service_status, sinks check, db) wo
function arguments ke tor par pass hoti hain:

    save_fn(topic_key, matched_keyword, platform, post)   # index.py ka _save_signal
    keywords_fn()                                          # index.py ka _get_keywords
    status_fn(service_name, running)                       # index.py ka _set_service_status
    sinks_ok_fn()                                          # MONGODB_DATA / MYSQL_DATA me se koi ek on?
    db                                                     # index.py ka MongoDB `db` object

Koi function thread ko crash karne wali exception nahi uthata. Sirf HNRateLimited (HTTP 429)
fetcher se cycle tak upar jata hai, jise run_hackernews_loop pakar ke backoff karta hai.
"""

import html
import logging
import re
import time
from datetime import datetime, timedelta, timezone

import requests

from . import config, database

log = logging.getLogger("flintel-fetch")

# Strict match / significant words mein nazar-andaz kiye jane wale aam alfaaz.
_STOPWORDS = {
    "a", "an", "the", "to", "for", "of", "in", "on", "my", "our", "is", "are", "and", "or",
    "with", "from", "at", "by", "your", "their",
}


class HNRateLimited(Exception):
    """Algolia ne HTTP 429 diya. Sirf ek signal hai; cycle / loop isay pakar ke backoff karta hai."""


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _strip_html(s):
    """
    HN comment / story text mein HTML tags hote hain (<p>, <a>, <i> ...). Entities unescape
    karke tags ko space se badalta hai, whitespace collapse karta hai aur strip karta hai.
    """
    text = re.sub(r"<[^>]+>", " ", html.unescape(s or ""))
    return re.sub(r"\s+", " ", text).strip()


def _significant_words(keyword):
    """
    Keyword ke ahem alfaaz: lowercase, sirf [a-z0-9'] tokens, stopwords aur len<=2 wale hata kar.
    """
    words = re.findall(r"[a-z0-9']+", (keyword or "").lower())
    return [w for w in words if w not in _STOPWORDS and len(w) > 2]


def keyword_in_post(keyword, post):
    """
    Strict match. text = title + selftext (lowercase). True agar:
      - poora keyword text mein substring ho, YA
      - keyword ke SAARE significant words text mein (alag-alag) maujood hon.
    Significant words khali hon to sirf poore keyword wala check hota hai.
    """
    text = f"{post.get('title', '')} {post.get('selftext', '')}".lower()
    kw = (keyword or "").lower()
    if kw and kw in text:
        return True
    words = _significant_words(keyword)
    if not words:
        return False
    return all(w in text for w in words)


# ──────────────────────────────────────────────────────────────────────────────
# Fetch (ek page)
# ──────────────────────────────────────────────────────────────────────────────

def _hit_to_post(hit, kind):
    """
    Algolia ke ek hit ko `_save_signal` ke post-dict shape mein badalta hai.
    Hit qabil-e-istemal na ho (objectID khali, ya title + selftext dono khali) to None.
    """
    object_id = str(hit.get("objectID") or "").strip()
    if not object_id:
        return None

    if kind == "story":
        title = hit.get("title") or ""
        selftext = _strip_html(hit.get("story_text"))
    else:
        title = _strip_html(hit.get("comment_text"))
        selftext = ""

    if not title.strip() and not selftext.strip():
        return None

    created_at_i = hit.get("created_at_i")
    try:
        created_at_i = int(created_at_i) if created_at_i is not None else None
    except (TypeError, ValueError):
        created_at_i = None

    return {
        "id": object_id,
        "title": title,
        "selftext": selftext,
        "author": hit.get("author") or "unknown",
        "subreddit": "",
        "post_url": f"https://news.ycombinator.com/item?id={object_id}",
        "created_utc": float(created_at_i) if created_at_i is not None else None,
        "score": hit.get("points") or 0,
        "num_comments": hit.get("num_comments") or 0,
        "_created_at_i": created_at_i,  # sirf checkpoint ke liye
    }


def fetch_hackernews_page(keyword, kind, since_ts, page):
    """
    Algolia search_by_date se ek page laata hai. Returns (posts, nbPages).
    HTTP 429 par HNRateLimited uthata hai; baqi har error par log.warning karke ([], 0).
    """
    query = keyword
    if config.HN_EXACT_PHRASE and " " in keyword:
        query = f'"{keyword}"'

    params = {
        "query": query,
        "tags": kind,
        "hitsPerPage": config.HN_RESULTS_PER_QUERY,
        "page": page,
    }
    if since_ts is not None:
        params["numericFilters"] = f"created_at_i>{since_ts}"

    try:
        resp = requests.get(
            config.HN_API_URL,
            params=params,
            headers={"User-Agent": config.HN_USER_AGENT},
            timeout=config.HN_REQUEST_TIMEOUT,
        )
    except Exception as exc:
        log.warning(f"[HACKERNEWS] search failed | keyword='{keyword}' | kind={kind} | {exc}")
        return [], 0

    # 429 ko try/except se bahar rakha hai taake wo "baqi errors" mein na phans jaye.
    if resp.status_code == 429:
        raise HNRateLimited()

    try:
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits") or []
        nb_pages = int(data.get("nbPages") or 0)
    except Exception as exc:
        log.warning(f"[HACKERNEWS] search failed | keyword='{keyword}' | kind={kind} | {exc}")
        return [], 0

    posts = []
    for hit in hits:
        try:
            post = _hit_to_post(hit, kind)
        except Exception as exc:
            log.warning(
                f"[HACKERNEWS] bad hit skipped | keyword='{keyword}' | kind={kind} | {exc}"
            )
            continue
        if post is not None:
            posts.append(post)
    return posts, nb_pages


# ──────────────────────────────────────────────────────────────────────────────
# Ek keyword + ek kind
# ──────────────────────────────────────────────────────────────────────────────

def process_keyword_kind(keyword, kind, save_fn, default_since_ts):
    """
    Ek keyword ke ek kind ("story" / "comment") ke pages fetch karta hai, strict match lagata
    hai aur matched posts save_fn se save karta hai. Naye save hue messages ki ginti deta hai.

    HNRateLimited ko yahan NAHI pakra jata (cycle khud sambhalta hai). Baqi har exception
    log.warning ke saath pakri jati hai aur ab tak ki ginti wapas hoti hai.
    """
    count = 0
    try:
        since_ts = database.get_last_seen_ts(keyword, kind)
        if since_ts is None:
            since_ts = default_since_ts

        platform = config.HN_PLATFORM_STORY if kind == "story" else config.HN_PLATFORM_COMMENT
        topic_key = keyword.strip().lower()

        skipped = 0
        max_ts = None
        fetched_any = False

        for page in range(config.HN_MAX_PAGES):
            posts, nb_pages = fetch_hackernews_page(keyword, kind, since_ts, page)
            if not posts:
                break

            fetched_any = True
            for post in posts:
                ts = post.get("_created_at_i")
                if ts is not None and (max_ts is None or ts > max_ts):
                    max_ts = ts

                if config.is_hn_strict_match_enabled() and not keyword_in_post(keyword, post):
                    skipped += 1
                    continue

                if save_fn(topic_key, keyword, platform, post):
                    count += 1

            if page + 1 >= nb_pages:
                break
            time.sleep(0.3)

        # Checkpoint: saare fetched posts (matched + skipped) ka max timestamp.
        if fetched_any and max_ts is not None:
            database.set_last_seen_ts(keyword, kind, max_ts)

        if skipped > 0:
            log.info(
                f"[HACKERNEWS] strict match skipped {skipped} | keyword='{keyword}' | kind={kind}"
            )
        return count

    except HNRateLimited:
        raise
    except Exception as exc:
        log.warning(
            f"[HACKERNEWS] process failed | keyword='{keyword}' | kind={kind} | {exc}"
        )
        return count


# ──────────────────────────────────────────────────────────────────────────────
# Ek cycle (rotation)
# ──────────────────────────────────────────────────────────────────────────────

def run_hackernews_cycle(save_fn, keywords_fn):
    """
    Ek cycle: cursor se shuru karke HN_KEYWORDS_PER_CYCLE keywords (list ke end par wrap-around)
    process karta hai. Har keyword mukammal hone ke BAAD cursor aage badhta hai.
    HNRateLimited aane par cycle rok kar `raise` karta hai (cursor wahin rehta hai jahan tak
    kaam mukammal hua). Naye save hue messages ka total deta hai.
    """
    keywords = keywords_fn()
    if not keywords:
        log.info("[HACKERNEWS] CYCLE SKIP | koi keyword nahi")
        return 0

    n = len(keywords)
    cursor = database.get_cursor() % n
    batch_size = n if n <= config.HN_KEYWORDS_PER_CYCLE else config.HN_KEYWORDS_PER_CYCLE

    default_since_ts = int(
        (datetime.now(timezone.utc) - timedelta(days=config.HN_LOOKBACK_DAYS)).timestamp()
    )

    kinds = []
    if config.is_hn_stories_enabled():
        kinds.append("story")
    if config.is_hn_comments_enabled():
        kinds.append("comment")

    log.info(
        f"[HACKERNEWS] CYCLE START | total_keywords={n} | batch={batch_size} | cursor={cursor}"
    )

    total = 0
    next_cursor = cursor
    try:
        for offset in range(batch_size):
            idx = (cursor + offset) % n
            keyword = keywords[idx]

            for kind in kinds:
                total += process_keyword_kind(keyword, kind, save_fn, default_since_ts)

            # Keyword mukammal hone ke BAAD hi cursor aage.
            next_cursor = (idx + 1) % n
            database.set_cursor(next_cursor)
            time.sleep(config.HN_KEYWORD_GAP_SECONDS)
    except HNRateLimited:
        log.warning(
            f"[HACKERNEWS] CYCLE STOPPED (rate limited) | new_messages_saved={total} "
            f"| next_cursor={next_cursor}"
        )
        raise

    log.info(
        f"[HACKERNEWS] CYCLE DONE | batch={batch_size} | new_messages_saved={total} "
        f"| next_cursor={next_cursor}"
    )
    return total


# ──────────────────────────────────────────────────────────────────────────────
# Always-on loop
# ──────────────────────────────────────────────────────────────────────────────

def run_hackernews_loop(save_fn, keywords_fn, status_fn, sinks_ok_fn, db):
    """
    Hamesha chalne wala loop (thread target). Kabhi exit nahi hota; sirf tab jab loop sach mein
    khatam ho (process shutdown) to status_fn(..., False) chalta hai.
    """
    database.set_db(db)
    try:
        while True:
            try:
                status_fn(config.HN_SERVICE_NAME, True)  # har iteration heartbeat

                if not config.is_hackernews_enabled():
                    log.info("[HACKERNEWS] HACKERNEWS_ENABLED=false: HN cycle skip")
                    time.sleep(config.HN_POLL_INTERVAL_SECONDS)
                    continue

                if not sinks_ok_fn():
                    # Koi fetch nahi; cursor / checkpoint nahi badhta.
                    log.warning("MONGODB_DATA aur MYSQL_DATA dono false: HN cycle skip")
                    time.sleep(config.HN_POLL_INTERVAL_SECONDS)
                    continue

                run_hackernews_cycle(save_fn, keywords_fn)
                time.sleep(config.HN_POLL_INTERVAL_SECONDS)

            except HNRateLimited:
                log.warning(
                    f"[HACKERNEWS] rate limited (429) | backoff "
                    f"{config.HN_RATE_LIMIT_BACKOFF_SECONDS}s"
                )
                time.sleep(config.HN_RATE_LIMIT_BACKOFF_SECONDS)
            except Exception as exc:
                log.error(f"[HACKERNEWS] loop error: {exc}")
                time.sleep(10)
    finally:
        status_fn(config.HN_SERVICE_NAME, False)
