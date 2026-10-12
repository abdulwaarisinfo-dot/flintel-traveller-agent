"""
hackernews/database.py — Hacker News poller ka state (MongoDB).

Ye module index.py ke `db` object se baat karta hai, jo bahar se `set_db(db)` ke
zariye pass hota hai. Is service mein sirf EK MongoDB hai, isliye yahan naya
MongoClient KABHI nahi banta. index.py se kuch import bhi nahi kiya jata
(circular import se bachne ke liye).

Is module ka kaam sirf do naye collections ka hai:
  - config.HN_STATE_COLLECTION  ("flintel_hn_state")  : har (keyword, kind) ka last seen timestamp
  - config.HN_CURSOR_COLLECTION ("flintel_hn_cursor")  : keyword rotation ka cursor

`flintel_signals` mein save karna iska kaam NAHI hai (wo index.py ka `_save_signal` karta hai).
Existing collections (flintel_signals, flintel_service_status) par koi index ya write nahi.

Koi bhi function raise nahi karta. `_db` None ho ya koi error aaye to None / 0 wapas
hota hai aur log.warning ho jati hai; fetch phir bhi LOOKBACK_DAYS wale cutoff se chalta hai.
"""

import logging

from . import config

log = logging.getLogger("flintel-fetch")

# index.py ka `db` object; set_db() se set hota hai.
_db = None

# Unique index (keyword, kind) sirf ek dafa ensure karne ke liye flag.
_state_index_ready = False


def set_db(db):
    """index.py ka `db` object yahan register karta hai (naya MongoClient nahi banta)."""
    global _db
    _db = db


def get_db():
    """Register kiya hua `db` wapas deta hai (set na hua ho to None)."""
    return _db


def _ensure_state_index():
    """
    flintel_hn_state par unique index (keyword, kind) lazily, sirf ek dafa banata hai.
    Fail ho to warning log hoti hai aur flag set nahi hota, taake agli call dobara try kare.
    """
    global _state_index_ready
    if _state_index_ready or _db is None:
        return
    try:
        _db[config.HN_STATE_COLLECTION].create_index(
            [("keyword", 1), ("kind", 1)], unique=True, name="uniq_keyword_kind"
        )
        _state_index_ready = True
    except Exception as e:
        log.warning(f"[HACKERNEWS] Could not ensure state index: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Incremental state (flintel_hn_state)
# ──────────────────────────────────────────────────────────────────────────────

def get_last_seen_ts(keyword, kind):
    """
    Is (keyword, kind) ka last seen `created_at_i` (unix seconds) deta hai.
    kind: "story" ya "comment". Doc na ho, _db None ho ya error aaye to None
    (is surat mein caller LOOKBACK_DAYS wala cutoff istemal karta hai).
    """
    if _db is None:
        return None
    try:
        _ensure_state_index()
        doc = _db[config.HN_STATE_COLLECTION].find_one(
            {"keyword": keyword, "kind": kind}, {"last_created_at_i": 1}
        )
        if doc and doc.get("last_created_at_i") is not None:
            return int(doc["last_created_at_i"])
        return None
    except Exception as e:
        log.warning(f"[HACKERNEWS] get_last_seen_ts failed for '{keyword}'/{kind}: {e}")
        return None


def set_last_seen_ts(keyword, kind, ts):
    """
    Is (keyword, kind) ka last seen timestamp upsert karta hai. `$max` istemal hota hai,
    yaani sirf bada timestamp save hota hai (purana ts kabhi aage wale ko overwrite nahi karta).
    Kuch nahi wapas karta; error par sirf log.warning.
    """
    if _db is None:
        return
    try:
        _ensure_state_index()
        _db[config.HN_STATE_COLLECTION].update_one(
            {"keyword": keyword, "kind": kind},
            {"$max": {"last_created_at_i": int(ts)}},
            upsert=True,
        )
    except Exception as e:
        log.warning(f"[HACKERNEWS] set_last_seen_ts failed for '{keyword}'/{kind}: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Rotation cursor (flintel_hn_cursor)
# ──────────────────────────────────────────────────────────────────────────────

def get_cursor():
    """
    Keyword rotation ka `next_index` deta hai (single doc, _id="main").
    Doc na ho, _db None ho ya error aaye to 0.
    """
    if _db is None:
        return 0
    try:
        doc = _db[config.HN_CURSOR_COLLECTION].find_one({"_id": "main"}, {"next_index": 1})
        if doc and doc.get("next_index") is not None:
            return int(doc["next_index"])
        return 0
    except Exception as e:
        log.warning(f"[HACKERNEWS] get_cursor failed: {e}")
        return 0


def set_cursor(n):
    """Rotation cursor (`next_index`) upsert + $set karta hai. Error par sirf log.warning."""
    if _db is None:
        return
    try:
        _db[config.HN_CURSOR_COLLECTION].update_one(
            {"_id": "main"}, {"$set": {"next_index": int(n)}}, upsert=True
        )
    except Exception as e:
        log.warning(f"[HACKERNEWS] set_cursor failed: {e}")
