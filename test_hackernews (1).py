"""Hermetic tests for the Hacker News poller (hackernews/ package + index.py wiring).

No real network, no real MongoDB:
  * pymongo.MongoClient is mocked while importing index.py (it connects at import time)
  * requests.get is blocked by default and monkeypatched per test
  * time.sleep is replaced with a recorder (nothing ever really sleeps)
  * the Mongo `db` is a tiny in-memory fake with only what hackernews/database.py uses
"""
import os
import sys
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

import pytest
import requests

REPO_ROOT = str(Path(__file__).resolve().parents[1])
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

with mock.patch("pymongo.MongoClient", return_value=MagicMock()):
    import index

from hackernews import config, database, fetcher
from hackernews.fetcher import HNRateLimited


# ──────────────────────────────────────────────────────────────────────────────
# Fakes / helpers
# ──────────────────────────────────────────────────────────────────────────────

class FakeCollection:
    """Minimal in-memory collection: find_one, update_one ($max / $set, upsert), create_index."""

    def __init__(self):
        self.docs = []
        self.indexes = []

    @staticmethod
    def _matches(doc, flt):
        return all(doc.get(k) == v for k, v in flt.items())

    def create_index(self, keys, **kwargs):
        self.indexes.append((keys, kwargs))

    def find_one(self, flt, projection=None):
        for doc in self.docs:
            if self._matches(doc, flt):
                return dict(doc)
        return None

    def update_one(self, flt, update, upsert=False):
        target = next((d for d in self.docs if self._matches(d, flt)), None)
        if target is None:
            if not upsert:
                return
            target = dict(flt)
            self.docs.append(target)
        for field, value in update.get("$set", {}).items():
            target[field] = value
        for field, value in update.get("$max", {}).items():
            if target.get(field) is None or value > target[field]:
                target[field] = value


class FakeDB:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        return self.collections.setdefault(name, FakeCollection())


class BrokenDB:
    """Simulates MongoDB being down: every collection access raises."""

    def __getitem__(self, name):
        raise RuntimeError("db down")


class FakeResp:
    def __init__(self, status_code=200, payload=None, json_error=None):
        self.status_code = status_code
        self._payload = payload
        self._json_error = json_error

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        if self._json_error is not None:
            raise self._json_error
        return self._payload


class StopLoop(BaseException):
    """Raised from the fake sleep to break out of run_hackernews_loop's `while True`.
    BaseException so the loop's own `except Exception` can't swallow it."""


def make_post(pid, title="", selftext="", ts=1700000000, author="someone", url=None):
    return {
        "id": str(pid),
        "title": title,
        "selftext": selftext,
        "author": author,
        "subreddit": "",
        "post_url": url or f"https://news.ycombinator.com/item?id={pid}",
        "created_utc": float(ts),
        "score": 1,
        "num_comments": 0,
        "_created_at_i": ts,
    }


@pytest.fixture(autouse=True)
def env(monkeypatch):
    """Per-test isolation: fresh fake DB, recorded sleeps, blocked network."""
    fake_db = FakeDB()
    monkeypatch.setattr(database, "_db", None)
    monkeypatch.setattr(database, "_state_index_ready", False)
    database.set_db(fake_db)

    sleeps = []
    monkeypatch.setattr(fetcher.time, "sleep", lambda s: sleeps.append(s))

    def _no_network(*a, **k):
        raise AssertionError("real network call attempted in a hermetic test")

    monkeypatch.setattr(requests, "get", _no_network)

    # Deterministic switches (config functions otherwise re-read .env on every call).
    monkeypatch.setattr(config, "is_hackernews_enabled", lambda: True)
    monkeypatch.setattr(config, "is_hn_stories_enabled", lambda: True)
    monkeypatch.setattr(config, "is_hn_comments_enabled", lambda: True)
    monkeypatch.setattr(config, "is_hn_strict_match_enabled", lambda: True)
    monkeypatch.setattr(config, "HN_KEYWORD_GAP_SECONDS", 0.0)

    class Env:
        db = fake_db
        sleep_calls = sleeps

    return Env


# ──────────────────────────────────────────────────────────────────────────────
# 1-2: hit -> post shape
# ──────────────────────────────────────────────────────────────────────────────

def test_1_story_hit_becomes_save_signal_post_shape(monkeypatch):
    hit = {
        "objectID": "123",
        "title": "Show HN: my Shopify app",
        "story_text": "<p>Hello &amp; <a href=\"https://x.io\">welcome</a></p>",
        "author": "bob",
        "points": 42,
        "num_comments": 7,
        "created_at_i": 1700000000,
    }
    seen = {}

    def fake_get(url, params=None, headers=None, timeout=None):
        seen["params"] = params
        return FakeResp(200, {"hits": [hit], "nbPages": 1})

    monkeypatch.setattr(requests, "get", fake_get)
    posts, nb_pages = fetcher.fetch_hackernews_page("shopify", "story", 1699999999, 0)

    assert nb_pages == 1 and len(posts) == 1
    p = posts[0]
    assert p["id"] == "123"
    assert p["title"] == "Show HN: my Shopify app"
    assert p["selftext"] == "Hello & welcome"          # HTML stripped + unescaped
    assert p["post_url"] == "https://news.ycombinator.com/item?id=123"
    assert isinstance(p["created_utc"], float) and p["created_utc"] == 1700000000.0
    assert p["score"] == 42
    assert p["num_comments"] == 7
    assert p["author"] == "bob"
    assert p["subreddit"] == ""
    assert seen["params"]["tags"] == "story"
    assert seen["params"]["numericFilters"] == "created_at_i>1699999999"
    # every key index._save_signal reads must be present
    for key in ("id", "title", "selftext", "author", "post_url", "score", "num_comments",
                "created_utc", "subreddit"):
        assert key in p


def test_2_comment_hit_strips_tags_and_uses_text_as_title():
    hit = {
        "objectID": "999",
        "comment_text": "<p>I love <a href=\"https://x.io\">Shopify</a></p><p>so much</p>",
        "author": "al",
        "created_at_i": 1700000100,
    }
    p = fetcher._hit_to_post(hit, "comment")
    assert p["title"] == "I love Shopify so much"
    assert "<" not in p["title"] and ">" not in p["title"]
    assert p["selftext"] == ""
    assert p["post_url"] == "https://news.ycombinator.com/item?id=999"
    assert p["author"] == "al"
    assert p["subreddit"] == ""


# ──────────────────────────────────────────────────────────────────────────────
# 3: errors / timeouts / 429
# ──────────────────────────────────────────────────────────────────────────────

def test_3_errors_return_empty_and_429_raises(monkeypatch):
    # timeout / connection error
    def boom(*a, **k):
        raise requests.Timeout("slow")

    monkeypatch.setattr(requests, "get", boom)
    assert fetcher.fetch_hackernews_page("shopify", "story", None, 0) == ([], 0)

    # HTTP 500
    monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResp(500, {}))
    assert fetcher.fetch_hackernews_page("shopify", "story", None, 0) == ([], 0)

    # invalid JSON
    monkeypatch.setattr(
        requests, "get", lambda *a, **k: FakeResp(200, json_error=ValueError("bad json"))
    )
    assert fetcher.fetch_hackernews_page("shopify", "story", None, 0) == ([], 0)

    # HTTP 429 -> HNRateLimited
    monkeypatch.setattr(requests, "get", lambda *a, **k: FakeResp(429, {}))
    with pytest.raises(HNRateLimited):
        fetcher.fetch_hackernews_page("shopify", "story", None, 0)


# ──────────────────────────────────────────────────────────────────────────────
# 4: keyword_in_post
# ──────────────────────────────────────────────────────────────────────────────

def test_4_keyword_in_post():
    # whole phrase
    assert fetcher.keyword_in_post("shopify store", {"title": "Building a shopify store fast"})
    # all significant words, separately
    assert fetcher.keyword_in_post(
        "shopify store", {"title": "My store is live", "selftext": "powered by Shopify"}
    )
    # one word missing -> False
    assert not fetcher.keyword_in_post("shopify store", {"title": "Shopify is great"})
    # case-insensitive both ways
    assert fetcher.keyword_in_post("SHOPIFY Store", {"title": "my shopify STORE"})
    # stopwords / short words are ignored when matching separately
    assert fetcher.keyword_in_post("a tool for shopify", {"title": "shopify tool"})


# ──────────────────────────────────────────────────────────────────────────────
# 5: strict ON / OFF
# ──────────────────────────────────────────────────────────────────────────────

def _patch_fetch(monkeypatch, pages):
    """pages: list of (posts, nb_pages) returned for page 0, 1, 2 ...; records calls."""
    calls = []

    def fake_fetch(keyword, kind, since_ts, page):
        calls.append(page)
        idx = min(page, len(pages) - 1)
        return pages[idx]

    monkeypatch.setattr(fetcher, "fetch_hackernews_page", fake_fetch)
    return calls


def test_5_strict_match_on_and_off(monkeypatch):
    url_only = make_post(1, title="Random thing", url="https://example.com/shopify")
    author_only = make_post(2, title="Another thing", author="shopify_guy")
    real_hit = make_post(3, title="Best shopify themes")
    _patch_fetch(monkeypatch, [([url_only, author_only], 1)])

    # strict ON: keyword only in URL / author -> save_fn NOT called
    save_fn = MagicMock(return_value=True)
    monkeypatch.setattr(config, "is_hn_strict_match_enabled", lambda: True)
    assert fetcher.process_keyword_kind("shopify", "story", save_fn, 0) == 0
    save_fn.assert_not_called()

    # strict ON: keyword in title -> saved
    _patch_fetch(monkeypatch, [([real_hit], 1)])
    save_fn = MagicMock(return_value=True)
    assert fetcher.process_keyword_kind("shopify", "story", save_fn, 0) == 1
    assert save_fn.call_count == 1

    # strict OFF: everything goes to save_fn
    _patch_fetch(monkeypatch, [([url_only, author_only, real_hit], 1)])
    save_fn = MagicMock(return_value=True)
    monkeypatch.setattr(config, "is_hn_strict_match_enabled", lambda: False)
    assert fetcher.process_keyword_kind("shopify", "story", save_fn, 0) == 3
    assert save_fn.call_count == 3


# ──────────────────────────────────────────────────────────────────────────────
# 6: save_fn call arguments
# ──────────────────────────────────────────────────────────────────────────────

def test_6_save_fn_called_with_topic_key_keyword_platform_post(monkeypatch):
    post = make_post(10, title="Best Shopify Store tips")

    _patch_fetch(monkeypatch, [([post], 1)])
    save_fn = MagicMock(return_value=True)
    assert fetcher.process_keyword_kind("Shopify Store", "story", save_fn, 0) == 1
    save_fn.assert_called_once_with("shopify store", "Shopify Store", "hackernews", post)

    _patch_fetch(monkeypatch, [([post], 1)])
    save_fn = MagicMock(return_value=True)
    assert fetcher.process_keyword_kind("Shopify Store", "comment", save_fn, 0) == 1
    save_fn.assert_called_once_with("shopify store", "Shopify Store", "hackernews_comment", post)


# ──────────────────────────────────────────────────────────────────────────────
# 7: checkpoint uses max created_at_i even when posts are skipped ($max)
# ──────────────────────────────────────────────────────────────────────────────

def test_7_checkpoint_goes_to_max_even_for_skipped_posts(monkeypatch, env):
    posts = [
        make_post(1, title="unrelated a", ts=100),
        make_post(2, title="unrelated b", ts=300),
        make_post(3, title="unrelated c", ts=200),
    ]
    _patch_fetch(monkeypatch, [(posts, 1)])
    save_fn = MagicMock(return_value=True)

    assert fetcher.process_keyword_kind("shopify", "story", save_fn, 0) == 0
    save_fn.assert_not_called()  # all skipped by strict match...
    assert database.get_last_seen_ts("shopify", "story") == 300  # ...checkpoint still advanced

    # $max: an older timestamp never overwrites a newer one
    database.set_last_seen_ts("shopify", "story", 50)
    assert database.get_last_seen_ts("shopify", "story") == 300
    database.set_last_seen_ts("shopify", "story", 500)
    assert database.get_last_seen_ts("shopify", "story") == 500

    # stories and comments are tracked separately
    assert database.get_last_seen_ts("shopify", "comment") is None


# ──────────────────────────────────────────────────────────────────────────────
# 8: pagination
# ──────────────────────────────────────────────────────────────────────────────

def test_8_pagination_respects_nbpages_and_max_pages(monkeypatch):
    save_fn = MagicMock(return_value=True)
    monkeypatch.setattr(config, "is_hn_strict_match_enabled", lambda: False)

    # nbPages=3, HN_MAX_PAGES big enough -> all 3 pages
    monkeypatch.setattr(config, "HN_MAX_PAGES", 5)
    calls = _patch_fetch(monkeypatch, [([make_post(1, "a")], 3)])
    fetcher.process_keyword_kind("shopify", "story", save_fn, 0)
    assert calls == [0, 1, 2]

    # HN_MAX_PAGES=2 -> stops at 2 even though nbPages=3
    monkeypatch.setattr(config, "HN_MAX_PAGES", 2)
    calls = _patch_fetch(monkeypatch, [([make_post(1, "a")], 3)])
    fetcher.process_keyword_kind("shopify", "story", save_fn, 0)
    assert calls == [0, 1]


# ──────────────────────────────────────────────────────────────────────────────
# 9: keyword rotation
# ──────────────────────────────────────────────────────────────────────────────

def test_9_rotation_wraps_around_and_cursor_advances_per_keyword(monkeypatch):
    keywords = ["k0", "k1", "k2", "k3", "k4"]
    monkeypatch.setattr(config, "HN_KEYWORDS_PER_CYCLE", 2)
    monkeypatch.setattr(config, "is_hn_comments_enabled", lambda: False)  # stories only

    processed = []
    cursor_at_call = []

    def spy_process(keyword, kind, save_fn, default_since_ts):
        processed.append(keyword)
        cursor_at_call.append(database.get_cursor())  # cursor must NOT move before the keyword finishes
        return 0

    monkeypatch.setattr(fetcher, "process_keyword_kind", spy_process)

    cursor_writes = []
    real_set_cursor = database.set_cursor

    def spy_set_cursor(n):
        cursor_writes.append(n)
        real_set_cursor(n)

    monkeypatch.setattr(database, "set_cursor", spy_set_cursor)
    save_fn = MagicMock()

    fetcher.run_hackernews_cycle(save_fn, lambda: keywords)
    assert processed == ["k0", "k1"]
    assert cursor_at_call == [0, 1]
    assert cursor_writes == [1, 2]

    processed.clear(); cursor_writes.clear()
    fetcher.run_hackernews_cycle(save_fn, lambda: keywords)
    assert processed == ["k2", "k3"]
    assert cursor_writes == [3, 4]

    processed.clear(); cursor_writes.clear()
    fetcher.run_hackernews_cycle(save_fn, lambda: keywords)
    assert processed == ["k4", "k0"]          # wrap-around
    assert cursor_writes == [0, 1]
    assert database.get_cursor() == 1


# ──────────────────────────────────────────────────────────────────────────────
# 10: rate limit stops the cycle, cursor stays, loop backs off
# ──────────────────────────────────────────────────────────────────────────────

def test_10_rate_limit_stops_cycle_keeps_cursor_and_loop_backs_off(monkeypatch, env):
    keywords = ["k0", "k1", "k2"]
    monkeypatch.setattr(config, "HN_KEYWORDS_PER_CYCLE", 3)
    monkeypatch.setattr(config, "is_hn_comments_enabled", lambda: False)
    monkeypatch.setattr(config, "HN_RATE_LIMIT_BACKOFF_SECONDS", 123)

    processed = []

    def spy_process(keyword, kind, save_fn, default_since_ts):
        processed.append(keyword)
        if keyword == "k1":
            raise HNRateLimited()
        return 0

    monkeypatch.setattr(fetcher, "process_keyword_kind", spy_process)

    # Cycle alone: raises, k2 never processed, cursor stays after the last COMPLETED keyword.
    with pytest.raises(HNRateLimited):
        fetcher.run_hackernews_cycle(MagicMock(), lambda: keywords)
    assert processed == ["k0", "k1"]
    assert database.get_cursor() == 1

    # Loop: catches it and sleeps HN_RATE_LIMIT_BACKOFF_SECONDS.
    processed.clear()

    def fake_sleep(seconds):
        env.sleep_calls.append(seconds)
        if seconds == 123:
            raise StopLoop()

    monkeypatch.setattr(fetcher.time, "sleep", fake_sleep)
    status_fn = MagicMock()
    with pytest.raises(StopLoop):
        fetcher.run_hackernews_loop(MagicMock(), lambda: keywords, status_fn, lambda: True, env.db)
    assert 123 in env.sleep_calls
    assert database.get_cursor() == 1   # resumed at k1, still not advanced past it
    assert processed == ["k1"]


# ──────────────────────────────────────────────────────────────────────────────
# 11: disabled switch / no sinks -> no cycle, no state writes
# ──────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("enabled,sinks_ok", [(False, True), (True, False)])
def test_11_disabled_or_no_sinks_skips_cycle(monkeypatch, env, enabled, sinks_ok):
    monkeypatch.setattr(config, "is_hackernews_enabled", lambda: enabled)
    monkeypatch.setattr(config, "HN_POLL_INTERVAL_SECONDS", 77)

    cycle = MagicMock()
    monkeypatch.setattr(fetcher, "run_hackernews_cycle", cycle)
    set_ts = MagicMock()
    set_cur = MagicMock()
    monkeypatch.setattr(database, "set_last_seen_ts", set_ts)
    monkeypatch.setattr(database, "set_cursor", set_cur)

    def fake_sleep(seconds):
        env.sleep_calls.append(seconds)
        if seconds == 77:
            raise StopLoop()

    monkeypatch.setattr(fetcher.time, "sleep", fake_sleep)

    with pytest.raises(StopLoop):
        fetcher.run_hackernews_loop(
            MagicMock(), lambda: ["k0"], MagicMock(), lambda: sinks_ok, env.db
        )

    cycle.assert_not_called()
    set_ts.assert_not_called()
    set_cur.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# 12: heartbeat every iteration
# ──────────────────────────────────────────────────────────────────────────────

def test_12_status_heartbeat_each_iteration(monkeypatch, env):
    monkeypatch.setattr(config, "HN_POLL_INTERVAL_SECONDS", 55)
    monkeypatch.setattr(fetcher, "run_hackernews_cycle", MagicMock(return_value=0))

    sleeps_seen = {"n": 0}

    def fake_sleep(seconds):
        env.sleep_calls.append(seconds)
        if seconds == 55:
            sleeps_seen["n"] += 1
            if sleeps_seen["n"] == 3:
                raise StopLoop()

    monkeypatch.setattr(fetcher.time, "sleep", fake_sleep)
    status_fn = MagicMock()

    with pytest.raises(StopLoop):
        fetcher.run_hackernews_loop(MagicMock(), lambda: ["k0"], status_fn, lambda: True, env.db)

    assert config.HN_SERVICE_NAME == "hackernews_poller"
    calls = [c.args for c in status_fn.call_args_list]
    assert calls[:3] == [("hackernews_poller", True)] * 3   # one heartbeat per iteration
    assert calls[-1] == ("hackernews_poller", False)        # cleared on shutdown (finally)


# ──────────────────────────────────────────────────────────────────────────────
# 13: DB helpers never raise
# ──────────────────────────────────────────────────────────────────────────────

def test_13_database_functions_never_raise_when_db_is_none_or_down(monkeypatch, caplog):
    # _db is None
    monkeypatch.setattr(database, "_db", None)
    assert database.get_last_seen_ts("k", "story") is None
    assert database.set_last_seen_ts("k", "story", 5) is None
    assert database.get_cursor() == 0
    assert database.set_cursor(3) is None

    # DB down (every access raises)
    monkeypatch.setattr(database, "_db", BrokenDB())
    monkeypatch.setattr(database, "_state_index_ready", False)
    with caplog.at_level("WARNING", logger="flintel-fetch"):
        assert database.get_last_seen_ts("k", "story") is None
        assert database.set_last_seen_ts("k", "story", 5) is None
        assert database.get_cursor() == 0
        assert database.set_cursor(3) is None
    assert any("[HACKERNEWS]" in r.getMessage() for r in caplog.records)


# ──────────────────────────────────────────────────────────────────────────────
# 14: empty keyword list
# ──────────────────────────────────────────────────────────────────────────────

def test_14_empty_keyword_list_returns_zero_and_makes_no_api_call(monkeypatch):
    process = MagicMock()
    monkeypatch.setattr(fetcher, "process_keyword_kind", process)
    get_mock = MagicMock()
    monkeypatch.setattr(requests, "get", get_mock)

    assert fetcher.run_hackernews_cycle(MagicMock(), lambda: []) == 0
    process.assert_not_called()
    get_mock.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# Extra: index.py wiring (start_all spawns the HN thread with the right args)
# ──────────────────────────────────────────────────────────────────────────────

def test_index_start_all_spawns_hackernews_thread(monkeypatch):
    assert index.run_hackernews_loop is fetcher.run_hackernews_loop
    assert index.hn_config is config

    created = []

    def fake_thread(*args, **kwargs):
        t = MagicMock()
        t.kwargs = kwargs
        created.append(t)
        return t

    monkeypatch.setattr(index.threading, "Thread", fake_thread)
    index.start_all()

    hn = [t for t in created if t.kwargs.get("name") == "HackerNewsPoller"]
    assert len(hn) == 1
    kw = hn[0].kwargs
    assert kw["target"] is fetcher.run_hackernews_loop
    assert kw["daemon"] is True
    save_fn, keywords_fn, status_fn, sinks_ok_fn, db_arg = kw["args"]
    assert save_fn is index._save_signal
    assert keywords_fn is index._get_keywords
    assert status_fn is index._set_service_status
    assert db_arg is index.db
    hn[0].start.assert_called_once()

    monkeypatch.setattr(index, "_is_mongodb_data_enabled", lambda: False)
    monkeypatch.setattr(index, "_is_mysql_data_enabled", lambda: False)
    assert sinks_ok_fn() is False
    monkeypatch.setattr(index, "_is_mysql_data_enabled", lambda: True)
    assert sinks_ok_fn() is True
