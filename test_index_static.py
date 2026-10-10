"""Tests for the 4 changes made to index.py (static-keyword poller).

index.py runs get_database() at import time, so it is imported here with
pymongo.MongoClient patched. MySQL tests are skipped if pymysql is missing.
"""
import datetime
import os
import sys
import unittest
from unittest import mock
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pymongo.errors import OperationFailure

with mock.patch("pymongo.MongoClient", return_value=MagicMock()):
    import index

LOGGER_NAME = "flintel-fetch"

ATOM_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>new</title>
  {entries}
</feed>"""


def _entry(entry_id, author_xml=""):
    return f"""
  <entry>
    {author_xml}
    <id>t3_{entry_id}</id>
    <link href="https://www.reddit.com/r/test/comments/{entry_id}/some_title/"/>
    <title>Title {entry_id}</title>
    <updated>2026-10-10T10:00:00+00:00</updated>
    <summary>Body {entry_id}</summary>
  </entry>"""


def _fake_db_with_create_index(side_effect):
    coll_signals = MagicMock()
    coll_signals.name = "flintel_signals"
    coll_signals.create_index.side_effect = side_effect
    coll_status = MagicMock()
    coll_status.name = "flintel_service_status"
    coll_status.create_index.side_effect = side_effect

    fake_db = MagicMock()
    fake_db.flintel_signals = coll_signals
    fake_db.flintel_service_status = coll_status

    client = MagicMock()
    client.__getitem__.return_value = fake_db
    return client, coll_signals, coll_status


class TestEnsureIndex(unittest.TestCase):
    def test_code_85_does_not_raise_and_logs_warning(self):
        err = OperationFailure(
            "Index already exists with a different name: topic_key_1",
            code=85,
            details={"errmsg": "Index already exists with a different name: topic_key_1"},
        )
        client, coll_signals, coll_status = _fake_db_with_create_index(err)
        with mock.patch.object(index, "MongoClient", return_value=client):
            with self.assertLogs(LOGGER_NAME, level="WARNING") as cm:
                result = index.get_database()
        self.assertIsNotNone(result)
        self.assertTrue(any("[INDEX]" in m and "different name" in m for m in cm.output))
        self.assertEqual(coll_signals.create_index.call_count, 4)
        self.assertEqual(coll_status.create_index.call_count, 1)

    def test_non_85_operation_failure_is_still_raised(self):
        err = OperationFailure("boom", code=13)
        client, _, _ = _fake_db_with_create_index(err)
        with mock.patch.object(index, "MongoClient", return_value=client):
            with self.assertLogs(LOGGER_NAME, level="CRITICAL"):
                with self.assertRaises(OperationFailure) as ctx:
                    index.get_database()
        self.assertEqual(ctx.exception.code, 13)

    def test_helper_passes_keys_and_kwargs_through(self):
        coll = MagicMock()
        index._ensure_index(coll, [("message_id", 1)], unique=True, name="n")
        coll.create_index.assert_called_once_with([("message_id", 1)], unique=True, name="n")


class TestRedditAuthor(unittest.TestCase):
    def _run(self, entries_xml):
        feed_bytes = ATOM_TEMPLATE.format(entries=entries_xml).encode("utf-8")
        resp = MagicMock()
        resp.content = feed_bytes
        resp.raise_for_status.return_value = None
        with mock.patch.object(index.requests, "get", return_value=resp):
            return index._fetch_reddit_rss_feed()

    def test_author_prefix_handling(self):
        xml = (
            _entry("a1", "<author><name>/u/uber_driver</name></author>")
            + _entry("a2", "<author><name>/u/username</name></author>")
            + _entry("a3", "")
        )
        entries = self._run(xml)
        by_id = {e["id"]: e["author"] for e in entries}
        self.assertEqual(by_id["t3_a1"], "uber_driver")
        self.assertEqual(by_id["t3_a2"], "username")
        self.assertEqual(by_id["t3_a3"], "unknown")


class TestKeywords(unittest.TestCase):
    def test_keyword_list_is_empty(self):
        self.assertEqual(index.keyword, [])
        self.assertEqual(index._get_keywords(), [])


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = 0
        self._result = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        if sql.startswith("INSERT"):
            self.conn.inserts.append(params)
            if self.conn.insert_error is not None:
                raise self.conn.insert_error
            self.rowcount = 1
        elif sql.startswith("SELECT COUNT"):
            if self.conn.count_error is not None:
                raise self.conn.count_error
            self.conn.count_calls += 1
            self._result = (len(self.conn.inserts),)
        else:
            self.rowcount = 0

    def fetchone(self):
        return self._result


class _FakeConn:
    def __init__(self):
        self.inserts = []
        self.insert_error = None
        self.count_error = None
        self.count_calls = 0

    def cursor(self):
        return _FakeCursor(self)

    def ping(self, reconnect=True):
        pass

    def close(self):
        pass


def _make_doc(n=1, embedding=True):
    now = datetime.datetime.now(datetime.timezone.utc)
    return {
        "message_id": f"reddit_t3_{n}",
        "topic_key": "shopify",
        "search_keyword": "Shopify",
        "platform": "reddit",
        "subreddit": "test",
        "username": "someone",
        "title": "SECRET POST TITLE",
        "text": "SECRET POST BODY",
        "post_url": "https://example.com",
        "score": 1,
        "num_comments": 2,
        "created_utc": now,
        "fetched_at": now,
        "embedding": [0.25] * index.EMBEDDING_EXPECTED_DIM if embedding else None,
    }


@unittest.skipIf(index.pymysql is None, "pymysql not installed")
class TestMysqlInsertLog(unittest.TestCase):
    def setUp(self):
        self.conn = _FakeConn()
        index._mysql_local.conn = self.conn
        self._table_ready = index._mysql_table_ready
        index._mysql_table_ready = True
        index._mysql_insert_count = 0
        self._db, self._host = index.MYSQL_DATABASE, index.MYSQL_HOST
        index.MYSQL_DATABASE, index.MYSQL_HOST = "testdb", "db.example"

    def tearDown(self):
        index._mysql_local.conn = None
        index._mysql_table_ready = self._table_ready
        index._mysql_insert_count = 0
        index.MYSQL_DATABASE, index.MYSQL_HOST = self._db, self._host

    def test_successful_insert_logs_inserted(self):
        with self.assertLogs(LOGGER_NAME, level="INFO") as cm:
            self.assertTrue(index._save_to_mysql(_make_doc(7)))
        inserted = [m for m in cm.output if "[MYSQL] INSERTED" in m]
        self.assertEqual(len(inserted), 1)
        line = inserted[0]
        self.assertIn("db=testdb table=flintel_signals", line)
        self.assertIn("message_id=reddit_t3_7", line)
        self.assertIn("platform=reddit", line)
        self.assertIn("search_keyword=Shopify", line)
        self.assertIn("embedding_bytes=6144", line)
        self.assertIn("host=db.example", line)
        self.assertNotIn("SECRET", line)

    def test_null_embedding_logs_zero_bytes(self):
        with self.assertLogs(LOGGER_NAME, level="INFO") as cm:
            self.assertTrue(index._save_to_mysql(_make_doc(8, embedding=False)))
        self.assertTrue(any("embedding_bytes=0 " in m for m in cm.output))

    def test_duplicate_1062_returns_false_without_inserted_log(self):
        self.conn.insert_error = index.pymysql.err.IntegrityError(1062, "Duplicate entry")
        with self.assertLogs(LOGGER_NAME, level="INFO") as cm:
            index.log.info("sentinel")  # assertLogs needs at least one record
            self.assertFalse(index._save_to_mysql(_make_doc(9)))
        self.assertFalse(any("[MYSQL] INSERTED" in m for m in cm.output))
        self.assertFalse(any("save_signal error" in m for m in cm.output))

    def test_total_rows_logged_every_25_inserts(self):
        with self.assertLogs(LOGGER_NAME, level="INFO") as cm:
            for i in range(25):
                self.assertTrue(index._save_to_mysql(_make_doc(i)))
        totals = [m for m in cm.output if "[MYSQL] TOTAL rows" in m]
        self.assertEqual(len(totals), 1)
        self.assertIn("flintel_signals = 25", totals[0])
        self.assertIn("(db=testdb)", totals[0])
        self.assertEqual(self.conn.count_calls, 1)

    def test_failing_count_still_returns_true(self):
        self.conn.count_error = index.pymysql.err.ProgrammingError(1146, "no table")
        index._mysql_insert_count = 24
        with self.assertLogs(LOGGER_NAME, level="INFO") as cm:
            self.assertTrue(index._save_to_mysql(_make_doc(100)))
        self.assertTrue(any(r for r in cm.output if "WARNING" in r and "row count" in r))
        self.assertFalse(any("[MYSQL] TOTAL rows" in m for m in cm.output))


if __name__ == "__main__":
    unittest.main()
