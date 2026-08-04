from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from pathlib import Path

from core.db import init_db
from core.db_writer import BatchedSQLiteWriter


class BatchedSQLiteWriterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self._tmpdir.name) / "writer_test.db")
        # Creates the schema (events/fall_warnings/state_snapshots) on this path; the writer opens
        # its OWN separate connection to the same file (per Core Layer Specialist's
        # recommendation), never reusing this one.
        self.setup_connection = init_db(self.db_path)
        self.writer = BatchedSQLiteWriter(
            self.db_path, batch_window_seconds=0.05, max_batch_size=50
        )
        self.writer.start()

    def tearDown(self) -> None:
        self.writer.stop()
        self.setup_connection.close()
        self._tmpdir.cleanup()

    async def test_future_resolves_only_after_commit_is_durable(self) -> None:
        future = self.writer.submit(
            "INSERT INTO events (device_id, room_id, type, ts, payload, received_at, late) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("dev_1", "room_1", "heartbeat", "2026-01-01T00:00:00+00:00", "{}", "2026-01-01T00:00:00+00:00", 0),
        )
        rowcount, lastrowid = await future
        self.assertEqual(rowcount, 1)
        self.assertIsNotNone(lastrowid)

        # A fresh, independent connection must already see the committed row -- proves the
        # future only resolved after a real commit, not merely at batch-enqueue time.
        reader = init_db(self.db_path)
        try:
            cursor = reader.cursor()
            cursor.execute("SELECT COUNT(*) FROM events WHERE device_id = ?", ("dev_1",))
            (count,) = cursor.fetchone()
            self.assertEqual(count, 1)
        finally:
            reader.close()

    async def test_priority_write_does_not_wait_for_batch_window(self) -> None:
        # batch_window_seconds is set high enough (0.05s) in setUp that a NORMAL write would wait
        # ~50ms; a priority write must resolve well under that, proving it skips the batch wait.
        start = time.monotonic()
        future = self.writer.submit(
            "INSERT INTO fall_warnings (device_id, room_id, ts, confidence, dedup_key, received_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("dev_1", "room_1", "2026-01-01T00:00:00+00:00", 0.9, "dedup-a", "2026-01-01T00:00:00+00:00"),
            priority=True,
        )
        rowcount, lastrowid = await future
        elapsed = time.monotonic() - start
        self.assertEqual(rowcount, 1)
        self.assertIsNotNone(lastrowid)
        self.assertLess(elapsed, 0.03)

    async def test_rowcount_zero_on_duplicate_dedup_key(self) -> None:
        sql = (
            "INSERT OR IGNORE INTO fall_warnings (device_id, room_id, ts, confidence, dedup_key, received_at) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        params = ("dev_1", "room_1", "2026-01-01T00:00:00+00:00", 0.9, "dedup-b", "2026-01-01T00:00:00+00:00")
        first_rowcount, first_lastrowid = await self.writer.submit(sql, params, priority=True)
        self.assertEqual(first_rowcount, 1)
        self.assertIsNotNone(first_lastrowid)

        second_rowcount, _second_lastrowid = await self.writer.submit(sql, params, priority=True)
        self.assertEqual(second_rowcount, 0)
        # lastrowid is meaningless when rowcount == 0 (INSERT OR IGNORE inserted nothing); callers
        # (FallWarnHandler) never read it in that branch -- only rowcount gates the dedup decision.

    async def test_batch_of_concurrent_writes_all_commit_and_resolve(self) -> None:
        futures = [
            self.writer.submit(
                "INSERT INTO events (device_id, room_id, type, ts, payload, received_at, late) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (f"dev_{i}", "room_1", "heartbeat", "2026-01-01T00:00:00+00:00", "{}", "2026-01-01T00:00:00+00:00", 0),
            )
            for i in range(20)
        ]
        results = await asyncio.gather(*futures)
        self.assertEqual(len(results), 20)
        for rowcount, lastrowid in results:
            self.assertEqual(rowcount, 1)
            self.assertIsNotNone(lastrowid)

        reader = init_db(self.db_path)
        try:
            cursor = reader.cursor()
            cursor.execute("SELECT COUNT(*) FROM events")
            (count,) = cursor.fetchone()
            self.assertEqual(count, 20)
        finally:
            reader.close()

    async def test_sql_error_fails_future_without_crashing_writer_thread(self) -> None:
        with self.assertRaises(Exception):
            await self.writer.submit("INSERT INTO not_a_real_table (x) VALUES (?)", (1,))

        # The writer thread must still be alive and able to process a subsequent, valid write --
        # a bad statement in one batch must not take down the dedicated thread.
        rowcount, lastrowid = await self.writer.submit(
            "INSERT INTO events (device_id, room_id, type, ts, payload, received_at, late) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("dev_recover", "room_1", "heartbeat", "2026-01-01T00:00:00+00:00", "{}", "2026-01-01T00:00:00+00:00", 0),
        )
        self.assertEqual(rowcount, 1)
        self.assertIsNotNone(lastrowid)


if __name__ == "__main__":
    unittest.main()
