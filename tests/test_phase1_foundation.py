from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import config
import models
from core.db import init_db
from core.redis_client import get_redis_client


class Phase1FoundationTests(unittest.TestCase):
    def test_config_contains_foundation_constants(self) -> None:
        self.assertTrue(config.REDIS_URL)
        self.assertTrue(config.SQLITE_PATH)
        self.assertGreater(config.NORMAL_QUEUE_MAX_SIZE, 0)
        self.assertGreater(config.WORKER_COUNT, 0)
        self.assertGreater(config.HEARTBEAT_WINDOW_SECONDS, 0)
        self.assertGreater(config.OCCUPANCY_WINDOW_SECONDS, 0)

    def test_models_define_required_domain_types(self) -> None:
        self.assertTrue(hasattr(models, "RawEvent"))
        self.assertTrue(hasattr(models, "ValidatedEvent"))
        self.assertTrue(hasattr(models, "Priority"))
        self.assertEqual(models.Priority.HIGH.value, "high")
        self.assertEqual(models.Priority.NORMAL.value, "normal")

    def test_db_initialization_creates_required_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "phase1.db"
            connection = init_db(str(db_path))
            try:
                cursor = connection.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('events', 'fall_warnings', 'state_snapshots')"
                )
                names = {row[0] for row in cursor.fetchall()}
            finally:
                connection.close()

        self.assertEqual(names, {"events", "fall_warnings", "state_snapshots"})

    def test_db_initialization_sets_throughput_pragmas(self) -> None:
        # Finding #3: WAL + synchronous=NORMAL removes the per-event fsync that capped SQLite
        # write throughput on the hot path; busy_timeout avoids spurious "database is locked".
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "pragmas.db"
            connection = init_db(str(db_path))
            try:
                journal_mode = connection.execute("PRAGMA journal_mode;").fetchone()[0]
                synchronous = connection.execute("PRAGMA synchronous;").fetchone()[0]
                busy_timeout = connection.execute("PRAGMA busy_timeout;").fetchone()[0]
            finally:
                connection.close()

        self.assertEqual(str(journal_mode).lower(), "wal")
        # synchronous=NORMAL is reported as 1.
        self.assertEqual(int(synchronous), 1)
        self.assertEqual(int(busy_timeout), 5000)

    @patch("core.redis_client.redis.from_url")
    def test_redis_client_initialization_pings_server(self, from_url_mock: MagicMock) -> None:
        fake_client = MagicMock()
        from_url_mock.return_value = fake_client

        result = get_redis_client()

        self.assertIs(result, fake_client)
        from_url_mock.assert_called_once()
        fake_client.ping.assert_called_once()


if __name__ == "__main__":
    unittest.main()
