from __future__ import annotations

import sqlite3
from pathlib import Path
from sqlite3 import Connection

from config import SQLITE_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    room_id TEXT NOT NULL,
    type TEXT NOT NULL,
    ts TEXT NOT NULL,
    payload TEXT NOT NULL,
    received_at TEXT NOT NULL,
    late INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_events_device_ts ON events(device_id, ts);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);

CREATE TABLE IF NOT EXISTS fall_warnings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    room_id TEXT NOT NULL,
    ts TEXT NOT NULL,
    confidence REAL NOT NULL,
    dedup_key TEXT NOT NULL UNIQUE,
    received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fall_ts ON fall_warnings(ts);
CREATE INDEX IF NOT EXISTS idx_fall_room ON fall_warnings(room_id, ts);

CREATE TABLE IF NOT EXISTS state_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_ts TEXT NOT NULL,
    state_json TEXT NOT NULL
);
"""


def init_db(path: str = SQLITE_PATH) -> Connection:
    database_file = Path(path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(database_file), check_same_thread=False)
    _apply_pragmas(connection)
    connection.executescript(SCHEMA)
    connection.commit()
    return connection


def _apply_pragmas(connection: Connection) -> None:
    # WAL + synchronous=NORMAL is the durability/throughput sweet spot. Under WAL, NORMAL fsyncs
    # only at checkpoints instead of on every commit, so the per-event INSERT+commit on the hot
    # path no longer pays an fsync each time -- that fsync-per-event was the SQLite throughput
    # ceiling at 5k-50k ev/s. Committed transactions stay crash-safe except on OS/power loss,
    # which the snapshot+replay recovery already tolerates. busy_timeout prevents spurious
    # "database is locked" errors when API read connections overlap the writer.
    connection.execute("PRAGMA journal_mode=WAL;")
    connection.execute("PRAGMA synchronous=NORMAL;")
    connection.execute("PRAGMA busy_timeout=5000;")


def get_db_connection(path: str = SQLITE_PATH) -> Connection:
    connection = sqlite3.connect(str(path), check_same_thread=False)
    connection.execute("PRAGMA busy_timeout=5000;")
    return connection
