from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routes.alarms import router as alarms_router
from api.routes.events import router as events_router
from api.routes.health import router as health_router
from api.routes.occupancy import router as occupancy_router
from core.recovery import RecoveryManager
from ingestion.queue import PriorityEventQueue
from processing.alarm_bus import AlarmBus
from tests.fakes import FakeRedis, ResponseLike


def _new_db() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    connection.executescript(
        """
        CREATE TABLE events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL,
            room_id TEXT NOT NULL,
            type TEXT NOT NULL,
            ts TEXT NOT NULL,
            payload TEXT NOT NULL,
            received_at TEXT NOT NULL,
            late INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE fall_warnings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL,
            room_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            confidence REAL NOT NULL,
            dedup_key TEXT NOT NULL UNIQUE,
            received_at TEXT NOT NULL
        );
        CREATE TABLE state_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_ts TEXT NOT NULL,
            state_json TEXT NOT NULL
        );
        """
    )
    connection.commit()
    return connection


class _NullAlarmBus:
    async def publish(self, alarm: object) -> None:
        return None


def _build_test_app(
    db_connection: sqlite3.Connection,
    redis_client: FakeRedis,
    event_queue: PriorityEventQueue | None = None,
) -> FastAPI:
    app = FastAPI()
    app.include_router(health_router)
    app.include_router(occupancy_router)
    app.include_router(alarms_router)
    app.include_router(events_router)
    app.state.db_connection = db_connection
    app.state.redis_client = redis_client
    app.state.alarm_bus = AlarmBus()
    app.state.event_queue = event_queue or PriorityEventQueue(normal_max_size=100)
    return app


class ApiContractHttpTests(unittest.TestCase):
    def setUp(self) -> None:
        self.db = _new_db()
        self.redis = FakeRedis()
        self.app = _build_test_app(self.db, self.redis)

    def tearDown(self) -> None:
        self.db.close()

    def test_health_endpoint_returns_404_for_unknown_device(self) -> None:
        with TestClient(self.app) as client:
            client_any = cast(Any, client)
            response = cast(ResponseLike, client_any.get("/devices/dev_unknown/health"))
        self.assertEqual(response.status_code, 404)

    def test_events_endpoint_accepts_valid_flat_event(self) -> None:
        payload: dict[str, object] = {
            "device_id": "dev_1",
            "room_id": "room_1",
            "type": "heartbeat",
            "ts": datetime.now(timezone.utc).isoformat(),
            "seq": 1,
        }
        with TestClient(self.app) as client:
            client_any = cast(Any, client)
            response = cast(ResponseLike, client_any.post("/events", json=payload))
        self.assertEqual(response.status_code, 202)
        self.assertEqual(cast(dict[str, object], response.json()), {"status": "accepted"})

    def test_alarms_endpoint_returns_challenge_shape(self) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        self.db.execute(
            "INSERT INTO fall_warnings (device_id, room_id, ts, confidence, dedup_key, received_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("dev_1", "room_1", ts, 0.9, "dedup-key-1", ts),
        )
        self.db.commit()

        with TestClient(self.app) as client:
            client_any = cast(Any, client)
            response = cast(ResponseLike, client_any.get("/alarms?since=0"))

        self.assertEqual(response.status_code, 200)
        body = cast(dict[str, object], response.json())
        self.assertIn("alarms", body)
        self.assertIn("since", body)
        self.assertEqual(body["since"], 0)
        alarms = cast(list[dict[str, object]], body["alarms"])
        self.assertEqual(len(alarms), 1)
        alarm = alarms[0]
        self.assertEqual(alarm["device_id"], "dev_1")
        self.assertEqual(
            set(alarm.keys()),
            {"device_id", "room_id", "ts", "confidence", "received_at"},
        )


class RecoveryToEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_recovery_restores_state_visible_through_read_endpoints(self) -> None:
        db = _new_db()
        redis_client = FakeRedis()
        now = datetime.now(timezone.utc)

        presence_enter = (now - timedelta(minutes=30)).isoformat()
        presence_exit = (now - timedelta(minutes=10)).isoformat()
        heartbeat_ts = (now - timedelta(minutes=5)).isoformat()
        received = now.isoformat()

        db.execute(
            "INSERT INTO events (device_id, room_id, type, ts, payload, received_at, late) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("dev_1", "room_1", "presence", presence_enter, json.dumps({"in_room": True}), received, 1),
        )
        db.execute(
            "INSERT INTO events (device_id, room_id, type, ts, payload, received_at, late) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("dev_1", "room_1", "heartbeat", heartbeat_ts, json.dumps({}), received, 0),
        )
        db.execute(
            "INSERT INTO events (device_id, room_id, type, ts, payload, received_at, late) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("dev_1", "room_1", "presence", presence_exit, json.dumps({"in_room": False}), received, 1),
        )
        db.commit()

        manager = RecoveryManager(db, cast(Any, redis_client), cast(Any, _NullAlarmBus()))
        await manager.restore_state()

        app = _build_test_app(db, redis_client)
        with TestClient(app) as client:
            client_any = cast(Any, client)
            health_response = cast(ResponseLike, client_any.get("/devices/dev_1/health"))
            occupancy_response = cast(
                ResponseLike, client_any.get("/rooms/room_1/occupancy?window=1h")
            )

        self.assertEqual(health_response.status_code, 200)
        health = cast(dict[str, object], health_response.json())
        self.assertEqual(set(health.keys()), {"device_id", "last_heartbeat_ts", "availability_5m"})
        self.assertEqual(health["last_heartbeat_ts"], heartbeat_ts)
        availability = cast(float, health["availability_5m"])
        self.assertGreaterEqual(availability, 0.0)
        self.assertLessEqual(availability, 1.0)

        self.assertEqual(occupancy_response.status_code, 200)
        occupancy = cast(dict[str, object], occupancy_response.json())
        self.assertEqual(set(occupancy.keys()), {"in_room", "occupied_pct", "window_seconds"})
        self.assertFalse(occupancy["in_room"])
        occupied_pct = cast(float, occupancy["occupied_pct"])
        self.assertGreater(occupied_pct, 0.0)
        self.assertLessEqual(occupied_pct, 1.0)
        self.assertEqual(occupancy["window_seconds"], 3600)

        db.close()


if __name__ == "__main__":
    unittest.main()