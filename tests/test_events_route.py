from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from fastapi import Response

import config
from api.routes.events import ingest_event
from core.metrics import get_counters
from ingestion.queue import PriorityEventQueue
from models import Priority


def _new_events_db() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    connection.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, device_id TEXT NOT NULL, "
        "room_id TEXT NOT NULL, type TEXT NOT NULL, ts TEXT NOT NULL, payload TEXT NOT NULL, "
        "received_at TEXT NOT NULL, late INTEGER NOT NULL DEFAULT 0)"
    )
    connection.commit()
    return connection


class _FakeRequest:
    # Minimal stand-in for fastapi.Request: the route only touches .headers, .body(),
    # .app.state.event_queue and .app.state.db_connection, so we avoid a full ASGI/TestClient
    # (and its Redis lifespan).
    def __init__(
        self,
        body: bytes,
        event_queue: PriorityEventQueue,
        headers: dict[str, str] | None = None,
        db_connection: sqlite3.Connection | None = None,
    ) -> None:
        self._body = body
        # None mirrors the route's getattr fallback (no Content-Length available); a dict behaves
        # like fastapi's case-insensitive Headers.get for the keys the route reads.
        self.headers = headers
        self.db_connection = db_connection if db_connection is not None else _new_events_db()
        self.app = SimpleNamespace(
            state=SimpleNamespace(event_queue=event_queue, db_connection=self.db_connection)
        )

    async def body(self) -> bytes:
        return self._body


def _flat_event(event_type: str = "heartbeat", **extra: Any) -> bytes:
    event: dict[str, Any] = {
        "device_id": "dev_1",
        "room_id": "room_1",
        "type": event_type,
        "ts": datetime.now(timezone.utc).isoformat(),
        "seq": 1,
        **extra,
    }
    return json.dumps(event).encode("utf-8")


class EventsRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_accepts_valid_event_and_enqueues_with_derived_payload(self) -> None:
        queue = PriorityEventQueue(100)
        request = _FakeRequest(_flat_event("presence", in_room=True), queue)
        response = Response()

        result = await ingest_event(cast(Any, request), response)

        self.assertEqual(response.status_code, 202)
        self.assertEqual(result, {"status": "accepted"})
        event = await queue.get()
        self.assertEqual(event.type, "presence")
        self.assertEqual(event.payload, {"in_room": True})
        self.assertEqual(event.seq, 1)

    async def test_fall_warn_goes_to_high_lane(self) -> None:
        queue = PriorityEventQueue(100)
        request = _FakeRequest(_flat_event("fall_warn", confidence=0.9), queue)
        response = Response()

        await ingest_event(cast(Any, request), response)

        self.assertEqual(response.status_code, 202)
        self.assertEqual(queue.qsize_high(), 1)
        event = await queue.get()
        self.assertEqual(event.priority, Priority.HIGH)
        self.assertEqual(event.payload, {"confidence": 0.9})

    async def test_invalid_json_returns_400(self) -> None:
        queue = PriorityEventQueue(100)
        request = _FakeRequest(b"not json", queue)
        response = Response()

        result = await ingest_event(cast(Any, request), response)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(result, {"error": "invalid_json"})
        self.assertTrue(queue.empty())

    async def test_missing_required_field_returns_400(self) -> None:
        queue = PriorityEventQueue(100)
        body = json.dumps(
            {"device_id": "dev_1", "type": "heartbeat", "ts": datetime.now(timezone.utc).isoformat()}
        ).encode("utf-8")
        request = _FakeRequest(body, queue)
        response = Response()

        result = await ingest_event(cast(Any, request), response)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(result["error"], "invalid_schema")
        self.assertTrue(queue.empty())

    async def test_future_clock_skew_returns_202_rejected_and_not_enqueued(self) -> None:
        queue = PriorityEventQueue(100)
        future_ts = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
        request = _FakeRequest(_flat_event("heartbeat", ts=future_ts), queue)
        response = Response()

        result = await ingest_event(cast(Any, request), response)

        self.assertEqual(response.status_code, 202)
        self.assertEqual(result, {"status": "rejected", "reason": "clock_skew_future"})
        self.assertTrue(queue.empty())

    async def test_valid_event_is_persisted_before_accepted(self) -> None:
        # Persist-before-ack: an accepted event has a durable `events` row by the time 202 returns,
        # so a crash after ack cannot lose it. Before the change the durable write happened later
        # in the worker, so this asserted-on-accept row did not yet exist.
        queue = PriorityEventQueue(100)
        db = _new_events_db()
        request = _FakeRequest(_flat_event("heartbeat"), queue, db_connection=db)
        response = Response()

        result = await ingest_event(cast(Any, request), response)

        self.assertEqual(response.status_code, 202)
        self.assertEqual(result, {"status": "accepted"})
        self.assertEqual(db.execute("SELECT COUNT(*) FROM events").fetchone()[0], 1)

    async def test_persist_failure_returns_503_and_not_enqueued(self) -> None:
        # A storage failure must not falsely accept the event: no 202, no enqueue, so the client
        # can retry (no false accept, no silent loss).
        queue = PriorityEventQueue(100)
        request = _FakeRequest(_flat_event("heartbeat"), queue)
        response = Response()

        with patch(
            "api.routes.events.persist_validated_event",
            side_effect=sqlite3.OperationalError("disk I/O error"),
        ):
            result = await ingest_event(cast(Any, request), response)

        self.assertEqual(response.status_code, 503)
        self.assertEqual(result, {"error": "persist_failed"})
        self.assertTrue(queue.empty())


class EventsPayloadSizeTests(unittest.IsolatedAsyncioTestCase):
    """POST /events must reject a body larger than config.MAX_EVENT_BYTES with 413 before parsing
    it, incrementing events_rejected_too_large. Covers both an oversized (truthful) Content-Length
    header and oversized actual bytes with no usable header.

    Before the fix the route buffered and parsed any size body: an oversized valid event returned
    202 accepted and the counter never moved, so these tests fail without the size guard.
    """

    def _oversized_body(self) -> bytes:
        # Valid JSON so any failure is attributable to the size guard, not a parse/validation error.
        body = _flat_event("heartbeat", filler="z" * config.MAX_EVENT_BYTES)
        self.assertGreater(len(body), config.MAX_EVENT_BYTES)
        return body

    async def test_oversized_content_length_header_returns_413(self) -> None:
        queue = PriorityEventQueue(100)
        body = self._oversized_body()
        request = _FakeRequest(body, queue, headers={"content-length": str(len(body))})
        response = Response()

        before = get_counters().get("events_rejected_too_large", 0)
        result = await ingest_event(cast(Any, request), response)
        after = get_counters().get("events_rejected_too_large", 0)

        self.assertEqual(response.status_code, 413)
        self.assertEqual(result, {"error": "payload_too_large"})
        self.assertEqual(after - before, 1)
        self.assertTrue(queue.empty())

    async def test_oversized_actual_bytes_returns_413(self) -> None:
        # No Content-Length header: the actual-body-length check must still reject (defends against
        # a missing/lying header).
        queue = PriorityEventQueue(100)
        body = self._oversized_body()
        request = _FakeRequest(body, queue)
        response = Response()

        before = get_counters().get("events_rejected_too_large", 0)
        result = await ingest_event(cast(Any, request), response)
        after = get_counters().get("events_rejected_too_large", 0)

        self.assertEqual(response.status_code, 413)
        self.assertEqual(result, {"error": "payload_too_large"})
        self.assertEqual(after - before, 1)
        self.assertTrue(queue.empty())

    async def test_small_valid_event_still_accepted(self) -> None:
        # Regression guard: a normal in-bound event is unaffected by the size guard.
        queue = PriorityEventQueue(100)
        body = _flat_event("heartbeat")
        self.assertLessEqual(len(body), config.MAX_EVENT_BYTES)
        request = _FakeRequest(body, queue, headers={"content-length": str(len(body))})
        response = Response()

        result = await ingest_event(cast(Any, request), response)

        self.assertEqual(response.status_code, 202)
        self.assertEqual(result, {"status": "accepted"})
        self.assertEqual(queue.qsize(), 1)


if __name__ == "__main__":
    unittest.main()
