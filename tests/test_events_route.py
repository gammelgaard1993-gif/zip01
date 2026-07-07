from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, cast

from fastapi import Response

from api.routes.events import ingest_event
from ingestion.queue import PriorityEventQueue
from models import Priority


class _FakeRequest:
    # Minimal stand-in for fastapi.Request: the route only touches .body() and
    # .app.state.event_queue, so we avoid a full ASGI/TestClient (and its Redis lifespan).
    def __init__(self, body: bytes, event_queue: PriorityEventQueue) -> None:
        self._body = body
        self.app = SimpleNamespace(state=SimpleNamespace(event_queue=event_queue))

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


if __name__ == "__main__":
    unittest.main()
