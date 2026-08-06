from __future__ import annotations

import asyncio
import json
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, AsyncIterator, cast
from unittest.mock import patch

from fastapi import Response
from starlette.datastructures import Headers

import config
from api.routes.events import ingest_event
from core.metrics import get_counters
from ingestion.queue import PriorityEventQueue
from models import Priority
from tests.fakes import FakeIngestRequest, flat_event, new_events_db


class EventsRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_accepts_valid_event_and_enqueues_with_derived_payload(self) -> None:
        queue = PriorityEventQueue(100)
        request = FakeIngestRequest(flat_event("presence", in_room=True), queue)
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
        request = FakeIngestRequest(flat_event("fall_warn", confidence=0.9), queue)
        response = Response()

        await ingest_event(cast(Any, request), response)

        self.assertEqual(response.status_code, 202)
        self.assertEqual(queue.qsize_high(), 1)
        event = await queue.get()
        self.assertEqual(event.priority, Priority.HIGH)
        self.assertEqual(event.payload, {"confidence": 0.9})

    async def test_invalid_json_returns_400(self) -> None:
        queue = PriorityEventQueue(100)
        request = FakeIngestRequest(b"not json", queue)
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
        request = FakeIngestRequest(body, queue)
        response = Response()

        result = await ingest_event(cast(Any, request), response)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(result["error"], "invalid_schema")
        self.assertTrue(queue.empty())

    async def test_future_clock_skew_returns_202_rejected_and_not_enqueued(self) -> None:
        queue = PriorityEventQueue(100)
        future_ts = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
        request = FakeIngestRequest(flat_event("heartbeat", ts=future_ts), queue)
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
        db = new_events_db()
        request = FakeIngestRequest(flat_event("heartbeat"), queue, db_connection=db)
        response = Response()

        result = await ingest_event(cast(Any, request), response)

        self.assertEqual(response.status_code, 202)
        self.assertEqual(result, {"status": "accepted"})
        self.assertEqual(db.execute("SELECT COUNT(*) FROM events").fetchone()[0], 1)

    async def test_persist_failure_returns_503_and_not_enqueued(self) -> None:
        # A storage failure must not falsely accept the event: no 202, no enqueue, so the client
        # can retry (no false accept, no silent loss).
        queue = PriorityEventQueue(100)
        request = FakeIngestRequest(flat_event("heartbeat"), queue)
        response = Response()

        with patch(
            "api.routes.events.persist_validated_event",
            side_effect=sqlite3.OperationalError("disk I/O error"),
        ):
            result = await ingest_event(cast(Any, request), response)

        self.assertEqual(response.status_code, 503)
        self.assertEqual(result, {"error": "persist_failed"})
        self.assertTrue(queue.empty())

    async def test_high_event_is_accepted_while_normal_event_is_backpressured(self) -> None:
        queue = PriorityEventQueue(1)
        db = new_events_db()

        # Fill NORMAL lane to capacity first.
        await ingest_event(
            cast(Any, FakeIngestRequest(flat_event("heartbeat", seq=1), queue, db_connection=db)),
            Response(),
        )

        blocked_normal_request = FakeIngestRequest(
            flat_event("heartbeat", seq=2), queue, db_connection=db
        )
        blocked_normal_response = Response()
        pending_normal = asyncio.create_task(
            ingest_event(cast(Any, blocked_normal_request), blocked_normal_response)
        )

        try:
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(pending_normal), timeout=0.2)

            # HIGH should bypass NORMAL backpressure and complete immediately.
            high_request = FakeIngestRequest(
                flat_event("fall_warn", confidence=0.91, seq=3), queue, db_connection=db
            )
            high_response = Response()
            high_result = await asyncio.wait_for(
                ingest_event(cast(Any, high_request), high_response), timeout=1.0
            )

            self.assertEqual(high_response.status_code, 202)
            self.assertEqual(high_result, {"status": "accepted"})

            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(pending_normal), timeout=0.2)

            # Free one NORMAL slot and ensure the blocked request resumes.
            first_out = await queue.get()
            self.assertEqual(first_out.priority, Priority.HIGH)

            second_out = await queue.get()
            self.assertEqual(second_out.priority, Priority.NORMAL)

            resumed_result = await asyncio.wait_for(pending_normal, timeout=1.0)
            self.assertEqual(blocked_normal_response.status_code, 202)
            self.assertEqual(resumed_result, {"status": "accepted"})
        finally:
            if not pending_normal.done():
                pending_normal.cancel()
                await asyncio.gather(pending_normal, return_exceptions=True)


class EventsPayloadSizeTests(unittest.IsolatedAsyncioTestCase):
    """POST /events must reject a body larger than config.MAX_EVENT_BYTES with 413 before parsing
    it, incrementing events_rejected_too_large. Covers both an oversized (truthful) Content-Length
    header and oversized actual bytes with no usable header.

    Before the fix the route buffered and parsed any size body: an oversized valid event returned
    202 accepted and the counter never moved, so these tests fail without the size guard.
    """

    def _oversized_body(self) -> bytes:
        # Valid JSON so any failure is attributable to the size guard, not a parse/validation error.
        body = flat_event("heartbeat", filler="z" * config.MAX_EVENT_BYTES)
        self.assertGreater(len(body), config.MAX_EVENT_BYTES)
        return body

    async def test_oversized_content_length_header_returns_413(self) -> None:
        queue = PriorityEventQueue(100)
        body = self._oversized_body()
        request = FakeIngestRequest(body, queue, headers={"content-length": str(len(body))})
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
        request = FakeIngestRequest(body, queue)
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
        body = flat_event("heartbeat")
        self.assertLessEqual(len(body), config.MAX_EVENT_BYTES)
        request = FakeIngestRequest(body, queue, headers={"content-length": str(len(body))})
        response = Response()

        result = await ingest_event(cast(Any, request), response)

        self.assertEqual(response.status_code, 202)
        self.assertEqual(result, {"status": "accepted"})
        self.assertEqual(queue.qsize(), 1)

    async def test_unbounded_stream_without_content_length_aborts_early(self) -> None:
        # Defends against a missing/lying Content-Length combined with a body far larger than
        # MAX_EVENT_BYTES (e.g. chunked transfer-encoding): the route must stop pulling chunks as
        # soon as the running total exceeds the cap, never fully draining the stream. Before the
        # streaming fix, `await request.body()` would have consumed the entire (here, unbounded)
        # stream before any size check ran.
        chunk_size = 4096
        chunks_yielded = 0
        # Far more data than could ever legitimately be needed to prove the cap trips.
        total_available_chunks = (config.MAX_EVENT_BYTES // chunk_size) * 1000

        class _UnboundedStreamRequest:
            # Real FastAPI requests always expose a headers mapping; omit Content-Length by
            # leaving the mapping empty.
            headers = Headers({})

            def __init__(self) -> None:
                self.app = SimpleNamespace(
                    state=SimpleNamespace(event_queue=queue, db_connection=new_events_db())
                )

            async def stream(self) -> AsyncIterator[bytes]:
                nonlocal chunks_yielded
                for _ in range(total_available_chunks):
                    chunks_yielded += 1
                    yield b"z" * chunk_size

        queue = PriorityEventQueue(100)
        request = _UnboundedStreamRequest()
        response = Response()

        before = get_counters().get("events_rejected_too_large", 0)
        result = await ingest_event(cast(Any, request), response)
        after = get_counters().get("events_rejected_too_large", 0)

        self.assertEqual(response.status_code, 413)
        self.assertEqual(result, {"error": "payload_too_large"})
        self.assertEqual(after - before, 1)
        # Aborted after only a handful of chunks -- nowhere near the full (huge) stream.
        self.assertLess(chunks_yielded * chunk_size, config.MAX_EVENT_BYTES * 2)


if __name__ == "__main__":
    unittest.main()
