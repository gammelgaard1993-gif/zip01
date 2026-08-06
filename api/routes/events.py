from __future__ import annotations

import json
import logging
import sqlite3
from typing import Any, cast

from fastapi import APIRouter, Request, Response

import config
from core.event_log import persist_validated_event, persist_validated_event_async
from core.db_writer import SQLiteWriterError
from core.metrics import increment_counter
from ingestion.queue import PriorityEventQueue
from ingestion.validator import ValidationError, validate_raw_event
from models import Priority

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/events")
async def ingest_event(request: Request, response: Response) -> dict[str, Any]:
    """Validate, durably persist, and enqueue one incoming device event."""
    # the reference generator POSTs one flat JSON event per request. This route uses the HTTP
    # response as the backpressure signal (a full NORMAL lane makes event_queue.put await,
    # delaying the reply instead of dropping the event).
    increment_counter("events_ingested_total")

    # 0) Bound per-request memory. Reject an oversized body before buffering/parsing it: check the
    #    declared Content-Length first, then the actual bytes (defends against a missing/lying header).
    headers = getattr(request, "headers", None)
    declared_length = headers.get("content-length") if headers is not None else None
    if declared_length is not None:
        try:
            if int(declared_length) > config.MAX_EVENT_BYTES:
                increment_counter("events_rejected_too_large")
                response.status_code = 413
                return {"error": "payload_too_large"}
        except (TypeError, ValueError):
            pass

    # A declared Content-Length over the cap is a fast reject, but the header is untrusted (it can
    # be missing or lie) and `request.body()` would buffer the whole thing before we can check it.
    # Read the body incrementally instead and abort as soon as the running total exceeds the cap,
    # so an oversized/chunked-transfer body is never fully materialized in memory.
    chunks: list[bytes] = []
    total_bytes = 0
    async for chunk in request.stream():
        total_bytes += len(chunk)
        if total_bytes > config.MAX_EVENT_BYTES:
            increment_counter("events_rejected_too_large")
            response.status_code = 413
            return {"error": "payload_too_large"}
        chunks.append(chunk)
    body = b"".join(chunks)

    # 1) Parse body. Non-JSON or non-object -> 400.
    try:
        raw = json.loads(body)
    except (ValueError, json.JSONDecodeError):
        increment_counter("events_rejected_invalid_json")
        response.status_code = 400
        return {"error": "invalid_json"}
    if not isinstance(raw, dict):
        increment_counter("events_rejected_invalid_json")
        response.status_code = 400
        return {"error": "invalid_json"}
    event: dict[str, Any] = cast(dict[str, Any], raw)

    # 2) Validate.
    try:
        validated = validate_raw_event(event)
    except ValidationError as exc:
        reason = getattr(exc, "reason", "validation_error")
        if reason in {"clock_skew_future", "clock_skew_past"}:
            increment_counter("events_rejected_clock_skew")
            increment_counter(f"events_rejected_{reason}")
            logger.warning(
                json.dumps(
                    {
                        "event": "clock_skew",
                        "device_id": event.get("device_id"),
                        "type": event.get("type"),
                        "reason": reason,
                        "offset_seconds": getattr(exc, "offset_seconds", None),
                    }
                )
            )
            # Received but dropped by the acceptance rules -> still a 202.
            response.status_code = 202
            return {"status": "rejected", "reason": reason}
        increment_counter("events_rejected_invalid_schema")
        logger.warning(
            json.dumps(
                {
                    "event": "validation_reject",
                    "device_id": event.get("device_id"),
                    "type": event.get("type"),
                    "reason": reason,
                }
            )
        )
        response.status_code = 400
        return {"error": reason}

    # 3) Persist-before-ack: write the durable `events` row BEFORE returning 202, so an accepted
    #    event survives a crash even though its hot-state handler runs later. On a storage error
    #    the event is NOT acknowledged (503) and NOT enqueued, so the client can retry (no false
    #    accept, no silent loss). Routes through the dedicated batched writer when present (keeps
    #    the loop unblocked); falls back to a direct synchronous write otherwise (unit tests /
    #    any deployment that hasn't wired one up).
    db_connection: sqlite3.Connection = request.app.state.db_connection
    sqlite_writer = getattr(request.app.state, "sqlite_writer", None)
    try:
        if sqlite_writer is not None:
            await persist_validated_event_async(sqlite_writer, validated)
        else:
            persist_validated_event(db_connection, validated)
    except (sqlite3.Error, SQLiteWriterError) as exc:
        increment_counter("events_persist_failed")
        logger.error(
            json.dumps(
                {
                    "event": "persist_failed",
                    "device_id": validated.device_id,
                    "type": validated.type,
                    "reason": "writer_unavailable" if isinstance(exc, SQLiteWriterError) else "sqlite_error",
                }
            )
        )
        response.status_code = 503
        return {"error": "persist_failed"}

    # Register the event as in-flight (admission -> applied) so a snapshot taken while it is still
    # queued/buffered uses a replay cutoff old enough to re-cover it on recovery.
    worker_pool = getattr(request.app.state, "worker_pool", None)
    if worker_pool is not None:
        worker_pool.mark_inflight(validated.received_at.isoformat())

    # 4) Enqueue for hot-state processing. HIGH returns immediately; a full NORMAL lane awaits
    #    capacity (backpressure: the HTTP response is delayed, the event is never dropped).
    event_queue: PriorityEventQueue = request.app.state.event_queue
    if validated.priority == Priority.NORMAL and event_queue.normal_is_full():
        increment_counter("queue_pressure")
        logger.warning(
            json.dumps(
                {
                    "event": "queue_pressure",
                    "lane": "NORMAL",
                    "depth": event_queue.qsize_normal(),
                }
            )
        )
    await event_queue.put(validated)

    logger.info(
        json.dumps(
            {
                "event": "ingested",
                "device_id": validated.device_id,
                "type": validated.type,
                "late": validated.late,
            }
        )
    )
    response.status_code = 202
    return {"status": "accepted"}
