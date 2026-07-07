from __future__ import annotations

import json
import logging
from typing import Any, cast

from fastapi import APIRouter, Request, Response

from core.metrics import increment_counter
from ingestion.queue import PriorityEventQueue
from ingestion.validator import ValidationError, validate_raw_event
from models import Priority

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/events")
async def ingest_event(request: Request, response: Response) -> dict[str, Any]:
    # the reference generator POSTs one flat JSON event per request. This route
    # mirrors the MQTT subscriber's validate -> enqueue path, using the HTTP response as the
    # backpressure signal (a full NORMAL lane makes event_queue.put await, delaying the reply
    # instead of dropping the event).
    increment_counter("events_ingested_total")

    # 1) Parse body. Non-JSON or non-object -> 400.
    try:
        raw = json.loads(await request.body())
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

    # 3) Enqueue. HIGH returns immediately; a full NORMAL lane awaits capacity (backpressure:
    #    the HTTP response is delayed, the event is never dropped).
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
