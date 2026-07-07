from __future__ import annotations

from typing import cast

from fastapi import APIRouter, Request
from pydantic import BaseModel

from core.metrics import get_alarm_feed_latency_ms_p95, get_counters
from ingestion.queue import PriorityEventQueue

router = APIRouter()


class MetricsResponse(BaseModel):
    counters: dict[str, int]


@router.get("/metrics")
async def metrics(request: Request) -> MetricsResponse:
    counters = get_counters()
    raw_event_queue = getattr(request.app.state, "event_queue", None)
    if raw_event_queue is not None:
        event_queue = cast(PriorityEventQueue, raw_event_queue)
        counters["queue_depth_high"] = event_queue.qsize_high()
        counters["queue_depth_normal"] = event_queue.qsize_normal()
    counters["alarm_feed_latency_ms_p95"] = get_alarm_feed_latency_ms_p95()
    return MetricsResponse(counters=counters)
