from __future__ import annotations

from typing import cast

from fastapi import APIRouter, Request
from pydantic import BaseModel

from core.metrics import (
    get_alarm_feed_latency_ms_p95,
    get_alarm_path_stage_latency_ms_p95,
    get_counters,
)
from ingestion.queue import PriorityEventQueue

router = APIRouter()


class MetricsResponse(BaseModel):
    counters: dict[str, int]


@router.get("/metrics")
async def metrics(request: Request) -> MetricsResponse:
    """Return runtime counters, queue depths, and alarm feed latency percentile."""
    counters = get_counters()
    raw_event_queue = getattr(request.app.state, "event_queue", None)
    if raw_event_queue is not None:
        event_queue = cast(PriorityEventQueue, raw_event_queue)
        counters["queue_depth_high"] = event_queue.qsize_high()
        counters["queue_depth_normal"] = event_queue.qsize_normal()
    sqlite_writer = getattr(request.app.state, "sqlite_writer", None)
    if sqlite_writer is not None:
        counters.update(sqlite_writer.metrics_snapshot())
    worker_pool = getattr(request.app.state, "worker_pool", None)
    if worker_pool is not None:
        counters.update(worker_pool.metrics_snapshot())
    counters["alarm_feed_latency_ms_p95"] = get_alarm_feed_latency_ms_p95()
    counters["alarm_bus_dispatch_latency_ms_p95"] = get_alarm_path_stage_latency_ms_p95(
        "alarm_bus_dispatch"
    )
    counters["sse_delivery_latency_ms_p95"] = get_alarm_path_stage_latency_ms_p95(
        "sse_delivery"
    )
    return MetricsResponse(counters=counters)
