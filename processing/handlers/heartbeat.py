from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Protocol, cast

from config import HEARTBEAT_WINDOW_SECONDS
from models import ValidatedEvent
from redis import Redis

logger = logging.getLogger(__name__)


class _RedisPipeline(Protocol):
    def set(self, name: str, value: str) -> object:
        ...

    def zadd(self, name: str, mapping: dict[str, float]) -> object:
        ...

    def zremrangebyscore(self, name: str, min: float, max: float) -> object:
        ...

    def execute(self) -> object:
        ...


class _PipelineCapableRedis(Protocol):
    def get(self, name: str) -> str | bytes | bytearray | memoryview | None:
        ...

    def pipeline(self) -> _RedisPipeline:
        ...


def _as_text(value: str | bytes | bytearray | memoryview) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, bytearray):
        return bytes(value).decode("utf-8")
    if isinstance(value, memoryview):
        return value.tobytes().decode("utf-8")
    return value


class HeartbeatHandler:
    def __init__(self, redis_client: Redis) -> None:
        self.redis = redis_client

    async def handle(self, event: ValidatedEvent) -> None:
        key_last = f"device:{event.device_id}:last_heartbeat"
        key_set = f"device:{event.device_id}:heartbeats"
        ts_score = event.ts.timestamp()
        ts_value = event.ts.isoformat()
        now_score = datetime.now(timezone.utc).timestamp()

        redis_client = cast(_PipelineCapableRedis, self.redis)
        current_last_raw = redis_client.get(key_last)
        # ts guard keeps last_heartbeat monotonic: a late/replayed beat must never overwrite a
        # newer one already recorded, so the handler stays idempotent under out-of-order replay.
        should_update_last = True
        if current_last_raw is not None:
            try:
                current_last = datetime.fromisoformat(_as_text(current_last_raw))
            except ValueError:
                current_last = None
            if current_last is not None and current_last >= event.ts:
                should_update_last = False

        pipeline = redis_client.pipeline()
        if should_update_last:
            pipeline.set(key_last, ts_value)
        pipeline.zadd(key_set, {ts_value: ts_score})
        cutoff = now_score - HEARTBEAT_WINDOW_SECONDS
        pipeline.zremrangebyscore(key_set, 0, cutoff)
        pipeline.execute()

        logger.info(
            json.dumps(
                {
                    "event": "heartbeat_applied",
                    "device_id": event.device_id,
                    "ts": ts_value,
                    "late": event.late,
                    "updated_last": should_update_last,
                }
            )
        )

    def availability(self, device_id: str) -> float:
        # Availability = fraction of the window that carried a heartbeat, one beat/sec expected.
        # Count in-window beats and normalize by the window length, clamped to 1.0.
        key_set = f"device:{device_id}:heartbeats"
        now_score = datetime.now(timezone.utc).timestamp()
        window_start = now_score - HEARTBEAT_WINDOW_SECONDS
        count = self.redis.zcount(key_set, window_start, now_score)
        return min(count / HEARTBEAT_WINDOW_SECONDS, 1.0)
