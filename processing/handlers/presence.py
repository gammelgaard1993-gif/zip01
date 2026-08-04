from __future__ import annotations

import asyncio
import json
import logging
from concurrent.futures import Executor
from datetime import datetime, timezone
from typing import Protocol, cast

from config import OCCUPANCY_WINDOW_SECONDS
from models import ValidatedEvent
from redis import Redis

logger = logging.getLogger(__name__)


class _RedisPipeline(Protocol):
    def hset(self, name: str, mapping: dict[str, str]) -> object:
        ...

    def zadd(self, name: str, mapping: dict[str, float]) -> object:
        ...

    def zremrangebyscore(self, name: str, min: float, max: float) -> object:
        ...

    def execute(self) -> object:
        ...


class _PipelineCapableRedis(Protocol):
    def hgetall(self, name: str) -> dict[str, str | bytes | bytearray | memoryview]:
        ...

    def zrevrangebyscore(
        self,
        name: str,
        max: float | str,
        min: float | str,
        start: int = ...,
        num: int = ...,
        withscores: bool = ...,
    ) -> list[tuple[str | bytes | bytearray | memoryview, float]]:
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


class PresenceHandler:
    def __init__(self, redis_client: Redis, executor: Executor | None = None) -> None:
        self.redis = redis_client
        # Optional shared thread pool: when present, the synchronous redis-py calls below run on
        # a background thread instead of the event loop (Phase 6 / #13). Left None for recovery
        # replay and tests, which run the same code synchronously with no behavior change.
        self._executor = executor

    async def handle(self, event: ValidatedEvent) -> None:
        if self._executor is not None:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(self._executor, self._apply, event)
        else:
            self._apply(event)

    def _apply(self, event: ValidatedEvent) -> None:
        key_state = f"room:{event.room_id}:presence"
        key_transitions = f"room:{event.room_id}:occupancy"
        ts_score = event.ts.timestamp()
        ts_value = event.ts.isoformat()
        # Defense in depth: ingestion-time validation guarantees this for the live path, but
        # recovery replay rebuilds events straight from durable storage without re-validating, so
        # a non-bool value (e.g. left over from before schema validation existed) must be rejected
        # here rather than silently coerced -- bool("false") is True and would corrupt occupancy.
        in_room_raw = event.payload.get("in_room", False)
        if not isinstance(in_room_raw, bool):
            raise TypeError(f"in_room must be a boolean, got {type(in_room_raw).__name__}")
        in_room = in_room_raw
        now_score = datetime.now(timezone.utc).timestamp()

        # Cast once to the precise read/pipeline Protocol so hgetall/zrevrangebyscore/pipeline all
        # carry fully-known types instead of the concrete client's partially-Any stub returns.
        redis_client = cast(_PipelineCapableRedis, self.redis)

        current = redis_client.hgetall(key_state)
        current_dt = None
        if current:
            current_ts = current.get("ts")
            if current_ts is not None:
                try:
                    current_ts_text = _as_text(current_ts)
                    current_dt = datetime.fromisoformat(current_ts_text)
                except ValueError:
                    current_dt = None

        transition_value = json.dumps({"ts": ts_value, "in_room": in_room})
        cutoff = now_score - OCCUPANCY_WINDOW_SECONDS

        # Preserve the most recent transition at or before the window cutoff as an initial-state
        # anchor so the 1h occupancy query can recover the room's state at the window start. A
        # plain zremrangebyscore(0, cutoff) would delete the only transition of a room occupied
        # since before the window, making it report artificially low occupancy. The anchor may be
        # an existing pre-cutoff transition OR this event itself when it is a late event older
        # than the window; we trim only transitions strictly older than that anchor, keeping the
        # zset bounded to the in-window transitions plus a single anchor.
        existing_anchor = redis_client.zrevrangebyscore(
            key_transitions, cutoff, "-inf", start=0, num=1, withscores=True
        )
        anchor_scores = [entry[1] for entry in existing_anchor]
        if ts_score <= cutoff:
            anchor_scores.append(ts_score)
        trim_cutoff = (max(anchor_scores) - 1e-3) if anchor_scores else cutoff

        pipeline = redis_client.pipeline()
        pipeline.zadd(key_transitions, {transition_value: ts_score})
        if current_dt is None or event.ts > current_dt:
            pipeline.hset(key_state, mapping={"in_room": json.dumps(in_room), "ts": ts_value})
        pipeline.zremrangebyscore(key_transitions, 0, trim_cutoff)
        pipeline.execute()

        logger.info(
            json.dumps(
                {
                    "event": "presence_applied",
                    "room_id": event.room_id,
                    "device_id": event.device_id,
                    "ts": ts_value,
                    "in_room": in_room,
                    "late": event.late,
                }
            )
        )
