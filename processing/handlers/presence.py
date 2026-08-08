from __future__ import annotations

import asyncio
import json
import logging
from concurrent.futures import Executor
from datetime import datetime, timezone
from typing import Protocol, cast

from config import OCCUPANCY_WINDOW_SECONDS
from core.metrics import increment_counter
from models import ValidatedEvent
from redis import Redis
from redis.exceptions import WatchError

logger = logging.getLogger(__name__)


class _RedisPipeline(Protocol):
    def watch(self, *names: str) -> object:
        ...

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

    def zrangebyscore(
        self,
        name: str,
        min: float | str,
        max: float | str,
    ) -> list[str | bytes | bytearray | memoryview]:
        ...

    def multi(self) -> object:
        ...

    def hset(self, name: str, mapping: dict[str, str]) -> object:
        ...

    def zadd(self, name: str, mapping: dict[str, float]) -> object:
        ...

    def zremrangebyscore(
        self, name: str, min: float | str, max: float | str
    ) -> object:
        ...

    def execute(self) -> object:
        ...

    def reset(self) -> None:
        ...


class _PipelineCapableRedis(Protocol):
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
        tie_breaker = f"{event.device_id}:{int(in_room)}"

        # Cast once to the precise read/pipeline Protocol so hgetall/zrevrangebyscore/pipeline all
        # carry fully-known types instead of the concrete client's partially-Any stub returns.
        redis_client = cast(_PipelineCapableRedis, self.redis)

        transition_value = json.dumps(
            {"ts": ts_value, "in_room": in_room, "tie_breaker": tie_breaker}
        )
        cutoff = now_score - OCCUPANCY_WINDOW_SECONDS

        while True:
            pipeline = redis_client.pipeline()
            try:
                pipeline.watch(key_state, key_transitions)
                current = pipeline.hgetall(key_state)
                current_dt = None
                current_tie_breaker = ""
                if current:
                    current_ts = current.get("ts")
                    if current_ts is not None:
                        try:
                            current_dt = datetime.fromisoformat(_as_text(current_ts))
                        except ValueError:
                            current_dt = None
                    raw_tie_breaker = current.get("tie_breaker")
                    if raw_tie_breaker is not None:
                        current_tie_breaker = _as_text(raw_tie_breaker)

                should_update_state = (
                    current_dt is None
                    or event.ts > current_dt
                    or (event.ts == current_dt and tie_breaker > current_tie_breaker)
                )
                same_timestamp_transitions = pipeline.zrangebyscore(
                    key_transitions, ts_score, ts_score
                )
                existing_tie_breakers: list[str] = []
                for raw_transition in same_timestamp_transitions:
                    try:
                        transition = json.loads(_as_text(raw_transition))
                    except (TypeError, ValueError):
                        continue
                    if isinstance(transition, dict):
                        existing_tie_breakers.append(str(transition.get("tie_breaker", "")))
                should_update_transition = (
                    not existing_tie_breakers
                    or tie_breaker > max(existing_tie_breakers)
                )

                # Preserve the most recent transition at or before the window cutoff as an
                # initial-state anchor. Watching both keys makes the comparison, anchor choice,
                # insertion, and trimming one optimistic transaction across concurrent devices.
                existing_anchor = pipeline.zrevrangebyscore(
                    key_transitions, cutoff, "-inf", start=0, num=1, withscores=True
                )
                anchor_scores = [entry[1] for entry in existing_anchor]
                if ts_score <= cutoff:
                    anchor_scores.append(ts_score)
                trim_cutoff: float | str = (
                    f"({max(anchor_scores)}" if anchor_scores else cutoff
                )

                pipeline.multi()
                if should_update_transition:
                    pipeline.zremrangebyscore(key_transitions, ts_score, ts_score)
                    pipeline.zadd(key_transitions, {transition_value: ts_score})
                if should_update_state:
                    pipeline.hset(
                        key_state,
                        mapping={
                            "in_room": json.dumps(in_room),
                            "ts": ts_value,
                            "tie_breaker": tie_breaker,
                        },
                    )
                pipeline.zremrangebyscore(key_transitions, 0, trim_cutoff)
                pipeline.execute()
                break
            except WatchError:
                increment_counter("presence_watch_conflicts")
                continue
            finally:
                pipeline.reset()

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
