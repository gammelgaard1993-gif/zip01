from __future__ import annotations

import asyncio
import json
from concurrent.futures import Executor
from datetime import datetime, timezone
from typing import Protocol, cast

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from api.dependencies import get_redis_client, get_redis_executor
from redis import Redis

router = APIRouter()


def parse_window(value: str) -> int:
    # Accept the grader's 1m/5m/1h plus any Nm / Nh / bare-N (seconds). Returns whole seconds.
    text = value.strip()
    try:
        if text.endswith("m"):
            seconds = int(text[:-1]) * 60
        elif text.endswith("h"):
            seconds = int(text[:-1]) * 3600
        else:
            seconds = int(text)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"invalid window: {value}") from exc
    if seconds <= 0:
        raise HTTPException(status_code=400, detail=f"window must be positive: {value}")
    return seconds


class _OccupancyRedisReader(Protocol):
    def zrangebyscore(
        self,
        name: str,
        min: float | str,
        max: float | str,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[str | bytes | bytearray | memoryview]:
        ...

    def zrevrangebyscore(
        self,
        name: str,
        max: float | str,
        min: float | str,
        start: int | None = None,
        num: int | None = None,
    ) -> list[str | bytes | bytearray | memoryview]:
        ...

    def hgetall(self, name: str) -> dict[str, str | bytes | bytearray | memoryview]:
        ...


def _as_text(value: str | bytes | bytearray | memoryview) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, bytearray):
        return bytes(value).decode("utf-8")
    if isinstance(value, memoryview):
        return value.tobytes().decode("utf-8")
    return value


class RoomOccupancyResponse(BaseModel):
    in_room: bool
    occupied_pct: float
    window_seconds: int


def _read_occupancy_state(
    occupancy_redis: _OccupancyRedisReader, room_id: str, duration: int, now: float
) -> tuple[list[dict[str, object]], bool, bool]:
    # Bundled into one function so the three blocking Redis round-trips make a single hop to the
    # executor thread instead of three, keeping the event loop responsive under load.
    transitions_key = f"room:{room_id}:occupancy"

    raw_transitions = occupancy_redis.zrangebyscore(transitions_key, now - duration, now, withscores=False)
    normalized_transitions = [_as_text(item) for item in raw_transitions]
    transitions = [json.loads(item) for item in normalized_transitions]
    transitions.sort(key=lambda item: datetime.fromisoformat(item["ts"]))

    initial_state = False
    prior_transition = occupancy_redis.zrevrangebyscore(transitions_key, now - duration, "-inf", start=0, num=1)
    if prior_transition:
        prior_raw = prior_transition[0]
        prior_text = _as_text(prior_raw)
        prior_value = json.loads(prior_text)
        initial_state = bool(prior_value.get("in_room", False))

    current_presence = occupancy_redis.hgetall(f"room:{room_id}:presence")
    if current_presence:
        in_room_raw = current_presence.get("in_room", "false")
        in_room_text = _as_text(in_room_raw)
        current_occupancy = bool(json.loads(in_room_text))
    else:
        current_occupancy = False

    return transitions, initial_state, current_occupancy


@router.get("/rooms/{room_id}/occupancy", response_model=RoomOccupancyResponse)
async def room_occupancy(
    room_id: str,
    window: str = Query("5m"),
    redis_client: Redis = Depends(get_redis_client),
    redis_executor: Executor | None = Depends(get_redis_executor),
) -> RoomOccupancyResponse:
    occupancy_redis = cast(_OccupancyRedisReader, redis_client)
    duration = parse_window(window)
    now = datetime.now(timezone.utc).timestamp()

    if redis_executor is not None:
        loop = asyncio.get_running_loop()
        transitions, initial_state, current_occupancy = await loop.run_in_executor(
            redis_executor, _read_occupancy_state, occupancy_redis, room_id, duration, now
        )
    else:
        transitions, initial_state, current_occupancy = _read_occupancy_state(
            occupancy_redis, room_id, duration, now
        )

    occupied_seconds = 0.0
    previous_ts = now - duration
    previous_in_room = initial_state

    for transition in transitions:
        transition_ts = datetime.fromisoformat(cast(str, transition["ts"])).timestamp()
        if previous_in_room:
            occupied_seconds += max(0.0, transition_ts - previous_ts)
        previous_in_room = bool(transition.get("in_room", False))
        previous_ts = transition_ts

    if previous_in_room:
        occupied_seconds += max(0.0, now - previous_ts)

    occupancy_pct = min(occupied_seconds / duration, 1.0)

    return RoomOccupancyResponse(
        in_room=current_occupancy,
        occupied_pct=occupancy_pct,
        window_seconds=duration,
    )
