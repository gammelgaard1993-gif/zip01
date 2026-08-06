from __future__ import annotations

import asyncio
from concurrent.futures import Executor

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.dependencies import get_redis_client, get_redis_executor
from processing.handlers.heartbeat import HeartbeatHandler
from redis import Redis

router = APIRouter()


class DeviceHealthResponse(BaseModel):
    device_id: str
    last_heartbeat_ts: str
    availability_5m: float


def _fetch_health(redis_client: Redis, device_id: str) -> tuple[object, float | None]:
    # Bundled into one function so both blocking Redis calls make a single hop to the executor
    # thread instead of two, keeping the event loop responsive under load. Availability is only
    # computed once the device is confirmed to exist, so a 404 costs a single Redis round-trip.
    key_last = f"device:{device_id}:last_heartbeat"
    last_heartbeat_value = redis_client.get(key_last)
    if last_heartbeat_value is None:
        return None, None
    availability = HeartbeatHandler(redis_client).availability(device_id)
    return last_heartbeat_value, availability


@router.get("/devices/{device_id}/health", response_model=DeviceHealthResponse)
async def device_health(
    device_id: str,
    redis_client: Redis = Depends(get_redis_client),
    redis_executor: Executor | None = Depends(get_redis_executor),
) -> DeviceHealthResponse:
    """Return latest heartbeat timestamp and 5-minute availability for a device."""
    if redis_executor is not None:
        loop = asyncio.get_running_loop()
        last_heartbeat_value, availability = await loop.run_in_executor(
            redis_executor, _fetch_health, redis_client, device_id
        )
    else:
        last_heartbeat_value, availability = _fetch_health(redis_client, device_id)

    if last_heartbeat_value is None:
        raise HTTPException(status_code=404, detail="device not found")
    if isinstance(last_heartbeat_value, bytes):
        last_heartbeat: str = last_heartbeat_value.decode("utf-8")
    elif isinstance(last_heartbeat_value, bytearray):
        last_heartbeat = bytes(last_heartbeat_value).decode("utf-8")
    elif isinstance(last_heartbeat_value, memoryview):
        last_heartbeat = last_heartbeat_value.tobytes().decode("utf-8")
    else:
        last_heartbeat = str(last_heartbeat_value)  

    return DeviceHealthResponse(
        device_id=device_id,
        last_heartbeat_ts=last_heartbeat,
        availability_5m=availability,
    )
