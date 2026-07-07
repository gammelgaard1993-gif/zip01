from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.dependencies import get_redis_client
from processing.handlers.heartbeat import HeartbeatHandler
from redis import Redis

router = APIRouter()


class DeviceHealthResponse(BaseModel):
    device_id: str
    last_heartbeat_ts: str
    availability_5m: float


@router.get("/devices/{device_id}/health", response_model=DeviceHealthResponse)
async def device_health(
    device_id: str,
    redis_client: Redis = Depends(get_redis_client),
) -> DeviceHealthResponse:
    key_last = f"device:{device_id}:last_heartbeat"
    last_heartbeat_value = redis_client.get(key_last)
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

    handler = HeartbeatHandler(redis_client)
    availability = handler.availability(device_id)
    return DeviceHealthResponse(
        device_id=device_id,
        last_heartbeat_ts=last_heartbeat,
        availability_5m=availability,
    )
