from __future__ import annotations

import json
from datetime import datetime, timezone
from sqlite3 import Connection
from typing import Any, AsyncIterator

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from api.dependencies import get_alarm_bus, get_db_connection
from models import AlarmEvent
from processing.alarm_bus import AlarmBus

router = APIRouter()


class AlarmListItem(BaseModel):
    device_id: str
    room_id: str
    ts: str
    confidence: float
    received_at: str


class AlarmsResponse(BaseModel):
    alarms: list[AlarmListItem]
    since: float


@router.get("/alarms", response_model=AlarmsResponse)
async def get_alarms(
    since: float = 0.0,
    room_id: str | None = None,
    db_connection: Connection = Depends(get_db_connection),
) -> AlarmsResponse:
    # `since` is a float epoch (matches the reference stub). Convert to a UTC ISO string so it
    # compares lexically against the stored `ts` (also UTC isoformat). Defaults to 0 (epoch),
    # which returns the full history.
    since_iso = datetime.fromtimestamp(since, tz=timezone.utc).isoformat()
    cursor = db_connection.cursor()
    query = "SELECT device_id, room_id, ts, confidence, received_at FROM fall_warnings"
    params: list[Any] = [since_iso]
    clauses: list[str] = ["ts >= ?"]

    if room_id is not None:
        clauses.append("room_id = ?")
        params.append(room_id)
    query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY ts ASC"

    cursor.execute(query, tuple(params))
    rows = cursor.fetchall()
    alarms = [
        AlarmListItem(
            device_id=device_id,
            room_id=row_room_id,
            ts=ts,
            confidence=confidence,
            received_at=received_at,
        )
        for device_id, row_room_id, ts, confidence, received_at in rows
    ]
    return AlarmsResponse(alarms=alarms, since=since)


def _sse_payload(alarm: AlarmEvent) -> str:
    payload = json.dumps(
        {
            "device_id": alarm.device_id,
            "room_id": alarm.room_id,
            "ts": alarm.ts.isoformat(),
            "confidence": alarm.confidence,
            "received_at": alarm.received_at.isoformat(),
        }
    )
    return f"data: {payload}\n\n"


@router.get("/alarms/stream")
async def alarms_stream(
    room_id: str = Query(...),
    since: str | None = None,
    db_connection: Connection = Depends(get_db_connection),
    alarm_bus: AlarmBus = Depends(get_alarm_bus),
) -> StreamingResponse:
    async def event_generator() -> AsyncIterator[str]:
        if since is not None:
            cursor = db_connection.cursor()
            cursor.execute(
                "SELECT device_id, room_id, ts, confidence, received_at FROM fall_warnings WHERE room_id = ? AND ts >= ? ORDER BY ts ASC",
                (room_id, since),
            )
            for device_id, row_room_id, ts, confidence, received_at in cursor.fetchall():
                payload = json.dumps(
                    {
                        "device_id": device_id,
                        "room_id": row_room_id,
                        "ts": ts,
                        "confidence": confidence,
                        "received_at": received_at,
                    }
                )
                yield f"data: {payload}\n\n"

        queue = await alarm_bus.subscribe(room_id)
        try:
            while True:
                alarm = await queue.get()
                # Feed latency is observed centrally in AlarmBus._dispatch_room (at dispatch time),
                # so it is measured even when no SSE client is connected. Sampling here as well
                # would double-count, so the stream only delivers frames.
                yield _sse_payload(alarm)
        finally:
            await alarm_bus.unsubscribe(room_id, queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")
