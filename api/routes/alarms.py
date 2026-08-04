from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from sqlite3 import Connection
from typing import AsyncIterator

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

import config
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


def _validated_since_iso(since: float) -> str:
    now = datetime.now(timezone.utc).timestamp()
    max_since = now + config.EVENT_FUTURE_LIMIT.total_seconds()
    if not math.isfinite(since) or since < 0.0 or since > max_since:
        raise HTTPException(status_code=400, detail="invalid since")
    try:
        return datetime.fromtimestamp(since, tz=timezone.utc).isoformat()
    except (OverflowError, OSError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="invalid since") from exc


@router.get("/alarms")
async def get_alarms(
    since: float = 0.0,
    room_id: str | None = None,
    db_connection: Connection = Depends(get_db_connection),
) -> StreamingResponse:
    # `since` is a float epoch (matches the reference stub). Convert to a UTC ISO string so it
    # compares lexically against the stored `ts` (also UTC isoformat). Defaults to 0 (epoch),
    # which returns the full history.
    since_iso = _validated_since_iso(since)

    async def response_body() -> AsyncIterator[str]:
        high_water_cursor = db_connection.cursor()
        high_water_query = "SELECT COALESCE(MAX(id), 0) FROM fall_warnings WHERE ts >= ?"
        high_water_params: list[object] = [since_iso]
        if room_id is not None:
            high_water_query += " AND room_id = ?"
            high_water_params.append(room_id)
        high_water_cursor.execute(high_water_query, tuple(high_water_params))
        high_water_id = int(high_water_cursor.fetchone()[0])

        yield '{"alarms":['
        first_item = True
        last_ts = ""
        last_id = 0
        while True:
            cursor = db_connection.cursor()
            query = (
                "SELECT id, device_id, room_id, ts, confidence, received_at "
                "FROM fall_warnings WHERE ts >= ? AND id <= ? "
                "AND (ts > ? OR (ts = ? AND id > ?))"
            )
            params: list[object] = [since_iso, high_water_id, last_ts, last_ts, last_id]
            if room_id is not None:
                query += " AND room_id = ?"
                params.append(room_id)
            query += " ORDER BY ts ASC, id ASC LIMIT ?"
            params.append(config.ALARM_REPLAY_BATCH_SIZE)
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()

            for row_id, device_id, row_room_id, ts, confidence, received_at in rows:
                item = AlarmListItem(
                    device_id=device_id,
                    room_id=row_room_id,
                    ts=ts,
                    confidence=confidence,
                    received_at=received_at,
                )
                prefix = "" if first_item else ","
                yield prefix + item.model_dump_json()
                first_item = False
                last_ts = ts
                last_id = row_id

            if len(rows) < config.ALARM_REPLAY_BATCH_SIZE:
                break

        yield f'],"since":{json.dumps(since)}}}'

    return StreamingResponse(response_body(), media_type="application/json")


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


def _persisted_sse_payload(
    device_id: str,
    room_id: str,
    ts: str,
    confidence: float,
    received_at: str,
) -> str:
    payload = json.dumps(
        {
            "device_id": device_id,
            "room_id": room_id,
            "ts": ts,
            "confidence": confidence,
            "received_at": received_at,
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
        queue = await alarm_bus.subscribe(room_id)
        try:
            replay_high_water = 0
            if since is not None:
                high_water_cursor = db_connection.cursor()
                high_water_cursor.execute(
                    "SELECT COALESCE(MAX(id), 0) FROM fall_warnings WHERE room_id = ?",
                    (room_id,),
                )
                replay_high_water = int(high_water_cursor.fetchone()[0])

                last_ts = ""
                last_id = 0
                while True:
                    cursor = db_connection.cursor()
                    cursor.execute(
                        "SELECT id, device_id, room_id, ts, confidence, received_at "
                        "FROM fall_warnings "
                        "WHERE room_id = ? AND ts >= ? AND id <= ? "
                        "AND (ts > ? OR (ts = ? AND id > ?)) "
                        "ORDER BY ts ASC, id ASC LIMIT ?",
                        (
                            room_id,
                            since,
                            replay_high_water,
                            last_ts,
                            last_ts,
                            last_id,
                            config.ALARM_REPLAY_BATCH_SIZE,
                        ),
                    )
                    rows = cursor.fetchall()
                    for row_id, device_id, row_room_id, ts, confidence, received_at in rows:
                        yield _persisted_sse_payload(
                            device_id, row_room_id, ts, confidence, received_at
                        )
                        last_ts = ts
                        last_id = row_id
                    if len(rows) < config.ALARM_REPLAY_BATCH_SIZE:
                        break

            while True:
                alarm = await queue.get()
                if (
                    since is not None
                    and alarm.fall_warning_id is not None
                    and alarm.fall_warning_id <= replay_high_water
                    and alarm.ts.isoformat() >= since
                ):
                    continue
                # Feed latency is observed centrally in AlarmBus._dispatch_room (at dispatch time),
                # so it is measured even when no SSE client is connected. Sampling here as well
                # would double-count, so the stream only delivers frames.
                yield _sse_payload(alarm)
        finally:
            await alarm_bus.unsubscribe(room_id, queue)

    return StreamingResponse(event_generator(), media_type="text/event-stream")
