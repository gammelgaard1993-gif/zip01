from __future__ import annotations

import json
from sqlite3 import Connection
from typing import TYPE_CHECKING, Any

from models import Priority, ValidatedEvent

if TYPE_CHECKING:
    from core.db_writer import BatchedSQLiteWriter

_INSERT_EVENT_SQL = (
    "INSERT INTO events (device_id, room_id, type, ts, payload, received_at, late) "
    "VALUES (?, ?, ?, ?, ?, ?, ?)"
)


def _build_insert(event: ValidatedEvent) -> tuple[str, tuple[Any, ...]]:
    payload = dict(event.payload)
    if event.seq is not None:
        payload["seq"] = event.seq
    return (
        _INSERT_EVENT_SQL,
        (
            event.device_id,
            event.room_id,
            event.type,
            event.ts.isoformat(),
            json.dumps(payload),
            event.received_at.isoformat(),
            int(event.late),
        ),
    )


def persist_validated_event(db_connection: Connection, event: ValidatedEvent) -> int:
    sql, params = _build_insert(event)
    cursor = db_connection.cursor()
    cursor.execute(sql, params)
    db_connection.commit()
    return int(cursor.lastrowid)


async def persist_validated_event_async(writer: "BatchedSQLiteWriter", event: ValidatedEvent) -> int:
    # Routes through the dedicated single-writer thread instead of committing on the loop thread.
    # fall_warn admission writes skip the batch wait (priority) so the alarm latency budget is
    # never taxed by NORMAL-lane batching; the awaited future only resolves after a real commit,
    # so persist-before-ack still holds.
    sql, params = _build_insert(event)
    _, durable_event_id = await writer.submit(sql, params, priority=event.priority == Priority.HIGH)
    if durable_event_id is None:
        raise RuntimeError("durable event insert did not return an ID")
    return durable_event_id

