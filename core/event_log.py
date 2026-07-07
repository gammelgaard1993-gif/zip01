from __future__ import annotations

import json
from sqlite3 import Connection

from models import ValidatedEvent


def persist_validated_event(db_connection: Connection, event: ValidatedEvent) -> None:
    cursor = db_connection.cursor()
    cursor.execute(
        "INSERT INTO events (device_id, room_id, type, ts, payload, received_at, late) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            event.device_id,
            event.room_id,
            event.type,
            event.ts.isoformat(),
            json.dumps(event.payload),
            event.received_at.isoformat(),
            int(event.late),
        ),
    )
    db_connection.commit()
