from __future__ import annotations

from sqlite3 import Connection

from models import ValidatedEvent


class GenericEventHandler:
    # Fallback handler for event types with no type-specific hot-state (motion, sleep_state,
    # net_status). Durable persistence is already done by the worker before dispatch, so there is
    # nothing left to do here — this exists so unknown/plain types still route to a valid handler.
    def __init__(self, db_connection: Connection) -> None:
        # Held for interface symmetry with the other handlers; intentionally unused.
        self.db_connection: Connection = db_connection

    async def handle(self, event: ValidatedEvent) -> None:
        return None
