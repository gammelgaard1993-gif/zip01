from __future__ import annotations

from typing import Protocol

from models import ValidatedEvent


class EventHandler(Protocol):
    async def handle(self, event: ValidatedEvent) -> None:
        ...
