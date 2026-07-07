from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict


class Priority(str, Enum):
    HIGH = "high"
    NORMAL = "normal"


@dataclass(frozen=True)
class RawEvent:
    device_id: str
    room_id: str
    type: str
    ts: str
    payload: Dict[str, Any]
    seq: int | None = None


@dataclass(frozen=True)
class ValidatedEvent:
    device_id: str
    room_id: str
    type: str
    ts: datetime
    payload: Dict[str, Any]
    late: bool
    priority: Priority
    received_at: datetime
    seq: int | None = None


@dataclass(frozen=True)
class AlarmEvent:
    device_id: str
    room_id: str
    ts: datetime
    confidence: float
    received_at: datetime
