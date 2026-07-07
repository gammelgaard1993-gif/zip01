from __future__ import annotations

from typing import Protocol, cast

from fastapi import Request
from redis import Redis
from sqlite3 import Connection
from processing.alarm_bus import AlarmBus


class _AppState(Protocol):
    redis_client: Redis
    db_connection: Connection
    alarm_bus: AlarmBus


def _typed_state(request: Request) -> _AppState:
    return cast(_AppState, request.app.state)


def get_redis_client(request: Request) -> Redis:
    return _typed_state(request).redis_client


def get_db_connection(request: Request) -> Connection:
    return _typed_state(request).db_connection


def get_alarm_bus(request: Request) -> AlarmBus:
    return _typed_state(request).alarm_bus
