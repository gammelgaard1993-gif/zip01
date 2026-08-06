from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from fnmatch import fnmatch
from types import SimpleNamespace
from typing import Any, AsyncIterator, Protocol, cast

from starlette.datastructures import Headers

from ingestion.queue import PriorityEventQueue


class _Pipeline:
    def __init__(self, redis: "FakeRedis") -> None:
        self._redis = redis
        self._ops: list[tuple[str, tuple[object, ...]]] = []

    def set(self, name: str, value: str) -> "_Pipeline":
        self._ops.append(("set", (name, value)))
        return self

    def hset(self, name: str, mapping: dict[str, str]) -> "_Pipeline":
        self._ops.append(("hset", (name, mapping)))
        return self

    def zadd(self, name: str, mapping: dict[str, float]) -> "_Pipeline":
        self._ops.append(("zadd", (name, mapping)))
        return self

    def zremrangebyscore(self, name: str, min: float, max: float) -> "_Pipeline":
        self._ops.append(("zremrangebyscore", (name, min, max)))
        return self

    def get(self, name: str) -> "_Pipeline":
        self._ops.append(("get", (name,)))
        return self

    def hgetall(self, name: str) -> "_Pipeline":
        self._ops.append(("hgetall", (name,)))
        return self

    def zrange(self, name: str, start: int, end: int, withscores: bool = False) -> "_Pipeline":
        self._ops.append(("zrange", (name, start, end, withscores)))
        return self

    def execute(self) -> list[object]:
        results: list[object] = []
        for op, args in self._ops:
            if op == "set":
                name, value = args
                self._redis.strings[str(name)] = str(value)
                results.append(True)
            elif op == "hset":
                name, mapping = args
                self._redis.hashes[str(name)] = dict(cast(dict[str, str], mapping))
                results.append(True)
            elif op == "zadd":
                name, mapping = args
                zset = self._redis.zsets.setdefault(str(name), {})
                zset.update(cast(dict[str, float], mapping))
                results.append(True)
            elif op == "zremrangebyscore":
                name, min_score, max_score = args
                zset = self._redis.zsets.get(str(name), {})
                min_bound = float(cast(float | str, min_score))
                max_bound = float(cast(float | str, max_score))
                to_remove = [m for m, score in zset.items() if min_bound <= score <= max_bound]
                for member in to_remove:
                    zset.pop(member, None)
                results.append(len(to_remove))
            elif op == "get":
                name = str(args[0])
                results.append(self._redis.get(name))
            elif op == "hgetall":
                name = str(args[0])
                results.append(self._redis.hgetall(name))
            elif op == "zrange":
                name, start, end, withscores = args
                results.append(
                    self._redis.zrange(
                        str(name),
                        int(cast(int | str, start)),
                        int(cast(int | str, end)),
                        withscores=bool(withscores),
                    )
                )
        self._ops = []
        return results


class FakeRedis:
    def __init__(self) -> None:
        self.strings: dict[str, str] = {}
        self._store = self.strings
        self.hashes: dict[str, dict[str, str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    def keys(self, pattern: str) -> list[str]:
        keys: list[str] = []
        keys.extend([key for key in self.strings if fnmatch(key, pattern)])
        keys.extend([key for key in self.hashes if fnmatch(key, pattern)])
        keys.extend([key for key in self.zsets if fnmatch(key, pattern)])
        return keys

    def type(self, name: str) -> str:
        if name in self.strings:
            return "string"
        if name in self.hashes:
            return "hash"
        if name in self.zsets:
            return "zset"
        return "none"

    def pipeline(self) -> _Pipeline:
        return _Pipeline(self)

    def get(self, name: str) -> str | None:
        return self.strings.get(name)

    def set(self, name: str, value: str, *args: object, **kwargs: object) -> object:
        nx = bool(kwargs.get("nx", False))
        if nx and name in self.strings:
            return False
        self.strings[name] = value
        return True

    def hgetall(self, name: str) -> dict[str, str]:
        return dict(self.hashes.get(name, {}))

    def hset(self, name: str, mapping: dict[str, str]) -> object:
        self.hashes[name] = dict(mapping)
        return True

    def zadd(self, name: str, mapping: dict[str, float]) -> object:
        zset = self.zsets.setdefault(name, {})
        zset.update(mapping)
        return True

    def zremrangebyscore(self, name: str, min: float, max: float) -> int:
        zset = self.zsets.get(name, {})
        to_remove = [member for member, score in zset.items() if float(min) <= score <= float(max)]
        for member in to_remove:
            zset.pop(member, None)
        return len(to_remove)

    def zrangebyscore(
        self,
        name: str,
        min: float | str,
        max: float | str,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[str]:
        min_score = float("-inf") if min == "-inf" else float(min)
        max_score = float("inf") if max == "+inf" else float(max)
        ordered = sorted(self.zsets.get(name, {}).items(), key=lambda item: item[1])
        values = [member for member, score in ordered if min_score <= score <= max_score]
        if start is not None and num is not None:
            values = values[start : start + num]
        return values

    def zrevrangebyscore(
        self,
        name: str,
        max: float | str,
        min: float | str,
        start: int | None = None,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[tuple[str, float]] | list[str]:
        max_score = float("inf") if max == "+inf" else float(max)
        min_score = float("-inf") if min == "-inf" else float(min)
        ordered = sorted(self.zsets.get(name, {}).items(), key=lambda item: item[1], reverse=True)
        filtered = [(member, score) for member, score in ordered if min_score <= score <= max_score]
        if start is not None and num is not None:
            filtered = filtered[start : start + num]
        if withscores:
            return filtered
        return [member for member, _ in filtered]

    def zcount(self, name: str, min: float, max: float) -> int:
        zset = self.zsets.get(name, {})
        return sum(1 for score in zset.values() if float(min) <= score <= float(max))

    def scan_iter(self, match: str, count: int = 500) -> list[str]:
        return self.keys(match)

    def zrange(
        self, name: str, start: int, end: int, withscores: bool = False
    ) -> list[tuple[str, float]] | list[str]:
        entries = sorted(self.zsets.get(name, {}).items(), key=lambda item: item[1])
        sliced = entries[start:] if end == -1 else entries[start : end + 1]
        if withscores:
            return sliced
        return [member for member, _ in sliced]

    def delete(self, *names: str) -> int:
        removed = 0
        for name in names:
            if name in self.strings:
                self.strings.pop(name, None)
                removed += 1
            if name in self.hashes:
                self.hashes.pop(name, None)
                removed += 1
            if name in self.zsets:
                self.zsets.pop(name, None)
                removed += 1
        return removed


class ResponseLike(Protocol):
    status_code: int

    def json(self) -> object:
        ...


def new_events_db() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    connection.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, device_id TEXT NOT NULL, "
        "room_id TEXT NOT NULL, type TEXT NOT NULL, ts TEXT NOT NULL, payload TEXT NOT NULL, "
        "received_at TEXT NOT NULL, late INTEGER NOT NULL DEFAULT 0)"
    )
    connection.commit()
    return connection


class FakeIngestRequest:
    # Minimal stand-in for fastapi.Request for POST /events tests.
    def __init__(
        self,
        body: bytes,
        event_queue: PriorityEventQueue,
        headers: dict[str, str] | None = None,
        db_connection: sqlite3.Connection | None = None,
    ) -> None:
        self._body = body
        self.headers = Headers(headers or {})
        self.db_connection = db_connection if db_connection is not None else new_events_db()
        self.app = SimpleNamespace(
            state=SimpleNamespace(event_queue=event_queue, db_connection=self.db_connection)
        )

    async def stream(self) -> AsyncIterator[bytes]:
        chunk_size = 4096
        for offset in range(0, len(self._body), chunk_size):
            yield self._body[offset : offset + chunk_size]


def flat_event(event_type: str = "heartbeat", **extra: Any) -> bytes:
    event: dict[str, Any] = {
        "device_id": "dev_1",
        "room_id": "room_1",
        "type": event_type,
        "ts": datetime.now(timezone.utc).isoformat(),
        "seq": 1,
        **extra,
    }
    return json.dumps(event).encode("utf-8")