from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
import logging
from time import perf_counter
from sqlite3 import Connection
from typing import Any, Iterable, Protocol, TypedDict, cast

from config import STATE_SNAPSHOT_INTERVAL_SECONDS
from ingestion.validator import ValidationError
from models import Priority, ValidatedEvent
from processing.alarm_bus import AlarmBus
from processing.handlers.base import EventHandler
from processing.handlers.fall_warn import FallWarnHandler
from processing.handlers.generic import GenericEventHandler
from processing.handlers.heartbeat import HeartbeatHandler
from processing.handlers.presence import PresenceHandler
from redis import Redis

logger = logging.getLogger(__name__)


class _RecoveryReadPipeline(Protocol):
    def get(self, name: str) -> object:
        ...

    def hgetall(self, name: str) -> object:
        ...

    def zrange(self, name: str, start: int, end: int, withscores: bool = False) -> object:
        ...

    def execute(self) -> list[Any]:
        ...


class _RecoveryRedis(Protocol):
    def scan_iter(self, match: str, count: int = 500) -> Iterable[str | bytes | bytearray | memoryview]:
        ...

    def pipeline(self) -> _RecoveryReadPipeline:
        ...

    def set(self, name: str, value: str) -> object:
        ...

    def hset(self, name: str, mapping: dict[str, str]) -> object:
        ...

    def zadd(self, name: str, mapping: dict[str, float]) -> object:
        ...

    def delete(self, *names: str) -> int:
        ...


class SnapshotZSetEntry(TypedDict):
    member: str
    score: float


class SnapshotState(TypedDict):
    strings: dict[str, str]
    hashes: dict[str, dict[str, str]]
    zsets: dict[str, list[SnapshotZSetEntry]]


# Redis key shapes owned by the recovery snapshot, grouped by value type. Handlers always write
# these exact shapes, so capture can SCAN by pattern and infer the type from the group instead of
# a TYPE probe per key. _capture_state and _clear_managed_state both derive from this single
# source, so a restore wipe covers exactly the shapes a snapshot captures -- the two can never
# drift out of sync.
_SNAPSHOT_STRING_PATTERNS = ["device:*:last_heartbeat"]
_SNAPSHOT_HASH_PATTERNS = ["room:*:presence"]
_SNAPSHOT_ZSET_PATTERNS = ["device:*:heartbeats", "room:*:occupancy"]
_SNAPSHOT_MANAGED_PATTERNS = (
    _SNAPSHOT_STRING_PATTERNS + _SNAPSHOT_HASH_PATTERNS + _SNAPSHOT_ZSET_PATTERNS
)


def _as_text(value: str | bytes | bytearray | memoryview | None) -> str:
    # Normalize any redis-py / sqlite value (bytes vs str, depending on decode_responses) to str.
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, bytearray):
        return bytes(value).decode("utf-8")
    if isinstance(value, memoryview):
        return value.tobytes().decode("utf-8")
    return value


class RecoveryManager:
    def __init__(self, db_connection: Connection, redis_client: Redis, alarm_bus: AlarmBus) -> None:
        self.db_connection: Connection = db_connection
        self.redis: Redis = redis_client
        self.alarm_bus: AlarmBus = alarm_bus
        self._snapshot_task: asyncio.Task[None] | None = None

    async def restore_state(self) -> None:
        redis_client = cast(_RecoveryRedis, self.redis)
        start = perf_counter()

        snapshot_ts, snapshot_state = self._load_latest_snapshot()
        logger.info(
            json.dumps(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "level": "INFO",
                    "event": "recovery_start",
                    "snapshot_ts": snapshot_ts,
                }
            )
        )
        # Rebuild hot state after a crash. Order matters: wipe stale keys -> apply the last
        # snapshot -> replay every event ingested since (see _replay_events for the cutoff).
        self._clear_managed_state(redis_client)
        if snapshot_state is not None:
            self._apply_snapshot(redis_client, snapshot_state)

        replayed_events = await self._replay_events(since_ts=snapshot_ts)
        duration_ms = int((perf_counter() - start) * 1000)
        logger.info(
            json.dumps(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "level": "INFO",
                    "event": "recovery_done",
                    "snapshot_ts": snapshot_ts,
                    "events_replayed": replayed_events,
                    "duration_ms": duration_ms,
                }
            )
        )

    async def start_snapshot_loop(self, interval_seconds: int = STATE_SNAPSHOT_INTERVAL_SECONDS) -> None:
        if self._snapshot_task is not None and not self._snapshot_task.done():
            return
        self._snapshot_task = asyncio.create_task(self._snapshot_loop(interval_seconds))

    async def stop_snapshot_loop(self) -> None:
        if self._snapshot_task is None:
            return
        self._snapshot_task.cancel()
        await asyncio.gather(self._snapshot_task, return_exceptions=True)
        self._snapshot_task = None

    async def _snapshot_loop(self, interval_seconds: int) -> None:
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(interval_seconds)
            # Stamp the snapshot time BEFORE capturing so any event applied while the capture runs
            # has received_at >= snapshot_ts and is therefore replayed on recovery (see
            # _replay_events). Capture the Redis state off the event loop: SCAN + pipelined reads
            # are still O(N) work and must not freeze ingestion or the alarm hot path. The Redis
            # client is thread-safe, but the SQLite write stays on the loop thread so the shared
            # connection is never used by two threads at once.
            snapshot_ts = datetime.now(timezone.utc).isoformat()
            state_json = await loop.run_in_executor(None, self._capture_state_json)
            self._persist_snapshot(snapshot_ts, state_json)

    def write_snapshot(self) -> None:
        # Synchronous, on-demand snapshot (e.g. graceful shutdown); the periodic path is _snapshot_loop.
        snapshot_ts = datetime.now(timezone.utc).isoformat()
        self._persist_snapshot(snapshot_ts, self._capture_state_json())

    def _capture_state_json(self) -> str:
        return json.dumps(self._capture_state())

    def _persist_snapshot(self, snapshot_ts: str, state_json: str) -> None:
        cursor = self.db_connection.cursor()
        cursor.execute(
            "INSERT INTO state_snapshots (snapshot_ts, state_json) VALUES (?, ?)",
            (snapshot_ts, state_json),
        )
        self.db_connection.commit()

    def _scan_keys(self, redis_client: _RecoveryRedis, patterns: list[str]) -> list[str]:
        # SCAN (cursor-based, non-blocking) instead of KEYS, which is O(N) and blocks the entire
        # single-threaded Redis server -- including the alarm hot path -- for the duration.
        keys: set[str] = set()
        for pattern in patterns:
            for item in redis_client.scan_iter(match=pattern, count=500):
                keys.add(_as_text(item))
        return sorted(keys)

    def _capture_state(self) -> SnapshotState:
        redis_client = cast(_RecoveryRedis, self.redis)
        # Each key suffix maps to a fixed Redis type (handlers always write these shapes), so we
        # can SCAN by pattern and then issue all value reads in a single pipeline instead of a
        # TYPE + GET/HGETALL/ZRANGE round-trip per key. This turns ~3N blocking round-trips into a
        # couple of batched ones.
        string_keys = self._scan_keys(redis_client, _SNAPSHOT_STRING_PATTERNS)
        hash_keys = self._scan_keys(redis_client, _SNAPSHOT_HASH_PATTERNS)
        zset_keys = self._scan_keys(redis_client, _SNAPSHOT_ZSET_PATTERNS)

        pipeline = redis_client.pipeline()
        for key in string_keys:
            pipeline.get(key)
        for key in hash_keys:
            pipeline.hgetall(key)
        for key in zset_keys:
            pipeline.zrange(key, 0, -1, withscores=True)
        results = list(pipeline.execute())

        strings: dict[str, str] = {}
        hashes: dict[str, dict[str, str]] = {}
        zsets: dict[str, list[SnapshotZSetEntry]] = {}

        cursor = 0
        for key in string_keys:
            value = results[cursor]
            cursor += 1
            if value is not None:
                strings[key] = _as_text(cast("str | bytes | bytearray | memoryview", value))
        for key in hash_keys:
            raw_hash = cast(dict[str, "str | bytes | bytearray | memoryview"], results[cursor])
            cursor += 1
            hashes[key] = {field: _as_text(field_value) for field, field_value in raw_hash.items()}
        for key in zset_keys:
            entries = cast(list[tuple["str | bytes | bytearray | memoryview", float]], results[cursor])
            cursor += 1
            zsets[key] = [{"member": _as_text(member), "score": float(score)} for member, score in entries]

        return {"strings": strings, "hashes": hashes, "zsets": zsets}

    def _load_latest_snapshot(self) -> tuple[str | None, SnapshotState | None]:
        cursor = self.db_connection.cursor()
        cursor.execute(
            "SELECT snapshot_ts, state_json FROM state_snapshots ORDER BY snapshot_ts DESC LIMIT 1"
        )
        row = cursor.fetchone()
        if row is None:
            return None, None
        snapshot_ts_raw, state_json_raw = row
        snapshot_ts = _as_text(cast(str | bytes | bytearray | memoryview, snapshot_ts_raw))
        state_json = _as_text(cast(str | bytes | bytearray | memoryview, state_json_raw))
        # A malformed / non-dict snapshot normalizes to state=None so recovery falls back to pure
        # replay instead of crashing on a partially written row.
        parsed = json.loads(state_json)
        if not isinstance(parsed, dict):
            return snapshot_ts, None
        parsed_obj = cast(dict[str, object], parsed)
        strings_raw = parsed_obj.get("strings", {})
        hashes_raw = parsed_obj.get("hashes", {})
        zsets_raw = parsed_obj.get("zsets", {})

        strings: dict[str, str] = {}
        if isinstance(strings_raw, dict):
            strings_map = cast(dict[str, object], strings_raw)
            for raw_key, raw_value in strings_map.items():
                key_text = str(raw_key)
                strings[key_text] = _as_text(cast(str | bytes | bytearray | memoryview | None, raw_value))

        hashes: dict[str, dict[str, str]] = {}
        if isinstance(hashes_raw, dict):
            hashes_map = cast(dict[str, object], hashes_raw)
            for raw_key, raw_value in hashes_map.items():
                key_text = str(raw_key)
                if not isinstance(raw_value, dict):
                    continue
                normalized_hash: dict[str, str] = {}
                hash_value_map = cast(dict[str, object], raw_value)
                for raw_field, raw_field_value in hash_value_map.items():
                    field_text = str(raw_field)
                    normalized_hash[field_text] = _as_text(
                        cast(str | bytes | bytearray | memoryview | None, raw_field_value)
                    )
                hashes[key_text] = normalized_hash

        zsets: dict[str, list[SnapshotZSetEntry]] = {}
        if isinstance(zsets_raw, dict):
            zsets_map = cast(dict[str, object], zsets_raw)
            for raw_key, raw_entries in zsets_map.items():
                key_text = str(raw_key)
                if not isinstance(raw_entries, list):
                    continue
                entry_items = cast(list[object], raw_entries)
                normalized_entries: list[SnapshotZSetEntry] = []
                for entry in entry_items:
                    if not isinstance(entry, dict):
                        continue
                    entry_obj = cast(dict[str, object], entry)
                    if "member" not in entry_obj or "score" not in entry_obj:
                        continue
                    member = _as_text(
                        cast(str | bytes | bytearray | memoryview | None, entry_obj["member"])
                    )
                    score_value = entry_obj["score"]
                    if not member or not isinstance(score_value, (int, float)):
                        continue
                    normalized_entries.append({"member": member, "score": float(score_value)})
                zsets[key_text] = normalized_entries

        return snapshot_ts, {"strings": strings, "hashes": hashes, "zsets": zsets}

    def _clear_managed_state(self, redis_client: _RecoveryRedis) -> None:
        keys_to_delete = self._scan_keys(redis_client, _SNAPSHOT_MANAGED_PATTERNS)
        if keys_to_delete:
            redis_client.delete(*keys_to_delete)

    def _apply_snapshot(self, redis_client: _RecoveryRedis, snapshot_state: SnapshotState) -> None:
        # Delete each key before rewriting so a restore is a full overwrite, never a merge --
        # hset/zadd would otherwise union new fields into stale ones left in the key.
        keys_to_clear: list[str] = []
        keys_to_clear.extend(snapshot_state["strings"].keys())
        keys_to_clear.extend(snapshot_state["hashes"].keys())
        keys_to_clear.extend(snapshot_state["zsets"].keys())
        if keys_to_clear:
            redis_client.delete(*keys_to_clear)

        for key_text, value_text in snapshot_state["strings"].items():
            redis_client.set(key_text, value_text)

        for key_text, hash_mapping in snapshot_state["hashes"].items():
            if hash_mapping:
                redis_client.hset(key_text, mapping=hash_mapping)

        for key_text, entries in snapshot_state["zsets"].items():
            zset_mapping: dict[str, float] = {}
            for entry in entries:
                zset_mapping[entry["member"]] = entry["score"]
            if zset_mapping:
                redis_client.zadd(key_text, zset_mapping)

    async def _replay_events(self, since_ts: str | None = None) -> int:
        cursor = self.db_connection.cursor()
        if since_ts is None:
            cursor.execute(
                "SELECT device_id, room_id, type, ts, payload, received_at, late FROM events ORDER BY ts ASC"
            )
        else:
            # Cut off on received_at (ingestion order), NOT ts (device clock). A snapshot is
            # stamped with wall-clock time, so it reflects exactly the events that were ingested
            # before that instant. Filtering on ts would silently drop a late event (old device
            # ts, e.g. now-20m, which the spec supports up to 1h) that was ingested AFTER the
            # snapshot: such an event is absent from the snapshot yet has ts < snapshot_ts, so it
            # would be lost forever on a hard-kill recovery. Filtering on received_at >=
            # snapshot_ts replays every event not already captured. The boundary is inclusive so
            # an event received at exactly snapshot_ts is never dropped; handlers are
            # timestamp-aware and idempotent, so re-applying a captured event is safe. Rows are
            # still ordered by ts so per-device state is rebuilt in chronological order.
            cursor.execute(
                "SELECT device_id, room_id, type, ts, payload, received_at, late FROM events WHERE received_at >= ? ORDER BY ts ASC",
                (since_ts,),
            )
        # fall_warn is the only handler with side effects beyond hot state (DB insert + alarm
        # publish), so it replays with replay=True: UNIQUE no-ops count as db_conflicts (not
        # dedups) and re-applied rows never re-publish alarms. Generic types just re-append events.
        handlers: dict[str, EventHandler] = {
            "heartbeat": HeartbeatHandler(self.redis),
            "presence": PresenceHandler(self.redis),
            "fall_warn": FallWarnHandler(self.redis, self.db_connection, self.alarm_bus, replay=True),
            "motion": GenericEventHandler(self.db_connection),
            "sleep_state": GenericEventHandler(self.db_connection),
            "net_status": GenericEventHandler(self.db_connection),
        }

        replayed = 0
        for device_id, room_id, event_type, ts_text, payload_text, received_at_text, late_flag in cursor.fetchall():
            try:
                ts = datetime.fromisoformat(_as_text(cast(str | bytes | bytearray | memoryview, ts_text))).astimezone(
                    timezone.utc
                )
                raw_payload = json.loads(payload_text)
                if not isinstance(raw_payload, dict):
                    raise ValidationError("replayed payload must be a JSON object", reason="invalid_schema")
                payload = cast(dict[str, Any], raw_payload)
                received_at = datetime.fromisoformat(
                    _as_text(cast(str | bytes | bytearray | memoryview, received_at_text))
                )
                received_at = received_at.astimezone(timezone.utc)
                priority = Priority.HIGH if event_type == "fall_warn" else Priority.NORMAL
                event = ValidatedEvent(
                    device_id=device_id,
                    room_id=room_id,
                    type=event_type,
                    ts=ts,
                    payload=payload,
                    late=bool(late_flag),
                    priority=priority,
                    received_at=received_at,
                )
                handler = handlers.get(event_type, handlers["motion"])
                await handler.handle(event)
                replayed += 1
            except (ValueError, ValidationError, json.JSONDecodeError):
                continue

        return replayed
