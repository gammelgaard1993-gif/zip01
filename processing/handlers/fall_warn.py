from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from concurrent.futures import Executor
from sqlite3 import Connection
from typing import TYPE_CHECKING, Any, Protocol, cast

from config import FALL_DEDUP_TTL_SECONDS
from core.metrics import increment_counter
from models import AlarmEvent, ValidatedEvent

if TYPE_CHECKING:
    from core.db_writer import BatchedSQLiteWriter

logger = logging.getLogger(__name__)


class _DedupRedis(Protocol):
    def set(self, name: str, value: str, *args: object, **kwargs: object) -> object:
        ...


class _AlarmPublisher(Protocol):
    async def publish(self, alarm: AlarmEvent) -> None:
        ...


class FallWarnHandler:
    def __init__(
        self,
        redis_client: Any,
        db_connection: Connection,
        alarm_bus: _AlarmPublisher,
        replay: bool = False,
        writer: "BatchedSQLiteWriter | None" = None,
        executor: Executor | None = None,
    ) -> None:
        self.redis: _DedupRedis = cast(_DedupRedis, redis_client)
        self.db_connection: Connection = db_connection
        self.alarm_bus: _AlarmPublisher = alarm_bus
        # When True this handler is re-applying durable history during recovery, so a SQLite
        # UNIQUE no-op is an expected replay artifact rather than a real duplicate detection.
        self.replay: bool = replay
        # Optional dedicated single-writer thread (Phase 6 / #13): when present, the
        # fall_warnings INSERT routes through the SAME writer/connection as the admission-time
        # events log, as a priority (non-batched) write, instead of committing on the loop thread.
        self._writer = writer
        # Optional shared thread pool for the best-effort redis dedup-cache write below. Both
        # default to None for recovery replay and tests, which run synchronously unchanged.
        self._executor = executor

    def _dedup_key(self, event: ValidatedEvent) -> str:
        second_ts = event.ts.replace(microsecond=0).isoformat()
        digest = hashlib.sha256(f"{event.device_id}:{event.room_id}:{second_ts}".encode("utf-8")).hexdigest()
        return f"dedup:{digest}"

    async def handle(self, event: ValidatedEvent) -> None:
        # SQLite UNIQUE(dedup_key) is the authoritative reservation (insert-first), not Redis.
        # Reserving in Redis before the durable insert would let a persistence failure permanently
        # suppress the alarm: a retry within the TTL window would see the Redis key and be
        # silently discarded even though nothing was ever durably stored. Inserting first means a
        # failed/rolled-back insert leaves no reservation behind, so a retry can still succeed.
        dedup_key = self._dedup_key(event)
        confidence = float(event.payload.get("confidence", 0.0))
        sql = (
            "INSERT OR IGNORE INTO fall_warnings (device_id, room_id, ts, confidence, dedup_key, received_at, published_at) "
            "VALUES (?, ?, ?, ?, ?, ?, NULL)"
        )
        params = (
            event.device_id,
            event.room_id,
            event.ts.isoformat(),
            confidence,
            dedup_key,
            event.received_at.isoformat(),
        )
        if self._writer is not None:
            # Priority write: never waits on NORMAL-lane batching, so alarm latency isn't taxed;
            # the future only resolves after a real commit, so the rowcount/lastrowid below are
            # still the authoritative per-row dedup + durable-id signal.
            rowcount, lastrowid = await self._writer.submit(sql, params, priority=True)
        else:
            cursor = self.db_connection.cursor()
            cursor.execute(sql, params)
            self.db_connection.commit()
            rowcount, lastrowid = cursor.rowcount, cursor.lastrowid

        if rowcount == 0:
            cursor = self.db_connection.cursor()
            cursor.execute("SELECT published_at FROM fall_warnings WHERE dedup_key = ?", (dedup_key,))
            row = cursor.fetchone()
            published_at = row[0] if row is not None else None

            # SQLite already holds a row for this dedup_key. Two situations reach here:
            #  - Recovery replay / crash recovery: the durable row exists but has not yet been
            #    published. We must deliver it now, then stamp published_at so the replay path is
            #    idempotent across restarts.
            #  - Live ingestion (not replay): a genuine duplicate of the same detection (in-window
            #    or arriving after the Redis cache entry expired -- SQLite has no TTL, so it keeps
            #    rejecting duplicates of the same event forever). The requirement implies a single
            #    dedup count ("two duplicates -> dedup counter += 2"), so this is counted as a
            #    dedup just like an in-window one.
            if published_at is not None:
                if self.replay:
                    increment_counter("fall_warnings_db_conflicts")
                    conflict_event = "fall_db_conflict"
                else:
                    increment_counter("fall_warnings_deduped")
                    conflict_event = "fall_dedup_post_ttl"
                logger.info(
                    json.dumps(
                        {
                            "event": conflict_event,
                            "device_id": event.device_id,
                            "room_id": event.room_id,
                            "dedup": True,
                        }
                    )
                )
                return

            logger.info(
                json.dumps(
                    {
                        "event": "fall_warn_republish",
                        "device_id": event.device_id,
                        "room_id": event.room_id,
                        "dedup": False,
                    }
                )
            )

            alarm = AlarmEvent(
                device_id=event.device_id,
                room_id=event.room_id,
                ts=event.ts,
                confidence=confidence,
                received_at=event.received_at,
                fall_warning_id=lastrowid,
            )
            await self.alarm_bus.publish(alarm)
            # Route through writer to avoid blocking the event loop with a direct commit, which
            # would stall _dispatch_room before it can deliver to SSE subscribers.
            if self._writer is not None:
                await self._writer.submit(
                    "UPDATE fall_warnings SET published_at = ? WHERE dedup_key = ? AND published_at IS NULL",
                    (event.received_at.isoformat(), dedup_key),
                )
            else:
                cursor.execute(
                    "UPDATE fall_warnings SET published_at = ? WHERE dedup_key = ? AND published_at IS NULL",
                    (event.received_at.isoformat(), dedup_key),
                )
                self.db_connection.commit()
            return

        # Best-effort cache write for fast in-process dedup visibility only; the durable insert
        # above already happened, so a Redis outage here must never block the alarm or be treated
        # as a persistence failure.
        if self._executor is not None:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(self._executor, self._cache_dedup, dedup_key)
        else:
            self._cache_dedup(dedup_key)

        increment_counter("fall_warnings_total")
        logger.info(
            json.dumps(
                {
                    "event": "fall_warn",
                    "device_id": event.device_id,
                    "room_id": event.room_id,
                    "dedup": False,
                }
            )
        )

        alarm = AlarmEvent(
            device_id=event.device_id,
            room_id=event.room_id,
            ts=event.ts,
            confidence=confidence,
            received_at=event.received_at,
            fall_warning_id=lastrowid,
        )
        await self.alarm_bus.publish(alarm)
        # Route through writer to avoid blocking the event loop with a direct commit, which
        # would stall _dispatch_room before it can deliver to SSE subscribers.
        if self._writer is not None:
            await self._writer.submit(
                "UPDATE fall_warnings SET published_at = ? WHERE dedup_key = ? AND published_at IS NULL",
                (event.received_at.isoformat(), dedup_key),
            )
        else:
            cursor = self.db_connection.cursor()
            cursor.execute(
                "UPDATE fall_warnings SET published_at = ? WHERE dedup_key = ? AND published_at IS NULL",
                (event.received_at.isoformat(), dedup_key),
            )
            self.db_connection.commit()

    def _cache_dedup(self, dedup_key: str) -> None:
        try:
            self.redis.set(dedup_key, "1", ex=FALL_DEDUP_TTL_SECONDS, nx=True)
        except Exception:
            logger.warning("fall_warn dedup cache write failed", exc_info=True)
