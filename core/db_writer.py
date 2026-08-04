from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import asyncio

from config import SQLITE_PATH, SQLITE_WRITER_BATCH_WINDOW_SECONDS, SQLITE_WRITER_MAX_BATCH_SIZE
from core.db import get_db_connection


@dataclass
class _WriteJob:
    sql: str
    params: tuple[Any, ...]
    loop: asyncio.AbstractEventLoop
    future: "asyncio.Future[tuple[int, int | None]]"
    priority: bool = False
    rowcount: int = field(default=0, init=False)
    lastrowid: int | None = field(default=None, init=False)


_STOP = object()


class BatchedSQLiteWriter:
    """Owns a single dedicated OS thread + its own `sqlite3.Connection`.

    Every durable write in the system (the `events` admission log and the
    `fall_warnings` alarm table) is funneled through this one thread so the
    database connection is never touched by two threads at once -- the same
    correctness requirement `check_same_thread=False` does NOT provide on its
    own. Pending writes are batched into a single commit (bounded by
    SQLITE_WRITER_BATCH_WINDOW_SECONDS / SQLITE_WRITER_MAX_BATCH_SIZE) to
    amortize the fsync/commit cost across many events, while a caller's
    future resolves only once its batch's commit has actually succeeded --
    preserving persist-before-ack. A `priority=True` submission (the
    fall_warn hot path) skips the batch wait entirely so alarm latency is
    never taxed by NORMAL-lane batching.
    """

    def __init__(
        self,
        path: str = SQLITE_PATH,
        batch_window_seconds: float = SQLITE_WRITER_BATCH_WINDOW_SECONDS,
        max_batch_size: int = SQLITE_WRITER_MAX_BATCH_SIZE,
    ) -> None:
        self._path = path
        self._batch_window_seconds = batch_window_seconds
        self._max_batch_size = max_batch_size
        self._queue: "queue.Queue[_WriteJob | object]" = queue.Queue()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="sqlite-writer", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        if self._thread is None:
            return
        self._queue.put(_STOP)
        self._thread.join(timeout=timeout)
        self._thread = None

    def submit(self, sql: str, params: tuple[Any, ...], *, priority: bool = False) -> "asyncio.Future[tuple[int, int | None]]":
        # Called from the event loop thread. The asyncio Future is resolved from the writer
        # thread via call_soon_threadsafe, which is the standard cross-thread bridge back onto
        # the loop -- awaiting it never blocks the loop itself.
        loop = asyncio.get_running_loop()
        future: "asyncio.Future[tuple[int, int | None]]" = loop.create_future()
        job = _WriteJob(sql=sql, params=params, loop=loop, future=future, priority=priority)
        self._queue.put(job)
        return future

    def _run(self) -> None:
        connection = get_db_connection(self._path)
        try:
            while True:
                job = self._queue.get()
                if job is _STOP:
                    return
                batch = [job]
                # Priority jobs (fall_warn) never wait for the batch window -- alarm latency must
                # not be taxed by NORMAL-lane batching. Opportunistically fold in whatever else is
                # already queued (non-blocking) so a burst still amortizes its commit.
                if isinstance(job, _WriteJob) and job.priority:
                    self._drain_ready(batch)
                else:
                    self._collect_batch(batch)
                stop_requested = any(item is _STOP for item in batch)
                real_jobs = [item for item in batch if isinstance(item, _WriteJob)]
                if real_jobs:
                    self._flush(connection, real_jobs)
                if stop_requested:
                    return
        finally:
            connection.close()

    def _drain_ready(self, batch: list[Any]) -> None:
        while len(batch) < self._max_batch_size:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            batch.append(item)
            if item is _STOP:
                break

    def _collect_batch(self, batch: list[Any]) -> None:
        deadline = time.monotonic() + self._batch_window_seconds
        while len(batch) < self._max_batch_size:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                item = self._queue.get(timeout=remaining)
            except queue.Empty:
                break
            batch.append(item)
            if item is _STOP or (isinstance(item, _WriteJob) and item.priority):
                break

    def _flush(self, connection: Any, jobs: list[_WriteJob]) -> None:
        cursor = connection.cursor()
        try:
            for job in jobs:
                cursor.execute(job.sql, job.params)
                job.rowcount = cursor.rowcount
                job.lastrowid = cursor.lastrowid
            connection.commit()
        except Exception as exc:  # noqa: BLE001 -- must propagate to every waiter, not swallow
            connection.rollback()
            for job in jobs:
                job.loop.call_soon_threadsafe(_fail_future, job.future, exc)
            return
        for job in jobs:
            job.loop.call_soon_threadsafe(_resolve_future, job.future, (job.rowcount, job.lastrowid))


def _resolve_future(future: "asyncio.Future[tuple[int, int | None]]", result: tuple[int, int | None]) -> None:
    if not future.done():
        future.set_result(result)


def _fail_future(future: "asyncio.Future[tuple[int, int | None]]", exc: Exception) -> None:
    if not future.done():
        future.set_exception(exc)
