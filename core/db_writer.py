from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import asyncio

from config import (
    SQLITE_PATH,
    SQLITE_WRITER_BATCH_WINDOW_SECONDS,
    SQLITE_WRITER_MAX_BATCH_SIZE,
    SQLITE_WRITER_PRIORITY_QUEUE_MAX_SIZE,
    SQLITE_WRITER_QUEUE_MAX_SIZE,
)
from core.db import get_db_connection

logger = logging.getLogger(__name__)


class SQLiteWriterError(RuntimeError):
    """Raised when a write can't be handed to the writer thread (queue full or thread dead)."""


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

# Cadence for _get_next's idle re-check of the priority lane while parked on the normal lane.
# Deliberately fixed and small, NOT tied to a writer instance's (configurable) batch_window_seconds
# -- that window governs NORMAL-lane commit batching, a different concern, and priority dispatch
# latency must stay bounded even when batch_window_seconds is configured much larger (e.g. tests).
_PRIORITY_POLL_INTERVAL_SECONDS = 0.005


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
        max_queue_size: int = SQLITE_WRITER_QUEUE_MAX_SIZE,
        max_priority_queue_size: int = SQLITE_WRITER_PRIORITY_QUEUE_MAX_SIZE,
    ) -> None:
        self._path = path
        self._batch_window_seconds = batch_window_seconds
        self._max_batch_size = max_batch_size
        # Two independently-bounded lanes so a NORMAL-lane flood can never reject a priority
        # (fall_warn) write with queue.Full -- see _get_next for how a single consumer thread
        # drains both without polling-free blocking on two queues at once.
        self._priority_queue: "queue.Queue[_WriteJob]" = queue.Queue(maxsize=max_priority_queue_size)
        self._normal_queue: "queue.Queue[_WriteJob | object]" = queue.Queue(maxsize=max_queue_size)
        self._thread: threading.Thread | None = None
        # Guards self._dead and self._thread together with enqueueing/thread-lifecycle reads so a
        # submit() can never land a job after the writer has drained-and-failed everything on a
        # crash, and stop()/the crash path can never race on self._thread (see submit()/_run()).
        self._lock = threading.Lock()
        # Set when _run exits on an unexpected (non-flush) fault, so submit() fails fast instead
        # of silently queuing into a thread that will never drain it again.
        self._dead = False
        # Signals shutdown out-of-band, never through a bounded queue (see stop()/_get_next): a
        # sentinel value competing for queue capacity can be rejected by queue.Full under a large
        # backlog, leaving the thread with nothing telling it to exit.
        self._stop_event = threading.Event()

    def start(self) -> None:
        with self._lock:
            if self._thread is not None:
                return
            # Clear any job/sentinel left behind by a prior crash racing a stop() call (the crash
            # path's own drain can run before stop()'s signal lands) so a fresh thread can never
            # find stale state and misbehave; anything real still here gets its future failed.
            self._drain_and_fail_all()
            self._dead = False
            self._stop_event.clear()
            thread = threading.Thread(target=self._run, name="sqlite-writer", daemon=True)
            self._thread = thread
        thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        with self._lock:
            thread = self._thread
            if thread is None:
                return
            # Reject any submit() racing with shutdown immediately -- closes the orphaned-future
            # window entirely (rather than narrowing it). Nothing new can land in either lane
            # after this, so _run is guaranteed to drain both down to empty in bounded time.
            self._dead = True
            self._stop_event.set()
        thread.join(timeout=timeout)
        if thread.is_alive():
            logger.warning("sqlite writer stop: thread did not exit within timeout, leaving it running")
            return  # do NOT clear self._thread here -- start() must never spawn a second thread over a live one
        with self._lock:
            if self._thread is thread:
                self._thread = None

    def submit(self, sql: str, params: tuple[Any, ...], *, priority: bool = False) -> "asyncio.Future[tuple[int, int | None]]":
        # Called from the event loop thread. The asyncio Future is resolved from the writer
        # thread via call_soon_threadsafe, which is the standard cross-thread bridge back onto
        # the loop -- awaiting it never blocks the loop itself.
        loop = asyncio.get_running_loop()
        future: "asyncio.Future[tuple[int, int | None]]" = loop.create_future()
        job = _WriteJob(sql=sql, params=params, loop=loop, future=future, priority=priority)
        with self._lock:
            if self._dead:
                future.set_exception(SQLiteWriterError("sqlite writer thread is not running"))
                return future
            try:
                (self._priority_queue if priority else self._normal_queue).put_nowait(job)
            except queue.Full:
                future.set_exception(SQLiteWriterError("sqlite writer queue is full"))
        return future

    def _run(self) -> None:
        connection = get_db_connection(self._path)
        try:
            while True:
                job = self._get_next(timeout=None)
                if job is _STOP:
                    return
                batch = [job]
                # Priority jobs (fall_warn) never wait for the batch window -- alarm latency must
                # not be taxed by NORMAL-lane batching. Opportunistically fold in whatever else is
                # already queued in EITHER lane (non-blocking) so a burst still amortizes its commit.
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
        except Exception:
            # A fault here (as opposed to inside _flush's per-batch try/except) means the loop
            # itself died -- e.g. a poisoned connection or an OS-level queue failure. Left
            # unhandled, this thread would exit silently and every future submit() would enqueue
            # into a queue nobody ever drains again, hanging callers forever. Fail everything
            # still queued so callers get a fast, observable error, and mark the writer dead so
            # new submissions reject immediately instead of queuing into the void. All done under
            # the same lock submit()/stop() take, so neither can race this thread's own lifecycle
            # bookkeeping (self._dead, self._thread).
            logger.exception("sqlite writer thread crashed")
            with self._lock:
                self._dead = True
                self._drain_and_fail_all()
                # Allow a future start() to spin up a replacement thread instead of silently
                # no-op'ing forever (start()'s guard only checks self._thread is None).
                self._thread = None
        finally:
            connection.close()

    def _get_next(self, timeout: float | None) -> Any:
        # Always prefer the priority lane so a fall_warn write is never delayed behind NORMAL-lane
        # backlog. Since these are two independent stdlib Queues (no single primitive can block on
        # both at once), poll the normal lane in short slices -- see _PRIORITY_POLL_INTERVAL_SECONDS.
        poll_interval = _PRIORITY_POLL_INTERVAL_SECONDS
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            try:
                return self._priority_queue.get_nowait()
            except queue.Empty:
                pass
            if self._stop_event.is_set() and self._normal_queue.empty():
                # Shutdown requested and both lanes drained (nothing new can land in either once
                # stop() has set _dead) -- signal exit without ever needing to place a sentinel
                # into a queue that could reject it under a full backlog.
                return _STOP
            wait_for = poll_interval
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise queue.Empty
                wait_for = min(poll_interval, remaining)
            try:
                return self._normal_queue.get(timeout=wait_for)
            except queue.Empty:
                if deadline is not None and time.monotonic() >= deadline:
                    raise

    def _get_nowait_either(self) -> Any:
        # Unlike _get_next(timeout=0) -- whose deadline expires before the normal-lane check is
        # ever reached -- this genuinely checks both lanes non-blocking, for callers (_drain_ready)
        # that want to opportunistically fold in whatever is ready right now in either lane.
        try:
            return self._priority_queue.get_nowait()
        except queue.Empty:
            pass
        return self._normal_queue.get_nowait()

    def _drain_and_fail_all(self) -> None:
        for lane in (self._priority_queue, self._normal_queue):
            while True:
                try:
                    item = lane.get_nowait()
                except queue.Empty:
                    break
                if isinstance(item, _WriteJob):
                    _safe_call_soon_threadsafe(
                        item.loop, _fail_future, item.future, SQLiteWriterError("sqlite writer thread crashed")
                    )

    def _drain_ready(self, batch: list[Any]) -> None:
        while len(batch) < self._max_batch_size:
            try:
                item = self._get_nowait_either()
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
                item = self._get_next(timeout=remaining)
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
                _safe_call_soon_threadsafe(job.loop, _fail_future, job.future, exc)
            return
        for job in jobs:
            _safe_call_soon_threadsafe(job.loop, _resolve_future, job.future, (job.rowcount, job.lastrowid))


def _safe_call_soon_threadsafe(loop: asyncio.AbstractEventLoop, callback: Any, *args: Any) -> None:
    # A caller's loop can already be closed (e.g. request cancelled/disconnected) by the time the
    # writer thread tries to resolve its future. That must only drop this one job's result, not
    # look like a fault in the writer loop itself -- callers of this helper are on the writer's
    # hot path (_flush, _drain_and_fail_all) and must never let one stale loop escalate into the
    # outer except in _run(), which would wrongly treat it as a full writer crash.
    try:
        loop.call_soon_threadsafe(callback, *args)
    except RuntimeError:
        logger.warning("sqlite writer: target event loop is closed, dropping result")


def _resolve_future(future: "asyncio.Future[tuple[int, int | None]]", result: tuple[int, int | None]) -> None:
    if not future.done():
        future.set_result(result)


def _fail_future(future: "asyncio.Future[tuple[int, int | None]]", exc: Exception) -> None:
    if not future.done():
        future.set_exception(exc)
