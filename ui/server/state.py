"""Run lifecycle state for the load-test UI control plane.

Owns the in-memory record of runs and the bounded set of SSE subscribers. The UI-app is a
control plane only: all measurement is produced by the runner subprocess and relayed here via
its JSON-lines stdout stream; this module never talks to the system-under-test during a run.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

# Matches the SSE frame shape consumed by ui/src/api/stream.ts (StreamFrame union).
# Frames carry a monotonic seq so a reconnecting browser can detect gaps.


@dataclass
class RunConfig:
    scenario: str
    devices: int
    duration_seconds: float
    rps: float
    connections: int
    metrics_interval_seconds: float
    burst: bool
    target: str

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "RunConfig":
        return cls(
            scenario=str(payload.get("scenario", "baseline")),
            devices=int(payload.get("devices", 500)),
            duration_seconds=float(payload.get("duration_seconds", 90.0)),
            rps=float(payload.get("rps", 1.0)),
            connections=int(payload.get("connections", 32)),
            metrics_interval_seconds=float(payload.get("metrics_interval_seconds", 1.0)),
            burst=bool(payload.get("burst", False)),
            target=str(payload.get("target", "http://127.0.0.1:8080")),
        )


@dataclass
class Run:
    run_id: str
    config: RunConfig
    state: str = "running"  # running | stopping | done | failed
    started_at: float = field(default_factory=time.time)
    sent: int = 0
    failed: int = 0
    summary: dict[str, Any] | None = None
    report_path: str | None = None

    def public(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "state": self.state,
            "started_at": self.started_at,
            "elapsed_seconds": max(0.0, time.time() - self.started_at),
            "config": vars(self.config),
            "progress": {"sent": self.sent, "failed": self.failed},
            "summary": self.summary,
            "report_path": self.report_path,
        }


class RunManager:
    """One active run at a time plus a broadcast bus for SSE subscribers."""

    def __init__(self, subscriber_queue_size: int = 1000) -> None:
        self._subscriber_queue_size = subscriber_queue_size
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._lock = asyncio.Lock()
        self.current: Run | None = None
        self.history: list[Run] = []
        self._seq = 0
        # Live runner subprocesses keyed by run_id, so stop() can terminate them.
        self._processes: dict[str, asyncio.subprocess.Process] = {}

    # -- subscription bus ---------------------------------------------------

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=self._subscriber_queue_size)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._subscribers.discard(queue)

    def _broadcast(self, frame: dict[str, Any]) -> None:
        self._seq += 1
        frame = {"seq": self._seq, **frame}
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(frame)
            except asyncio.QueueFull:
                # Slow consumer: evict rather than grow memory or block the run.
                self._subscribers.discard(queue)

    # -- run lifecycle ------------------------------------------------------

    async def start(self, config: RunConfig) -> Run:
        async with self._lock:
            if self.current is not None and self.current.state in {"running", "stopping"}:
                raise RunInProgressError(self.current)
            run = Run(run_id=uuid.uuid4().hex[:12], config=config)
            self.current = run
            self.history.append(run)
        self._broadcast({"type": "state", "state": run.state, "run_id": run.run_id})
        return run

    def active(self) -> Run | None:
        if self.current is not None and self.current.state in {"running", "stopping"}:
            return self.current
        return None

    def apply_frame(self, frame: dict[str, Any]) -> None:
        """Fold a runner JSON-lines record into run state and fan it out to subscribers."""
        run = self.current
        frame_type = frame.get("type")
        if run is not None and frame_type == "progress":
            run.sent = int(frame.get("sent", run.sent))
            run.failed = int(frame.get("failed", run.failed))
        if run is not None and frame_type == "done":
            run.state = "done"
            run.summary = frame.get("summary")
            run.report_path = frame.get("report_path")
        if run is not None and frame_type == "error":
            run.state = "failed"
        self._broadcast(frame)
        if frame_type in {"done", "error"} and run is not None:
            self._broadcast({"type": "state", "state": run.state, "run_id": run.run_id})

    def mark_stopping(self) -> None:
        if self.current is not None and self.current.state == "running":
            self.current.state = "stopping"
            self._broadcast({"type": "state", "state": "stopping", "run_id": self.current.run_id})

    def snapshot(self) -> dict[str, Any]:
        return {
            "current": self.current.public() if self.current else None,
            "runs": [run.public() for run in self.history[-20:]],
        }


class RunInProgressError(Exception):
    def __init__(self, active: Run) -> None:
        super().__init__("a load test run is already in progress")
        self.active = active
