"""Load-test UI control plane.

A separate FastAPI process from the zip01 backend (the system-under-test). It serves the React
SPA, owns the load-test runner subprocess lifecycle, and streams live run data to the browser
over a single multiplexed SSE channel. It talks to the SUT only to hand the runner a target; all
measurement flows runner -> this app -> browser.

Run:  python -m ui.server.app   (serves on :5174 by default)
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .runner import stop_process, supervise
from .state import RunConfig, RunInProgressError, RunManager

_SSE_COMMENT = b": keep-alive\n\n"
_SSE_KEEPALIVE_S = 15

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DIST = _REPO_ROOT / "ui" / "dist"
_REPORTS = _REPO_ROOT / "ui" / "server" / "reports"

manager = RunManager()
app = FastAPI(title="zip01 Load Test UI", docs_url=None, redoc_url=None, openapi_url=None)


# -- run control ------------------------------------------------------------


@app.post("/api/runs", status_code=202)
async def start_run(payload: dict[str, Any]) -> dict[str, Any]:
    config = RunConfig.from_payload(payload)
    try:
        run = await manager.start(config)
    except RunInProgressError as exc:
        raise HTTPException(
            status_code=409,
            detail={"error": "run_in_progress", "active": exc.active.public()},
        ) from exc
    asyncio.create_task(supervise(run, manager))
    return run.public()


@app.post("/api/runs/{run_id}/stop")
async def stop_run(run_id: str) -> dict[str, Any]:
    run = manager.current
    if run is None or run.run_id != run_id:
        raise HTTPException(status_code=409, detail={"error": "no_active_run"})
    if run.state in {"done", "failed"}:
        return run.public()  # idempotent: already finished
    await stop_process(manager, run_id)
    return run.public()


@app.get("/api/runs")
def list_runs() -> dict[str, Any]:
    return {"runs": [run.public() for run in manager.history[-20:]]}


@app.get("/api/runs/current")
def current_run() -> dict[str, Any]:
    return manager.current.public() if manager.current else {"state": "idle", "run_id": None}


@app.get("/api/runs/{run_id}")
def get_run(run_id: str) -> dict[str, Any]:
    for run in manager.history:
        if run.run_id == run_id:
            return run.public()
    raise HTTPException(status_code=404, detail={"error": "run_not_found"})


@app.get("/api/runs/{run_id}/report")
def get_report(run_id: str) -> FileResponse:
    path = _REPORTS / f"{run_id}.html"
    if not path.exists():
        raise HTTPException(status_code=404, detail={"error": "report_not_found"})
    return FileResponse(path, media_type="text/html", filename=f"loadtest-{run_id}.html")


# -- live stream ------------------------------------------------------------


def _sse(frame: dict[str, Any]) -> bytes:
    seq = frame.get("seq", 0)
    frame_type = frame.get("type", "message")
    data = json.dumps(frame)
    return f"id: {seq}\nevent: {frame_type}\ndata: {data}\n\n".encode()


@app.get("/api/stream")
async def stream(request: Request) -> StreamingResponse:
    queue = manager.subscribe()

    async def events() -> AsyncIterator[bytes]:
        try:
            # Snapshot first so a (re)connecting client resyncs before live deltas.
            yield _sse({"type": "snapshot", "snapshot": manager.snapshot()})
            while True:
                if await request.is_disconnected():
                    break
                try:
                    frame = await asyncio.wait_for(queue.get(), timeout=_SSE_KEEPALIVE_S)
                except asyncio.TimeoutError:
                    yield _SSE_COMMENT
                    continue
                yield _sse(frame)
        finally:
            manager.unsubscribe(queue)

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# -- SPA static serving (mounted last so /api wins) ---------------------------

if _DIST.exists():
    app.mount("/", StaticFiles(directory=_DIST, html=True), name="spa")


def main() -> None:
    import uvicorn

    uvicorn.run("ui.server.app:app", host="127.0.0.1", port=5174, log_level="info")


if __name__ == "__main__":
    main()
