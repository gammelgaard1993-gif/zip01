"""Spawns and supervises the load-test runner subprocess.

The runner (helpers/_loadtest.py) is launched with --jsonl so it emits one JSON object per
line on stdout. We read that pipe continuously from the moment the process starts (a full pipe
would block the runner and distort the very load profile being measured), fold each record into
RunManager state, and fan it out to SSE subscribers. The runner does all measurement; this module
is a dumb relay.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from .state import Run, RunManager

_REPO_ROOT = Path(__file__).resolve().parents[2]
_RUNNER = _REPO_ROOT / "helpers" / "_loadtest.py"


def _runner_argv(run: Run) -> list[str]:
    cfg = run.config
    argv = [
        sys.executable,
        "-u",  # unbuffered stdout so JSON-lines flush through the pipe promptly
        str(_RUNNER),
        "--jsonl",
        "--target", cfg.target,
        "--devices", str(cfg.devices),
        "--duration", str(cfg.duration_seconds),
        "--rps", str(cfg.rps),
        "--connections", str(cfg.connections),
        "--metrics-interval", str(cfg.metrics_interval_seconds),
        "--report", str(_REPO_ROOT / "ui" / "server" / "reports" / f"{run.run_id}.html"),
    ]
    if cfg.burst:
        argv.append("--burst")
    return argv


async def _pump_stdout(process: asyncio.subprocess.Process, manager: RunManager) -> None:
    assert process.stdout is not None
    while True:
        line = await process.stdout.readline()
        if not line:
            break
        text = line.decode(errors="replace").strip()
        if not text.startswith("{"):
            continue  # human-readable banner lines interleaved with JSONL
        try:
            frame = json.loads(text)
        except json.JSONDecodeError:
            continue
        manager.apply_frame(frame)


async def supervise(run: Run, manager: RunManager) -> None:
    """Run the runner subprocess to completion, streaming its frames into the manager."""
    report_dir = _REPO_ROOT / "ui" / "server" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    process = await asyncio.create_subprocess_exec(
        *_runner_argv(run),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=str(_REPO_ROOT),
    )
    manager._processes[run.run_id] = process  # noqa: SLF001 - registry for stop()
    try:
        await _pump_stdout(process, manager)
        returncode = await process.wait()
        # If the runner died without emitting a terminal frame, mark it so the UI settles.
        if manager.current is run and run.state in {"running", "stopping"}:
            run.state = "done" if returncode == 0 else "failed"
            manager.apply_frame({"type": "state", "state": run.state, "run_id": run.run_id})
    finally:
        manager._processes.pop(run.run_id, None)  # noqa: SLF001


async def stop_process(manager: RunManager, run_id: str) -> bool:
    process = manager._processes.get(run_id)  # noqa: SLF001
    if process is None or process.returncode is not None:
        return False
    manager.mark_stopping()
    try:
        process.terminate()
    except ProcessLookupError:
        return False
    return True
