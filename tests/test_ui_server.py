"""Tests for the load-test UI control plane (ui/server).

Covers the run state machine and the runner subprocess wiring without touching the real
system-under-test or spawning real load. RunManager/SSE-bus behavior is exercised in-process;
runner argv and stdout parsing are tested with fakes so no subprocess is launched.
"""

from __future__ import annotations

import asyncio
import unittest

from ui.server.runner import _pump_stdout, _runner_argv
from ui.server.state import RunConfig, RunInProgressError, RunManager


def _config(**overrides) -> RunConfig:
    base = {
        "scenario": "custom",
        "devices": 25,
        "duration_seconds": 15.0,
        "rps": 1.0,
        "connections": 8,
        "metrics_interval_seconds": 1.0,
        "burst": False,
        "target": "http://127.0.0.1:8080",
    }
    base.update(overrides)
    return RunConfig(**base)


class RunConfigTests(unittest.TestCase):
    def test_from_payload_applies_defaults_for_missing_fields(self) -> None:
        config = RunConfig.from_payload({})
        self.assertEqual(config.devices, 500)
        self.assertEqual(config.duration_seconds, 90.0)
        self.assertEqual(config.rps, 1.0)
        self.assertEqual(config.target, "http://127.0.0.1:8080")
        self.assertFalse(config.burst)

    def test_from_payload_coerces_types(self) -> None:
        config = RunConfig.from_payload(
            {"devices": "40", "duration_seconds": "12", "burst": True, "scenario": "custom"}
        )
        self.assertEqual(config.devices, 40)
        self.assertEqual(config.duration_seconds, 12.0)
        self.assertTrue(config.burst)


class RunManagerLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_creates_running_run_and_records_history(self) -> None:
        manager = RunManager()
        run = await manager.start(_config())
        self.assertEqual(run.state, "running")
        self.assertIs(manager.current, run)
        self.assertIn(run, manager.history)
        self.assertIs(manager.active(), run)

    async def test_second_start_while_running_raises_run_in_progress(self) -> None:
        manager = RunManager()
        await manager.start(_config())
        with self.assertRaises(RunInProgressError) as ctx:
            await manager.start(_config())
        self.assertIsNotNone(ctx.exception.active)

    async def test_start_allowed_after_run_finishes(self) -> None:
        manager = RunManager()
        first = await manager.start(_config())
        manager.apply_frame({"type": "done", "summary": {}, "report_path": "x"})
        second = await manager.start(_config())
        self.assertIs(manager.current, second)
        self.assertNotEqual(first.run_id, second.run_id)

    async def test_progress_frame_updates_sent_and_failed(self) -> None:
        manager = RunManager()
        run = await manager.start(_config())
        manager.apply_frame({"type": "progress", "sent": 120, "failed": 3})
        self.assertEqual(run.sent, 120)
        self.assertEqual(run.failed, 3)

    async def test_done_frame_marks_done_and_stores_summary(self) -> None:
        manager = RunManager()
        run = await manager.start(_config())
        manager.apply_frame({"type": "done", "summary": {"sent": 10}, "report_path": "/r.html"})
        self.assertEqual(run.state, "done")
        self.assertEqual(run.summary, {"sent": 10})
        self.assertEqual(run.report_path, "/r.html")
        self.assertIsNone(manager.active())

    async def test_error_frame_marks_failed(self) -> None:
        manager = RunManager()
        run = await manager.start(_config())
        manager.apply_frame({"type": "error", "message": "boom"})
        self.assertEqual(run.state, "failed")

    async def test_mark_stopping_only_from_running(self) -> None:
        manager = RunManager()
        run = await manager.start(_config())
        manager.mark_stopping()
        self.assertEqual(run.state, "stopping")
        manager.apply_frame({"type": "done", "summary": {}, "report_path": "x"})
        manager.mark_stopping()
        self.assertEqual(run.state, "done")  # terminal state not overwritten


class RunManagerBusTests(unittest.IsolatedAsyncioTestCase):
    async def test_broadcast_assigns_monotonic_seq(self) -> None:
        manager = RunManager()
        queue = manager.subscribe()
        manager.apply_frame({"type": "progress", "sent": 1, "failed": 0})
        manager.apply_frame({"type": "progress", "sent": 2, "failed": 0})
        first = queue.get_nowait()
        second = queue.get_nowait()
        self.assertEqual(second["seq"], first["seq"] + 1)

    async def test_slow_subscriber_is_evicted_when_queue_full(self) -> None:
        manager = RunManager(subscriber_queue_size=2)
        queue = manager.subscribe()
        for i in range(5):  # overflow the bounded queue
            manager.apply_frame({"type": "progress", "sent": i, "failed": 0})
        self.assertNotIn(queue, manager._subscribers)  # noqa: SLF001

    async def test_unsubscribe_removes_queue(self) -> None:
        manager = RunManager()
        queue = manager.subscribe()
        manager.unsubscribe(queue)
        manager.apply_frame({"type": "progress", "sent": 1, "failed": 0})
        self.assertTrue(queue.empty())


class RunnerArgvTests(unittest.IsolatedAsyncioTestCase):
    async def test_argv_includes_jsonl_and_core_flags(self) -> None:
        manager = RunManager()
        run = await manager.start(_config())
        argv = _runner_argv(run)
        self.assertIn("--jsonl", argv)
        self.assertIn("-u", argv)
        self.assertIn("--devices", argv)
        self.assertIn("25", argv)
        self.assertNotIn("--burst", argv)

    async def test_argv_adds_burst_flag_when_enabled(self) -> None:
        manager = RunManager()
        run = await manager.start(_config(burst=True))
        self.assertIn("--burst", _runner_argv(run))

    async def test_argv_report_path_uses_run_id(self) -> None:
        manager = RunManager()
        run = await manager.start(_config())
        argv = _runner_argv(run)
        report_index = argv.index("--report")
        self.assertIn(run.run_id, argv[report_index + 1])


class _FakeStream:
    """Async readline() over a fixed list of byte lines, then EOF."""

    def __init__(self, lines: list[bytes]) -> None:
        self._lines = list(lines)

    async def readline(self) -> bytes:
        if not self._lines:
            return b""
        return self._lines.pop(0)


class _FakeProcess:
    def __init__(self, lines: list[bytes]) -> None:
        self.stdout = _FakeStream(lines)


class PumpStdoutTests(unittest.IsolatedAsyncioTestCase):
    async def test_parses_json_lines_and_skips_banner_text(self) -> None:
        manager = RunManager()
        await manager.start(_config())
        lines = [
            b"Target : http://127.0.0.1:8080\n",  # human banner -> skipped
            b'{"type": "progress", "sent": 42, "failed": 1}\n',
            b"not json at all\n",
            b'{"type": "done", "summary": {"sent": 42}, "report_path": "/r.html"}\n',
        ]
        process = _FakeProcess(lines)
        await _pump_stdout(process, manager)  # type: ignore[arg-type]
        run = manager.current
        self.assertIsNotNone(run)
        assert run is not None
        self.assertEqual(run.sent, 42)
        self.assertEqual(run.failed, 1)
        self.assertEqual(run.state, "done")

    async def test_malformed_json_line_is_ignored(self) -> None:
        manager = RunManager()
        await manager.start(_config())
        process = _FakeProcess([b'{"type": "progress", "sent": \n', b'{"type": "progress", "sent": 7, "failed": 0}\n'])
        await _pump_stdout(process, manager)  # type: ignore[arg-type]
        assert manager.current is not None
        self.assertEqual(manager.current.sent, 7)


if __name__ == "__main__":
    unittest.main()
