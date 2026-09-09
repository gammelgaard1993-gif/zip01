import unittest

from _loadtest import Stats, build_dashboard_html, build_pressure_graph, build_rooms_to_watch, summarize_metrics


class LoadTestRoomCoverageTests(unittest.TestCase):
    def test_build_rooms_to_watch_covers_all_generated_rooms(self) -> None:
        self.assertEqual(build_rooms_to_watch(200), [f"room_{i:03d}" for i in range(100)])
        self.assertEqual(build_rooms_to_watch(1), ["room_000"])


class LoadTestMetricsTests(unittest.TestCase):
    def test_summarize_metrics_returns_counter_deltas_and_queue_peaks(self) -> None:
        summary = summarize_metrics(
            [
                {
                    "elapsed_seconds": 0.0,
                    "phase": "before",
                    "counters": {"events_ingested_total": 10, "queue_depth_normal": 0},
                },
                {
                    "elapsed_seconds": 1.0,
                    "phase": "burst",
                    "counters": {"events_ingested_total": 25, "queue_depth_normal": 8},
                },
                {
                    "elapsed_seconds": 2.0,
                    "phase": "after",
                    "counters": {"events_ingested_total": 30, "queue_depth_normal": 0},
                },
            ]
        )

        self.assertEqual(summary["delta_events_ingested_total"], 20)
        self.assertEqual(summary["max_queue_depth_normal"], 8)
        self.assertEqual(summary["final_queue_depth_normal"], 0)

    def test_summarize_metrics_reports_worker_activity_as_gauges(self) -> None:
        summary = summarize_metrics(
            [
                {"elapsed_seconds": 0.0, "phase": "baseline", "counters": {"worker_active_device_buffers": 3}},
                {"elapsed_seconds": 1.0, "phase": "after", "counters": {"worker_active_device_buffers": 0}},
            ]
        )

        self.assertEqual(summary["max_worker_active_device_buffers"], 3)
        self.assertEqual(summary["final_worker_active_device_buffers"], 0)
        self.assertNotIn("delta_worker_active_device_buffers", summary)

    def test_pressure_graph_marks_normal_and_priority_peaks(self) -> None:
        graph = build_pressure_graph(
            [
                {"elapsed_seconds": 0.0, "phase": "baseline", "counters": {}},
                {
                    "elapsed_seconds": 1.0,
                    "phase": "burst",
                    "counters": {
                        "queue_depth_normal": 4,
                        "sqlite_writer_queue_depth_priority": 2,
                        "worker_queue_depth_normal": 1,
                    },
                },
            ]
        )

        self.assertIn("    Ingress  .+  peak=4", graph)
        self.assertIn("    Writer   .#  peak=2", graph)
        self.assertIn("    Workers  .+  peak=1", graph)

    def test_stats_tracks_planned_dispatches_and_non_negative_schedule_lag(self) -> None:
        stats = Stats()
        stats.record_dispatch(-5.0)
        stats.record_dispatch(12.5)

        self.assertEqual(stats.planned, 2)
        self.assertEqual(stats.dispatched, 2)
        self.assertEqual(stats.schedule_lag_ms, [0.0, 12.5])

    def test_dashboard_contains_pressure_panels_and_room_heatmap_data(self) -> None:
        dashboard = build_dashboard_html(
            [{"elapsed_seconds": 0.0, "phase": "baseline", "counters": {"queue_depth_normal": 2}}],
            {"room_000": {0: 3}},
        )

        self.assertIn("Ingress Requests / Second", dashboard)
        self.assertIn("SQLite Writer Batches / Second", dashboard)
        self.assertIn("Worker Events / Second", dashboard)
        self.assertIn("Room Traffic Profile", dashboard)
        self.assertIn("Top 10 Busiest Rooms", dashboard)
        self.assertIn("all rooms, sorted busiest to quietest", dashboard)
        self.assertIn("const chartDefinitions", dashboard)
        self.assertIn("'events_ingested_total'", dashboard)
        self.assertIn('id="phase"', dashboard)
        self.assertIn('id="focus"', dashboard)
        self.assertIn("function updateSummary", dashboard)
        self.assertIn("function roomDistribution", dashboard)
        self.assertIn('"room_000": {"0": 3}', dashboard)

if __name__ == "__main__":
    unittest.main()
