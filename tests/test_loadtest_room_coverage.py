import unittest

from _loadtest import build_rooms_to_watch, summarize_metrics


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

if __name__ == "__main__":
    unittest.main()
