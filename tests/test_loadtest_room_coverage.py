import unittest

from _loadtest import build_rooms_to_watch


class LoadTestRoomCoverageTests(unittest.TestCase):
    def test_build_rooms_to_watch_covers_all_generated_rooms(self) -> None:
        self.assertEqual(build_rooms_to_watch(200), [f"room_{i:03d}" for i in range(100)])
        self.assertEqual(build_rooms_to_watch(1), ["room_000"])


if __name__ == "__main__":
    unittest.main()
