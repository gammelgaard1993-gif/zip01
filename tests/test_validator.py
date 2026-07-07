from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from ingestion.validator import ValidationError, validate_raw_event


class ValidatorTests(unittest.TestCase):
    def _base_event(self) -> dict[str, object]:
        return {
            "device_id": "dev_1",
            "room_id": "room_1",
            "type": "heartbeat",
            "ts": datetime.now(timezone.utc).isoformat(),
            "seq": 1,
        }

    def test_rejects_timestamps_more_than_one_hour_in_future(self) -> None:
        event = self._base_event()
        event["ts"] = (datetime.now(timezone.utc) + timedelta(hours=1, seconds=1)).isoformat()

        with self.assertRaises(ValidationError) as ctx:
            validate_raw_event(event)

        self.assertEqual(ctx.exception.reason, "clock_skew_future")

    def test_rejects_timestamps_more_than_one_hour_in_past(self) -> None:
        event = self._base_event()
        event["ts"] = (datetime.now(timezone.utc) - timedelta(hours=1, seconds=1)).isoformat()

        with self.assertRaises(ValidationError) as ctx:
            validate_raw_event(event)

        self.assertEqual(ctx.exception.reason, "clock_skew_past")

    def test_flags_late_events_within_past_hour(self) -> None:
        event = self._base_event()
        event["ts"] = (datetime.now(timezone.utc) - timedelta(seconds=45)).isoformat()

        validated = validate_raw_event(event)

        self.assertTrue(validated.late)

    def test_rejects_non_string_ts_as_invalid_schema(self) -> None:
        event = self._base_event()
        event["ts"] = 1719960000

        with self.assertRaises(ValidationError) as ctx:
            validate_raw_event(event)

        self.assertEqual(ctx.exception.reason, "invalid_schema")

    def test_rejects_unparseable_ts_string(self) -> None:
        event = self._base_event()
        event["ts"] = "not-a-timestamp"

        with self.assertRaises(ValidationError) as ctx:
            validate_raw_event(event)

        self.assertEqual(ctx.exception.reason, "invalid_timestamp")

    def test_builds_payload_from_flat_type_fields(self) -> None:
        event = self._base_event()
        event["type"] = "fall_warn"
        event["confidence"] = 0.92

        validated = validate_raw_event(event)

        # Envelope fields are stripped; only the type-specific field lands in payload.
        self.assertEqual(validated.payload, {"confidence": 0.92})
        self.assertEqual(validated.seq, 1)

    def test_accepts_flat_event_without_payload_key(self) -> None:
        event = self._base_event()  # heartbeat, no nested "payload" key at all

        validated = validate_raw_event(event)

        self.assertEqual(validated.payload, {})

    def test_seq_is_optional(self) -> None:
        event = self._base_event()
        del event["seq"]

        validated = validate_raw_event(event)

        self.assertIsNone(validated.seq)

    def test_rejects_non_int_seq(self) -> None:
        event = self._base_event()
        event["seq"] = "42"

        with self.assertRaises(ValidationError) as ctx:
            validate_raw_event(event)

        self.assertEqual(ctx.exception.reason, "invalid_schema")

    def test_rejects_missing_required_field(self) -> None:
        event = self._base_event()
        del event["room_id"]

        with self.assertRaises(ValidationError) as ctx:
            validate_raw_event(event)

        self.assertEqual(ctx.exception.reason, "invalid_schema")


if __name__ == "__main__":
    unittest.main()
