from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, cast

from config import EVENT_FUTURE_LIMIT, EVENT_PAST_LIMIT, LATE_EVENT_THRESHOLD_SECONDS
from core.metrics import increment_counter
from models import Priority, ValidatedEvent


class ValidationError(ValueError):
    # `reason` is a stable machine code (e.g. invalid_schema / clock_skew_past) that the subscriber
    # buckets into reject metrics; `offset_seconds` carries the skew magnitude for skew rejects.
    def __init__(self, message: str, reason: str, offset_seconds: float | None = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.offset_seconds = offset_seconds


# Envelope fields carried on every event. Everything else in the flat payload is a type-specific
# field (in_room / magnitude / state / confidence / rssi) and is collected into `payload`.
_ENVELOPE_KEYS = {"device_id", "room_id", "type", "ts", "seq"}


def parse_iso_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValidationError(f"invalid timestamp: {value}", reason="invalid_timestamp") from exc
    # Treat a naive timestamp as UTC, then normalise to UTC so downstream comparisons (skew,
    # lateness, ordering) all run on the same clock.
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def validate_raw_event(raw: Any) -> ValidatedEvent:
    # Untrusted MQTT input: every field is type-checked here so malformed events become clean
    # ValidationErrors (rejected + acked) rather than exceptions escaping onto the paho thread.
    if not isinstance(raw, dict):
        raise ValidationError("raw event must be a JSON object", reason="invalid_schema")
    raw_object = cast(Dict[str, Any], raw)

    required_keys = {"device_id", "room_id", "type", "ts"}
    missing = required_keys - raw_object.keys()
    if missing:
        raise ValidationError(f"missing required keys: {sorted(missing)}", reason="invalid_schema")

    device_id = raw_object["device_id"]
    room_id = raw_object["room_id"]
    event_type = raw_object["type"]
    ts_value = raw_object["ts"]

    if not isinstance(device_id, str) or not device_id:
        raise ValidationError("device_id must be a non-empty string", reason="invalid_schema")
    if not isinstance(room_id, str) or not room_id:
        raise ValidationError("room_id must be a non-empty string", reason="invalid_schema")
    if not isinstance(event_type, str) or not event_type:
        raise ValidationError("type must be a non-empty string", reason="invalid_schema")

    # Guard the type before parsing: a non-str ts would make datetime.fromisoformat raise
    # TypeError (not ValueError), which parse_iso_timestamp does not catch.
    if not isinstance(ts_value, str) or not ts_value:
        raise ValidationError("ts must be a non-empty string", reason="invalid_schema")

    # seq is optional and diagnostics-only; when present it must be a real int (bool is an int
    # subclass, so exclude it explicitly).
    seq_value = raw_object.get("seq")
    if seq_value is not None and (not isinstance(seq_value, int) or isinstance(seq_value, bool)):
        raise ValidationError("seq must be an integer", reason="invalid_schema")

    # payload = every non-envelope field (in_room / magnitude / state / confidence / rssi ...).
    payload: Dict[str, Any] = {k: v for k, v in raw_object.items() if k not in _ENVELOPE_KEYS}

    ts = parse_iso_timestamp(ts_value)
    now = datetime.now(timezone.utc)
    # Reject events whose clock is off by more than the allowed window in either direction.
    offset_seconds = (ts - now).total_seconds()
    if ts > now + EVENT_FUTURE_LIMIT:
        raise ValidationError(
            "event timestamp is too far in the future",
            reason="clock_skew_future",
            offset_seconds=offset_seconds,
        )
    if ts < now - EVENT_PAST_LIMIT:
        raise ValidationError(
            "event timestamp is too far in the past",
            reason="clock_skew_past",
            offset_seconds=offset_seconds,
        )

    # Accepted but old events are flagged late (not rejected) so handlers can reconcile them;
    # fall_warn is the only high-priority type.
    late = (now - ts).total_seconds() > LATE_EVENT_THRESHOLD_SECONDS
    priority = Priority.HIGH if event_type == "fall_warn" else Priority.NORMAL

    if late:
        increment_counter("events_late")

    return ValidatedEvent(
        device_id=device_id,
        room_id=room_id,
        type=event_type,
        ts=ts,
        payload=payload,
        late=late,
        priority=priority,
        received_at=now,
        seq=seq_value,
    )
