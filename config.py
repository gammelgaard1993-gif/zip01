import os
from datetime import timedelta

# HTTP ingestion / API server (primary transport). The reference generator POSTs one flat JSON
# event per request to /events and defaults its --target to port 8080.
HTTP_HOST = "0.0.0.0"
HTTP_PORT = 8080

# Optional secondary transport. Off by default so the service runs HTTP-only without a broker
# present; the MQTT_* settings below are only used when this is enabled.
ENABLE_MQTT = False

MQTT_BROKER_URL = "localhost"
MQTT_BROKER_PORT = 1883
MQTT_TOPIC = "teton/devices/+/events"
MQTT_CLIENT_ID = "zip01-backend"

REDIS_URL = "redis://localhost:6379/0"
SQLITE_PATH = "./teton.db"

# Redis client resiliency knobs. socket_connect_timeout makes the startup liveness PING fail fast
# instead of blocking app boot if Redis is unreachable; socket_timeout bounds every command read
# so a hot-path op can't hang forever on a dead peer; health_check_interval makes redis-py PING
# idle pooled connections before reuse, so a connection killed by a server restart or idle TCP
# reset is detected proactively rather than surfacing as a failure on the next hot-path command.
REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS = 5
REDIS_SOCKET_TIMEOUT_SECONDS = 5
REDIS_HEALTH_CHECK_INTERVAL_SECONDS = 30

NORMAL_QUEUE_MAX_SIZE = 500_000
WORKER_COUNT = 8
# Experimental I/O offload (A/B benchmark switch; see tools/bench.py). When enabled, the worker
# pool moves the blocking per-event SQLite INSERT+commit off the single uvicorn event loop and
# onto a dedicated background thread, so the loop stays free to process other devices' handlers
# during the disk write. Off by default so runtime behaviour is unchanged unless explicitly
# opted in via USE_EXECUTOR_IO=1 in the environment.
USE_EXECUTOR_IO = os.getenv("USE_EXECUTOR_IO", "0") == "1"
# Two sequential reorder stages (per-device in the worker pool, per-room in the alarm bus)
# sit on the alarm hot path. Keep each at 100ms so the cumulative reorder budget stays well
# under the 1s p95 alarm-delivery target, even with queue draining + handler time on top.
DEVICE_REORDER_BUFFER_MS = 100
ALARM_REORDER_BUFFER_MS = 100

HEARTBEAT_WINDOW_SECONDS = 300
OCCUPANCY_WINDOW_SECONDS = 3600
LATE_EVENT_THRESHOLD_SECONDS = 30
EVENT_FUTURE_LIMIT = timedelta(hours=1)
EVENT_PAST_LIMIT = timedelta(hours=1)

STATE_SNAPSHOT_INTERVAL_SECONDS = 60

FALL_DEDUP_TTL_SECONDS = 10
