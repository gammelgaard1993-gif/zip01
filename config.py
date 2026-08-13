from datetime import timedelta

# HTTP ingestion / API server (primary transport). The reference generator POSTS one flat JSON
# event per request to /events and defaults its --target to port 8080.
HTTP_HOST = "0.0.0.0"
HTTP_PORT = 8080

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
# Very large bound far above any real fall_warn burst (fall_warn is dedup-gated), so it only
# triggers backpressure under an adversarial flood — it awaits capacity, never a silent drop.
HIGH_QUEUE_MAX_SIZE = 100_000
WORKER_COUNT = 32
# Per-worker NORMAL lane bound. Each worker owns a two-lane priority queue (HIGH drained first),
# so a full worker NORMAL lane backpressures the router (and in turn the HTTP ingress)
# instead of growing an unbounded downstream FIFO. HIGH uses HIGH_QUEUE_MAX_SIZE per worker.
WORKER_NORMAL_QUEUE_MAX_SIZE = 100_000
# Small pool of router tasks sharing the global queue, instead of a single router task, so one
# congested worker's queue can't head-of-line-block routing to the rest. Raises (does not
# eliminate) the stall threshold: routing only stalls fully if >= ROUTER_TASK_COUNT workers are
# congested at once. No cross-task ordering coordination is needed: per-device processing order
# is decided by each event's own ts field (re-sorted in the worker pool), not by put() order.
ROUTER_TASK_COUNT = 12
# Two sequential reorder stages (per-device in the worker pool, per-room in the alarm bus)
# sit on the alarm hot path. The previous 100ms budget was enough to preserve ordering but was
# too large for the latency SLO under bursty traffic. Trimming this down makes the live path
# respond sooner while still avoiding pathological out-of-order delivery.
DEVICE_REORDER_BUFFER_MS = 5
ALARM_REORDER_BUFFER_MS = 5
ALARM_REPLAY_BATCH_SIZE = 500
# Bound per-SSE-subscriber fan-out memory. A subscriber that stops draining its queue (stalled
# connection, slow client) is evicted once full rather than growing this queue unbounded or
# blocking delivery to other subscribers in the same room; the evicted client must reconnect
# with `since` to resume without a gap.
SSE_SUBSCRIBER_QUEUE_MAX_SIZE = 1_000
# SSE keep-alive comment interval. Clients with no alarms for this many seconds would otherwise
# time out waiting on queue.get(); a comment line keeps the TCP connection alive.
SSE_KEEPALIVE_INTERVAL_S = 15

HEARTBEAT_WINDOW_SECONDS = 300
OCCUPANCY_WINDOW_SECONDS = 3600
LATE_EVENT_THRESHOLD_SECONDS = 30
EVENT_FUTURE_LIMIT = timedelta(hours=1)
EVENT_PAST_LIMIT = timedelta(hours=1)
# Real events are a few hundred bytes; this is a generous ceiling to bound per-request memory.
MAX_EVENT_BYTES = 16_384

STATE_SNAPSHOT_INTERVAL_SECONDS = 60
# Snapshots are only ever read by "newest one" (see core/recovery.py _load_latest_snapshot), so
# older rows exist purely for audit/debugging. Pruned after every insert to keep state_snapshots
# from growing unbounded over a long-running deployment.
STATE_SNAPSHOT_RETENTION_COUNT = 20

FALL_DEDUP_TTL_SECONDS = 10

# Dedicated single-writer-thread SQLite batching (Phase 6 / #13): amortizes the commit/fsync cost
# of persist-before-ack across many events instead of paying it per-event. Kept small so batching
# never taxes the alarm p95<=1s SLO; priority (fall_warn) writes skip the wait entirely (see
# core/db_writer.py).
SQLITE_WRITER_BATCH_WINDOW_SECONDS = 0.005
SQLITE_WRITER_MAX_BATCH_SIZE = 200
# Bounds the writer thread's internal NORMAL-lane backlog so a stalled/slow disk backpressures
# callers (submit() fails fast with SQLiteWriterError) instead of growing memory unbounded -- the
# same backpressure-over-drop policy already applied to every other queue in this system.
SQLITE_WRITER_QUEUE_MAX_SIZE = 200_000
# Separate, independently-bounded PRIORITY-lane (fall_warn) backlog so a NORMAL-lane flood can
# never reject a priority write with queue.Full -- sized the same as HIGH_QUEUE_MAX_SIZE since
# both bound the same upstream fall_warn traffic, far above any real fall_warn burst.
SQLITE_WRITER_PRIORITY_QUEUE_MAX_SIZE = HIGH_QUEUE_MAX_SIZE

# Shared thread pool for offloading synchronous redis-py calls (handlers + recovery snapshot
# capture) off the event loop. Local Redis round trips are sub-millisecond, so this comfortably
# covers the 5k sustained / 50k burst target without the loop ever blocking on I/O.
REDIS_EXECUTOR_MAX_WORKERS = 32
