# Submission — Teton Real-Time Streaming Backend (zip01)

## 1. Link to fork

`https://github.com/gammelgaard1993-gif/zip01` (branch: `main`)

## 2. Writeup (< 400 words)

**Stack & storage, why.** Python 3.12 + FastAPI/uvicorn, single async process.
Redis holds hot ephemeral state — last heartbeat, presence zsets, dedup keys —
because it's fast and TTL-native, a good fit for rolling windows. SQLite (WAL,
`synchronous=NORMAL`) is the append-only durable log and the sole source of
truth for restart recovery: zero ops overhead for a single-node submission,
while still giving durable-before-ack semantics. Both sit behind narrow
interfaces, so either is swappable later.

**Late events & ordering.** Every event carries its own `ts`; ingestion never
trusts arrival order. Each device is routed by consistent hashing to one of 8
workers, which buffers that device's events for 100ms and re-sorts by `ts`
before applying handlers. Heartbeat/presence handlers are idempotent and
timestamp-gated — they only update state when the incoming `ts` is newer than
what's stored — so a replayed backlog from a reconnecting offline device
retroactively corrects rolling occupancy/availability windows with no special
"replay" code path. Fall-warning dedup uses a deterministic key
`SHA256(device_id:room_id:ts-truncated-to-second)`, so jitter-duplicates or a
client retry after a dropped connection always collapse to the same key. A
SQLite `UNIQUE(dedup_key)` `INSERT OR IGNORE` is the authoritative, insert-first
check; Redis is only a best-effort fast-path cache on top of it.

**Backpressure.** Two lanes: an unbounded HIGH lane for `fall_warn`, a bounded
(500k) NORMAL lane for everything else — workers always drain HIGH first. When
NORMAL fills, `POST /events` simply delays its response until capacity frees up.
Backpressure is pushed to the sender; nothing is silently dropped, and fall
alarms never queue behind a normal-event backlog even at 10x burst.

**Restart correctness.** Every accepted event is durably logged before it's
acknowledged. A Redis state snapshot is captured every 60s off the hot path. On
restart, the latest snapshot loads, then events since its inclusive cutoff
replay through the same idempotent handlers, so recovered state matches live
state exactly.

**With another week:** fix a rough edge in my own eval workflow — my scorecard
script queries alarms since the beginning of time, so re-running scenarios
against a persistent service without a manual reset makes cumulative alarm
counts look like duplicate leaks when they're not. I'd add a `--since` flag (or
auto-record each run's start time) so a scenario's result is self-contained.

## 3. How to run locally

```bash
# One-time setup
python -m pip install -r requirements.txt

# Start Redis (required — Docker Desktop or native Memurai)
docker compose up -d redis
# Or: winget install Memurai.MemuraiDeveloper

# Start the service (leave running)
python main.py                      # → http://localhost:8080

# Drive load (separate terminal)
python event_generator/generate.py --mode baseline --devices 100 --target http://localhost:8080
python event_generator/generate.py --mode adversarial --devices 50 --duration 240 --target http://localhost:8080

# Inspect
curl.exe -s http://localhost:8080/metrics
curl.exe -s http://localhost:8080/devices/dev_0000/health
curl.exe -s "http://localhost:8080/rooms/room_000/occupancy?window=5m"
curl.exe -s "http://localhost:8080/alarms?since=0"
curl.exe -N  http://localhost:8080/alarms/stream    # live SSE (Ctrl+C to stop)

# Run the test suite
python -m unittest discover -s tests -v              # 108 tests

# Clean reset between scored runs (stop service first)
Get-Process -Id (Get-NetTCPConnection -LocalPort 8080 -State Listen).OwningProcess | Stop-Process -Force
Remove-Item .\teton.db, .\teton.db-wal, .\teton.db-shm -Force
docker exec teton-redis redis-cli FLUSHALL
python main.py
```

Test suite: **108/108 pass** (`python -m unittest discover -s tests -v`).
Eval scorecard (clean-slate `adversarial` run — burst + offline + ±30s clock
skew): 206 events generated, 3 distinct falls, 3 alarms returned, exact match.

## 4. CV, LinkedIn, GitHub

- **CV:** attached separately
- **LinkedIn:** `https://www.linkedin.com/in/max-gammelgaard/`
- **GitHub:** `https://github.com/gammelgaard1993-gif`
