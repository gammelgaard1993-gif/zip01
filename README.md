# zip01

Real-time streaming backend for sensor events with prioritized processing, Redis hot state, SQLite durability, and FastAPI APIs.

## Quickstart

Requires Docker (for Mosquitto + Redis) and Python 3.12+.

```bash
make deps     # install pinned dependencies
make run      # start Mosquitto + Redis (Docker) and the service on :8080
```

In a second shell, drive load and inspect the API:

```bash
make test                 # 37 unit + integration tests
make smoke                # quick end-to-end check (service must be running)
DEVICES=500 make burst    # 10x burst — verify no drops + alarm p95 <= 1s
make offline              # offline device replays a 20-min backlog of late events
```

See [SUBMISSION.md](SUBMISSION.md) for the design summary and [Makefile](Makefile) for all targets.

### Running on Windows (no `make`/`docker`)

Start Redis and Mosquitto however you prefer (native installs or Docker Desktop),
then run the service and simulator directly:

```powershell
python -m pip install -r requirements.txt
python main.py                                              # service on :8080
python tools/simulator.py steady  --devices 500 --duration 30          # baseline
python tools/simulator.py burst   --devices 500 --duration 30 --rate 50000
python tools/simulator.py offline --offline-minutes 20 --events 1200
python -m unittest discover -s tests -v                    # tests
```

The service connects to `localhost:6379` (Redis, required) by default and listens on `:8080`.
HTTP `POST /events` is the primary transport (what the reference generator
[event_generator/generate.py](event_generator/generate.py) posts to); the MQTT subscriber on
`localhost:1883` is optional and off by default (`ENABLE_MQTT=False`) — see [config.py](config.py).

## Documentation

Project documentation is available in the docs folder:

- [Documentation Index](docs/README.md)
- [Architecture](docs/architecture.md)
- [Event Flow](docs/event-flow.md)
- [Critical Functions](docs/critical-functions.md)
- [API Reference](docs/api-reference.md)
- [Storage and Recovery](docs/storage-recovery.md)
