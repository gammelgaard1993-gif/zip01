from __future__ import annotations

import uvicorn

from api.app import app
from config import HTTP_HOST, HTTP_PORT

if __name__ == "__main__":
    server = uvicorn.Server(uvicorn.Config(app, host=HTTP_HOST, port=HTTP_PORT, log_level="info"))
    # Exposes a self-shutdown handle (server.should_exit = True) to in-process code that detects
    # an unrecoverable invariant violation (see api/routes/events.py's fatal enqueue-failure path).
    app.state.server = server
    server.run()
