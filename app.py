from __future__ import annotations

import uvicorn

from config import HTTP_HOST, HTTP_PORT

if __name__ == "__main__":
    uvicorn.run("api.app:app", host=HTTP_HOST, port=HTTP_PORT, log_level="info")
