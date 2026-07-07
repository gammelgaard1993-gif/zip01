from __future__ import annotations

import os
import uvicorn

from config import HTTP_HOST, HTTP_PORT

if __name__ == "__main__":
    port = int(os.getenv("PORT", str(HTTP_PORT)))
    uvicorn.run("api.app:app", host=HTTP_HOST, port=port, log_level="info")
