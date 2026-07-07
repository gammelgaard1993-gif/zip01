from __future__ import annotations

import os
import redis
from typing import Protocol, cast

from config import (
    REDIS_HEALTH_CHECK_INTERVAL_SECONDS,
    REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS,
    REDIS_SOCKET_TIMEOUT_SECONDS,
    REDIS_URL,
)


class _PingCapableRedis(Protocol):
    def ping(self) -> bool:
        ...


def get_redis_client() -> redis.Redis:
    # Local smoke-test fallback: an in-process fakeredis when no real Redis server is available
    # (e.g. no Docker on the dev box). Gated behind USE_FAKE_REDIS=1 so production paths are
    # unaffected. fakeredis speaks the same command surface (SET EX/NX, sorted sets, variadic
    # HSET, pipelines) the handlers rely on.
    if os.getenv("USE_FAKE_REDIS") == "1":
        import fakeredis

        return cast(redis.Redis, fakeredis.FakeRedis(decode_responses=True))

    client = redis.from_url(
        REDIS_URL,
        decode_responses=True,
        protocol=2,
        socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS,
        socket_timeout=REDIS_SOCKET_TIMEOUT_SECONDS,
        socket_keepalive=True,
        health_check_interval=REDIS_HEALTH_CHECK_INTERVAL_SECONDS,
    )

    cast(_PingCapableRedis, client).ping()
    return client
