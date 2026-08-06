from __future__ import annotations

import unittest
from typing import Any, cast
from unittest.mock import patch

from api.app import app
from core.db import init_db
from tests.fakes import FakeRedis, ResponseLike

try:
    from fastapi.testclient import TestClient
except RuntimeError as exc:
    TestClient = None
    _testclient_import_error = str(exc)
else:
    _testclient_import_error = ""


class AppLifespanSmokeTests(unittest.TestCase):
    def test_real_app_lifespan_starts_and_serves_read_endpoints(self) -> None:
        if TestClient is None:
            self.skipTest(f"TestClient unavailable in this environment: {_testclient_import_error}")

        # Use the production FastAPI app + lifespan with patched infra factories so startup/
        # shutdown wiring is exercised without requiring external Redis or filesystem DB state.
        db_connection = init_db(":memory:")

        with patch("core.db.init_db", return_value=db_connection), patch(
            "core.redis_client.get_redis_client", return_value=FakeRedis()
        ):
            with TestClient(app) as client:
                client_any = cast(Any, client)
                metrics_response = cast(ResponseLike, client_any.get("/metrics"))
                self.assertEqual(metrics_response.status_code, 200)
                metrics_body = cast(dict[str, object], metrics_response.json())
                self.assertIn("counters", metrics_body)

                health_response = cast(ResponseLike, client_any.get("/devices/dev_unknown/health"))
                self.assertEqual(health_response.status_code, 404)


if __name__ == "__main__":
    unittest.main()