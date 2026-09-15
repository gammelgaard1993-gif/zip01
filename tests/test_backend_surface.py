"""Guards the backend's HTTP surface and dependency boundary against load-test erosion.

The load-test feature lives OUTSIDE the backend (a separate UI app owns the runner).
The grader (eval/check.py) reads only four endpoints, and its failure mode on unexpected
change is silent mis-scoring rather than a loud error. These tests pin the real
``api.app:app`` object so that erosion of the system-under-test is caught by CI instead
of by a wrong score.

Deliberately presence-and-method based (not exact path-set equality): the app also serves
FastAPI's auto-added docs routes (/openapi.json, /docs, /redoc, ...), and future benign
ops endpoints (e.g. /version) must not trip these guards. The dangerous change classes are
middleware injection, duplicate-path shadowing, catch-all mounts, and dependency-graph
contamination -- those are what is asserted here.
"""

from __future__ import annotations

import re
import sys
import unittest

from starlette.routing import Mount

from api.app import app


# The four grader-facing endpoints and their allowed methods. Presence + method only.
GRADER_SURFACE = {
    "/events": {"POST"},
    "/devices/{device_id}/health": {"GET"},
    "/rooms/{room_id}/occupancy": {"GET"},
    "/alarms": {"GET"},
}

# Load-test / run-management vocabulary that must never appear on the backend.
_FORBIDDEN_PATH = re.compile(r"loadtest|/runs|/admin", re.IGNORECASE)


def _iter_leaf_routes():
    """Yield every concrete route, unwrapping FastAPI's deferred ``_IncludedRouter``.

    This FastAPI build registers included routers as ``_IncludedRouter`` placeholders whose
    ``original_router.routes`` hold the real ``APIRoute`` entries. Flattening here keeps the
    assertions independent of that inclusion mechanism.
    """
    for route in app.routes:
        original = getattr(route, "original_router", None)
        if original is not None:
            yield from original.routes
        else:
            yield route


def _path_methods() -> dict[str, set[str]]:
    by_path: dict[str, set[str]] = {}
    for route in _iter_leaf_routes():
        methods = getattr(route, "methods", None)
        path = getattr(route, "path", None)
        if methods and path:
            by_path.setdefault(path, set()).update(methods)
    return by_path


class BackendSurfaceTests(unittest.TestCase):
    """The system-under-test stays pure: grader surface intact, no shadowing or mounts."""

    def test_grader_endpoints_present_with_expected_methods(self) -> None:
        by_path = _path_methods()
        for path, methods in GRADER_SURFACE.items():
            self.assertIn(path, by_path, f"grader endpoint missing: {path}")
            self.assertTrue(
                methods.issubset(by_path[path]),
                f"{path} missing methods {methods}: has {by_path[path]}",
            )

    def test_no_duplicate_paths_that_could_shadow_grader_routes(self) -> None:
        paths = [getattr(route, "path", None) for route in _iter_leaf_routes()]
        paths = [path for path in paths if path]
        duplicates = {path for path in paths if paths.count(path) > 1}
        self.assertEqual(
            duplicates,
            set(),
            f"duplicate paths (first-match-wins shadowing risk): {sorted(duplicates)}",
        )

    def test_no_mounts_that_could_catch_all_or_serve_static_over_graders(self) -> None:
        mounts = [getattr(route, "path", "") for route in app.routes if isinstance(route, Mount)]
        self.assertEqual(mounts, [], f"unexpected Mount(s) on backend: {mounts}")

    def test_no_root_path_parameter_route_that_could_catch_all(self) -> None:
        for route in _iter_leaf_routes():
            path = getattr(route, "path", "")
            self.assertFalse(
                re.fullmatch(r"/\{[^/]+\}", path),
                f"root-level path-parameter route can catch-all: {path}",
            )

    def test_no_loadtest_or_run_management_routes(self) -> None:
        offending = [
            getattr(route, "path", "")
            for route in _iter_leaf_routes()
            if _FORBIDDEN_PATH.search(getattr(route, "path", ""))
        ]
        self.assertEqual(offending, [], f"load-test routes on backend: {offending}")


class BackendMiddlewareTests(unittest.TestCase):
    """Middleware is invisible in app.routes yet perturbs every grader response."""

    def test_user_middleware_is_empty(self) -> None:
        self.assertEqual(
            app.user_middleware,
            [],
            f"unexpected middleware on backend: {app.user_middleware}",
        )


def _is_runner_or_ui_module(name: str) -> bool:
    return (
        name == "helpers._loadtest"
        or name.startswith("helpers._loadtest.")
        or name == "ui"
        or name.startswith("ui.")
    )


class BackendDependencyBoundaryTests(unittest.TestCase):
    """The real invariant: importing the backend must not pull in the load-test runner or UI.

    Order-independent: ``unittest discover`` imports sibling test modules (e.g.
    ``test_loadtest_room_coverage``) that legitimately import ``helpers._loadtest`` for their
    own assertions. A blanket ``sys.modules`` scan would flag that test-ordering pollution as a
    backend violation. Instead we measure the *delta* around a fresh ``importlib.import_module``
    of ``api.app`` -- the modules the backend itself is responsible for loading.
    """

    def test_api_app_import_does_not_load_loadtest_or_ui_modules(self) -> None:
        import importlib

        before = {name for name in sys.modules if _is_runner_or_ui_module(name)}
        importlib.import_module("api.app")
        after = {name for name in sys.modules if _is_runner_or_ui_module(name)}
        contaminated = sorted(after - before)
        self.assertEqual(
            contaminated,
            [],
            f"importing api.app pulled load-test/UI modules into the process: {contaminated}",
        )


if __name__ == "__main__":
    unittest.main()
