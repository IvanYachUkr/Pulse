"""Tests for the dashboard backend and API.

Tests the DashboardBackend, TimeWindow computation, API endpoint wiring,
and data sanitization — all using mocked DB readers.
"""
import math
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# TimeWindow + compute_time_window
# ---------------------------------------------------------------------------

class TestTimeWindow:
    """Verify per-request time window computation."""

    def test_imports(self):
        from backend_connection import (
            DashboardBackend,
            TimeWindow,
            compute_time_window,
            PROBLEM_TYPES,
            WINDOW_MAP,
        )

    def test_time_window_is_frozen(self):
        """TimeWindow should be immutable (frozen dataclass)."""
        from backend_connection import TimeWindow

        tw = TimeWindow(
            window_start=datetime(2024, 1, 1),
            prev_window_start=datetime(2023, 12, 31),
        )
        with pytest.raises(AttributeError):
            tw.window_start = datetime(2024, 2, 1)

    def test_compute_time_window_24h(self):
        """24h window should span 24 hours back from latest data point."""
        from backend_connection import compute_time_window, TimeWindow

        mock_reader = MagicMock()
        mock_reader.tablename = "minute_stats"
        latest = datetime(2024, 3, 15, 12, 0, 0)
        mock_reader.query.return_value = pd.DataFrame({"window_start": [latest]})

        tw = compute_time_window(mock_reader, "24h")
        assert isinstance(tw, TimeWindow)
        assert tw.window_start == latest - timedelta(hours=24)
        assert tw.prev_window_start == latest - timedelta(hours=48)

    def test_compute_time_window_1week(self):
        """1week window should span 7 days back."""
        from backend_connection import compute_time_window

        mock_reader = MagicMock()
        mock_reader.tablename = "minute_stats"
        latest = datetime(2024, 3, 15, 12, 0, 0)
        mock_reader.query.return_value = pd.DataFrame({"window_start": [latest]})

        tw = compute_time_window(mock_reader, "1week")
        assert tw.window_start == latest - timedelta(days=7)
        assert tw.prev_window_start == latest - timedelta(days=14)

    def test_compute_time_window_empty_db(self):
        """Empty database should fall back to datetime.now()."""
        from backend_connection import compute_time_window

        mock_reader = MagicMock()
        mock_reader.tablename = "minute_stats"
        mock_reader.query.return_value = pd.DataFrame()

        tw = compute_time_window(mock_reader, "24h")
        # Should be roughly 24h ago from now (within a few seconds)
        assert (datetime.now() - tw.window_start).total_seconds() < 86500

    def test_compute_time_window_db_error(self):
        """Database error should fall back gracefully to datetime.now()."""
        from backend_connection import compute_time_window

        mock_reader = MagicMock()
        mock_reader.tablename = "minute_stats"
        mock_reader.query.side_effect = RuntimeError("DB down")

        tw = compute_time_window(mock_reader, "24h")
        assert tw.window_start is not None

    def test_compute_time_window_unknown_type(self):
        """Unknown window type should raise NotImplementedError."""
        from backend_connection import compute_time_window

        mock_reader = MagicMock()
        mock_reader.tablename = "minute_stats"
        mock_reader.query.return_value = pd.DataFrame({"window_start": [datetime.now()]})

        with pytest.raises(NotImplementedError):
            compute_time_window(mock_reader, "unknown_type")


# ---------------------------------------------------------------------------
# DashboardBackend helpers
# ---------------------------------------------------------------------------

class TestDashboardBackendHelpers:
    """Test static helper methods that don't need a DB."""

    def test_id_params(self):
        """_id_params should produce correct placeholders and values."""
        from backend_connection import DashboardBackend

        ph, ids = DashboardBackend._id_params([1, 2, 3])
        assert ph == "?, ?, ?"
        assert ids == [1, 2, 3]

    def test_id_params_coerces_to_int(self):
        """String IDs should be coerced to int."""
        from backend_connection import DashboardBackend

        ph, ids = DashboardBackend._id_params(["10", "20"])
        assert ids == [10, 20]

    def test_id_params_single(self):
        from backend_connection import DashboardBackend

        ph, ids = DashboardBackend._id_params([42])
        assert ph == "?"
        assert ids == [42]


# ---------------------------------------------------------------------------
# API endpoint wiring
# ---------------------------------------------------------------------------

class TestAPIEndpoints:
    """Verify API endpoint structure and helpers."""

    def test_imports(self):
        from api import app, _sanitise, _parse_ids

    def test_sanitise_nan(self):
        """NaN and Infinity should be replaced with None."""
        from api import _sanitise

        assert _sanitise(float("nan")) is None
        assert _sanitise(float("inf")) is None
        assert _sanitise(float("-inf")) is None
        assert _sanitise(42.0) == 42.0
        assert _sanitise(0) == 0

    def test_sanitise_nested(self):
        """Nested dicts/lists should be recursively sanitised."""
        from api import _sanitise

        data = {"a": [1.0, float("nan")], "b": {"c": float("inf"), "d": "ok"}}
        result = _sanitise(data)
        assert result == {"a": [1.0, None], "b": {"c": None, "d": "ok"}}

    def test_parse_ids(self):
        """Comma-separated IDs should be parsed to int list."""
        from api import _parse_ids

        assert _parse_ids("1,2,3") == [1, 2, 3]
        assert _parse_ids("42") == [42]
        assert _parse_ids(" 10 , 20 , 30 ") == [10, 20, 30]
        assert _parse_ids("") == []

    def test_app_routes_exist(self):
        """All expected API routes should be registered."""
        from api import app

        routes = {r.path for r in app.routes if hasattr(r, "path")}
        expected = {
            "/api/status",
            "/api/instances",
            "/api/instances/critical",
            "/api/metrics",
            "/api/classification/chart",
            "/api/classification/table",
            "/api/anomalies",
            "/api/critical/types",
            "/api/recommendations/{rec_type}",
            "/",
            "/style.css",
            "/logo.png",
            "/favicon.ico",
        }
        for route in expected:
            assert route in routes, f"Missing route: {route}"

    def test_recommendations_paths_defined(self):
        """All recommendation file paths should be defined."""
        from api import RECOMMENDATIONS

        assert "cpu_bound" in RECOMMENDATIONS
        assert "io_bound" in RECOMMENDATIONS
        assert "network_bound" in RECOMMENDATIONS
        assert "queue_wlm_bound" in RECOMMENDATIONS
        assert "normal" in RECOMMENDATIONS

    def test_recommendations_files_exist(self):
        """All recommendation markdown files should exist on disk."""
        from api import RECOMMENDATIONS

        for key, path in RECOMMENDATIONS.items():
            assert path.exists(), f"Missing recommendation file: {key} -> {path}"
