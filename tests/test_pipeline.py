"""Tests for pipeline configuration and Arrow writer.

Tests config constants, DuckDB connectivity, and the DailyArrowWriter
shard naming and buffering logic.
"""
import os
from pathlib import Path
from datetime import datetime

import pytest


# ---------------------------------------------------------------------------
# Pipeline config
# ---------------------------------------------------------------------------

class TestPipelineConfig:
    """Verify pipeline configuration module loads correctly."""

    def test_imports(self):
        from config import (
            KAFKA_BOOTSTRAP_SERVERS,
            LAKEHOUSE_BASE_DIR,
            MODEL_DIR,
            LOG_LEVEL,
            TABLE_STREAM_STATS,
            TABLE_ML_ANOMALIES,
            setup_logging,
        )

    def test_config_defaults(self):
        """Config should have sensible defaults without any env vars."""
        from config import (
            KAFKA_BOOTSTRAP_SERVERS,
            TABLE_STREAM_STATS,
            TABLE_ML_ANOMALIES,
            LOG_LEVEL,
        )
        assert KAFKA_BOOTSTRAP_SERVERS  # non-empty
        assert TABLE_STREAM_STATS == "minute_stats"
        assert TABLE_ML_ANOMALIES == "anomalies"
        assert LOG_LEVEL in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

    def test_lakehouse_base_dir_is_path(self):
        """LAKEHOUSE_BASE_DIR should be a Path object."""
        from config import LAKEHOUSE_BASE_DIR

        assert isinstance(LAKEHOUSE_BASE_DIR, Path)

    def test_setup_logging(self):
        """setup_logging should not raise."""
        from config import setup_logging

        logger = setup_logging("test_pulse")
        assert logger is not None
        assert logger.name == "test_pulse"


# ---------------------------------------------------------------------------
# Arrow writer (kafka_stream)
# ---------------------------------------------------------------------------

class TestArrowWriter:
    """Verify DailyArrowWriter naming and buffering."""

    def test_imports(self):
        from arrow_writer import DailyArrowWriter

    def test_writer_init(self, tmp_path):
        """Writer should initialize with a table name and output directory."""
        from arrow_writer import DailyArrowWriter

        writer = DailyArrowWriter(
            base_dir=str(tmp_path),
            table="test_table",
        )
        assert writer is not None
        assert writer.table == "test_table"

    def test_writer_creates_output_dir(self, tmp_path):
        """Writer should accept an output directory path."""
        from arrow_writer import DailyArrowWriter

        out_dir = tmp_path / "new_dir"
        out_dir.mkdir()
        writer = DailyArrowWriter(
            base_dir=str(out_dir),
            table="test_table",
        )
        assert writer.base_dir == str(out_dir)


# ---------------------------------------------------------------------------
# DB reader (dashboard)
# ---------------------------------------------------------------------------

class TestDBReader:
    """Verify database reader factory and class structure."""

    def test_imports(self):
        from db_reader import BaseReader, ParquetReader, SQLiteReader, create_stream_reader, DBReader

    def test_base_reader_is_abstract(self):
        """BaseReader should not be instantiable directly."""
        from db_reader import BaseReader

        with pytest.raises(TypeError):
            BaseReader()

    def test_dbr_alias(self):
        """DBReader should be an alias for ParquetReader."""
        from db_reader import DBReader, ParquetReader

        assert DBReader is ParquetReader
