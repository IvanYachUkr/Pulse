"""Shared fixtures and path setup for Pulse tests."""
import sys
from pathlib import Path

# Add all source directories to sys.path so tests can import project modules
PROJECT_ROOT = Path(__file__).parent.parent
for subdir in ("pipeline", "dashboard", "outlier_tool", "kafka_stream"):
    path = str(PROJECT_ROOT / subdir)
    if path not in sys.path:
        sys.path.insert(0, path)

# Also add project root itself
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
