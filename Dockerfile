# =============================================================================
# Pulse ML Pipeline — Docker Image
# Runs all pipeline components (producer, consumers, dashboard) in one container.
# =============================================================================

FROM python:3.11-slim

# System dependencies for DuckDB, ONNX, and Arrow
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project code
COPY pipeline/ pipeline/
COPY dashboard/ dashboard/
COPY kafka_stream/*.py kafka_stream/
COPY outlier_tool/ outlier_tool/
COPY assets/ assets/
COPY docs/ docs/
COPY docker-entrypoint.sh .

# Create runtime directories
RUN mkdir -p _data/arrow _data/models _data/logs _data/checkpoints \
    _data/input _data/archive logs

# Make entrypoint executable
RUN chmod +x docker-entrypoint.sh

# Dashboard API port
EXPOSE 8507

ENTRYPOINT ["./docker-entrypoint.sh"]
