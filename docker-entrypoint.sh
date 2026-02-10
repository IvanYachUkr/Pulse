#!/bin/bash
# =============================================================================
# Pulse Pipeline — Docker Entrypoint
# Starts all pipeline components inside the container.
# =============================================================================

set -e

# Configuration (overridable via environment)
SPEEDUP="${SPEEDUP:-600}"
BATCH_SIZE="${BATCH_SIZE:-65536}"
IDLE_TIMEOUT="${IDLE_TIMEOUT:-10}"
QUIET_FLAG="${QUIET_FLAG:---quiet}"
DATA_FILE="${DATA_FILE:-_data/input/sorted_4days.parquet}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

# Track child PIDs for graceful shutdown
PIDS=()

cleanup() {
    echo ""
    echo "[entrypoint] Shutting down all components..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    # Wait briefly for graceful exit
    sleep 2
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
    echo "[entrypoint] All components stopped."
    exit 0
}

trap cleanup SIGTERM SIGINT

# ---------------------------------------------------------------------------
# Handle --clean flag
# ---------------------------------------------------------------------------
if [ "$1" == "--clean" ] || [ "$CLEAN" == "true" ]; then
    echo "[entrypoint] Cleaning previous run data..."
    rm -rf _data/arrow _data/models _data/logs _data/checkpoints producer_state.json
    rm -f _data/stream_stats.sqlite _data/stream_stats.sqlite-wal _data/stream_stats.sqlite-shm
    rm -rf _data/store_stats _data/store_ml
    mkdir -p _data/arrow _data/models _data/logs _data/checkpoints _data/input
    echo "[entrypoint] Clean complete."
fi

# ---------------------------------------------------------------------------
# Wait for Kafka to be reachable
# ---------------------------------------------------------------------------
echo "[entrypoint] Waiting for Kafka at ${KAFKA_BOOTSTRAP_SERVERS}..."
KAFKA_HOST="${KAFKA_BOOTSTRAP_SERVERS%%:*}"
KAFKA_PORT="${KAFKA_BOOTSTRAP_SERVERS##*:}"

until python3 -c "
import socket, sys
try:
    s = socket.create_connection(('${KAFKA_HOST}', ${KAFKA_PORT}), timeout=2)
    s.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
" 2>/dev/null; do
    echo "[entrypoint] Kafka not ready yet, retrying in 2s..."
    sleep 2
done
echo "[entrypoint] Kafka is reachable!"

# ---------------------------------------------------------------------------
# Check that data file exists
# ---------------------------------------------------------------------------
if [ ! -f "$DATA_FILE" ]; then
    echo "[entrypoint] WARNING: Data file not found: $DATA_FILE"
    echo "[entrypoint] Mount your test parquet file to /app/_data/input/"
    echo "[entrypoint] Starting dashboard only (no producer)..."
    NO_PRODUCER=true
fi

# ---------------------------------------------------------------------------
# Start pipeline components
# ---------------------------------------------------------------------------
echo ""
echo "============================================"
echo " Pulse Pipeline (Docker)"
echo " Speedup: ${SPEEDUP}x"
echo " Data:    ${DATA_FILE}"
echo "============================================"
echo ""

# 1. Aggregate Consumer (stream analytics → SQLite)
echo "[entrypoint] Starting Aggregate Consumer..."
python3 pipeline/consumer_aggregate.py $QUIET_FLAG &
PIDS+=($!)
sleep 2

# 2. ML Consumer (anomaly sink → DuckLake)
echo "[entrypoint] Starting ML Consumer..."
python3 pipeline/consumer_ml.py $QUIET_FLAG &
PIDS+=($!)
sleep 2

# 3. Engine Consumer (ML inference)
echo "[entrypoint] Starting Engine Consumer..."
python3 pipeline/consumer.py $QUIET_FLAG \
    --inference-batch-size "$BATCH_SIZE" \
    --idle-timeout "$IDLE_TIMEOUT" &
PIDS+=($!)
sleep 3

# 4. Producer (data ingress)
if [ "$NO_PRODUCER" != "true" ]; then
    echo "[entrypoint] Starting Producer (speedup=${SPEEDUP}x)..."
    python3 pipeline/producer.py \
        --data-file "$DATA_FILE" \
        --speedup "$SPEEDUP" \
        --reset &
    PIDS+=($!)
    sleep 2
fi

# 5. Dashboard API (foreground — keeps container alive)
echo "[entrypoint] Starting Dashboard API on port ${DASHBOARD_PORT:-8507}..."
echo ""
echo "============================================"
echo " Dashboard:  http://localhost:${DASHBOARD_PORT:-8507}"
echo "============================================"
echo ""

# Run dashboard in foreground so Docker tracks it
exec python3 dashboard/api.py
