#!/bin/bash
# ============================================================================
# Pulse ML Pipeline - Full Stack Launcher (Linux/macOS)
# Starts all components with 600x speedup (1 hour = 6 seconds)
# Usage: ./run_pipeline.sh [--clean]
# ============================================================================

set -e

# Configuration
SPEEDUP=600
BATCH_SIZE=65536
IDLE_TIMEOUT=10
# Set to "--quiet" for silent mode, or "" for verbose output (demos)
QUIET_FLAG="--quiet"
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Handle --clean flag
if [ "$1" == "--clean" ]; then
    echo "============================================"
    echo " Cleaning previous run data..."
    echo "============================================"
    echo ""
    echo "Removing Arrow shards..."
    rm -rf _data/arrow
    echo "Removing trained models..."
    rm -rf _data/models
    echo "Removing producer state..."
    rm -f producer_state.json
    echo "Removing lakehouse data..."
    rm -rf _data/store_stats
    rm -rf _data/store_ml
    echo "Removing training logs..."
    rm -rf _data/logs
    echo "Removing consumer checkpoints..."
    rm -rf _data/checkpoints
    echo "Resetting Kafka consumer groups..."
    docker exec redpanda rpk group delete redshift-stats-consumer 2>/dev/null || true
    docker exec redpanda rpk group delete anomalous-lakehouse-sink 2>/dev/null || true
    docker exec redpanda rpk group delete redshift-stats-consumer-optimized 2>/dev/null || true
    echo "Removing QuixStreams internal topics..."
    for topic in $(docker exec redpanda rpk topic list 2>/dev/null | grep -E "repartition__|changelog__"); do
        docker exec redpanda rpk topic delete "$topic" 2>/dev/null || true
    done
    echo "Killing stale dashboard on port 8507..."
    lsof -ti :8507 2>/dev/null | xargs kill -9 2>/dev/null || true
    echo ""
    echo "Done! Starting fresh run..."
    echo ""
fi

echo "============================================"
echo " Pulse ML Pipeline Launcher"
echo " Speedup: ${SPEEDUP}x (4 days = ~10 minutes)"
echo "============================================"
echo ""

# Check if Kafka/Redpanda is running
echo -e "${YELLOW}[1/6] Checking Kafka (Redpanda)...${NC}"
if ! docker ps --filter "name=redpanda" --format "{{.Names}}" | grep -qi "redpanda"; then
    echo "Starting Redpanda..."
    cd kafka_stream
    docker-compose -f docker-compose-local.yml up -d
    cd ..
    echo "Waiting 10s for Kafka to start..."
    sleep 10
else
    echo -e "${GREEN}Redpanda is already running.${NC}"
fi
echo ""

# Create logs directory
mkdir -p logs

# Start Aggregate Consumer (stream analytics)
echo -e "${YELLOW}[2/6] Starting Aggregate Consumer...${NC}"
nohup python3 pipeline/consumer_aggregate.py $QUIET_FLAG > logs/aggregate_consumer.log 2>&1 &
echo $! > logs/aggregate_consumer.pid
sleep 2

# Start ML Consumer (anomaly sink)
echo -e "${YELLOW}[3/6] Starting ML Consumer...${NC}"
nohup python3 pipeline/consumer_ml.py $QUIET_FLAG > logs/ml_consumer.log 2>&1 &
echo $! > logs/ml_consumer.pid
sleep 2

# Start Engine Consumer (ML inference)
echo -e "${YELLOW}[4/6] Starting Engine Consumer...${NC}"
nohup python3 pipeline/consumer.py $QUIET_FLAG --inference-batch-size $BATCH_SIZE --idle-timeout $IDLE_TIMEOUT > logs/engine_consumer.log 2>&1 &
echo $! > logs/engine_consumer.pid
sleep 3

# Start Producer with 600x speedup
echo -e "${YELLOW}[5/6] Starting Producer (speedup=${SPEEDUP}x)...${NC}"
nohup python3 pipeline/producer.py --speedup $SPEEDUP --reset > logs/producer.log 2>&1 &
echo $! > logs/producer.pid
sleep 2

# Start Dashboard (Preact)
echo -e "${YELLOW}[6/6] Starting Dashboard (Preact)...${NC}"
nohup python3 dashboard/api.py > logs/dashboard.log 2>&1 &
echo $! > logs/dashboard.pid
sleep 3

echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN} All components started!${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""
echo "  Dashboard:  http://localhost:8507"
echo "  Kafka UI:   http://localhost:8080"
echo ""
echo "  Logs:       ./logs/"
echo "  PIDs:       ./logs/*.pid"
echo ""
echo "  Monitor producer:  tail -f logs/producer.log"
echo "  Monitor consumer:  tail -f logs/engine_consumer.log"
echo ""
echo "  To stop all:  ./stop_pipeline.sh"
echo ""
