#!/bin/bash
# ============================================================================
# Pulse ML Pipeline - Stop All Components
# ============================================================================

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

echo "Stopping Pulse Pipeline..."
echo ""

# Stop processes using PID files
for pidfile in logs/*.pid; do
    if [ -f "$pidfile" ]; then
        pid=$(cat "$pidfile")
        name=$(basename "$pidfile" .pid)
        if kill -0 "$pid" 2>/dev/null; then
            echo "Stopping $name (PID: $pid)..."
            kill "$pid" 2>/dev/null
        else
            echo "$name already stopped"
        fi
        rm -f "$pidfile"
    fi
done

echo ""
echo -e "${GREEN}All components stopped.${NC}"
echo ""
echo "Note: Kafka (Redpanda) is still running."
echo "To stop Kafka: cd kafka_stream && docker-compose -f docker-compose-local.yml down"
