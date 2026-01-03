#!/bin/bash

# ==============================================================================
# PORT FORWARD SCRIPT: Tự động mở các port forwards cần thiết
# ==============================================================================

echo "=========================================="
echo "🔌 STARTING PORT FORWARDS"
echo "=========================================="
echo ""

# Màu sắc
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Lấy pod names
KAFKA_POD=$(kubectl get pods -n crypto-bigdata -l app=kafka -o jsonpath='{.items[0].metadata.name}')
POSTGRES_POD=$(kubectl get pods -n crypto-bigdata -l app=postgres -o jsonpath='{.items[0].metadata.name}')

if [ -z "$KAFKA_POD" ] || [ -z "$POSTGRES_POD" ]; then
    echo -e "${RED}Error: Required pods not found!${NC}"
    echo "Make sure the cluster is running."
    exit 1
fi

echo -e "${BLUE}Pod names:${NC}"
echo "  Kafka: $KAFKA_POD"
echo "  PostgreSQL: $POSTGRES_POD"
echo ""

# Kiểm tra OS để chọn terminal emulator
if command -v gnome-terminal &> /dev/null; then
    TERM_CMD="gnome-terminal"
elif command -v xterm &> /dev/null; then
    TERM_CMD="xterm -e"
elif command -v konsole &> /dev/null; then
    TERM_CMD="konsole -e"
else
    echo -e "${YELLOW}⚠ No terminal emulator found. Please run manually:${NC}"
    echo ""
    echo "Terminal 1: kubectl port-forward $KAFKA_POD 9092:9092 -n crypto-bigdata"
    echo "Terminal 2: kubectl port-forward pod/$POSTGRES_POD 5433:5432 -n crypto-bigdata"
    echo "Terminal 3: kubectl port-forward deployment/spark-master 8080:8080 -n crypto-bigdata"
    echo ""
    exit 0
fi

# Mở terminals
echo -e "${YELLOW}Opening terminals for port forwarding...${NC}"

$TERM_CMD bash -c "echo 'Port forwarding Kafka...'; kubectl port-forward $KAFKA_POD 9092:9092 -n crypto-bigdata; exec bash" &
sleep 1

$TERM_CMD bash -c "echo 'Port forwarding PostgreSQL...'; kubectl port-forward pod/$POSTGRES_POD 5433:5432 -n crypto-bigdata; exec bash" &
sleep 1

$TERM_CMD bash -c "echo 'Port forwarding Spark UI...'; kubectl port-forward deployment/spark-master 8080:8080 -n crypto-bigdata; exec bash" &

echo ""
echo -e "${GREEN}✓ Port forward terminals opened!${NC}"
echo ""
echo "Ports:"
echo "  - Kafka: localhost:9092"
echo "  - PostgreSQL: localhost:5433"
echo "  - Spark UI: localhost:8080"
echo ""
echo "⚠️  Keep these terminals open while the system is running!"
echo ""
