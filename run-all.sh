#!/bin/bash

# ==============================================================================
# RUN ALL: Script chạy toàn bộ hệ thống (Producer + Dashboard)
# Yêu cầu: Port forwards đã được setup trước
# ==============================================================================

echo "=========================================="
echo "🚀 RUNNING FULL CRYPTO PIPELINE"
echo "=========================================="
echo ""

# Màu sắc
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# Kiểm tra virtual environment
if [ ! -d "venv" ]; then
    echo -e "${RED}Error: Virtual environment not found!${NC}"
    echo "Run: ./setup.sh first"
    exit 1
fi

# Kiểm tra port forwards
echo -e "${YELLOW}Checking port forwards...${NC}"
if ! nc -z localhost 9092 2>/dev/null; then
    echo -e "${RED}Error: Kafka port 9092 not accessible!${NC}"
    echo "Please start port forwarding first:"
    echo "  kubectl port-forward kafka-0 9092:9092 -n crypto-bigdata"
    exit 1
fi

if ! nc -z localhost 5433 2>/dev/null; then
    echo -e "${RED}Error: PostgreSQL port 5433 not accessible!${NC}"
    echo "Please start port forwarding first:"
    POSTGRES_POD=$(kubectl get pods -n crypto-bigdata -l app=postgres -o jsonpath='{.items[0].metadata.name}')
    echo "  kubectl port-forward pod/$POSTGRES_POD 5433:5432 -n crypto-bigdata"
    exit 1
fi

echo -e "${GREEN}✓ Port forwards are ready${NC}"
echo ""

# Activate venv
source venv/bin/activate

# Kiểm tra terminal emulator
if command -v gnome-terminal &> /dev/null; then
    TERM_CMD="gnome-terminal"
elif command -v xterm &> /dev/null; then
    TERM_CMD="xterm -e"
elif command -v konsole &> /dev/null; then
    TERM_CMD="konsole -e"
else
    echo -e "${YELLOW}⚠ No terminal emulator found${NC}"
    echo "Please run manually in separate terminals:"
    echo ""
    echo "Terminal 1: source venv/bin/activate && python producer/main.py"
    echo "Terminal 2: source venv/bin/activate && streamlit run dashboard.py"
    echo ""
    exit 0
fi

# Chạy Producer
echo -e "${BLUE}Starting Producer...${NC}"
$TERM_CMD bash -c "cd $(pwd); source venv/bin/activate; echo '🚀 Starting Producer...'; python producer/main.py; exec bash" &
sleep 2

# Chạy Dashboard
echo -e "${BLUE}Starting Dashboard...${NC}"
$TERM_CMD bash -c "cd $(pwd); source venv/bin/activate; echo '📊 Starting Dashboard...'; streamlit run dashboard.py; exec bash" &
sleep 2

echo ""
echo "=========================================="
echo -e "${GREEN}✅ SYSTEM RUNNING!${NC}"
echo "=========================================="
echo ""
echo "Services:"
echo "  📈 Producer: Running (fetching crypto data)"
echo "  📊 Dashboard: http://localhost:8501"
echo ""
echo "⚠️  Remember to start Spark Streaming manually:"
echo "  1. Get pod name: kubectl get pods -n crypto-bigdata -l app=spark-master"
echo "  2. Exec into pod: kubectl exec -it -n crypto-bigdata <pod-name> -- bash"
echo "  3. Run spark-submit (see README.md for command)"
echo ""
echo "To stop: Close the terminal windows or press Ctrl+C in each"
echo ""
