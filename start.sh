#!/bin/bash

# ==============================================================================
# START SCRIPT: Khởi động hệ thống (các lần sau khi đã deploy)
# ==============================================================================

echo "=========================================="
echo "▶️  STARTING CRYPTO BIG DATA SYSTEM"
echo "=========================================="
echo ""

# Màu sắc
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ==============================================================================
# 1. START MINIKUBE
# ==============================================================================
echo -e "${YELLOW}Starting Minikube...${NC}"
if minikube status | grep -q "host: Running"; then
    echo -e "${GREEN}✓ Minikube already running${NC}"
else
    minikube start
    echo -e "${GREEN}✓ Minikube started${NC}"
fi

# ==============================================================================
# 2. SET NAMESPACE
# ==============================================================================
kubectl config set-context --current --namespace=crypto-bigdata
echo -e "${GREEN}✓ Namespace set to crypto-bigdata${NC}"

# ==============================================================================
# 3. KIỂM TRA PODS
# ==============================================================================
echo ""
echo -e "${YELLOW}Checking pods status...${NC}"
kubectl get pods -n crypto-bigdata
echo ""

# ==============================================================================
# 4. LẤY POD NAMES
# ==============================================================================
KAFKA_POD=$(kubectl get pods -n crypto-bigdata -l app=kafka -o jsonpath='{.items[0].metadata.name}')
POSTGRES_POD=$(kubectl get pods -n crypto-bigdata -l app=postgres -o jsonpath='{.items[0].metadata.name}')
SPARK_MASTER_POD=$(kubectl get pods -n crypto-bigdata -l app=spark-master -o jsonpath='{.items[0].metadata.name}')

echo -e "${BLUE}Pod names:${NC}"
echo "  Kafka: $KAFKA_POD"
echo "  PostgreSQL: $POSTGRES_POD"
echo "  Spark Master: $SPARK_MASTER_POD"
echo ""

# ==============================================================================
# HƯỚNG DẪN TIẾP THEO
# ==============================================================================
echo "=========================================="
echo -e "${GREEN}✅ SYSTEM READY!${NC}"
echo "=========================================="
echo ""
echo -e "${BLUE}Start the following in separate terminals:${NC}"
echo ""
echo "1. Port forwarding (3 terminals):"
echo "   ${YELLOW}Terminal 1:${NC} kubectl port-forward $KAFKA_POD 9092:9092 -n crypto-bigdata"
echo "   ${YELLOW}Terminal 2:${NC} kubectl port-forward pod/$POSTGRES_POD 5433:5432 -n crypto-bigdata"
echo "   ${YELLOW}Terminal 3:${NC} kubectl port-forward deployment/spark-master 8080:8080 -n crypto-bigdata"
echo ""
echo "2. Run producer:"
echo "   ${YELLOW}source venv/bin/activate${NC}"
echo "   ${YELLOW}python producer/main.py${NC}"
echo ""
echo "3. Run Spark Streaming (in pod):"
echo "   ${YELLOW}kubectl exec -it -n crypto-bigdata $SPARK_MASTER_POD -- bash${NC}"
echo ""
echo "   Inside pod:"
echo "   ${YELLOW}/opt/spark/bin/spark-submit \\${NC}"
echo "     --master local[2] \\"
echo "     --name \"CryptoProcessor\" \\"
echo "     --driver-memory 512M \\"
echo "     --conf spark.driver.maxResultSize=200M \\"
echo "     --conf spark.jars.ivy=/tmp/.ivy2 \\"
echo "     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \\"
echo "     /tmp/spark-apps/streaming/realtime_processor.py"
echo ""
echo "4. Run dashboard:"
echo "   ${YELLOW}source venv/bin/activate${NC}"
echo "   ${YELLOW}streamlit run dashboard.py${NC}"
echo ""
echo "📖 See DEPLOYMENT_GUIDE_UBUNTU.md for full details"
echo ""
