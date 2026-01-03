#!/bin/bash

# ==============================================================================
# DEPLOY SCRIPT: Deploy toàn bộ K8s stack
# ==============================================================================

# Don't exit on error - we handle errors manually
# set -e removed to allow graceful error handling

echo "=========================================="
echo "🚀 DEPLOYING CRYPTO BIG DATA PROJECT"
echo "=========================================="
echo ""

# Màu sắc
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Helper function to wait for pods
wait_for_pod() {
    local label=$1
    local timeout=${2:-180}
    local max_attempts=6
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        echo "  Waiting for pod with label $label (attempt $attempt/$max_attempts)..."
        if kubectl wait --for=condition=ready pod -l $label --timeout=${timeout}s 2>/dev/null; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 10
    done
    
    echo -e "${YELLOW}  Warning: Pod with label $label not ready yet, continuing...${NC}"
    return 1
}

# ==============================================================================
# 1. KHỞI ĐỘNG MINIKUBE
# ==============================================================================
echo -e "${YELLOW}[1/6] Starting Minikube...${NC}"
if minikube status | grep -q "host: Running"; then
    echo -e "${GREEN}✓ Minikube already running${NC}"
else
    echo "Starting Minikube with Docker driver..."
    minikube start --driver=docker --memory=6144 --cpus=3 --disk-size=30g
    echo -e "${GREEN}✓ Minikube started${NC}"
fi

# ==============================================================================
# 2. TẠO NAMESPACE
# ==============================================================================
echo ""
echo -e "${YELLOW}[2/6] Setting up namespace...${NC}"
if kubectl get namespace crypto-bigdata &> /dev/null; then
    echo -e "${GREEN}✓ Namespace crypto-bigdata exists${NC}"
else
    kubectl create namespace crypto-bigdata
    echo -e "${GREEN}✓ Namespace crypto-bigdata created${NC}"
fi

kubectl config set-context --current --namespace=crypto-bigdata
echo -e "${GREEN}✓ Switched to namespace: crypto-bigdata${NC}"

# ==============================================================================
# 3. DEPLOY K8S COMPONENTS
# ==============================================================================
echo ""
echo -e "${YELLOW}[3/6] Deploying Kubernetes components...${NC}"

# Zookeeper
echo -e "${BLUE}Deploying Zookeeper...${NC}"
kubectl apply -f k8s/kafka/zookeeper.yaml
sleep 5  # Give time for pod to be created
wait_for_pod "app=zookeeper" 180
echo -e "${GREEN}✓ Zookeeper deployed${NC}"

# Kafka
echo -e "${BLUE}Deploying Kafka...${NC}"
kubectl apply -f k8s/kafka/kafka.yaml
sleep 5
wait_for_pod "app=kafka" 180
echo -e "${GREEN}✓ Kafka deployed${NC}"

# PostgreSQL
echo -e "${BLUE}Deploying PostgreSQL...${NC}"
kubectl apply -f k8s/postgres/postgres.yaml
sleep 5
wait_for_pod "app=postgres" 180
echo -e "${GREEN}✓ PostgreSQL deployed${NC}"

# HDFS
echo -e "${BLUE}Deploying HDFS...${NC}"
kubectl apply -f k8s/hdfs/hdfs-configmap.yaml
kubectl apply -f k8s/hdfs/namenode.yaml
kubectl apply -f k8s/hdfs/datanode.yaml
sleep 5
wait_for_pod "app=namenode" 180
wait_for_pod "app=datanode" 180
echo -e "${GREEN}✓ HDFS deployed${NC}"

# Spark
echo -e "${BLUE}Deploying Spark...${NC}"
kubectl apply -f k8s/spark/spark-master.yaml
kubectl apply -f k8s/spark/spark-worker.yaml
sleep 5
wait_for_pod "app=spark-master" 180
wait_for_pod "app=spark-worker" 180
echo -e "${GREEN}✓ Spark deployed${NC}"

# Grafana (optional)
if [ -f "k8s/grafana/grafana.yaml" ]; then
    echo -e "${BLUE}Deploying Grafana...${NC}"
    kubectl apply -f k8s/grafana/grafana.yaml
    echo -e "${GREEN}✓ Grafana deployed${NC}"
fi

# ==============================================================================
# 4. TẠO KAFKA TOPIC
# ==============================================================================
echo ""
echo -e "${YELLOW}[4/6] Creating Kafka topic...${NC}"
sleep 10  # Đợi Kafka hoàn toàn ready

# Get Kafka pod name with retry
KAFKA_POD=""
for i in {1..10}; do
    KAFKA_POD=$(kubectl get pods -n crypto-bigdata -l app=kafka -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [ -n "$KAFKA_POD" ]; then
        break
    fi
    echo "  Waiting for Kafka pod... (attempt $i/10)"
    sleep 5
done

if [ -z "$KAFKA_POD" ]; then
    echo -e "${RED}Error: Kafka pod not found after waiting${NC}"
    echo "Please run './check-status.sh' and try creating topic manually later"
else
    echo "Kafka pod: $KAFKA_POD"

    # Xóa topic cũ nếu có (để reset)
    kubectl exec -it $KAFKA_POD -n crypto-bigdata -- kafka-topics \
      --delete --topic crypto-prices --bootstrap-server localhost:9092 2>/dev/null || echo "  Topic not exists yet"

    # Tạo topic mới
    kubectl exec -it $KAFKA_POD -n crypto-bigdata -- kafka-topics \
      --create \
      --topic crypto-prices \
      --bootstrap-server localhost:9092 \
      --partitions 1 \
      --replication-factor 1 2>/dev/null || echo -e "${YELLOW}  Topic may already exist${NC}"

    echo -e "${GREEN}✓ Kafka topic 'crypto-prices' created${NC}"
fi

# ==============================================================================
# 5. UPLOAD SPARK APPLICATIONS
# ==============================================================================
echo ""
echo -e "${YELLOW}[5/6] Uploading Spark applications...${NC}"

# Get Spark Master pod name with retry
SPARK_MASTER_POD=""
for i in {1..10}; do
    SPARK_MASTER_POD=$(kubectl get pods -n crypto-bigdata -l app=spark-master -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    if [ -n "$SPARK_MASTER_POD" ]; then
        break
    fi
    echo "  Waiting for Spark Master pod... (attempt $i/10)"
    sleep 5
done

if [ -z "$SPARK_MASTER_POD" ]; then
    echo -e "${RED}Error: Spark Master pod not found after waiting${NC}"
    echo "Please run './update-spark-apps.sh' manually later"
else
    echo "Spark Master pod: $SPARK_MASTER_POD"

    # Tạo directories
    kubectl exec -n crypto-bigdata $SPARK_MASTER_POD -- mkdir -p /tmp/spark-apps/common
    kubectl exec -n crypto-bigdata $SPARK_MASTER_POD -- mkdir -p /tmp/spark-apps/batch
    kubectl exec -n crypto-bigdata $SPARK_MASTER_POD -- mkdir -p /tmp/spark-apps/streaming

    # Copy files
    kubectl cp spark-apps/common/postgres_config.py crypto-bigdata/$SPARK_MASTER_POD:/tmp/spark-apps/common/
    kubectl cp spark-apps/streaming/config.py crypto-bigdata/$SPARK_MASTER_POD:/tmp/spark-apps/streaming/
    kubectl cp spark-apps/streaming/realtime_processor.py crypto-bigdata/$SPARK_MASTER_POD:/tmp/spark-apps/streaming/
    kubectl cp spark-apps/batch/batch_processor.py crypto-bigdata/$SPARK_MASTER_POD:/tmp/spark-apps/batch/
    kubectl cp spark-apps/batch/hdfs_reader.py crypto-bigdata/$SPARK_MASTER_POD:/tmp/spark-apps/batch/

    echo -e "${GREEN}✓ Spark applications uploaded${NC}"
fi

# ==============================================================================
# 6. HIỂN thị TRẠNG THÁI
# ==============================================================================
echo ""
echo -e "${YELLOW}[6/6] Checking deployment status...${NC}"
echo ""
kubectl get pods -n crypto-bigdata
echo ""
kubectl get svc -n crypto-bigdata

# ==============================================================================
# HOÀN THÀNH
# ==============================================================================
echo ""
echo "=========================================="
echo -e "${GREEN}✅ DEPLOYMENT COMPLETED!${NC}"
echo "=========================================="
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo ""
echo "1. Start port forwarding (open 3 separate terminals):"
echo "   Terminal 1: kubectl port-forward kafka-0 9092:9092 -n crypto-bigdata"
echo ""
POSTGRES_POD=$(kubectl get pods -n crypto-bigdata -l app=postgres -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -n "$POSTGRES_POD" ]; then
    echo "   Terminal 2: kubectl port-forward pod/$POSTGRES_POD 5433:5432 -n crypto-bigdata"
else
    echo "   Terminal 2: kubectl port-forward pod/postgres-0 5433:5432 -n crypto-bigdata"
fi
echo ""
echo "   Terminal 3: kubectl port-forward deployment/spark-master 8080:8080 -n crypto-bigdata"
echo ""
echo "2. Run producer:"
echo "   source venv/bin/activate"
echo "   python producer/main.py"
echo ""
echo "3. Run Spark Streaming (in spark pod):"
if [ -n "$SPARK_MASTER_POD" ]; then
    echo "   kubectl exec -it -n crypto-bigdata $SPARK_MASTER_POD -- bash"
else
    echo "   # First get pod name: kubectl get pods -n crypto-bigdata -l app=spark-master"
    echo "   # Then: kubectl exec -it -n crypto-bigdata <spark-master-pod> -- bash"
fi
echo "   # Then run spark-submit (see DEPLOYMENT_GUIDE_UBUNTU.md)"
echo ""
echo "4. Run dashboard:"
echo "   source venv/bin/activate"
echo "   streamlit run dashboard.py"
echo ""
echo "For more details, see: DEPLOYMENT_GUIDE_UBUNTU.md"
echo ""
