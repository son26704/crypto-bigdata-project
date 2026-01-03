#!/bin/bash

# ==============================================================================
# CHECK STATUS: Kiểm tra trạng thái hệ thống
# ==============================================================================

echo "=========================================="
echo "🔍 SYSTEM STATUS CHECK"
echo "=========================================="
echo ""

# Màu sắc
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

# ==============================================================================
# 1. MINIKUBE STATUS
# ==============================================================================
echo -e "${BLUE}[1] Minikube Status:${NC}"
if minikube status | grep -q "host: Running"; then
    echo -e "${GREEN}✓ Minikube is running${NC}"
else
    echo -e "${RED}✗ Minikube is not running${NC}"
    echo "  Run: ./start.sh to start the system"
    exit 1
fi
echo ""

# ==============================================================================
# 2. KUBERNETES PODS
# ==============================================================================
echo -e "${BLUE}[2] Kubernetes Pods:${NC}"
kubectl get pods -n crypto-bigdata
echo ""

# ==============================================================================
# 3. KUBERNETES SERVICES
# ==============================================================================
echo -e "${BLUE}[3] Kubernetes Services:${NC}"
kubectl get svc -n crypto-bigdata
echo ""

# ==============================================================================
# 4. KAFKA TOPICS
# ==============================================================================
echo -e "${BLUE}[4] Kafka Topics:${NC}"
KAFKA_POD=$(kubectl get pods -n crypto-bigdata -l app=kafka -o jsonpath='{.items[0].metadata.name}')
if [ -n "$KAFKA_POD" ]; then
    kubectl exec -it $KAFKA_POD -n crypto-bigdata -- kafka-topics \
      --list \
      --bootstrap-server localhost:9092 2>/dev/null || echo -e "${YELLOW}  Could not connect to Kafka${NC}"
else
    echo -e "${RED}  Kafka pod not found${NC}"
fi
echo ""

# ==============================================================================
# 5. POSTGRESQL DATA
# ==============================================================================
echo -e "${BLUE}[5] PostgreSQL Data Count:${NC}"
if command -v psql &> /dev/null; then
    if nc -z localhost 5433 2>/dev/null; then
        PGPASSWORD=cryptopass123 psql -h localhost -p 5433 -U cryptouser -d cryptodb -t -c "
            SELECT 
                'realtime_prices' as table, COUNT(*) as rows FROM realtime_prices
            UNION ALL
            SELECT 
                'alerts', COUNT(*) FROM alerts
            UNION ALL
            SELECT 
                'hourly_stats', COUNT(*) FROM hourly_stats
            UNION ALL
            SELECT 
                'daily_stats', COUNT(*) FROM daily_stats;
        " 2>/dev/null || echo -e "${YELLOW}  Could not connect to PostgreSQL${NC}"
    else
        echo -e "${YELLOW}  PostgreSQL port 5433 not forwarded${NC}"
    fi
else
    echo -e "${YELLOW}  psql client not installed${NC}"
fi
echo ""

# ==============================================================================
# 6. PORT FORWARDS
# ==============================================================================
echo -e "${BLUE}[6] Port Forwards:${NC}"
if nc -z localhost 9092 2>/dev/null; then
    echo -e "${GREEN}  ✓ Kafka port 9092 is open${NC}"
else
    echo -e "${RED}  ✗ Kafka port 9092 not forwarded${NC}"
fi

if nc -z localhost 5433 2>/dev/null; then
    echo -e "${GREEN}  ✓ PostgreSQL port 5433 is open${NC}"
else
    echo -e "${RED}  ✗ PostgreSQL port 5433 not forwarded${NC}"
fi

if nc -z localhost 8080 2>/dev/null; then
    echo -e "${GREEN}  ✓ Spark UI port 8080 is open${NC}"
else
    echo -e "${YELLOW}  ! Spark UI port 8080 not forwarded (optional)${NC}"
fi
echo ""

# ==============================================================================
# 7. RESOURCE USAGE
# ==============================================================================
echo -e "${BLUE}[7] Resource Usage:${NC}"
kubectl top nodes 2>/dev/null || echo -e "${YELLOW}  Metrics not available${NC}"
echo ""

# ==============================================================================
# SUMMARY
# ==============================================================================
echo "=========================================="
echo -e "${GREEN}Status check completed!${NC}"
echo "=========================================="
echo ""
