#!/bin/bash

# ==============================================================================
# FIX KAFKA HOSTNAME: Thêm Kafka hostname vào /etc/hosts
# ==============================================================================

echo "=========================================="
echo "🔧 FIXING KAFKA HOSTNAME RESOLUTION"
echo "=========================================="
echo ""

# Màu sắc
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

KAFKA_HOST="kafka-0.kafka.crypto-bigdata.svc.cluster.local"

# Kiểm tra xem đã có entry chưa
if grep -q "$KAFKA_HOST" /etc/hosts; then
    echo -e "${GREEN}✓ Kafka hostname already configured in /etc/hosts${NC}"
    echo ""
    grep "$KAFKA_HOST" /etc/hosts
else
    echo -e "${YELLOW}Adding Kafka hostname to /etc/hosts...${NC}"
    echo "127.0.0.1 $KAFKA_HOST" | sudo tee -a /etc/hosts
    echo ""
    echo -e "${GREEN}✓ Kafka hostname added successfully${NC}"
fi

echo ""
echo "You can now connect to Kafka from localhost using:"
echo "  - localhost:9092"
echo "  - kafka-0.kafka.crypto-bigdata.svc.cluster.local:9092"
echo ""
echo "Make sure port-forward is running:"
echo "  kubectl port-forward kafka-0 9092:9092 -n crypto-bigdata"
echo ""
