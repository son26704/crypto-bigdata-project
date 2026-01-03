#!/bin/bash

# ==============================================================================
# QUICK START: Script nhanh cho lần chạy đầu tiên
# ==============================================================================

echo "=========================================="
echo "⚡ CRYPTO BIG DATA - QUICK START"
echo "=========================================="
echo ""

# Màu sắc
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}This script will:${NC}"
echo "  1. Install all dependencies"
echo "  2. Deploy Kubernetes stack"
echo "  3. Setup port forwarding"
echo ""
echo -e "${YELLOW}Prerequisites:${NC}"
echo "  - Ubuntu 20.04+"
echo "  - 8GB RAM, 4 CPU cores, 30GB disk"
echo "  - Internet connection"
echo ""
read -p "Continue? (y/n) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 0
fi

# 1. Setup
echo ""
echo -e "${YELLOW}[1/3] Installing dependencies...${NC}"
./setup.sh

# 2. Deploy
echo ""
echo -e "${YELLOW}[2/3] Deploying Kubernetes stack...${NC}"
./deploy.sh

# 2.5 Fix Kafka hostname
echo ""
echo -e "${YELLOW}[2.5/3] Fixing Kafka hostname...${NC}"
./fix-kafka-hostname.sh

# 3. Instructions
echo ""
echo -e "${YELLOW}[3/3] Next Steps:${NC}"
echo ""
echo "=========================================="
echo -e "${GREEN}✅ INSTALLATION COMPLETE!${NC}"
echo "=========================================="
echo ""
echo -e "${BLUE}IMPORTANT: Start port forwarding in 3 separate terminals:${NC}"
echo ""
echo "Terminal 1: ${YELLOW}kubectl port-forward kafka-0 9092:9092 -n crypto-bigdata${NC}"
echo ""

POSTGRES_POD=$(kubectl get pods -n crypto-bigdata -l app=postgres -o jsonpath='{.items[0].metadata.name}')
echo "Terminal 2: ${YELLOW}kubectl port-forward pod/$POSTGRES_POD 5433:5432 -n crypto-bigdata${NC}"
echo ""
echo "Terminal 3: ${YELLOW}kubectl port-forward deployment/spark-master 8080:8080 -n crypto-bigdata${NC}"
echo ""
echo "Or use automated script (if supported): ${YELLOW}./port-forward.sh${NC}"
echo ""
echo "Then run the system:"
echo "  ${YELLOW}./run-all.sh${NC}  (Auto-start Producer + Dashboard)"
echo ""
echo "And start Spark Streaming manually in Spark pod (see README.md)"
echo ""
echo "📖 Full guide: ${BLUE}DEPLOYMENT_GUIDE_UBUNTU.md${NC}"
echo ""
