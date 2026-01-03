#!/bin/bash

# ==============================================================================
# STOP SCRIPT: Dừng hệ thống
# ==============================================================================

echo "=========================================="
echo "⏹️  STOPPING CRYPTO BIG DATA SYSTEM"
echo "=========================================="
echo ""

# Màu sắc
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}Stopping Minikube...${NC}"
minikube stop

echo -e "${GREEN}✓ Minikube stopped${NC}"
echo ""
echo "All Kubernetes pods and services are stopped."
echo "Docker containers are still running but minikube VM is stopped."
echo ""
echo "To start again, run: ${GREEN}./start.sh${NC}"
echo "To completely remove everything, run: ${RED}minikube delete${NC}"
echo ""
