#!/bin/bash

# ==============================================================================
# SETUP SCRIPT: Cài đặt tất cả dependencies cho Ubuntu
# ==============================================================================

set -e  # Exit on error

echo "=========================================="
echo "🚀 CRYPTO BIG DATA PROJECT - SETUP"
echo "=========================================="
echo ""

# Màu sắc cho output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ==============================================================================
# 1. KIỂM TRA VÀ CÀI ĐẶT DOCKER
# ==============================================================================
echo -e "${YELLOW}[1/5] Checking Docker...${NC}"
if ! command -v docker &> /dev/null; then
    echo "Docker not found. Installing Docker..."
    sudo apt update
    sudo apt install -y docker.io docker-compose
    sudo systemctl start docker
    sudo systemctl enable docker
    sudo usermod -aG docker $USER
    echo -e "${GREEN}✓ Docker installed${NC}"
    echo -e "${YELLOW}⚠ Please logout and login again to apply docker group permissions${NC}"
else
    echo -e "${GREEN}✓ Docker already installed: $(docker --version)${NC}"
fi

# ==============================================================================
# 2. KIỂM TRA VÀ CÀI ĐẶT MINIKUBE
# ==============================================================================
echo ""
echo -e "${YELLOW}[2/5] Checking Minikube...${NC}"
if ! command -v minikube &> /dev/null; then
    echo "Minikube not found. Installing Minikube..."
    curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
    sudo install minikube-linux-amd64 /usr/local/bin/minikube
    rm minikube-linux-amd64
    echo -e "${GREEN}✓ Minikube installed: $(minikube version --short)${NC}"
else
    echo -e "${GREEN}✓ Minikube already installed: $(minikube version --short)${NC}"
fi

# ==============================================================================
# 3. KIỂM TRA VÀ CÀI ĐẶT KUBECTL
# ==============================================================================
echo ""
echo -e "${YELLOW}[3/5] Checking kubectl...${NC}"
if ! command -v kubectl &> /dev/null; then
    echo "kubectl not found. Installing kubectl..."
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
    sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
    rm kubectl
    echo -e "${GREEN}✓ kubectl installed: $(kubectl version --client --short 2>/dev/null || kubectl version --client)${NC}"
else
    echo -e "${GREEN}✓ kubectl already installed${NC}"
fi

# ==============================================================================
# 4. KIỂM TRA VÀ CÀI ĐẶT PYTHON DEPENDENCIES
# ==============================================================================
echo ""
echo -e "${YELLOW}[4/5] Checking Python and dependencies...${NC}"

# Kiểm tra Python3
if ! command -v python3 &> /dev/null; then
    echo "Python3 not found. Installing..."
    sudo apt update
    sudo apt install -y python3 python3-pip python3-venv
    echo -e "${GREEN}✓ Python3 installed${NC}"
else
    echo -e "${GREEN}✓ Python3 already installed: $(python3 --version)${NC}"
fi

# Tạo virtual environment nếu chưa có
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo -e "${GREEN}✓ Virtual environment created${NC}"
else
    echo -e "${GREEN}✓ Virtual environment exists${NC}"
fi

# Kích hoạt và cài packages
echo "Installing Python packages..."
source venv/bin/activate
pip install --upgrade pip -q
pip install kafka-python requests python-dotenv -q
pip install streamlit pandas psycopg2-binary plotly streamlit-autorefresh -q
echo -e "${GREEN}✓ Python packages installed${NC}"

# ==============================================================================
# 5. CÀI ĐẶT PSQL CLIENT (optional, để test PostgreSQL)
# ==============================================================================
echo ""
echo -e "${YELLOW}[5/6] Checking PostgreSQL client...${NC}"
if ! command -v psql &> /dev/null; then
    echo "psql not found. Installing PostgreSQL client..."
    sudo apt install -y postgresql-client
    echo -e "${GREEN}✓ PostgreSQL client installed${NC}"
else
    echo -e "${GREEN}✓ PostgreSQL client already installed${NC}"
fi

# ==============================================================================
# 6. FIX KAFKA HOSTNAME
# ==============================================================================
echo ""
echo -e "${YELLOW}[6/6] Configuring Kafka hostname...${NC}"
KAFKA_HOST="kafka-0.kafka.crypto-bigdata.svc.cluster.local"
if grep -q "$KAFKA_HOST" /etc/hosts 2>/dev/null; then
    echo -e "${GREEN}✓ Kafka hostname already configured${NC}"
else
    echo "Adding Kafka hostname to /etc/hosts..."
    echo "127.0.0.1 $KAFKA_HOST" | sudo tee -a /etc/hosts > /dev/null
    echo -e "${GREEN}✓ Kafka hostname configured${NC}"
fi

# ==============================================================================
# HOÀN THÀNH
# ==============================================================================
echo ""
echo "=========================================="
echo -e "${GREEN}✅ SETUP COMPLETED!${NC}"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. If Docker was just installed, logout and login again"
echo "  2. Run: ./deploy.sh to deploy the full stack"
echo ""
