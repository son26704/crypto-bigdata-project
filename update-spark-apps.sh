#!/bin/bash

# ==============================================================================
# UPDATE SPARK APPS: Cập nhật code Spark vào pod khi có thay đổi
# ==============================================================================

echo "=========================================="
echo "📤 UPDATING SPARK APPLICATIONS"
echo "=========================================="
echo ""

# Màu sắc
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Lấy pod name
SPARK_MASTER_POD=$(kubectl get pods -n crypto-bigdata -l app=spark-master -o jsonpath='{.items[0].metadata.name}')

if [ -z "$SPARK_MASTER_POD" ]; then
    echo -e "${RED}Error: Spark Master pod not found!${NC}"
    echo "Make sure the cluster is running and namespace is correct."
    exit 1
fi

echo "Spark Master pod: $SPARK_MASTER_POD"
echo ""

# Upload files
echo -e "${YELLOW}Uploading files...${NC}"

echo "  → postgres_config.py"
kubectl cp spark-apps/common/postgres_config.py crypto-bigdata/$SPARK_MASTER_POD:/tmp/spark-apps/common/

echo "  → streaming/config.py"
kubectl cp spark-apps/streaming/config.py crypto-bigdata/$SPARK_MASTER_POD:/tmp/spark-apps/streaming/

echo "  → streaming/realtime_processor.py"
kubectl cp spark-apps/streaming/realtime_processor.py crypto-bigdata/$SPARK_MASTER_POD:/tmp/spark-apps/streaming/

echo "  → batch/batch_processor.py"
kubectl cp spark-apps/batch/batch_processor.py crypto-bigdata/$SPARK_MASTER_POD:/tmp/spark-apps/batch/

echo "  → batch/hdfs_reader.py"
kubectl cp spark-apps/batch/hdfs_reader.py crypto-bigdata/$SPARK_MASTER_POD:/tmp/spark-apps/batch/

echo ""
echo -e "${GREEN}✅ Spark applications updated!${NC}"
echo ""
echo "⚠️  Remember to restart Spark Streaming if it's running:"
echo "   1. Stop current spark-submit (Ctrl+C in pod)"
echo "   2. Run spark-submit again with the same command"
echo ""
