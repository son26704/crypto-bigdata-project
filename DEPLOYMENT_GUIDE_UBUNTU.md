# 🚀 HƯỚNG DẪN TRIỂN KHAI CRYPTO BIG DATA PROJECT TRÊN UBUNTU

## 📌 Tổng quan Pipeline

```
CoinGecko API → Producer → Kafka → Spark Streaming → PostgreSQL → Dashboard
                                  ↓
                           Spark Batch (Hourly/Daily aggregation)
```

---

## 🔧 BƯỚC 1: CÀI ĐẶT CÁC CÔNG CỤ CẦN THIẾT

### 1.1. Cài đặt Docker và Docker Compose

```bash
# Cập nhật hệ thống
sudo apt update && sudo apt upgrade -y

# Cài Docker
sudo apt install -y docker.io docker-compose
sudo systemctl start docker
sudo systemctl enable docker

# Thêm user vào group docker (để chạy không cần sudo)
sudo usermod -aG docker $USER
newgrp docker

# Kiểm tra
docker --version
docker-compose --version
```

### 1.2. Cài đặt Minikube

```bash
# Download Minikube
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
rm minikube-linux-amd64

# Kiểm tra
minikube version
```

### 1.3. Cài đặt kubectl

```bash
# Download kubectl
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
rm kubectl

# Kiểm tra
kubectl version --client
```

### 1.4. Cài đặt Python và các thư viện

```bash
# Cài Python 3 và pip
sudo apt install -y python3 python3-pip python3-venv

# Tạo virtual environment
cd /home/son/Documents/crypto-bigdata-project
python3 -m venv venv

# Kích hoạt venv
source venv/bin/activate

# Cài các thư viện cần thiết
pip install --upgrade pip
pip install kafka-python requests python-dotenv
pip install streamlit pandas psycopg2-binary plotly streamlit-autorefresh
```

---

## 🎯 BƯỚC 2: KHỞI ĐỘNG MINIKUBE VÀ KUBERNETES

### 2.1. Khởi động Minikube với Docker driver

```bash
# Khởi động minikube với cấu hình đủ mạnh
minikube start --driver=docker --memory=6144 --cpus=3 --disk-size=30g

# Kiểm tra trạng thái
minikube status
kubectl cluster-info
```

### 2.2. Tạo namespace

```bash
# Tạo namespace cho project
kubectl create namespace crypto-bigdata

# Set namespace mặc định
kubectl config set-context --current --namespace=crypto-bigdata

# Kiểm tra
kubectl get namespaces
kubectl config view --minify | grep namespace:
```

---

## 📦 BƯỚC 3: DEPLOY CÁC THÀNH PHẦN K8S

### 3.1. Deploy theo thứ tự

```bash
cd /home/son/Documents/crypto-bigdata-project

# 1. Zookeeper (cần trước Kafka)
kubectl apply -f k8s/kafka/zookeeper.yaml

# Đợi Zookeeper ready
kubectl wait --for=condition=ready pod -l app=zookeeper --timeout=300s

# 2. Kafka
kubectl apply -f k8s/kafka/kafka.yaml

# Đợi Kafka ready
kubectl wait --for=condition=ready pod -l app=kafka --timeout=300s

# 3. PostgreSQL
kubectl apply -f k8s/postgres/postgres.yaml

# Đợi PostgreSQL ready
kubectl wait --for=condition=ready pod -l app=postgres --timeout=300s

# 4. HDFS (NameNode và DataNode)
kubectl apply -f k8s/hdfs/hdfs-configmap.yaml
kubectl apply -f k8s/hdfs/namenode.yaml
kubectl apply -f k8s/hdfs/datanode.yaml

# Đợi HDFS ready
kubectl wait --for=condition=ready pod -l app=namenode --timeout=300s
kubectl wait --for=condition=ready pod -l app=datanode --timeout=300s

# 5. Spark (Master và Worker)
kubectl apply -f k8s/spark/spark-master.yaml
kubectl apply -f k8s/spark/spark-worker.yaml

# Đợi Spark ready
kubectl wait --for=condition=ready pod -l app=spark-master --timeout=300s
kubectl wait --for=condition=ready pod -l app=spark-worker --timeout=300s

# 6. Grafana (optional)
kubectl apply -f k8s/grafana/grafana.yaml
```

### 3.2. Kiểm tra trạng thái

```bash
# Xem tất cả pods
kubectl get pods -n crypto-bigdata

# Xem services
kubectl get svc -n crypto-bigdata

# Kiểm tra logs nếu có lỗi
kubectl logs <pod-name> -n crypto-bigdata
```

---

## � BƯỚC 3.5: FIX KAFKA HOSTNAME (Quan trọng!)

Kafka trong Kubernetes advertise hostname internal, cần thêm vào `/etc/hosts` để Producer từ localhost có thể kết nối:

```bash
# Tự động fix
./fix-kafka-hostname.sh

# Hoặc thủ công
echo "127.0.0.1 kafka-0.kafka.crypto-bigdata.svc.cluster.local" | sudo tee -a /etc/hosts
```

**Giải thích**: Khi Producer connect tới `localhost:9092`, Kafka sẽ trả về advertised hostname là `kafka-0.kafka.crypto-bigdata.svc.cluster.local:9092`. Nếu không có entry trong `/etc/hosts`, Producer sẽ không resolve được hostname này và fail với lỗi `KafkaTimeoutError`.

---

## �🔌 BƯỚC 4: TẠO KAFKA TOPIC

```bash
# Lấy tên pod kafka chính xác
KAFKA_POD=$(kubectl get pods -n crypto-bigdata -l app=kafka -o jsonpath='{.items[0].metadata.name}')
echo "Kafka pod: $KAFKA_POD"

# Tạo topic crypto-prices
kubectl exec -it $KAFKA_POD -n crypto-bigdata -- kafka-topics \
  --create \
  --topic crypto-prices \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Kiểm tra topic đã tạo
kubectl exec -it $KAFKA_POD -n crypto-bigdata -- kafka-topics \
  --list \
  --bootstrap-server localhost:9092
```

---

## 📤 BƯỚC 5: UPLOAD SPARK APPLICATIONS VÀO SPARK MASTER POD

```bash
# Lấy tên pod spark-master chính xác
SPARK_MASTER_POD=$(kubectl get pods -n crypto-bigdata -l app=spark-master -o jsonpath='{.items[0].metadata.name}')
echo "Spark Master pod: $SPARK_MASTER_POD"

# Tạo thư mục trong pod
kubectl exec -n crypto-bigdata $SPARK_MASTER_POD -- mkdir -p /tmp/spark-apps/common
kubectl exec -n crypto-bigdata $SPARK_MASTER_POD -- mkdir -p /tmp/spark-apps/batch
kubectl exec -n crypto-bigdata $SPARK_MASTER_POD -- mkdir -p /tmp/spark-apps/streaming

# Copy files vào pod
kubectl cp spark-apps/common/postgres_config.py crypto-bigdata/$SPARK_MASTER_POD:/tmp/spark-apps/common/
kubectl cp spark-apps/streaming/config.py crypto-bigdata/$SPARK_MASTER_POD:/tmp/spark-apps/streaming/
kubectl cp spark-apps/streaming/realtime_processor.py crypto-bigdata/$SPARK_MASTER_POD:/tmp/spark-apps/streaming/
kubectl cp spark-apps/batch/batch_processor.py crypto-bigdata/$SPARK_MASTER_POD:/tmp/spark-apps/batch/
kubectl cp spark-apps/batch/hdfs_reader.py crypto-bigdata/$SPARK_MASTER_POD:/tmp/spark-apps/batch/

# Kiểm tra files đã copy
kubectl exec -n crypto-bigdata $SPARK_MASTER_POD -- ls -la /tmp/spark-apps/common/
kubectl exec -n crypto-bigdata $SPARK_MASTER_POD -- ls -la /tmp/spark-apps/streaming/
kubectl exec -n crypto-bigdata $SPARK_MASTER_POD -- ls -la /tmp/spark-apps/batch/
```

---

## 🚦 BƯỚC 6: PORT FORWARDING

Mở 3 terminal riêng biệt và chạy các lệnh sau (mỗi terminal 1 lệnh):

### Terminal 1: Kafka Port Forward

```bash
kubectl port-forward kafka-0 9092:9092 -n crypto-bigdata
```

### Terminal 2: PostgreSQL Port Forward

```bash
POSTGRES_POD=$(kubectl get pods -n crypto-bigdata -l app=postgres -o jsonpath='{.items[0].metadata.name}')
kubectl port-forward pod/$POSTGRES_POD 5433:5432 -n crypto-bigdata
```

### Terminal 3: Spark UI Port Forward (Optional - để xem Spark UI)

```bash
kubectl port-forward deployment/spark-master 8080:8080 -n crypto-bigdata
```

**Lưu ý:** Giữ các terminal này mở để các port forward hoạt động!

---

## 🎬 BƯỚC 7: CHẠY CÁC THÀNH PHẦN CHÍNH

### 7.1. Chạy Producer (Terminal mới)

```bash
cd /home/son/Documents/crypto-bigdata-project
source venv/bin/activate
python producer/main.py
```

**Kết quả:** Producer sẽ fetch dữ liệu từ CoinGecko API mỗi 60s và gửi vào Kafka topic `crypto-prices`

### 7.2. Chạy Spark Streaming (Terminal mới)

```bash
# Vào pod spark-master
SPARK_MASTER_POD=$(kubectl get pods -n crypto-bigdata -l app=spark-master -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it -n crypto-bigdata $SPARK_MASTER_POD -- bash

# Trong pod, chạy spark-submit
/opt/spark/bin/spark-submit \
   --master local[2] \
   --name "CryptoProcessor" \
   --driver-memory 512M \
   --conf spark.driver.maxResultSize=200M \
   --conf spark.jars.ivy=/tmp/.ivy2 \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
   /tmp/spark-apps/streaming/realtime_processor.py
```

**Kết quả:** Spark Streaming sẽ đọc từ Kafka, tính toán MA (Moving Average), alerts và ghi vào PostgreSQL

### 7.3. Chạy Spark Batch (Terminal mới - chạy sau khi đã có dữ liệu)

```bash
# Vào pod spark-master (nếu chưa vào)
kubectl exec -it -n crypto-bigdata $SPARK_MASTER_POD -- bash

# Trong pod, chạy batch processor
/opt/spark/bin/spark-submit \
  --master local[2] \
  --name "BatchProcessor" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.postgresql:postgresql:42.7.1 \
  /tmp/spark-apps/batch/batch_processor.py
```

**Kết quả:** Tính toán hourly và daily statistics và ghi vào bảng `hourly_stats`, `daily_stats`

### 7.4. Chạy Dashboard (Terminal mới)

```bash
cd /home/son/Documents/crypto-bigdata-project
source venv/bin/activate
streamlit run dashboard.py
```

**Kết quả:** Dashboard sẽ mở tại `http://localhost:8501` hiển thị realtime prices, charts, alerts

---

## 🔄 WORKFLOW HOÀN CHỈNH

### Khởi động hệ thống (Các lần sau)

```bash
# 1. Start Minikube
minikube start

# 2. Set namespace
kubectl config set-context --current --namespace=crypto-bigdata

# 3. Kiểm tra pods đã running
kubectl get pods

# 4. Port forwarding (3 terminals)
kubectl port-forward kafka-0 9092:9092 -n crypto-bigdata
kubectl port-forward pod/<postgres-pod> 5433:5432 -n crypto-bigdata

# 5. Copy lại spark apps nếu cần update
SPARK_MASTER_POD=$(kubectl get pods -n crypto-bigdata -l app=spark-master -o jsonpath='{.items[0].metadata.name}')
kubectl cp spark-apps/streaming/realtime_processor.py crypto-bigdata/$SPARK_MASTER_POD:/tmp/spark-apps/streaming/

# 6. Chạy Producer
source venv/bin/activate
python producer/main.py

# 7. Chạy Spark Streaming (trong pod)
kubectl exec -it -n crypto-bigdata $SPARK_MASTER_POD -- bash
# Chạy spark-submit như bước 7.2

# 8. Chạy Dashboard
streamlit run dashboard.py
```

---

## 🐛 TROUBLESHOOTING

### Kiểm tra logs

```bash
# Logs của producer (ngoài host)
# Xem trong terminal đang chạy producer

# Logs của Kafka
kubectl logs kafka-0 -n crypto-bigdata

# Logs của PostgreSQL
kubectl logs <postgres-pod> -n crypto-bigdata

# Logs của Spark Master
kubectl logs <spark-master-pod> -n crypto-bigdata

# Logs của Spark Streaming (trong pod)
# Xem output trong terminal đang chạy spark-submit
```

### Pods không chạy

```bash
# Xem chi tiết pod
kubectl describe pod <pod-name> -n crypto-bigdata

# Restart pod
kubectl delete pod <pod-name> -n crypto-bigdata
# Pod sẽ tự động recreate
```

### Kafka topic không có data

```bash
# Consumer test
kubectl exec -it kafka-0 -n crypto-bigdata -- kafka-console-consumer \
  --topic crypto-prices \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 5
```

### PostgreSQL connection failed

```bash
# Test connection từ host
psql -h localhost -p 5433 -U cryptouser -d cryptodb
# Password: cryptopass123

# Xem tables
\dt

# Xem dữ liệu
SELECT COUNT(*) FROM realtime_prices;
```

### Spark không kết nối Kafka

- Kiểm tra Kafka bootstrap server trong config
- Đảm bảo port-forward đang chạy
- Kiểm tra network giữa các pods

---

## 📊 KIỂM TRA HỆ THỐNG

### Kiểm tra data flow

```bash
# 1. Producer → Kafka
# Xem logs producer, phải thấy "Sent to Kafka"

# 2. Kafka có data
kubectl exec -it kafka-0 -n crypto-bigdata -- kafka-console-consumer \
  --topic crypto-prices \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 2

# 3. Spark Streaming đọc được
# Xem logs spark-submit, phải thấy "Processing Batch"

# 4. PostgreSQL có data
psql -h localhost -p 5433 -U cryptouser -d cryptodb -c "SELECT symbol, price, timestamp FROM realtime_prices ORDER BY timestamp DESC LIMIT 10;"

# 5. Dashboard hiển thị
# Mở browser: http://localhost:8501
```

---

## 🛑 TẮT HỆ THỐNG

```bash
# Stop producer (Ctrl+C trong terminal)

# Stop spark streaming (Ctrl+C trong pod)

# Stop dashboard (Ctrl+C)

# Stop port-forwards (Ctrl+C trong các terminal)

# Stop minikube
minikube stop

# Xóa hoàn toàn (nếu cần reset)
minikube delete
```

---

## 📝 GHI CHÚ QUAN TRỌNG

1. **Pod Names**: Pod names có suffix random (e.g., `spark-master-5f778b99f7-rpd6z`). Sử dụng script tự động lấy pod name bằng kubectl get pods với label selector.

2. **Virtual Environment**: Luôn activate venv trước khi chạy Python scripts:

   ```bash
   source venv/bin/activate
   ```

3. **Port Forwarding**: Cần giữ các terminal port-forward mở suốt quá trình chạy hệ thống.

4. **Thứ tự khởi động**:

   - Minikube → K8s pods → Port forwards → Producer → Spark Streaming → Dashboard

5. **Resource**: Đảm bảo máy có ít nhất:

   - RAM: 8GB (6GB cho Minikube + 2GB cho host)
   - CPU: 4 cores (3 cho Minikube + 1 cho host)
   - Disk: 30GB free

6. **API Key**: Đảm bảo file `.env` có API key hợp lệ của CoinGecko

---

## ✅ CHECKLIST DEPLOYMENT

- [ ] Docker và Minikube đã cài
- [ ] kubectl đã cài và cấu hình
- [ ] Python venv đã tạo và cài đủ thư viện
- [ ] Minikube đã start với đủ resources
- [ ] Namespace crypto-bigdata đã tạo
- [ ] Tất cả K8s yamls đã apply
- [ ] Kafka topic crypto-prices đã tạo
- [ ] Spark apps đã copy vào spark-master pod
- [ ] Port forwards đang chạy (Kafka, PostgreSQL)
- [ ] Producer đang chạy và gửi data
- [ ] Spark Streaming đang chạy
- [ ] Dashboard đang chạy và hiển thị data

---

**Good luck! 🚀**
