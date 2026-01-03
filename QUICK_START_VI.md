# 🚀 HƯỚNG DẪN NHANH - TIẾNG VIỆT

## ⚡ Chạy lần đầu tiên (Toàn bộ setup tự động)

```bash
cd /home/son/Documents/crypto-bigdata-project
./quick-start.sh
```

Script này sẽ:

1. ✅ Cài đặt Docker, Minikube, kubectl
2. ✅ Tạo virtual environment Python
3. ✅ Cài tất cả thư viện cần thiết
4. ✅ Deploy toàn bộ Kubernetes stack
5. ✅ Tạo Kafka topics
6. ✅ Upload Spark applications
7. ✅ **Fix Kafka hostname** (tự động thêm vào /etc/hosts)

**Thời gian**: ~10-15 phút (tùy tốc độ mạng)

---

## 🔧 Nếu gặp lỗi Kafka Connection

Nếu Producer báo lỗi `KafkaTimeoutError: Failed to update metadata`, chạy:

```bash
./fix-kafka-hostname.sh
```

**Lý do**: Kafka advertise hostname internal của K8s (`kafka-0.kafka.crypto-bigdata.svc.cluster.local`), cần map vào localhost để Producer kết nối được.

---

## 📡 Sau khi setup xong, mở 3 terminals để Port Forwarding:

### Terminal 1 - Kafka:

```bash
kubectl port-forward kafka-0 9092:9092 -n crypto-bigdata
```

### Terminal 2 - PostgreSQL:

```bash
POSTGRES_POD=$(kubectl get pods -n crypto-bigdata -l app=postgres -o jsonpath='{.items[0].metadata.name}')
kubectl port-forward pod/$POSTGRES_POD 5433:5432 -n crypto-bigdata
```

### Terminal 3 - Spark UI (optional):

```bash
kubectl port-forward deployment/spark-master 8080:8080 -n crypto-bigdata
```

💡 **Hoặc dùng script tự động** (nếu có gnome-terminal):

```bash
./port-forward.sh
```

⚠️ **Lưu ý**: Giữ các terminal này mở!

---

## 🎬 Chạy hệ thống:

### 1. Chạy Producer và Dashboard tự động:

```bash
./run-all.sh
```

Hoặc chạy thủ công:

#### Producer (Terminal mới):

```bash
source venv/bin/activate
python producer/main.py
```

#### Dashboard (Terminal mới):

```bash
source venv/bin/activate
streamlit run dashboard.py
```

→ Mở trình duyệt: http://localhost:8501

---

### 2. Chạy Spark Streaming (Bắt buộc):

#### Bước 1: Vào pod Spark Master

```bash
SPARK_MASTER_POD=$(kubectl get pods -n crypto-bigdata -l app=spark-master -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it -n crypto-bigdata $SPARK_MASTER_POD -- bash
```

#### Bước 2: Trong pod, chạy Spark Streaming

```bash
/opt/spark/bin/spark-submit \
   --master local[2] \
   --name "CryptoProcessor" \
   --driver-memory 512M \
   --conf spark.driver.maxResultSize=200M \
   --conf spark.jars.ivy=/tmp/.ivy2 \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
   /tmp/spark-apps/streaming/realtime_processor.py
```

---

### 3. Chạy Spark Batch (Sau khi đã có data - Optional):

Trong cùng pod Spark Master:

```bash
/opt/spark/bin/spark-submit \
  --master local[2] \
  --name "BatchProcessor" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.postgresql:postgresql:42.7.1 \
  /tmp/spark-apps/batch/batch_processor.py
```

---

## 🔄 Các lần chạy tiếp theo

```bash
# 1. Khởi động Minikube
./start.sh

# 2. Port forwarding (3 terminals hoặc dùng ./port-forward.sh)
kubectl port-forward kafka-0 9092:9092 -n crypto-bigdata
kubectl port-forward pod/<postgres-pod> 5433:5432 -n crypto-bigdata

# 3. Chạy hệ thống
./run-all.sh

# 4. Spark Streaming (manual - xem bước 2 ở trên)
```

---

## 🛠️ Scripts tiện ích

| Script                   | Chức năng                            |
| ------------------------ | ------------------------------------ |
| `./check-status.sh`      | Kiểm tra trạng thái toàn bộ hệ thống |
| `./stop.sh`              | Tắt Minikube                         |
| `./update-spark-apps.sh` | Cập nhật code Spark khi có thay đổi  |

---

## 🐛 Troubleshooting nhanh

### Kiểm tra trạng thái:

```bash
./check-status.sh
```

### Xem logs:

```bash
# Xem pods
kubectl get pods -n crypto-bigdata

# Xem logs của pod
kubectl logs <tên-pod> -n crypto-bigdata

# Logs Kafka
kubectl logs kafka-0 -n crypto-bigdata

# Logs PostgreSQL
POSTGRES_POD=$(kubectl get pods -n crypto-bigdata -l app=postgres -o jsonpath='{.items[0].metadata.name}')
kubectl logs $POSTGRES_POD -n crypto-bigdata
```

### Test Kafka có nhận data:

```bash
kubectl exec -it kafka-0 -n crypto-bigdata -- kafka-console-consumer \
  --topic crypto-prices \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 5
```

### Kiểm tra PostgreSQL:

```bash
PGPASSWORD=cryptopass123 psql -h localhost -p 5433 -U cryptouser -d cryptodb

# Trong psql:
SELECT COUNT(*) FROM realtime_prices;
SELECT symbol, price, timestamp FROM realtime_prices ORDER BY timestamp DESC LIMIT 10;
\q
```

---

## 🛑 Tắt hệ thống

```bash
# Dừng Producer (Ctrl+C trong terminal)
# Dừng Dashboard (Ctrl+C trong terminal)
# Dừng Spark Streaming (Ctrl+C trong pod)
# Dừng Port forwards (Ctrl+C trong terminals)

# Tắt Minikube
./stop.sh

# Hoặc xóa hoàn toàn (reset về ban đầu)
minikube delete
```

---

## 📊 Luồng dữ liệu

```
1. Producer fetch từ CoinGecko API (mỗi 60s)
   ↓
2. Gửi vào Kafka topic "crypto-prices"
   ↓
3. Spark Streaming đọc từ Kafka (micro-batch 30s)
   ↓
4. Tính toán: MA (5min, 15min, 1hour), Alerts
   ↓
5. Ghi vào PostgreSQL bảng: realtime_prices, alerts
   ↓
6. Dashboard đọc từ PostgreSQL và hiển thị (auto-refresh 30s)

Parallel:
7. Spark Batch đọc từ realtime_prices
   ↓
8. Tính toán hourly và daily statistics
   ↓
9. Ghi vào bảng: hourly_stats, daily_stats
   ↓
10. Dashboard hiển thị candlestick charts
```

---

## 📚 Tài liệu chi tiết

- **Hướng dẫn đầy đủ**: `DEPLOYMENT_GUIDE_UBUNTU.md`
- **README project**: `README.md`
- **Notes migration**: `MIGRATION_NOTES.md`
- **Script Windows cũ**: `script.sh` (reference)

---

## ✅ Checklist hoàn thành

Đảm bảo tất cả các bước sau đã OK:

- [ ] Minikube đã chạy (`minikube status`)
- [ ] Tất cả pods đang Running (`kubectl get pods`)
- [ ] Port forwards đang hoạt động (3 terminals)
- [ ] Producer đang chạy và in "Sent to Kafka"
- [ ] Spark Streaming đang chạy và in "Processing Batch"
- [ ] Dashboard đã mở tại http://localhost:8501
- [ ] Dashboard hiển thị dữ liệu thời gian thực

---

## 🎯 Mục tiêu cuối cùng

Khi mọi thứ hoạt động đúng:

1. ✅ Dashboard hiển thị giá real-time của 50 coins
2. ✅ Charts với MA indicators (5min, 15min, 1hour)
3. ✅ Volume charts
4. ✅ Alerts khi có biến động giá
5. ✅ Hourly candlestick charts (sau khi batch chạy)
6. ✅ Auto-refresh mỗi 30s

---

**Chúc bạn thành công! 🚀📊💎**

Nếu có vấn đề, check file `DEPLOYMENT_GUIDE_UBUNTU.md` phần Troubleshooting!
