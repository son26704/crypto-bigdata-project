# 🚀 Crypto Big Data Project

> Real-time cryptocurrency data processing pipeline using Kafka, Spark, PostgreSQL, and Streamlit on Kubernetes (Minikube)

![Architecture](https://img.shields.io/badge/Stack-Kafka%20%7C%20Spark%20%7C%20PostgreSQL%20%7C%20Streamlit-blue)
![Platform](https://img.shields.io/badge/Platform-Kubernetes-326CE5)
![Status](https://img.shields.io/badge/Status-Production-success)

## 📊 Architecture Overview

```
┌─────────────┐     ┌───────┐     ┌──────────────────┐     ┌────────────┐     ┌───────────┐
│ CoinGecko   │────▶│ Kafka │────▶│ Spark Streaming  │────▶│ PostgreSQL │────▶│ Dashboard │
│     API     │     │       │     │   (Real-time)    │     │            │     │ Streamlit │
└─────────────┘     └───────┘     └──────────────────┘     └────────────┘     └───────────┘
                                           │                       ▲
                                           │                       │
                                           ▼                       │
                                   ┌──────────────────┐           │
                                   │  Spark Batch     │───────────┘
                                   │ (Hourly/Daily)   │
                                   └──────────────────┘
                                           │
                                           ▼
                                   ┌──────────────────┐
                                   │      HDFS        │
                                   │   (Parquet)      │
                                   └──────────────────┘
```

## ✨ Features

- **Real-time Data Ingestion**: Fetch top 50 cryptocurrencies from CoinGecko API every 60 seconds
- **Stream Processing**: Spark Structured Streaming with micro-batch (30s intervals)
  - Moving Averages (5min, 15min, 1hour)
  - Price change alerts
  - Volume spike detection
- **Batch Processing**: Hourly and daily aggregations
  - OHLC (Open, High, Low, Close) candles
  - Volatility metrics
  - Volume statistics
- **Interactive Dashboard**: Real-time Streamlit dashboard with auto-refresh
  - Price charts with MA indicators
  - Volume analysis
  - Alert notifications
  - Hourly candlestick charts
- **Scalable Infrastructure**: Kubernetes-based deployment with Minikube

## 🛠️ Technology Stack

| Component               | Technology            | Version |
| ----------------------- | --------------------- | ------- |
| **Orchestration**       | Kubernetes (Minikube) | Latest  |
| **Message Queue**       | Apache Kafka          | 7.5.0   |
| **Stream Processing**   | Apache Spark          | 3.5.0   |
| **Batch Processing**    | Apache Spark          | 3.5.0   |
| **Storage**             | PostgreSQL            | 15      |
| **Distributed Storage** | HDFS                  | Latest  |
| **Visualization**       | Streamlit             | 1.29.0  |
| **Data Source**         | CoinGecko API         | v3      |
| **Language**            | Python                | 3.x     |

## 📋 Prerequisites

- **Ubuntu 20.04+** (or compatible Linux distribution)
- **Docker** 20.x+
- **8GB RAM** minimum (6GB for Minikube + 2GB for host)
- **4 CPU cores** (3 for Minikube + 1 for host)
- **30GB free disk space**
- **CoinGecko API Key** (free tier available)

## 🚀 Quick Start

### 1. Initial Setup (First Time Only)

```bash
# Clone repository (if not done)
cd /home/son/Documents/crypto-bigdata-project

# Run setup script to install all dependencies
./setup.sh

# If Docker was installed, logout and login again to apply permissions
```

### 2. Deploy Full Stack

```bash
# Deploy all Kubernetes components
./deploy.sh

# This will:
# - Start Minikube
# - Create namespace
# - Deploy Kafka, PostgreSQL, HDFS, Spark
# - Create Kafka topics
# - Upload Spark applications
```

### 3. Start Port Forwarding

Open **3 separate terminals** and run:

```bash
# Terminal 1: Kafka
kubectl port-forward kafka-0 9092:9092 -n crypto-bigdata

# Terminal 2: PostgreSQL
kubectl port-forward pod/postgres-0 5433:5432 -n crypto-bigdata

# Terminal 3: Spark UI (optional)
kubectl port-forward deployment/spark-master 8080:8080 -n crypto-bigdata
```

Or use the automated script (if gnome-terminal available):

```bash
./port-forward.sh
```

### 4. Run Producer

```bash
# Activate virtual environment
source venv/bin/activate

# Start producer
python producer/main.py
```

### 5. Run Spark Streaming

```bash
# Get Spark Master pod name
SPARK_MASTER_POD=$(kubectl get pods -n crypto-bigdata -l app=spark-master -o jsonpath='{.items[0].metadata.name}')

# Execute into pod
kubectl exec -it -n crypto-bigdata $SPARK_MASTER_POD -- bash

# Inside pod, run Spark Streaming
/opt/spark/bin/spark-submit \
   --master local[2] \
   --name "CryptoProcessor" \
   --driver-memory 512M \
   --conf spark.driver.maxResultSize=200M \
   --conf spark.jars.ivy=/tmp/.ivy2 \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
   /tmp/spark-apps/streaming/realtime_processor.py
```

### 6. Run Spark Batch (After collecting some data)

```bash
# In the same Spark Master pod or a new session
/opt/spark/bin/spark-submit \
  --master local[2] \
  --name "BatchProcessor" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.postgresql:postgresql:42.7.1 \
  /tmp/spark-apps/batch/batch_processor.py
```

### 7. Run Dashboard

```bash
# In a new terminal
source venv/bin/activate
streamlit run dashboard.py

# Dashboard will open at: http://localhost:8501
```

## 📜 Available Scripts

| Script                   | Description                                                           |
| ------------------------ | --------------------------------------------------------------------- |
| `./setup.sh`             | Install all dependencies (Docker, Minikube, kubectl, Python packages) |
| `./deploy.sh`            | Deploy full Kubernetes stack                                          |
| `./start.sh`             | Start system (for subsequent runs)                                    |
| `./stop.sh`              | Stop Minikube cluster                                                 |
| `./check-status.sh`      | Check system status and health                                        |
| `./port-forward.sh`      | Automatically open port forward terminals                             |
| `./update-spark-apps.sh` | Update Spark application code in pods                                 |

## 🔧 Configuration

### Environment Variables (`.env`)

```env
COINGECKO_API_URL=https://api.coingecko.com/api/v3
COINGECKO_API_KEY=your-api-key-here
FETCH_INTERVAL=60
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=crypto-prices
```

### Spark Streaming Config (`spark-apps/streaming/config.py`)

- Kafka bootstrap servers
- Micro-batch interval (30 seconds)
- Moving average windows (5min, 15min, 1hour)
- Alert thresholds

### Spark Batch Config (`spark-apps/batch/batch_processor.py`)

- PostgreSQL connection
- HDFS paths
- Aggregation windows

## 📊 Database Schema

### `realtime_prices` table

- Real-time price snapshots with MA indicators
- Indexed on: `symbol`, `timestamp`

### `alerts` table

- Price change alerts
- Volume spike alerts
- MA crossover alerts

### `hourly_stats` table

- OHLC candles per hour
- Volume aggregations
- Price volatility metrics

### `daily_stats` table

- Daily OHLC candles
- Daily volume totals
- Price change percentages

## 🔍 Monitoring & Debugging

### Check System Status

```bash
./check-status.sh
```

### View Logs

```bash
# Kafka logs
kubectl logs kafka-0 -n crypto-bigdata

# PostgreSQL logs
kubectl logs postgres-0 -n crypto-bigdata

# Spark Master logs
kubectl logs deployment/spark-master -n crypto-bigdata
```

### Test Kafka Consumer

```bash
kubectl exec -it kafka-0 -n crypto-bigdata -- kafka-console-consumer \
  --topic crypto-prices \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --max-messages 5
```

### Query PostgreSQL

```bash
PGPASSWORD=cryptopass123 psql -h localhost -p 5433 -U cryptouser -d cryptodb

# View recent prices
SELECT symbol, price, timestamp
FROM realtime_prices
ORDER BY timestamp DESC
LIMIT 10;
```

### Spark UI

Access at: http://localhost:8080 (after port-forward)

## 🚦 Workflow for Subsequent Runs

After initial setup and deployment:

```bash
# 1. Start Minikube
./start.sh

# 2. Port forwarding (3 terminals)
kubectl port-forward kafka-0 9092:9092 -n crypto-bigdata
kubectl port-forward pod/postgres-0 5433:5432 -n crypto-bigdata

# 3. Start producer
source venv/bin/activate && python producer/main.py

# 4. Start Spark Streaming (in pod)
# See Quick Start step 5

# 5. Start dashboard
source venv/bin/activate && streamlit run dashboard.py
```

## 🛑 Stopping the System

```bash
# Stop all processes (Ctrl+C in terminals)
# Then stop Minikube
./stop.sh

# Or completely delete cluster
minikube delete
```

## 📖 Documentation

- [Full Deployment Guide](DEPLOYMENT_GUIDE_UBUNTU.md) - Detailed step-by-step instructions
- [Original Script Reference](script.sh) - Windows/PowerShell commands reference

## 🐛 Troubleshooting

### Pods not starting

```bash
kubectl describe pod <pod-name> -n crypto-bigdata
kubectl logs <pod-name> -n crypto-bigdata
```

### Port forward connection refused

- Ensure pods are in Running state
- Check pod names are correct
- Verify namespace is crypto-bigdata

### Producer cannot connect to Kafka

- Verify Kafka port-forward is active
- Check Kafka pod is running
- Test with Kafka console consumer

### Spark cannot read from Kafka

- Check Kafka bootstrap server in config
- Verify topic exists: `kubectl exec -it kafka-0 -- kafka-topics --list --bootstrap-server localhost:9092`
- Check Spark logs for connection errors

### Dashboard shows no data

- Verify PostgreSQL port-forward is active
- Check data exists: `psql -h localhost -p 5433 -U cryptouser -d cryptodb`
- Ensure Spark Streaming has processed batches

## 📝 Project Structure

```
crypto-bigdata-project/
├── k8s/                           # Kubernetes manifests
│   ├── kafka/                     # Kafka & Zookeeper
│   ├── postgres/                  # PostgreSQL with init SQL
│   ├── hdfs/                      # HDFS NameNode & DataNode
│   ├── spark/                     # Spark Master & Worker
│   └── grafana/                   # Grafana (optional)
├── producer/                      # Data producer
│   ├── main.py                    # Main producer script
│   ├── crypto_fetcher.py          # CoinGecko API client
│   └── config.py                  # Producer config
├── spark-apps/                    # Spark applications
│   ├── streaming/                 # Real-time processing
│   │   ├── realtime_processor.py
│   │   └── config.py
│   ├── batch/                     # Batch processing
│   │   ├── batch_processor.py
│   │   └── hdfs_reader.py
│   └── common/                    # Shared configs
│       └── postgres_config.py
├── dashboard.py                   # Streamlit dashboard
├── .env                          # Environment variables
├── requirements.txt               # Python dependencies
├── setup.sh                       # Setup script
├── deploy.sh                      # Deployment script
├── start.sh                       # Start script
├── stop.sh                        # Stop script
├── check-status.sh                # Status checker
├── port-forward.sh                # Port forwarding helper
├── update-spark-apps.sh           # Update Spark code
└── DEPLOYMENT_GUIDE_UBUNTU.md     # Full guide
```

## 🤝 Contributing

Feel free to open issues or submit pull requests for improvements.

## 📄 License

This project is for educational purposes.

## 👤 Author

**Son Nguyen**

---

**Happy Data Engineering! 🚀📊**
