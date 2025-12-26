# spark-apps/streaming/config.py
KAFKA_BOOTSTRAP_SERVERS = "kafka-0.kafka.crypto-bigdata.svc.cluster.local:9092"
KAFKA_TOPIC = "crypto-prices"

# Spark Configuration
APP_NAME = "CryptoStreamingProcessor"
CHECKPOINT_LOCATION = "/tmp/spark-streaming-checkpoint"

# Processing Configuration
MICRO_BATCH_INTERVAL = "10 seconds"  # Process every 10 seconds

# Moving Average Windows (in minutes)
MA_WINDOWS = {
    'ma_5min': 5,
    'ma_15min': 15,
    'ma_1hour': 60
}

# Alert Thresholds
ALERT_THRESHOLDS = {
    'price_change_percent': 5.0,     # Alert if price changes > 5%
    'volume_spike_multiplier': 3.0   # Alert if volume > 3x average
}