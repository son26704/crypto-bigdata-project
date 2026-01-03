# spark-apps/streaming/config.py
KAFKA_BOOTSTRAP_SERVERS = "kafka-0.kafka.crypto-bigdata.svc.cluster.local:9092"
KAFKA_TOPIC = "crypto-prices"

APP_NAME = "CryptoStreamingProcessor"
CHECKPOINT_LOCATION = "/tmp/spark-streaming-checkpoint"

MICRO_BATCH_INTERVAL = "30 seconds"

MA_WINDOWS = {
    "ma_5min": 5,
    "ma_15min": 15,
    "ma_1hour": 60
}

ALERT_THRESHOLDS = {
    "price_change_percent": 5.0,
    "volume_spike_multiplier": 3.0
}

ENABLE_MA_CROSSOVER_ALERT = True