# producer/config.py
from pathlib import Path
from dotenv import load_dotenv
import os

# Try to locate .env from project root safely (backward compatible)
THIS_FILE = Path(__file__).resolve()
CANDIDATES = [
    THIS_FILE.parents[2] / ".env",  # keep your original intent
    THIS_FILE.parents[1] / ".env",
    THIS_FILE.parents[0] / ".env",
]
ENV_PATH = next((p for p in CANDIDATES if p.exists()), CANDIDATES[0])

load_dotenv(dotenv_path=ENV_PATH)

COINGECKO_API_URL = os.getenv("COINGECKO_API_URL", "https://api.coingecko.com/api/v3")
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", 60))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",") # 127.0.0.1 kafka-0.kafka.crypto-bigdata.svc.cluster.local
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-prices")
