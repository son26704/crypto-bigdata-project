from pathlib import Path
from dotenv import load_dotenv
import os

BASE_DIR = Path(__file__).resolve().parents[2]

ENV_PATH = BASE_DIR / ".env"

load_dotenv(dotenv_path=ENV_PATH)

COINGECKO_API_URL = os.getenv("COINGECKO_API_URL", "https://api.coingecko.com/api/v3")
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", 60))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",") # 127.0.0.1 kafka-0.kafka.crypto-bigdata.svc.cluster.local
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-prices")
