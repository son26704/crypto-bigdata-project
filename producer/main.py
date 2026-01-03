# producer/main.py
import json
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from crypto_fetcher import CryptoFetcher
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, FETCH_INTERVAL

class CryptoProducer:
    def __init__(self):
        self.fetcher = CryptoFetcher()
        self.producer = self._create_producer()

    def _create_producer(self):
        """Create Kafka producer with retry logic"""
        max_retries = 5
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    acks="all",
                    retries=3,
                    request_timeout_ms=30000,
                    max_in_flight_requests_per_connection=1,
                )
                print("✓ Connected to Kafka")
                return producer
            except KafkaError as e:
                print(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise

    def send_to_kafka(self, records):
        """Send records to Kafka topic"""
        for record in records:
            try:
                # IMPORTANT: Removed record['timestamp'] because Spark consumer doesn't use it.
                # Keep ingestion_timestamp from fetcher as the canonical time field.

                key = (record.get("symbol") or "").encode("utf-8")  # stable partitioning by symbol
                self.producer.send(KAFKA_TOPIC, key=key, value=record)

            except KafkaError as e:
                print(f"Error sending to Kafka: {e}")
            except Exception as e:
                print(f"Unexpected error: {e}")

        # Flush to ensure all messages are sent for this fetch batch
        self.producer.flush()

    def run(self):
        print("Starting Crypto Producer...")
        preview = self.fetcher.fetch_prices()
        print(f"Tracking {len(preview)} cryptocurrencies")
        print(f"Fetch interval: {FETCH_INTERVAL} seconds")
        print(f"Kafka topic: {KAFKA_TOPIC}")
        print("-" * 50)

        iteration = 0

        try:
            while True:
                iteration += 1
                print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Iteration {iteration}")

                records = self.fetcher.fetch_prices()
                if records:
                    print(f"✓ Fetched {len(records)} crypto prices")
                    self.send_to_kafka(records)
                    print(f"✓ Sent to Kafka topic: {KAFKA_TOPIC}")

                    sample = records[0]
                    print(f"  Sample: {sample['symbol']} = ${sample['price']:.2f} ({sample['price_change_percentage_24h']:.2f}%)")
                else:
                    print("✗ No data fetched")

                time.sleep(FETCH_INTERVAL)

        except KeyboardInterrupt:
            print("\n\nShutting down producer...")
            self.producer.close()
            print("✓ Producer closed")

if __name__ == "__main__":
    CryptoProducer().run()