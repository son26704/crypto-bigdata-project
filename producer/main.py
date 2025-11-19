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
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3,
                    # api_version=(2, 5, 0),
                    request_timeout_ms=30000,
                    max_in_flight_requests_per_connection=1
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
                # Add timestamp
                record['timestamp'] = datetime.now().isoformat()
                
                # Send to Kafka
                future = self.producer.send(KAFKA_TOPIC, value=record)
                
                # Wait for send to complete (optional, for debugging)
                # metadata = future.get(timeout=10)
                # print(f"Sent {record['symbol']}: ${record['price']:.2f}")
                
            except KafkaError as e:
                print(f"Error sending to Kafka: {e}")
            except Exception as e:
                print(f"Unexpected error: {e}")
        
        # Flush to ensure all messages are sent
        self.producer.flush()
    
    def run(self):
        """Main loop"""
        print(f"Starting Crypto Producer...")
        print(f"Tracking {len(CryptoFetcher().fetch_prices())} cryptocurrencies")
        print(f"Fetch interval: {FETCH_INTERVAL} seconds")
        print(f"Kafka topic: {KAFKA_TOPIC}")
        print("-" * 50)
        
        iteration = 0
        
        try:
            while True:
                iteration += 1
                print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Iteration {iteration}")
                
                # Fetch data
                records = self.fetcher.fetch_prices()
                
                if records:
                    print(f"✓ Fetched {len(records)} crypto prices")
                    
                    # Send to Kafka
                    self.send_to_kafka(records)
                    print(f"✓ Sent to Kafka topic: {KAFKA_TOPIC}")
                    
                    # Print sample
                    sample = records[0]
                    print(f"  Sample: {sample['symbol']} = ${sample['price']:.2f} ({sample['price_change_24h']:.2f}%)")
                else:
                    print("✗ No data fetched")
                
                # Wait before next fetch
                time.sleep(FETCH_INTERVAL)
                
        except KeyboardInterrupt:
            print("\n\nShutting down producer...")
            self.producer.close()
            print("✓ Producer closed")

if __name__ == "__main__":
    producer = CryptoProducer()
    producer.run()