# producer/crypto_fetcher.py
import requests
import time
from datetime import datetime
from typing import List, Dict
from config import COINGECKO_API_URL, COINGECKO_API_KEY

class CryptoFetcher:
    def __init__(self):
        self.api_url = COINGECKO_API_URL
        self.headers = {
            "x-cg-demo-api-key": COINGECKO_API_KEY,
            "accept": "application/json"
        }
           
    def fetch_prices(self) -> List[Dict]:
        try:
            url = f"{self.api_url}/coins/markets"
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': 50,
                'page': 1,
                'sparkline': 'false',
                'locale': 'en'
            }
           
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            records = []
            
            # Timestamp hiện tại của lần fetch này
            current_ts = datetime.now().isoformat()
            current_unixtime = int(time.time())
           
            for item in data:
                record = {
                    'id': item.get('id'),
                    'symbol': item.get('symbol', '').upper(),
                    'name': item.get('name'),
                    'image': item.get('image'),
                    'price': item.get('current_price', 0),
                    'market_cap': item.get('market_cap', 0),
                    'volume_24h': item.get('total_volume', 0),
                    'high_24h': item.get('high_24h', 0),
                    'low_24h': item.get('low_24h', 0),
                    'ath': item.get('ath', 0),
                    'atl': item.get('atl', 0),
                    'price_change_24h': item.get('price_change_24h', 0),
                    'price_change_percentage_24h': item.get('price_change_percentage_24h', 0),
                    'last_updated': current_unixtime,
                    'ingestion_timestamp': current_ts
                }
                records.append(record)
            
            return records
                   
        except Exception as e:
            print(f"Error fetching data: {e}")
            return []