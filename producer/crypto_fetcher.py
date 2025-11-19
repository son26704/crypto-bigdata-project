import requests
import time
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
        """Fetch market data for top cryptocurrencies"""
        try:
            # Dùng endpoint markets để lấy nhiều thông tin hơn (Symbol, Image, Rank...)
            url = f"{self.api_url}/coins/markets"
            
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc', # Lấy theo vốn hóa giảm dần
                'per_page': 20,
                'page': 1,
                'sparkline': 'false',
                'locale': 'en'
            }
            
            # Thêm headers chứa API Key
            response = requests.get(url, params=params, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            records = []
            current_time = int(time.time())
            
            for item in data:
                record = {
                    'id': item.get('id'),
                    'symbol': item.get('symbol', '').upper(),  # Sẽ ra BTC, ETH chuẩn
                    'name': item.get('name'),
                    'price': item.get('current_price', 0),
                    'market_cap': item.get('market_cap', 0),
                    'volume_24h': item.get('total_volume', 0),
                    'price_change_24h': item.get('price_change_percentage_24h', 0),
                    'last_updated': current_time, # CoinGecko trả về string, ta dùng time server cho đồng bộ
                    'ingestion_timestamp': datetime.now().isoformat() # Thời gian nạp vào hệ thống
                }
                records.append(record)
                        
            return records
                    
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return []
        except Exception as e:
            print(f"Unexpected error: {e}")
            return []

# Import datetime ở đầu file nếu chưa có
from datetime import datetime