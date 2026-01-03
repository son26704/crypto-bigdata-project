# producer/crypto_fetcher.py
import requests
import time
from datetime import datetime
from typing import List, Dict
from config import COINGECKO_API_URL, COINGECKO_API_KEY

class CryptoFetcher:
    def __init__(self):
        self.api_url = COINGECKO_API_URL.rstrip("/")
        self.headers = {
            "x-cg-demo-api-key": COINGECKO_API_KEY,
            "accept": "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def fetch_prices(self) -> List[Dict]:
        """
        Fetch top 50 coins by market cap from CoinGecko.
        Returns List[Dict], each Dict is one coin snapshot.
        """
        url = f"{self.api_url}/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 50,
            "page": 1,
            "sparkline": "false",
            "locale": "en"
        }

        # One timestamp per fetch (snapshot semantics)
        ingestion_ts = datetime.now().isoformat()
        fallback_unixtime = int(time.time())

        max_retries = 3
        backoff_seconds = 2

        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, params=params, timeout=10)

                # Handle rate limit explicitly
                if resp.status_code == 429:
                    sleep_s = backoff_seconds * (2 ** attempt)
                    print(f"Rate limited (429). Backing off {sleep_s}s...")
                    time.sleep(sleep_s)
                    continue

                resp.raise_for_status()
                data = resp.json()

                records: List[Dict] = []
                for item in data:
                    # Prefer source last_updated if present, else fallback to ingestion time
                    # CoinGecko often returns ISO string for last_updated
                    source_last_updated = item.get("last_updated")
                    if isinstance(source_last_updated, str) and source_last_updated:
                        # Keep it as ISO string? Your Spark schema expects LongType for last_updated,
                        # so we keep backward compatibility by using fallback unix seconds.
                        # If you later want true source time, add another field instead.
                        last_updated = fallback_unixtime
                    else:
                        last_updated = fallback_unixtime

                    record = {
                        "id": item.get("id"),
                        "symbol": (item.get("symbol") or "").upper(),
                        "name": item.get("name"),
                        "image": item.get("image"),
                        "price": float(item.get("current_price") or 0),
                        "market_cap": float(item.get("market_cap") or 0),
                        "volume_24h": float(item.get("total_volume") or 0),
                        "high_24h": float(item.get("high_24h") or 0),
                        "low_24h": float(item.get("low_24h") or 0),
                        "ath": float(item.get("ath") or 0),
                        "atl": float(item.get("atl") or 0),
                        "price_change_24h": float(item.get("price_change_24h") or 0),
                        "price_change_percentage_24h": float(item.get("price_change_percentage_24h") or 0),
                        "last_updated": int(last_updated),
                        "ingestion_timestamp": ingestion_ts,
                    }
                    records.append(record)

                return records

            except requests.RequestException as e:
                sleep_s = backoff_seconds * (2 ** attempt)
                print(f"Error fetching data (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(sleep_s)
                else:
                    return []
            except Exception as e:
                print(f"Unexpected error fetching data: {e}")
                return []

        return []