import json
import requests
import boto3
import logging
import os
import time
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CoinGeckoFetcher:
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.s3_client = boto3.client('s3')
        logging.info("CoinGeckoFetcher initialized")

    @staticmethod
    def get_unix_timestamp(date):
        return int(datetime.timestamp(datetime.strptime(date, '%Y%m%d')))

    @staticmethod
    def save_to_local(data, file_name):
        export_path = os.path.join(os.path.dirname(__file__), '..', 'exports', file_name)
        with open(export_path, 'w') as f:
            json.dump(data, f, indent=2)
        logging.info(f"Data saved to local file {file_name}")

    @staticmethod
    def read_config():
        config_path = os.path.join(os.path.dirname(__file__), 'coin.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config.get('coin_id')

    def fetch_market_chart_range(self, coin_id, vs_currency, from_date, to_date):
        from_timestamp = self.get_unix_timestamp(from_date)
        to_timestamp = self.get_unix_timestamp(to_date)
        url = f"{self.base_url}/coins/{coin_id}/market_chart/range"
        params = {'vs_currency': vs_currency, 'from': from_timestamp, 'to': to_timestamp}

        retries = 5
        for i in range(retries):
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    wait_time = 2 ** i  # Exponential backoff
                    logging.warning(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise e
