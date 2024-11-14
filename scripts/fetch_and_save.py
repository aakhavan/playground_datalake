from coingecko.coingecko_fetcher import CoinGeckoFetcher
from datetime import datetime, timedelta


def fetch_data(start_date, end_date, vs_currency='eur'):
    fetcher = CoinGeckoFetcher()
    coins = fetcher.read_config()
    # print(coins)

    for coin_id in coins:

        historical_data = fetcher.fetch_and_save_daily_data(coin_id, vs_currency, start_date
                                                           , end_date)
        file_name = f"{coin_id}_{start_date}_{end_date}.json"
        fetcher.save_to_local(historical_data, file_name)


if __name__ == "__main__":
    start_date = '20240901'
    end_date = datetime.now().strftime('%Y%m%d')

    fetch_data(start_date=start_date, end_date=end_date)
