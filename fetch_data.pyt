import pandas as pd
import time
from pytrends.request import TrendReq
import requests
import warnings
import datetime

# FutureWarning 억제
warnings.simplefilter(action='ignore', category=FutureWarning)

def fetch_top_coins_with_symbols(limit=20):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': limit,
        'page': 1
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    return pd.DataFrame({
        'Name': [coin['name'] for coin in data],
        'Symbol': [coin['symbol'].upper() for coin in data]
    })

def fetch_google_trends_with_progress(keywords, timeframe, batch_size=2, delay=60):
    pytrends = TrendReq()
    trends_data = []
    total_batches = (len(keywords) + batch_size - 1) // batch_size
    start_time = datetime.datetime.now()

    for i, batch_start in enumerate(range(0, len(keywords), batch_size), 1):
        batch = keywords[batch_start:batch_start + batch_size]
        try:
            print(f"Processing batch {i}/{total_batches}: {batch}")
            pytrends.build_payload(batch, timeframe=timeframe, geo='US')
            data = pytrends.interest_over_time()

            if not data.empty:
                batch_data = data[batch].sum().reset_index()
                batch_data.columns = ['Name', 'Search Volume']
                trends_data.append(batch_data)
        except Exception as e:
            print(f"Error processing batch {batch}: {e}")

        elapsed_time = datetime.datetime.now() - start_time
        remaining_batches = total_batches - i
        eta = elapsed_time / i * remaining_batches
        print(f"Elapsed time: {elapsed_time}, ETA: {eta}")

        time.sleep(delay)

    if trends_data:
        return pd.concat(trends_data).sort_values(by='Search Volume', ascending=False).reset_index(drop=True)
    else:
        return pd.DataFrame(columns=['Name', 'Search Volume'])

if __name__ == "__main__":
    start_time = datetime.datetime.now()

    print("Fetching top 20 cryptocurrencies by market cap...")
    top_coins_df = fetch_top_coins_with_symbols(limit=20)
    top_names = top_coins_df['Name'].tolist()

    # 제외 조건 추가
    exclusion_terms = {
        "BNB": "-bed -breakfast -Airbnb",
        "USDC": "-stablecoin -usd coin",
    }
    filtered_keywords = [
        f"{name} {exclusion_terms.get(name, '')}".strip()
        for name in top_names
    ]

    if filtered_keywords:
        print("\nFetching 1-day Google Trends data...")
        trends_1day = fetch_google_trends_with_progress(filtered_keywords, 'now 1-d', batch_size=2, delay=60)
        print("\nFetching 7-day Google Trends data...")
        trends_7days = fetch_google_trends_with_progress(filtered_keywords, 'now 7-d', batch_size=2, delay=60)
        print("\nFetching 30-day Google Trends data...")
        trends_30days = fetch_google_trends_with_progress(filtered_keywords, 'today 1-m', batch_size=2, delay=60)

        if not trends_1day.empty:
            trends_1day = trends_1day.merge(top_coins_df, on='Name')
            trends_1day = trends_1day[['Name', 'Symbol', 'Search Volume']].head(10)
            trends_1day.rename(columns={'Search Volume': 'Search Volume (1d)'}, inplace=True)

        if not trends_7days.empty:
            trends_7days = trends_7days.merge(top_coins_df, on='Name')
            trends_7days = trends_7days[['Name', 'Symbol', 'Search Volume']].head(10)
            trends_7days.rename(columns={'Search Volume': 'Search Volume (7d)'}, inplace=True)

        if not trends_30days.empty:
            trends_30days = trends_30days.merge(top_coins_df, on='Name')
            trends_30days = trends_30days[['Name', 'Symbol', 'Search Volume']].head(10)
            trends_30days.rename(columns={'Search Volume': 'Search Volume (30d)'}, inplace=True)

        print("\nTop 10 Search Volume Rankings (1-day):")
        print(trends_1day)

        print("\nTop 10 Search Volume Rankings (7-day):")
        print(trends_7days)

        print("\nTop 10 Search Volume Rankings (30-day):")
        print(trends_30days)

        trends_1day.to_csv("trends_1day_top10.csv", index=False)
        trends_7days.to_csv("trends_7days_top10.csv", index=False)
        trends_30days.to_csv("trends_30days_top10.csv", index=False)
        print("\nResults saved to CSV files.")

    end_time = datetime.datetime.now()
    elapsed_time = end_time - start_time
    print(f"\nTotal elapsed time: {elapsed_time}")
