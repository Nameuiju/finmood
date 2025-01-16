import pandas as pd
import time
from pytrends.request import TrendReq
import requests
import warnings
import datetime

# FutureWarning 억제
warnings.simplefilter(action='ignore', category=FutureWarning)

def fetch_top_coins_with_symbols(limit=20):
    """
    CoinGecko API에서 시가총액 기준 상위 코인 데이터를 가져옵니다.
    Parameters:
        limit (int): 가져올 상위 코인의 개수 (기본값: 20).
    Returns:
        pd.DataFrame: 코인 이름과 약자(Symbol) 정보가 포함된 데이터프레임.
    """
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

    # 이름(Name)과 약자(Symbol)만 반환
    return pd.DataFrame({
        'Name': [coin['name'] for coin in data],
        'Symbol': [coin['symbol'].upper() for coin in data]
    })

def fetch_google_trends_with_progress(keywords, timeframe, batch_size=2, delay=60):
    """
    Google Trends에서 키워드 검색 데이터를 가져옵니다.
    진행 상황과 예상 완료 시간을 콘솔에 출력합니다.
    Parameters:
        keywords (list): 검색할 키워드 리스트.
        timeframe (str): 'now 1-d', 'now 7-d', 'today 1-m' 등 기간 지정.
        batch_size (int): 요청당 처리할 키워드 수 (기본값: 2).
        delay (int): 요청 간 딜레이 시간 (초).
    Returns:
        pd.DataFrame: 키워드별 검색량 데이터프레임.
    """
    pytrends = TrendReq()
    trends_data = []
    total_batches = (len(keywords) + batch_size - 1) // batch_size  # 총 배치 수
    start_time = datetime.datetime.now()  # 시작 시간 기록

    for i, batch_start in enumerate(range(0, len(keywords), batch_size), 1):
        batch = keywords[batch_start:batch_start + batch_size]
        try:
            print(f"Processing batch {i}/{total_batches}: {batch}")
            pytrends.build_payload(batch, timeframe=timeframe, geo='US')
            data = pytrends.interest_over_time()

            if not data.empty:
                batch_data = data[batch].sum().reset_index()
                batch_data.columns = ['Name', 'Search Volume']  # Name 컬럼으로 변경
                trends_data.append(batch_data)
        except Exception as e:
            print(f"Error processing batch {batch}: {e}")

        # 진행 상황 업데이트
        elapsed_time = datetime.datetime.now() - start_time
        remaining_batches = total_batches - i
        eta = elapsed_time / i * remaining_batches  # 예상 완료 시간 계산
        print(f"Elapsed time: {elapsed_time}, ETA: {eta}")

        # 딜레이 추가
        time.sleep(delay)

    if trends_data:
        return pd.concat(trends_data).sort_values(by='Search Volume', ascending=False).reset_index(drop=True)
    else:
        return pd.DataFrame(columns=['Name', 'Search Volume'])

if __name__ == "__main__":
    start_time = datetime.datetime.now()  # 시작 시간 기록

    # 상위 20개 코인 이름과 약자 가져오기
    print("Fetching top 20 cryptocurrencies by market cap...")
    top_coins_df = fetch_top_coins_with_symbols(limit=20)
    top_names = top_coins_df['Name'].tolist()  # 코인 이름 리스트 추출

    if top_names:
        # 각각의 기간별 데이터 수집
        print("\nFetching 1-day Google Trends data...")
        trends_1day = fetch_google_trends_with_progress(top_names, 'now 1-d', batch_size=2, delay=60)
        print("\nFetching 7-day Google Trends data...")
        trends_7days = fetch_google_trends_with_progress(top_names, 'now 7-d', batch_size=2, delay=60)
        print("\nFetching 30-day Google Trends data...")
        trends_30days = fetch_google_trends_with_progress(top_names, 'today 1-m', batch_size=2, delay=60)

        # 각각의 데이터프레임 정리
        if not trends_1day.empty:
            trends_1day = trends_1day.merge(top_coins_df, on='Name')  # Name 컬럼으로 병합
            trends_1day = trends_1day[['Name', 'Symbol', 'Search Volume']].head(10)
            trends_1day.rename(columns={'Search Volume': 'Search Volume (1d)'}, inplace=True)

        if not trends_7days.empty:
            trends_7days = trends_7days.merge(top_coins_df, on='Name')  # Name 컬럼으로 병합
            trends_7days = trends_7days[['Name', 'Symbol', 'Search Volume']].head(10)
            trends_7days.rename(columns={'Search Volume': 'Search Volume (7d)'}, inplace=True)

        if not trends_30days.empty:
            trends_30days = trends_30days.merge(top_coins_df, on='Name')  # Name 컬럼으로 병합
            trends_30days = trends_30days[['Name', 'Symbol', 'Search Volume']].head(10)
            trends_30days.rename(columns={'Search Volume': 'Search Volume (30d)'}, inplace=True)

        # 각각 결과 출력
        print("\nTop 10 Search Volume Rankings (1-day):")
        print(trends_1day)

        print("\nTop 10 Search Volume Rankings (7-day):")
        print(trends_7days)

        print("\nTop 10 Search Volume Rankings (30-day):")
        print(trends_30days)

        # 결과 저장
        trends_1day.to_csv("trends_1day_top10.csv", index=False)
        trends_7days.to_csv("trends_7days_top10.csv", index=False)
        trends_30days.to_csv("trends_30days_top10.csv", index=False)
        print("\nResults saved to 'trends_1day_top10.csv', 'trends_7days_top10.csv', and 'trends_30days_top10.csv'")

    end_time = datetime.datetime.now()  # 종료 시간 기록
    elapsed_time = end_time - start_time  # 소요 시간 계산
    print(f"\nTotal elapsed time: {elapsed_time}")
