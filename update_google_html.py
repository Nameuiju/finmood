import pandas as pd

def generate_table_html(dataframe, search_volume_column):
    """
    데이터프레임을 HTML 테이블 형식으로 변환합니다.
    Parameters:
        dataframe (pd.DataFrame): 변환할 데이터프레임.
        search_volume_column (str): 검색량 열 이름.
    Returns:
        str: HTML 테이블 문자열.
    """
    table_html = ""
    base_url = "https://www.coingecko.com/en/coins/"  # CoinGecko 기본 URL

    for idx, row in dataframe.iterrows():
        # CoinGecko URL 형식 생성 (소문자 변환, 공백은 하이픈으로)
        coin_name_url = row['Name'].replace(" ", "-").lower()
        coingecko_url = f"{base_url}{coin_name_url}"
        
        # HTML 생성
        table_html += f"""
        <tr>
            <td>{idx + 1}</td>
            <td>
                <a href="{coingecko_url}" target="_blank" style="text-decoration: none; color: inherit;">
                    {row['Name']} ({row['Symbol']})
                </a>
            </td>
            <td>{row[search_volume_column]}</td>
        </tr>
        """
    return table_html

def update_google_html():
    try:
        # 1. 데이터 읽기
        trends_1day = pd.read_csv("trends_1day_top10.csv")
        trends_7days = pd.read_csv("trends_7days_top10.csv")
        trends_30days = pd.read_csv("trends_30days_top10.csv")

        # 2. HTML 테이블 생성
        table_1day = generate_table_html(trends_1day, "Search Volume (1d)")
        table_7days = generate_table_html(trends_7days, "Search Volume (7d)")
        table_30days = generate_table_html(trends_30days, "Search Volume (30d)")

        # 3. 기존 index.html 읽기
        with open("index.html", "r", encoding="utf-8") as file:
            html_content = file.read()

        # 4. 자리표시자를 HTML 테이블로 대체
        updated_html = html_content.replace("{1day_table}", table_1day)
        updated_html = updated_html.replace("{7days_table}", table_7days)
        updated_html = updated_html.replace("{30days_table}", table_30days)

        # 5. 업데이트된 HTML 파일 저장
        with open("index.html", "w", encoding="utf-8") as file:
            file.write(updated_html)

        print("index.html has been updated with the latest data!")

    except FileNotFoundError as e:
        print(f"Error: {e}. Ensure the required CSV files and HTML file are in the correct location.")
    except KeyError as e:
        print(f"Error: Missing column in CSV file: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    update_google_html()
