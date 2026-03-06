import requests
from datetime import datetime, timedelta
import json
import os
from dotenv import load_dotenv

load_dotenv()

# API 端點與認證
url = "https://google-flights2.p.rapidapi.com/api/v1/getPriceGraph"
headers = {
    "x-rapidapi-key": os.getenv("AERODATABOX_API_KEY"),  # 自動讀取env裡的RapidAPI key
    "x-rapidapi-host": "google-flights2.p.rapidapi.com"
}

# 查詢參數
departure_id = "IKA"  # 德黑蘭國際機場
arrival_id = "HKG"    # 香港國際機場
today = datetime.now().strftime("%Y-%m-%d")
one_month_later = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")

querystring = {
    "departure_id": departure_id,
    "arrival_id": arrival_id,
    "outbound_date": today,
    "start_date": today,
    "end_date": one_month_later,
    "travel_class": "ECONOMY",
    "adults": "1",
    "currency": "HKD",  # 換成港元
    "country_code": "HK"
}

# 執行查詢並保存結果
try:
    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()
    data = response.json()
    # 自動命名檔案：futureticket_ika_hkg_YYYYMMDD.json
    fname = f"futureticket_ika_hkg_{today.replace('-', '')}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"查詢成功，結果已保存至 {fname}")
except Exception as e:
    print(f"查詢或保存失敗：{e}")