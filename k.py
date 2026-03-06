import requests, json, time

API_KEY = "b73157f6b8mshce084ba8314c873p14806bjsn3c92c3bff6c9"
HOST    = "aerodatabox.p.rapidapi.com"
headers = {"x-rapidapi-key": API_KEY, "x-rapidapi-host": HOST}

# 查 DXB (OMDB) 昨天全天（拆两段，每段 ≤12h）
segments = [
    ("2026-03-04T00:00", "2026-03-04T12:00"),
    ("2026-03-04T12:00", "2026-03-04T23:59"),
]

dep_cancelled = arr_cancelled = dep_total = arr_total = 0

for date_from, date_to in segments:
    url = f"https://{HOST}/airports/icao/OMDB/delays/{date_from}/{date_to}"
    resp = requests.get(url, headers=headers)
    print(f"\n--- {date_from} ~ {date_to}  status={resp.status_code} ---")
    if resp.status_code == 200:
        items = resp.json()
        for item in items:
            dep = item.get("departuresDelayInformation", {})
            arr = item.get("arrivalsDelayInformation", {})
            dep_cancelled += dep.get("numCancelled", 0)
            arr_cancelled += arr.get("numCancelled", 0)
            dep_total     += dep.get("numTotal", 0)
            arr_total     += arr.get("numTotal", 0)
        print(json.dumps(items[:2], indent=2, ensure_ascii=False))  # 只印前2条
    else:
        print(resp.text)
    time.sleep(2)  # 避免触发速率限制

total = dep_cancelled + arr_cancelled
flights = dep_total + arr_total
print(f"\n=== DXB 2026-03-04 汇总 ===")
print(f"出发取消: {dep_cancelled}  到达取消: {arr_cancelled}  合计: {total}")
print(f"出发班次: {dep_total}  到达班次: {arr_total}  取消率: {round(total/flights*100,2) if flights else 0}%")
