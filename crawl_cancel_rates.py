#!/usr/bin/env python3
"""
crawl_cancel_rates.py
爬取 Aerodatabox FIDS API — 指定日期、指定機場的航班取消率與取消班次
輸出：output/cancel_rates_YYYYMMDD_HHMMSS.json

目標機場：DXB, DOH, AUH, JED, RUH
目標日期：2026-03-09
"""

import os, json, time, datetime
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY  = os.getenv("AERODATABOX_RAPIDAPI_KEY") or os.getenv("AERODATABOX_API_KEY")
HOST     = "aerodatabox.p.rapidapi.com"
AIRPORTS = ["DXB", "DOH", "AUH", "JED", "RUH"]
DATES    = ["2026-03-09"]

# Aerodatabox 每個時間段最多 12 小時，用兩個時段覆蓋全天
# 時段A: 00:00–11:59  時段B: 12:00–23:59
SLOTS = [
    ("00:00", "11:59"),
    ("12:00", "23:59"),
]

HEADERS = {
    "x-rapidapi-key":  API_KEY,
    "x-rapidapi-host": HOST,
}

RETRIES = 5
BACKOFF_BASE = 1.0  # seconds


def fetch_slot(iata: str, date: str, from_t: str, to_t: str) -> dict:
    """抓取單個機場、單個時間段的 FIDS 數據（包含重試與退避）。

    对 429 Too Many Requests、5xx 和网络错误进行重试。
    """
    url = f"https://{HOST}/flights/airports/iata/{iata}/{date}T{from_t}/{date}T{to_t}"
    params = {
        "withLeg": "false",
        "direction": "Both",       # 出發+到達
        "withCancelled": "true",
        "withCodeshared": "false",
        "withCargo": "false",
        "withPrivate": "false",
        "withLocation": "false",
    }

    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
            # 如果是速率限制或服务器错误，尝试重试
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                raise requests.HTTPError(f"{resp.status_code} {resp.reason}")
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            # 对 4xx 中非 429 的错误不重试
            status = getattr(e.response, 'status_code', None) if isinstance(e, requests.HTTPError) and hasattr(e, 'response') else None
            if status is not None and status != 429 and not (500 <= status < 600):
                print(f"  !! 非重试性 HTTP 錯誤 ({status})：{e}")
                raise

            if attempt > RETRIES:
                print(f"  !! 超過重試次數，最後錯誤：{e}")
                raise

            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            jitter = min(1.0, wait * 0.1)
            wait += (jitter * (0.5 - (time.time() % 1)))
            print(f"  >> HTTP {getattr(e, 'response', '')} 於 {iata} {date} {from_t}-{to_t}，第 {attempt} 次重試，等待 {wait:.1f}s ...")
            time.sleep(wait)
        except Exception as e:
            if attempt > RETRIES:
                print(f"  !! 超過重試次數，最後錯誤：{e}")
                raise
            wait = BACKOFF_BASE * (2 ** (attempt - 1))
            print(f"  >> 網絡錯誤於 {iata} {date} {from_t}-{to_t}，第 {attempt} 次重試，等待 {wait:.1f}s ...")
            time.sleep(wait)


def count_flights(data: dict) -> tuple[int, int]:
    """從 FIDS 回應計算 (total, cancelled)"""
    total = cancelled = 0
    for direction in ("departures", "arrivals"):
        flights = data.get(direction) or []
        for f in flights:
            total += 1
            status = (f.get("status") or "").lower()
            if "cancel" in status:
                cancelled += 1
    return total, cancelled


def crawl():
    results = {}   # results[date][iata] = {total, cancelled, rate}

    for date in DATES:
        results[date] = {}
        for iata in AIRPORTS:
            day_total = day_cancelled = 0
            ok = True
            for from_t, to_t in SLOTS:
                try:
                    data = fetch_slot(iata, date, from_t, to_t)
                    t, c = count_flights(data)
                    day_total     += t
                    day_cancelled += c
                    print(f"  [{date}] {iata} {from_t}-{to_t}: total={t}, cancelled={c}")
                    time.sleep(0.6)   # 避免超過 Rate Limit
                except requests.HTTPError as e:
                    print(f"  !! [{date}] {iata} {from_t}-{to_t} HTTP Error: {e}")
                    ok = False
                    time.sleep(2)
                except Exception as e:
                    print(f"  !! [{date}] {iata} {from_t}-{to_t} Error: {e}")
                    ok = False
                    time.sleep(2)

            rate = round(day_cancelled / day_total * 100, 2) if day_total > 0 else None
            results[date][iata] = {
                "total":     day_total,
                "cancelled": day_cancelled,
                "rate_pct":  rate,
                "ok":        ok,
            }
            print(f"  ✓ [{date}] {iata}: total={day_total}, cancelled={day_cancelled}, rate={rate}%\n")

    # 輸出 JSON
    os.makedirs("output", exist_ok=True)
    ts  = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out = f"output/cancel_rates_{ts}.json"
    payload = {
        "crawl_time": datetime.datetime.now().isoformat(timespec="seconds"),
        "airports":   AIRPORTS,
        "dates":      DATES,
        "results":    results,
    }
    with open(out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"\n=== 完成！結果已儲存至 {out} ===")
    print_summary(results)
    return out


def print_summary(results: dict):
    """在終端列出摘要表格"""
    header = f"{'機場':>5} | " + " | ".join(f"{d[5:]}" for d in DATES)
    print("\n" + header)
    print("-" * len(header))
    for iata in AIRPORTS:
        row = f"{iata:>5} | "
        for date in DATES:
            r = results[date].get(iata, {})
            rate = r.get("rate_pct")
            row += f"{str(rate)+'%':>10} | " if rate is not None else f"{'N/A':>10} | "
        print(row)


if __name__ == "__main__":
    if not API_KEY:
        raise SystemExit("錯誤：找不到 AERODATABOX_RAPIDAPI_KEY，請確認 .env 文件")
    print(f"開始爬取...（{len(DATES)} 天 × {len(AIRPORTS)} 機場 × 2 時段）\n")
    crawl()
