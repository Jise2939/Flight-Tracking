"""
FR24 S1/R1 实时航线采集器
使用 flightradar243.p.rapidapi.com /v1/airports/arrivals 端点

策略（200次配额预算）：
  S1: 查10个欧洲枢纽入港 → 筛出发地=中国大陆/香港
  R1: 查3个高风险目标机场入港 → 筛出发地=中国大陆/香港
  合计主查询: ~13-20次（含翻页），剩余配额充裕

数据优势：实时/当前运营数据，远优于 OpenFlights 2012年快照
"""

import requests
import json
import time
import os
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()  # 自动读取 .env 文件

# ── 配置 ──────────────────────────────────────────────────────────────────────
RAPIDAPI_KEY = os.getenv("FR24_RAPIDAPI_KEY", "YOUR_KEY_HERE")
RAPIDAPI_HOST = "flightradar243.p.rapidapi.com"
BASE_URL = f"https://{RAPIDAPI_HOST}/v1/airports/arrivals"

HEADERS = {
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": RAPIDAPI_HOST,
}

# ── 目标机场定义 ───────────────────────────────────────────────────────────────

# S1: 欧洲主要枢纽（查这些机场的入港，找来自中国/HK的直飞）
S1_EUROPE_AIRPORTS = {
    "LHR": "伦敦希思罗",
    "CDG": "巴黎戴高乐",
    "FRA": "法兰克福",
    "AMS": "阿姆斯特丹",
    "MUC": "慕尼黑",
    "FCO": "罗马菲乌米奇诺",
    "MAD": "马德里巴拉哈斯",
    "ZRH": "苏黎世",
    "VIE": "维也纳",
    "BRU": "布鲁塞尔",
    "HEL": "赫尔辛基",  # 芬兰航空中欧直飞重要枢纽
    "ARN": "斯德哥尔摩阿兰达",
}

# R1: 高风险地区目标机场
R1_TARGET_AIRPORTS = {
    "IKA": "德黑兰伊玛目霍梅尼国际机场",
    "THR": "德黑兰梅赫拉巴德机场（国内为主）",
    "BGW": "巴格达国际机场",
}

# 中国大陆 + 香港机场 IATA 代码集合（出发地筛选用）
CHINA_HK_AIRPORTS = {
    # 中国大陆主要国际机场
    "PEK", "PKX",  # 北京首都/大兴
    "PVG", "SHA",  # 上海浦东/虹桥
    "CAN",         # 广州白云
    "SZX",         # 深圳宝安
    "CTU",         # 成都天府
    "KMG",         # 昆明长水
    "XIY",         # 西安咸阳
    "CKG",         # 重庆江北
    "WUH",         # 武汉天河
    "NKG",         # 南京禄口
    "HGH",         # 杭州萧山
    "CSX",         # 长沙黄花
    "XMN",         # 厦门高崎
    "TSN",         # 天津滨海
    "DLC",         # 大连周水子
    "TAO",         # 青岛胶东
    "URC",         # 乌鲁木齐地窝堡
    "HAK",         # 海口美兰
    "SYX",         # 三亚凤凰
    "TNA",         # 济南遥墙
    "CGO",         # 郑州新郑
    "HET",         # 呼和浩特
    "LHW",         # 兰州中川
    "NNG",         # 南宁吴圩
    "KWE",         # 贵阳龙洞堡
    # 香港
    "HKG",         # 香港国际机场
    # 澳门（补充）
    "MFM",         # 澳门国际机场
}

# ── 工具函数 ───────────────────────────────────────────────────────────────────

request_count = 0

def fetch_arrivals(airport_code: str, page: int = 1, limit: int = 100) -> dict | None:
    """调用FR24 arrivals API，返回JSON或None（失败时）"""
    global request_count
    params = {"code": airport_code, "limit": str(limit), "page": str(page)}
    try:
        resp = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=15)
        request_count += 1
        print(f"  [#{request_count}] GET arrivals/{airport_code} page={page} → HTTP {resp.status_code}")
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            print("  ⚠️  限流(429)，等待60秒...")
            time.sleep(60)
            return None
        elif resp.status_code == 403:
            print("  ❌ 403 未订阅或Key无效，终止")
            return None
        else:
            print(f"  ⚠️  HTTP {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        print(f"  ❌ 请求异常: {e}")
        return None


def parse_flights(data: dict) -> list[dict]:
    """
    从FR24 arrivals响应中提取航班列表
    兼容两种已知数据结构：
      1. data['data']['result']['response']['airport']['pluginData']['schedule']['arrivals']['data']
      2. data['data']['airport']['pluginData']['schedule']['arrivals']['data']
    """
    try:
        # 尝试路径1（带result层）
        arr = (data.get("data", {})
                   .get("result", {})
                   .get("response", {})
                   .get("airport", {})
                   .get("pluginData", {})
                   .get("schedule", {})
                   .get("arrivals", {}))
        if arr:
            return arr.get("data", []), arr.get("item", {}).get("total", 0)
    except Exception:
        pass
    try:
        # 尝试路径2（无result层）
        arr = (data.get("data", {})
                   .get("airport", {})
                   .get("pluginData", {})
                   .get("schedule", {})
                   .get("arrivals", {}))
        if arr:
            return arr.get("data", []), arr.get("item", {}).get("total", 0)
    except Exception:
        pass
    return [], 0


def extract_flight_info(flight_entry: dict) -> dict | None:
    """从单条航班entry提取关键字段"""
    try:
        f = flight_entry.get("flight", {})
        origin = f.get("airport", {}).get("origin", {})
        origin_iata = origin.get("code", {}).get("iata", "")
        origin_name = origin.get("name", "")
        airline = f.get("airline", {})
        airline_name = airline.get("name", "") or airline.get("short", "")
        airline_iata = airline.get("code", {}).get("iata", "")
        flight_num = f.get("identification", {}).get("number", {}).get("default", "")
        status_text = f.get("status", {}).get("text", "")
        sched_dep = f.get("time", {}).get("scheduled", {}).get("departure")
        sched_arr = f.get("time", {}).get("scheduled", {}).get("arrival")
        return {
            "flight": flight_num,
            "airline": airline_name,
            "airline_iata": airline_iata,
            "origin_iata": origin_iata,
            "origin_name": origin_name,
            "status": status_text,
            "sched_dep_ts": sched_dep,
            "sched_arr_ts": sched_arr,
        }
    except Exception:
        return None


def collect_airport(airport_code: str, max_pages: int = 3) -> list[dict]:
    """
    采集一个机场的所有入港航班（翻页），返回航班列表
    max_pages: 最多翻几页（每页100条），控制配额消耗
    """
    all_flights = []
    for page in range(1, max_pages + 1):
        data = fetch_arrivals(airport_code, page=page, limit=100)
        if data is None:
            break
        flights, total = parse_flights(data)
        if not flights:
            break
        all_flights.extend(flights)
        print(f"    已获取 {len(all_flights)}/{total} 条")
        # 如果已经获取完了，不用继续翻页
        if len(all_flights) >= total:
            break
        # 礼貌性延迟
        time.sleep(1.5)
    return all_flights


def filter_china_hk_origin(flights: list[dict]) -> list[dict]:
    """筛选出发地为中国大陆/香港的航班"""
    results = []
    for entry in flights:
        info = extract_flight_info(entry)
        if info and info["origin_iata"] in CHINA_HK_AIRPORTS:
            results.append(info)
    return results

# ── 主采集逻辑 ────────────────────────────────────────────────────────────────

def run_s1_collection():
    """S1：采集中国/HK → 欧洲直飞航线"""
    print("\n" + "="*60)
    print("S1 采集：中国/HK → 欧洲 直飞航班")
    print("="*60)
    s1_routes = defaultdict(list)  # key=(origin_iata, dest_iata), value=[航班信息]

    for iata, name in S1_EUROPE_AIRPORTS.items():
        print(f"\n▶ 查询 {iata} ({name}) 入港...")
        flights = collect_airport(iata, max_pages=3)
        matches = filter_china_hk_origin(flights)
        print(f"  → 找到来自中国/HK的直飞: {len(matches)} 班")
        for m in matches:
            key = (m["origin_iata"], iata)
            m["dest_iata"] = iata
            m["dest_name"] = name
            s1_routes[key].append(m)
        time.sleep(1)

    return s1_routes


def run_r1_collection():
    """R1：采集中国/HK → 伊朗/伊拉克直飞航班"""
    print("\n" + "="*60)
    print("R1 采集：中国/HK → 高风险地区 直飞航班")
    print("="*60)
    r1_routes = defaultdict(list)

    for iata, name in R1_TARGET_AIRPORTS.items():
        print(f"\n▶ 查询 {iata} ({name}) 入港...")
        flights = collect_airport(iata, max_pages=5)  # R1机场航班少，多翻几页
        matches = filter_china_hk_origin(flights)
        print(f"  → 找到来自中国/HK的直飞: {len(matches)} 班")
        for m in matches:
            key = (m["origin_iata"], iata)
            m["dest_iata"] = iata
            m["dest_name"] = name
            r1_routes[key].append(m)
        time.sleep(1)

    return r1_routes


def print_summary(s1_routes: dict, r1_routes: dict):
    """打印汇总结果"""
    print("\n" + "="*60)
    print("S1 结果汇总：中国/HK → 欧洲 确认直飞航线")
    print("="*60)
    if not s1_routes:
        print("  未发现直飞（采集窗口内无班次，或数据尚未就绪）")
    else:
        for (orig, dest), flights in sorted(s1_routes.items()):
            airlines = list({f["airline"] for f in flights if f["airline"]})
            print(f"  {orig} → {dest}  |  航司: {', '.join(airlines)}  |  本次采集班次数: {len(flights)}")

    print("\n" + "="*60)
    print("R1 结果汇总：中国/HK → 高风险地区 确认直飞航线")
    print("="*60)
    if not r1_routes:
        print("  ⚠️  BGW/IKA/THR 入港中未发现来自中国/HK的直飞")
        print("  → 可能原因：班次极少、采集时间窗口未覆盖、或真实无直飞")
    else:
        for (orig, dest), flights in sorted(r1_routes.items()):
            airlines = list({f["airline"] for f in flights if f["airline"]})
            print(f"  {orig} → {dest}  |  航司: {', '.join(airlines)}  |  本次采集班次数: {len(flights)}")
            if dest in ("IKA", "THR"):
                print(f"    ⚠️  注意：伊朗航线运营方可能受美国/欧盟制裁（Mahan Air/Iran Air）")
            if dest == "BGW":
                print(f"    ⚠️  注意：巴格达航线受伊拉克战乱影响，请核实当前通航状态")

    print(f"\n本次共消耗API请求: {request_count} / 200")


def save_results(s1_routes: dict, r1_routes: dict):
    """保存结果到JSON和CSV"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 转换defaultdict为普通list
    s1_list = [
        {
            "origin": orig, "destination": dest,
            "airlines": list({f["airline"] for f in flights if f["airline"]}),
            "flight_count_in_window": len(flights),
            "sample_flights": flights[:5],
        }
        for (orig, dest), flights in sorted(s1_routes.items())
    ]
    r1_list = [
        {
            "origin": orig, "destination": dest,
            "airlines": list({f["airline"] for f in flights if f["airline"]}),
            "flight_count_in_window": len(flights),
            "sanctions_note": "伊朗航线可能涉及受制裁航司" if dest in ("IKA", "THR") else None,
            "sample_flights": flights[:5],
        }
        for (orig, dest), flights in sorted(r1_routes.items())
    ]

    output = {
        "timestamp": datetime.now().isoformat(),
        "data_source": "FlightRadar24 via RapidAPI (flightradar243)",
        "data_type": "实时入港航班（当前时刻表窗口）",
        "requests_used": request_count,
        "s1_china_hk_to_europe": {
            "description": "中国大陆/香港 → 欧洲 直飞航班（实时采集）",
            "airports_queried": list(S1_EUROPE_AIRPORTS.keys()),
            "routes_found": len(s1_list),
            "routes": s1_list,
        },
        "r1_china_hk_to_high_risk": {
            "description": "中国大陆/香港 → 伊朗/伊拉克 直飞航班（实时采集）",
            "airports_queried": list(R1_TARGET_AIRPORTS.keys()),
            "routes_found": len(r1_list),
            "caveats": [
                "IKA/THR: Mahan Air、Iran Air受美国/欧盟制裁，部分国家禁止着陆",
                "BGW: 伊拉克政治局势复杂，航班实际运营情况需交叉核实",
            ],
            "routes": r1_list,
        },
    }

    json_path = f"output/fr24_s1r1_{ts}.json"
    os.makedirs("output", exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n✅ 结果已保存: {json_path}")

    # CSV（S1）
    csv_path = f"output/fr24_s1_{ts}.csv"
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("出发地IATA,目的地IATA,目的地名称,航司,采集窗口班次数\n")
        for row in s1_list:
            f.write(f"{row['origin']},{row['destination']},"
                    f"{S1_EUROPE_AIRPORTS.get(row['destination'], '')},")
            f.write(f"{'/'.join(row['airlines'])},{row['flight_count_in_window']}\n")
    print(f"✅ S1 CSV已保存: {csv_path}")


# ── 入口 ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"FR24 S1/R1 实时采集器 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"API Key: {RAPIDAPI_KEY[:8]}...（已隐藏）")
    print(f"配额预算: 200次请求\n")

    if RAPIDAPI_KEY == "YOUR_KEY_HERE":
        print("❌ 请先设置 FR24_RAPIDAPI_KEY 环境变量，或直接修改脚本中的 RAPIDAPI_KEY")
        print("   export FR24_RAPIDAPI_KEY='your_actual_key'")
        exit(1)

    # 先测试API连通性
    print("── 测试API连通性 ──")
    test = fetch_arrivals("LHR", page=1, limit=5)
    if test is None:
        print("❌ API连接失败，请检查Key和订阅状态")
        exit(1)
    print("✅ API连通正常，开始采集...\n")

    # 采集
    s1_routes = run_s1_collection()
    r1_routes = run_r1_collection()

    # 输出
    print_summary(s1_routes, r1_routes)
    save_results(s1_routes, r1_routes)
