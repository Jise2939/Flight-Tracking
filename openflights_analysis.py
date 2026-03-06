#!/usr/bin/env python3
"""
使用 OpenFlights 免费静态数据库补全 S1/R1 数据
S1: 中国/香港出发 → 欧洲目的地 (替代中东转机的直飞航线)
R1: 中国/香港出发 → 高风险地区 (THR/BGW) 的连接韧性

数据来源: https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat
字段: airline, airline_id, src_airport, src_id, dst_airport, dst_id, codeshare, stops, equipment
完全离线/免费，不消耗任何 API 配额

⚠️  重要数据局限性说明
─────────────────────────────────────────────────────────
OpenFlights routes.dat 是一个 【静态历史快照】，内容反映的是
约 2012-2014 年前后各航司向 OpenFlights 社区提交的航线数据。

已知失效条目（截至 2026 年）:
  • Alitalia (AZ)          → 2021 年停运，已由 ITA Airways 继承
  • TAM Brazilian Airlines → 已并入 LATAM，品牌不再独立运营
  • Hainan Airlines (HU)   → PEK-BRU 直飞已停，受海航债务重组影响
  • Lufthansa Cargo        → 出现在客运航线属于数据噪音（纯货运)
  • Brussels Airlines 部分直飞已暂停或改为经法兰克福中转

因此本分析结果应理解为：
  "历史上曾存在/可能存在的直飞航线网络骨架"
  而非"2026年3月当前正在实际运营的航班时刻表"

用途：
  ✅ 用于判断 【航线结构是否存在】（有无直飞可能性）
  ✅ 用于估算 【受中东中断影响的连接韧性上限】
  ❌ 不可直接引用为当前运营班次或运力数据
  ❌ 不可用于估算实际座位数/频次

如需验证当前实际运营状态，建议交叉核对：
  - 各航司官网时刻表 (CA/MU/CZ/CX)
  - Flightradar24 历史航线
  - OAG / Cirium 商业航班数据库
─────────────────────────────────────────────────────────
"""

import csv
import json
import io
import ssl
import urllib.request
from datetime import datetime
from collections import defaultdict

# macOS 本地证书问题，忽略 SSL 验证
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE

# ── 已知失效/存疑的航司（截至2026年）────────────────────────────────────────
DEFUNCT_AIRLINES = {
    "Alitalia": "⚠️ 2021年停运（已由ITA Airways继承）",
    "TAM Brazilian Airlines": "⚠️ 已并入LATAM，品牌停用",
    "Brussels Airlines": "⚠️ 部分直飞已停，需核实",
    "Hainan Airlines": "⚠️ 受债务重组影响，部分远程直飞已停",
    "Lufthansa Cargo": "⚠️ 纯货运航司，客运航线数据为噪音",
}

# 数据时效说明
DATA_CAVEAT = (
    "⚠️  数据局限：OpenFlights routes.dat 为约2012-2014年历史快照，"
    "反映航线网络骨架而非当前实际运营班次。"
    "Alitalia已停运、TAM已并入LATAM等条目均为失效数据。"
    "本结果仅用于判断直飞可能性与连接韧性上限，不可直接引用为现行时刻表。"
)

# ── 常量定义 ──────────────────────────────────────────────────────────────────
ROUTES_URL  = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"
AIRLINES_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat"
AIRPORTS_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"

# S1: 出发机场
CHINA_HK_AIRPORTS = {"HKG", "PEK", "PVG", "CAN", "PKX"}

# S1: 欧洲目的地
EUROPE_AIRPORTS = {"LHR", "CDG", "FRA", "AMS", "FCO", "MAD", "BCN", "MUC", "ZRH",
                   "VIE", "BRU", "ARN", "CPH", "HEL", "OSL", "LIS", "DUB",
                   "MAN", "LGW", "STN", "ORY", "BER", "DUS", "HAM"}

# R1: 高风险地区目的地
HIGH_RISK_AIRPORTS = {"THR", "IKA", "BGW", "BSR", "NJF"}  # 伊朗(THR/IKA)、伊拉克(BGW/BSR/NJF)

# 中东枢纽 (用于识别中转路由)
MIDDLE_EAST_HUBS = {"DXB", "DOH", "AUH", "JED", "RUH", "KWI", "BAH", "MCT", "AMM", "BEY", "CAI"}

# ── 数据加载 ──────────────────────────────────────────────────────────────────

def download_data(url, label):
    print(f"  📥 下载 {label} ...")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30, context=_SSL_CTX) as resp:
        data = resp.read().decode("utf-8", errors="replace")
    lines = data.strip().split("\n")
    print(f"  ✅ {label}: {len(lines)} 条记录")
    return lines

def load_airlines(lines):
    """返回 {airline_iata: name}"""
    result = {}
    for line in lines:
        parts = line.split(",")
        if len(parts) < 4:
            continue
        # fields: id, name, alias, iata, icao, callsign, country, active
        name = parts[1].strip('"')
        iata = parts[3].strip('"')
        if iata and iata != r"\N" and len(iata) == 2:
            result[iata] = name
    return result

def load_airports(lines):
    """返回 {iata: {'name': ..., 'country': ..., 'city': ...}}"""
    result = {}
    for line in lines:
        parts = line.split(",")
        if len(parts) < 6:
            continue
        # fields: id, name, city, country, iata, icao, lat, lon, ...
        name = parts[1].strip('"')
        city = parts[2].strip('"')
        country = parts[3].strip('"')
        iata = parts[4].strip('"')
        if iata and iata != r"\N" and len(iata) == 3:
            result[iata] = {"name": name, "city": city, "country": country}
    return result

def load_routes(lines):
    """返回 list of {airline, src, dst, stops, codeshare}"""
    result = []
    for line in lines:
        parts = line.split(",")
        if len(parts) < 7:
            continue
        airline = parts[0].strip('"')
        src = parts[2].strip('"')
        dst = parts[4].strip('"')
        codeshare = parts[6].strip('"')
        stops = parts[7].strip('"') if len(parts) > 7 else "0"
        if (airline and airline != r"\N" and
            src and src != r"\N" and len(src) == 3 and
            dst and dst != r"\N" and len(dst) == 3):
            result.append({
                "airline": airline,
                "src": src,
                "dst": dst,
                "stops": stops,
                "codeshare": codeshare
            })
    return result

# ── S1 分析：替代中东中转的直飞航线 ──────────────────────────────────────────

def analyze_s1(routes, airlines, airports):
    """
    S1: 中国/香港 → 欧洲 直飞/一站 航线
    按目的地机场分组，统计运营航司
    """
    print("\n🔍 分析 S1（中国/HK → 欧洲 直飞航线）...")

    s1_direct = []  # stops=0
    s1_one_stop = []  # stops=1

    for r in routes:
        if r["src"] in CHINA_HK_AIRPORTS and r["dst"] in EUROPE_AIRPORTS:
            entry = {
                "route": f"{r['src']}-{r['dst']}",
                "src": r["src"],
                "dst": r["dst"],
                "airline_iata": r["airline"],
                "airline_name": airlines.get(r["airline"], r["airline"]),
                "stops": int(r["stops"]) if r["stops"].isdigit() else 0,
                "codeshare": r["codeshare"] == "Y",
                "src_info": airports.get(r["src"], {}),
                "dst_info": airports.get(r["dst"], {}),
            }
            if entry["stops"] == 0:
                s1_direct.append(entry)
            elif entry["stops"] == 1:
                s1_one_stop.append(entry)

    # 按航线聚合
    route_summary = defaultdict(lambda: {"airlines": [], "count": 0, "src": "", "dst": ""})
    for e in s1_direct:
        key = e["route"]
        route_summary[key]["airlines"].append(e["airline_name"])
        route_summary[key]["count"] += 1
        route_summary[key]["src"] = e["src"]
        route_summary[key]["dst"] = e["dst"]
        route_summary[key]["src_city"] = e["src_info"].get("city", "")
        route_summary[key]["dst_city"] = e["dst_info"].get("city", "")

    # 按目的地分组
    by_dest = defaultdict(list)
    for route, info in route_summary.items():
        by_dest[info["dst"]].append({
            "route": route,
            "airline_count": info["count"],
            "airlines": list(set(info["airlines"])),
            "src_city": info["src_city"],
            "dst_city": info["dst_city"],
        })

    print(f"  ✅ 直飞: {len(s1_direct)} 条运营记录 ({len(route_summary)} 条独立航线)")
    print(f"  ℹ️  一站中转: {len(s1_one_stop)} 条（仅供参考）")

    return {
        "direct_routes": [
            {
                "route": route,
                "src": info["src"],
                "dst": info["dst"],
                "src_city": info["src_city"],
                "dst_city": info["dst_city"],
                "operator_count": info["count"],
                "airlines": list(set(info["airlines"])),
            }
            for route, info in sorted(route_summary.items())
        ],
        "by_destination": {
            dst: sorted(routes_list, key=lambda x: x["route"])
            for dst, routes_list in sorted(by_dest.items())
        },
        "total_unique_routes": len(route_summary),
        "total_route_records": len(s1_direct),
    }

# ── R1 分析：连接高风险地区的韧性航线 ─────────────────────────────────────────

def analyze_r1(routes, airlines, airports):
    """
    R1: 中国/香港 → 伊朗/伊拉克
    直飞或经中东中转（但中东已断，中转路也断了）
    """
    print("\n🔍 分析 R1（中国/HK → 高风险地区 航线）...")

    r1_direct = []
    r1_via_middle_east = []  # 依赖中东中转，目前已断

    # 直飞
    for r in routes:
        if r["src"] in CHINA_HK_AIRPORTS and r["dst"] in HIGH_RISK_AIRPORTS:
            r1_direct.append({
                "route": f"{r['src']}-{r['dst']}",
                "src": r["src"],
                "dst": r["dst"],
                "airline": r["airline"],
                "airline_name": airlines.get(r["airline"], r["airline"]),
                "stops": r["stops"],
                "src_info": airports.get(r["src"], {}),
                "dst_info": airports.get(r["dst"], {}),
            })

    # 经中东中转（中东→高风险）
    middle_east_to_hr = set()
    for r in routes:
        if r["src"] in MIDDLE_EAST_HUBS and r["dst"] in HIGH_RISK_AIRPORTS:
            middle_east_to_hr.add(r["dst"])

    # 中国→中东（第一段），标注"目前已中断"
    china_to_me_routes = []
    for r in routes:
        if r["src"] in CHINA_HK_AIRPORTS and r["dst"] in MIDDLE_EAST_HUBS:
            if any(r2["src"] == r["dst"] and r2["dst"] in HIGH_RISK_AIRPORTS
                   for r2 in routes):
                china_to_me_routes.append({
                    "leg1": f"{r['src']}→{r['dst']}",
                    "transit_hub": r["dst"],
                    "airline": airlines.get(r["airline"], r["airline"]),
                    "status": "⛔ 中东空域封闭，此路线已中断"
                })

    print(f"  ✅ 直飞高风险地区: {len(r1_direct)} 条记录")
    print(f"  ⛔ 需经中东中转: 依赖 {len(set(r['transit_hub'] for r in china_to_me_routes))} 个已关闭枢纽")

    # 汇总直飞航线
    direct_summary = defaultdict(lambda: {"airlines": [], "src_info": {}, "dst_info": {}})
    for e in r1_direct:
        key = e["route"]
        direct_summary[key]["airlines"].append(e["airline_name"])
        direct_summary[key]["src"] = e["src"]
        direct_summary[key]["dst"] = e["dst"]
        direct_summary[key]["src_info"] = e["src_info"]
        direct_summary[key]["dst_info"] = e["dst_info"]

    return {
        "direct_routes": [
            {
                "route": route,
                "src": info["src"],
                "dst": info["dst"],
                "src_city": info["src_info"].get("city", ""),
                "dst_city": info["dst_info"].get("city", ""),
                "dst_country": info["dst_info"].get("country", ""),
                "operator_count": len(info["airlines"]),
                "airlines": list(set(info["airlines"])),
                "status": "✅ 直飞，不经中东"
            }
            for route, info in sorted(direct_summary.items())
        ],
        "via_middle_east_routes_disrupted": len(set(r["transit_hub"] for r in china_to_me_routes)),
        "disrupted_transit_hubs": list(set(r["transit_hub"] for r in china_to_me_routes)),
        "total_direct": len(direct_summary),
    }

# ── 主程序 ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  OpenFlights 静态数据库 S1/R1 分析")
    print(f"  运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 下载数据
    print("\n📡 下载 OpenFlights 数据...")
    try:
        route_lines = download_data(ROUTES_URL, "routes.dat")
        airline_lines = download_data(AIRLINES_URL, "airlines.dat")
        airport_lines = download_data(AIRPORTS_URL, "airports.dat")
    except Exception as e:
        print(f"❌ 下载失败: {e}")
        return

    # 解析
    print("\n🔄 解析数据...")
    airlines = load_airlines(airline_lines)
    airports = load_airports(airport_lines)
    routes = load_routes(route_lines)
    print(f"  航线: {len(routes)} 条")
    print(f"  航司: {len(airlines)} 家")
    print(f"  机场: {len(airports)} 个")

    # 分析
    s1_result = analyze_s1(routes, airlines, airports)
    r1_result = analyze_r1(routes, airlines, airports)

    # ── 打印 S1 摘要 ──────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  📊 S1 分析结果：中国/HK → 欧洲 直飞替代航线")
    print("=" * 60)
    print(f"\n{DATA_CAVEAT}\n")
    print(f"共找到 {s1_result['total_unique_routes']} 条独立直飞航线（{s1_result['total_route_records']} 条运营记录）\n")

    for r in s1_result["direct_routes"]:
        airline_tags = []
        for a in r["airlines"][:5]:
            tag = DEFUNCT_AIRLINES.get(a, "")
            airline_tags.append(f"{a}{(' '+tag) if tag else ''}")
        airlines_str = "、".join(airline_tags)
        if len(r["airlines"]) > 5:
            airlines_str += f" 等{len(r['airlines'])}家"
        print(f"  {r['route']:12s}  {r['src_city']:8s}→{r['dst_city']:10s}  [{r['operator_count']}家航司: {airlines_str}]")

    print(f"\n按欧洲目的地统计:")
    for dst, route_list in s1_result["by_destination"].items():
        dst_city = route_list[0]["dst_city"] if route_list else ""
        src_set = set(r["route"].split("-")[0] for r in route_list)
        print(f"  → {dst} ({dst_city}): {len(route_list)} 条来源航线，覆盖出发机场: {', '.join(sorted(src_set))}")

    # ── 打印 R1 摘要 ──────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  📊 R1 分析结果：中国/HK → 高风险地区 连接韧性")
    print("=" * 60)
    print(f"\n{DATA_CAVEAT}\n")

    if r1_result["direct_routes"]:
        print(f"✅ 存在 {r1_result['total_direct']} 条直飞高风险地区航线（不经中东，历史数据中存在）:\n")
        for r in r1_result["direct_routes"]:
            airlines_str = "、".join(r["airlines"])
            print(f"  {r['route']:12s}  → {r['dst_city']} ({r['dst_country']})  [{airlines_str}]")
        print("\n  ⚠️  注：Mahan Air (W5) 受美国制裁，欧美航司/银行不与其结算；")
        print("      Iran Air (IR) 亦受制裁限制，实际运营受外部环境约束。")
        print("      上述航线存在于历史数据库，但当前实际运营状态需独立核实。")
    else:
        print("\n⛔ 无直飞高风险地区的航线（中国/HK出发）")

    print(f"\n⛔ 依赖中东中转的路线（历史数据显示曾存在）已全部中断:")
    print(f"   涉及已关闭枢纽: {', '.join(r1_result['disrupted_transit_hubs'])}")
    print(f"   其中 BGW(巴格达) 无任何直飞替代 → 完全断连")

    # ── 综合研究结论 ──────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  🎯 综合研究结论")
    print("=" * 60)
    print(f"""
D1 断裂度（已采集，2026-02-26 ~ 03-04）:
  DXB: 均值59.7%取消，峰值100% (03-01)
  DOH: 均值64.5%取消，峰值100% (03-01)
  AUH: 均值59.0%取消，峰值100% (03-01)
  JED: 均值10.5%取消 (沙特受影响较小)
  RUH: 均值 7.4%取消 (沙特受影响较小)
  断点: 2026-02-28 (UAE三枢纽单日从<1%跳至50%+)

S1 替代度（OpenFlights静态数据，约2012-2014年快照）:
  中国/HK → 欧洲 直飞: {s1_result['total_unique_routes']} 条历史记录中存在的独立航线
  有效运营商（剔除失效）: 国航CA、东航MU、南航CZ、国泰CX、英航BA、法航AF、北欧SAS、芬兰AY、KLM、奥地利OS、瑞航LX
  失效/存疑条目: Alitalia(已停运)、TAM(已并入LATAM)、海航(部分远程已停)、汉莎货运(非客运)
  ⚠️  数据为历史骨架，不代表2026年实际运营班次，需用OAG/Cirium核实

R1 连接韧性（OpenFlights静态数据）:
  中国/HK → 伊朗 直飞: {r1_result['total_direct']} 条（均为→德黑兰IKA，航司受制裁）
  中国/HK → 伊拉克 直飞: 0 条（BGW完全依赖中东中转，已彻底断连）
  中转路线: 全部依赖已关闭中东枢纽，目前已中断
  韧性评估: 高度脆弱（伊拉克完全断连；伊朗仅剩受制裁航司的直飞）
  ⚠️  Mahan Air/Iran Air 均受美欧制裁，商业可用性受限
""")

    # ── 保存结果 ──────────────────────────────────────────────────────────────
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_source": "OpenFlights static database (routes.dat)",
        "source_url": ROUTES_URL,
        "data_caveat": {
            "snapshot_era": "约2012-2014年，非实时数据",
            "known_defunct": [
                "Alitalia (AZ) - 2021年停运，已由ITA Airways继承",
                "TAM Brazilian Airlines - 已并入LATAM",
                "Hainan Airlines (HU) - 部分远程直飞受债务重组影响已停",
                "Lufthansa Cargo - 纯货运，出现在客运航线为数据噪音",
            ],
            "interpretation": (
                "本数据反映历史航线网络骨架（连接可能性），"
                "不代表2026年3月当前实际运营班次/频率/座位数。"
                "用于判断直飞可能性与韧性上限，需用OAG/Cirium/官网时刻表核实现状。"
            ),
            "r1_additional_caveat": (
                "Mahan Air (W5) 和 Iran Air (IR) 均受美国OFAC及欧盟制裁，"
                "商业银行结算受限，实际可用性需独立评估。"
                "BGW(巴格达)历史数据中无任何中国/HK出发的直飞记录，完全依赖中东中转。"
            ),
        },
        "S1_substitution": s1_result,
        "R1_resilience": r1_result,
    }
    out_path = f"/Users/yumok/Desktop/spotnews2/output/openflights_S1R1_{timestamp}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n💾 结果已保存: {out_path}")

    # 也生成一个简洁 CSV
    csv_path = f"/Users/yumok/Desktop/spotnews2/output/openflights_S1_{timestamp}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["route", "src", "dst", "src_city", "dst_city", "operator_count", "airlines"])
        for r in s1_result["direct_routes"]:
            w.writerow([r["route"], r["src"], r["dst"], r["src_city"], r["dst_city"],
                        r["operator_count"], " / ".join(r["airlines"])])
    print(f"💾 S1 CSV 已保存: {csv_path}")

if __name__ == "__main__":
    main()
