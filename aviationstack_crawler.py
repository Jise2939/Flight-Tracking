#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AviationStack API 航线数据爬虫
自动获取航班取消/延误、受影响旅客、历史趋势、绕飞时间、运力对比等数据
"""

import os
import sys
import json
import csv
import time
import yaml
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests


class AviationStackAPIClient:
    """AviationStack API 客户端"""

    def __init__(self, api_key: str, api_base: str = "https://api.aviationstack.com/v1"):
        self.api_key = api_key
        self.api_base = api_base
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)

    def _make_request(self, endpoint: str, params: dict) -> dict:
        """发送API请求"""
        params['access_key'] = self.api_key
        url = f"{self.api_base}/{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # 检查错误响应
            if 'error' in data:
                error_msg = data['error'].get('message', 'Unknown error')
                error_code = data['error'].get('code', 'unknown')
                self.logger.error(f"API Error [{error_code}]: {error_msg}")
                raise Exception(f"API Error: {error_msg}")

            return data

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            raise

    def get_routes(self, **params) -> List[Dict]:
        """获取航线数据"""
        return self._make_request('routes', params).get('data', [])

    def get_flights(self, **params) -> List[Dict]:
        """获取航班数据（支持历史查询）"""
        return self._make_request('flights', params).get('data', [])


class AerodataboxClient:
    """Aerodatabox client — 通过 RapidAPI 端点查询机场延误/取消数据。
    端点: GET /airports/icao/{icaoCode}/delays/{dateFromLocal}/{dateToLocal}
    使用 ICAO 代码和时间范围（格式 YYYY-MM-DDTHH:MM）查询延误快照。
    """

    HOST = "aerodatabox.p.rapidapi.com"
    BASE = f"https://{HOST}"

    # 常用机场 IATA → ICAO 映射表
    IATA_TO_ICAO: Dict[str, str] = {
        "DXB": "OMDB",  # Dubai
        "DOH": "OTHH",  # Doha
        "AUH": "OMAA",  # Abu Dhabi
        "JED": "OEJN",  # Jeddah
        "RUH": "OERK",  # Riyadh
        "HKG": "VHHH",  # Hong Kong
        "PEK": "ZBAA",  # Beijing Capital
        "PKX": "ZBAD",  # Beijing Daxing
        "PVG": "ZSPD",  # Shanghai Pudong
        "SHA": "ZSSS",  # Shanghai Hongqiao
        "CAN": "ZGGG",  # Guangzhou
        "LHR": "EGLL",  # London Heathrow
        "CDG": "LFPG",  # Paris CDG
        "FRA": "EDDF",  # Frankfurt
        "AMS": "EHAM",  # Amsterdam
        "IST": "LTFM",  # Istanbul
        "TLV": "LLBG",  # Tel Aviv
        "THR": "OIIE",  # Tehran
    }

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)

    def iata_to_icao(self, iata: str) -> str:
        """将 IATA 代码转换为 ICAO 代码，未知时原样返回"""
        return self.IATA_TO_ICAO.get(iata.upper(), iata.upper())

    def get_airport_delays(self, icao: str, date_from: str, date_to: str,
                           max_retries: int = 4) -> dict:
        """查询某机场在指定时间范围内的延误/取消数据（统计滑动窗口，仅备用）。
        遇到 429 时自动指数退避重试（最多 max_retries 次）。
        """
        url = f"{self.BASE}/airports/icao/{icao}/delays/{date_from}/{date_to}"
        headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": self.HOST,
        }
        wait = 5
        for attempt in range(1, max_retries + 1):
            resp = self.session.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 204:
                return {}
            elif resp.status_code == 429:
                self.logger.warning(
                    f"429 Too Many Requests [{icao} {date_from}] — "
                    f"retry {attempt}/{max_retries} after {wait}s"
                )
                time.sleep(wait)
                wait *= 2
            else:
                resp.raise_for_status()
        raise Exception(f"Aerodatabox 429 max retries exceeded [{icao} {date_from}~{date_to}]")

    def get_airport_fids(self, icao: str, date_from: str, date_to: str,
                         max_retries: int = 2) -> dict:
        """用 FIDS 端点查询机场航班列表（返回实际航班，可统计取消数）。
        端点: GET /flights/airports/icao/{icao}/{fromLocal}/{toLocal}
        date_from/date_to 格式: YYYY-MM-DDTHH:mm（本地时间，最多12小时间隔）
        返回: {"departures": [...], "arrivals": [...]}，每条航班含 status 字段
        status 枚举: Unknown/Expected/EnRoute/CheckIn/Boarding/GateClosed/
                     Departed/Delayed/Approaching/Arrived/Canceled/Diverted/CanceledUncertain

        ⚠️ 配额极其有限：遇到 429 最多重试 2 次（各等 30s），
           超过则抛出异常终止整个爬虫，防止白白消耗重试次数。
        """
        url = f"{self.BASE}/flights/airports/icao/{icao}/{date_from}/{date_to}"
        headers = {
            "x-rapidapi-key": self.api_key,
            "x-rapidapi-host": self.HOST,
        }
        params = {
            "withCancelled": "true",    # 包含取消航班
            "withCodeshared": "true",   # 包含代码共享（代码内用 IsOperator 过滤去重）
            "withCargo": "false",       # 仅统计客运航班
            "withPrivate": "false",
            "direction": "Departure",   # 只查出发，避免同一航班被到达端重复计算
        }
        wait = 30  # 429 等待：直接等 30s，不用指数退避（配额有限，不值得多试）
        for attempt in range(1, max_retries + 1):
            resp = self.session.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 204:
                return {}
            elif resp.status_code == 429:
                if attempt == max_retries:
                    # 最后一次重试仍 429 → 立刻终止，不浪费更多配额
                    raise Exception(
                        f"⛔ 429 配额耗尽，已终止爬虫 [{icao} {date_from}]"
                    )
                self.logger.warning(
                    f"429 FIDS [{icao} {date_from}] — "
                    f"retry {attempt}/{max_retries} after {wait}s"
                )
                time.sleep(wait)
            else:
                self.logger.error(f"FIDS error {resp.status_code}: {resp.text[:200]}")
                resp.raise_for_status()
        raise Exception(f"Aerodatabox FIDS 429 max retries exceeded [{icao} {date_from}~{date_to}]")


class FlightDataCrawler:
    """航班数据爬虫"""

    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self.api_key = self._load_api_key()
        self.client = AviationStackAPIClient(self.api_key)
        # Aerodatabox client 初始化（prod.api.market 端点）
        try:
            self.aerodatabox_key = self._load_aerodatabox_key()
            self.aerodatabox_client = AerodataboxClient(self.aerodatabox_key)
        except Exception:
            self.aerodatabox_client = None

        self.results = []
        self.cancellation_stats = []
        self.route_comparison = []
        self.capacity_data = []
        self.historical_data = []
        self.route_substitution = []   # S1: 替代度航线
        self.route_resilience = []     # R1: 连接韧性航线
        self.aerodatabox_delays = []   # D1: 中东枢纽断裂度
        self._setup_logging()
        self._setup_output_dir()

        # 机型容量数据（座位数）
        self.aircraft_capacity = {
            # 宽体机
            'A380-800': 525,
            'B747-8': 467,
            'B747-400': 416,
            'A350-900': 325,
            'A350-1000': 369,
            'B777-300ER': 365,
            'B777-200LR': 301,
            'B787-9': 296,
            'B787-8': 242,
            'A330-300': 277,
            'A330-200': 246,
            # 窄体机
            'A321neo': 220,
            'A321-200': 185,
            'B737-MAX8': 178,
            'B737-800': 160,
            'A320-200': 150,
            'A320neo': 180,
            'B737-900': 175,
            'B737-700': 128,
            'A319-100': 125,
            # 支线机
            'E195-E2': 132,
            'E190-E2': 114,
            'A220-300': 145,
            'A220-100': 125,
            'B757-300': 243,
            'B757-200': 200,
        }
        # Aerodatabox 延误查询结果容器
        self.aerodatabox_delays = []

    def _load_api_key(self) -> str:
        """从环境变量加载API Key"""
        api_key = os.getenv('AVIATIONSTACK_API_KEY')
        if not api_key:
            raise ValueError(
                "AVIATIONSTACK_API_KEY not found in environment variables. "
                "Please set it in .env file or export it."
            )
        return api_key

    def _load_aerodatabox_key(self) -> str:
        """从环境变量或配置加载 Aerodatabox (RapidAPI) Key"""
        # 优先从环境变量读取，环境变量名可通过 config 指定
        env_name = self.config.get('aerodatabox', {}).get('rapidapi_key_env', 'AERODATABOX_RAPIDAPI_KEY')
        key = os.getenv(env_name) or self.config.get('aerodatabox', {}).get('rapidapi_key')
        if not key:
            raise ValueError(
                f"{env_name} not found. Set it as environment variable or add 'aerodatabox.rapidapi_key' to config.yaml"
            )
        return key

    def _load_config(self, config_path: str) -> dict:
        """加载配置文件"""
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_file, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def _setup_logging(self):
        """设置日志"""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        log_file = log_config.get('file', 'logs/crawler.log')

        Path(log_file).parent.mkdir(exist_ok=True)

        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _setup_output_dir(self):
        """创建输出目录"""
        output_dir = Path(self.config['output']['directory'])
        output_dir.mkdir(exist_ok=True)

    def _normalize_route_data(self, route: Dict) -> Dict:
        """标准化航线数据"""
        return {
            'flight_number': route.get('flight_number', ''),
            'flight_iata': route.get('flight_iata', ''),
            'flight_icao': route.get('flight_icao', ''),
            'airline_iata': route.get('airline_iata', ''),
            'airline_icao': route.get('airline_icao', ''),
            'from_airport': route.get('dep_iata', ''),
            'to_airport': route.get('arr_iata', ''),
            'from_airport_icao': route.get('dep_icao', ''),
            'to_airport_icao': route.get('arr_icao', ''),
            'departure_time': route.get('dep_time', ''),
            'departure_time_utc': route.get('dep_time_utc', ''),
            'arrival_time': route.get('arr_time', ''),
            'arrival_time_utc': route.get('arr_time_utc', ''),
            'duration_minutes': route.get('duration', 0),
            'days': ','.join(route.get('days', [])),
            'aircraft': route.get('aircraft_icao', ''),
            'updated': route.get('updated', ''),
            'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

    def analyze_flight_cancellations(self, airports: List[str], days_back: int = 5):
        """
        分析航班取消/延误数据（支持历史趋势）
        Args:
            airports: 要分析的机场列表
            days_back: 回溯天数（最多3个月）
        """
        self.logger.info(f"Analyzing flight cancellations for {len(airports)} airports (past {days_back} days)")

        end_date = datetime.now()
        date_range = [(end_date - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days_back, 0, -1)]

        cancellation_stats = []

        for airport in airports:
            self.logger.info(f"  Checking {airport}")

            daily_stats = {}
            total_cancelled = 0
            total_delayed = 0
            total_affected = 0

            for date in date_range:
                try:
                    # 查询历史航班数据
                    flights = self.client.get_flights(
                        dep_iata=airport,
                        flight_date=date,
                        limit=100
                    )

                    cancelled = 0
                    delayed = 0
                    affected = 0

                    for flight in flights:
                        status = flight.get('flight_status', '').lower()

                        if 'cancelled' in status:
                            cancelled += 1
                            # 估算受影响旅客
                            aircraft = flight.get('aircraft', {}).get('iata', 'B737-800')
                            capacity = self.aircraft_capacity.get(aircraft, 150)
                            affected += capacity

                        elif 'landed' in status:
                            # 检查是否有延误
                            dep_delay = flight.get('departure', {}).get('delay', 0)
                            arr_delay = flight.get('arrival', {}).get('delay', 0)
                            if dep_delay > 0 or arr_delay > 0:
                                delayed += 1

                    daily_stats[date] = {
                        'cancelled': cancelled,
                        'delayed': delayed,
                        'affected': affected,
                        'total': len(flights)
                    }

                    total_cancelled += cancelled
                    total_delayed += delayed
                    total_affected += affected

                except Exception as e:
                    self.logger.error(f"    Error on {date}: {e}")

            stat = {
                'airport': airport,
                'cancelled_flights': total_cancelled,
                'delayed_flights': total_delayed,
                'affected_passengers': total_affected,
                'daily_breakdown': daily_stats,
                'analysis_period_days': days_back,
                'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            cancellation_stats.append(stat)
            self.logger.info(f"    Cancelled: {total_cancelled}, Delayed: {total_delayed}, Affected: {total_affected}")

            time.sleep(self.config['query']['delay'])

        self.cancellation_stats = cancellation_stats
        self.logger.info(f"✓ Cancellation analysis completed for {len(cancellation_stats)} airports")

        # 计算趋势
        self._calculate_trends(cancellation_stats)

    def _calculate_trends(self, stats: List[Dict]):
        """计算变化趋势"""
        self.logger.info("Calculating trends...")

        for stat in stats:
            daily = stat['daily_breakdown']
            dates = sorted(daily.keys())

            if len(dates) < 2:
                continue

            # 获取首尾数据
            first_date = dates[0]
            last_date = dates[-1]

            first_cancelled = daily[first_date]['cancelled']
            last_cancelled = daily[last_date]['cancelled']

            first_affected = daily[first_date]['affected']
            last_affected = daily[last_date]['affected']

            # 计算变化
            cancelled_change = last_cancelled - first_cancelled
            cancelled_change_pct = ((cancelled_change / first_cancelled) * 100) if first_cancelled > 0 else 0

            affected_change = last_affected - first_affected
            affected_change_pct = ((affected_change / first_affected) * 100) if first_affected > 0 else 0

            stat['trend'] = {
                'cancelled_change': cancelled_change,
                'cancelled_change_percent': round(cancelled_change_pct, 2),
                'affected_change': affected_change,
                'affected_change_percent': round(affected_change_pct, 2),
                'period_start': first_date,
                'period_end': last_date
            }

            self.logger.info(f"  {stat['airport']} trends: Cancelled {cancelled_change:+d} ({cancelled_change_pct:+.1f}%), Affected {affected_change:+d} ({affected_change_pct:+.1f}%)")

    def analyze_route_duration_changes(self, routes: List[str]):
        """
        分析航线飞行时间变化（绕飞检测）
        Args:
            routes: 航线列表，格式为 [('PEK', 'LHR'), ('PVG', 'FRA')]
        """
        self.logger.info(f"Analyzing route duration changes for {len(routes)} routes")

        duration_changes = []

        for dep, arr in routes:
            self.logger.info(f"  Checking {dep} -> {arr}")

            try:
                # 获取该航线的所有航班
                flights = self.client.get_flights(
                    dep_iata=dep,
                    arr_iata=arr,
                    limit=50
                )

                scheduled_durations = []
                actual_durations = []

                for flight in flights:
                    # AviationStack API返回格式不同
                    dep_scheduled = flight.get('departure', {}).get('scheduled', '')
                    arr_scheduled = flight.get('arrival', {}).get('scheduled', '')

                    if dep_scheduled and arr_scheduled:
                        try:
                            # 计算计划飞行时间（分钟）
                            from datetime import datetime
                            dep_time = datetime.fromisoformat(dep_scheduled.replace('Z', '+00:00'))
                            arr_time = datetime.fromisoformat(arr_scheduled.replace('Z', '+00:00'))
                            scheduled_dur = (arr_time - dep_time).total_seconds() / 60
                            scheduled_durations.append(scheduled_dur)
                        except:
                            pass

                    # 实际飞行时间（如果可用）
                    dep_actual = flight.get('departure', {}).get('actual', '')
                    arr_actual = flight.get('arrival', {}).get('actual', '')

                    if dep_actual and arr_actual:
                        try:
                            dep_time = datetime.fromisoformat(dep_actual.replace('Z', '+00:00'))
                            arr_time = datetime.fromisoformat(arr_actual.replace('Z', '+00:00'))
                            actual_dur = (arr_time - dep_time).total_seconds() / 60
                            actual_durations.append(actual_dur)
                        except:
                            pass

                if scheduled_durations and actual_durations:
                    avg_scheduled = sum(scheduled_durations) / len(scheduled_durations)
                    avg_actual = sum(actual_durations) / len(actual_durations)
                    time_increase = avg_actual - avg_scheduled

                    change = {
                        'route': f"{dep}-{arr}",
                        'from_airport': dep,
                        'to_airport': arr,
                        'avg_scheduled_duration_minutes': round(avg_scheduled, 2),
                        'avg_actual_duration_minutes': round(avg_actual, 2),
                        'time_increase_minutes': round(time_increase, 2),
                        'time_increase_percent': round((time_increase / avg_scheduled) * 100, 2) if avg_scheduled > 0 else 0,
                        'flights_analyzed': len(flights),
                        'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }

                    duration_changes.append(change)
                    self.logger.info(f"    Increase: {time_increase:.1f} min ({(time_increase/avg_scheduled*100):.1f}%)")

                time.sleep(self.config['query']['delay'])

            except Exception as e:
                self.logger.error(f"    Error analyzing {dep}->{arr}: {e}")

        self.route_comparison = duration_changes
        self.logger.info(f"✓ Route duration analysis completed for {len(duration_changes)} routes")

    def analyze_aerodatabox_delays(self, airports: List[str], days: int = 15):
        """使用 Aerodatabox FIDS 端点，按天统计过去 N 天各机场的实际取消航班数。

        端点: GET /flights/airports/icao/{icao}/{fromLocal}/{toLocal}  (TIER 2)
        每天拆成两段各 12h（T00:00~T12:00 + T12:00~T23:59），再合并当天全部出发航班。
        直接统计 status == "Canceled" 或 "CanceledUncertain" 的条目数。

        总请求数: airports × days × 2 = 5 × 15 × 2 = 150 次
        airports: IATA 代码列表（内部自动转换为 ICAO）
        days:     回溯天数（默认15）
        """
        if not getattr(self, 'aerodatabox_client', None):
            self.logger.warning("Aerodatabox client not configured, skipping delays analysis")
            return

        self.logger.info(
            f"Querying Aerodatabox FIDS: {len(airports)} airports × {days} days "
            f"(~{len(airports) * days * 2} requests)"
        )

        # 生成每日时间段（每天2段，每段12h，覆盖本地零点到次日零点）
        today = datetime.utcnow().date()
        daily_slots = []
        for i in range(1, days + 1):   # 不含今天（今天数据不完整）
            d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            daily_slots.append((d, f"{d}T00:00", f"{d}T12:00"))
            daily_slots.append((d, f"{d}T12:00", f"{d}T23:59"))

        # 按机场外层循环，日期内层——避免连续请求同一机场
        airport_totals: Dict[str, Dict] = {ap: {
            "daily_data":      {},   # date → {cancelled, total}
            "days_collected":  0,
        } for ap in airports}

        CANCELLED_STATUSES = {"Canceled", "CanceledUncertain", "Cancelled"}

        for airport in airports:
            icao = self.aerodatabox_client.iata_to_icao(airport)
            self.logger.info(f"  [{airport}/{icao}] querying {days} days × 2 slots ...")

            for (date_str, slot_from, slot_to) in daily_slots:
                try:
                    data = self.aerodatabox_client.get_airport_fids(icao, slot_from, slot_to)
                    departures = data.get("departures", []) if isinstance(data, dict) else []

                    # 只统计实际运营方（IsOperator），排除代码共享重复计数
                    # Unknown 也保留（部分机场无代码共享信息）
                    operators = [
                        f for f in departures
                        if f.get("codeshareStatus") in ("IsOperator", "Unknown")
                    ]
                    cancelled_this_slot = sum(
                        1 for f in operators
                        if f.get("status", "") in CANCELLED_STATUSES
                    )
                    total_this_slot = len(operators)

                    dd = airport_totals[airport]["daily_data"]
                    if date_str not in dd:
                        dd[date_str] = {"cancelled": 0, "total": 0}
                    dd[date_str]["cancelled"] += cancelled_this_slot
                    dd[date_str]["total"]     += total_this_slot

                    self.logger.debug(
                        f"    {slot_from}~{slot_to[-5:]}: "
                        f"{cancelled_this_slot}/{total_this_slot} cancelled"
                    )
                except Exception as e:
                    self.logger.error(f"    Error [{airport}/{icao} @ {slot_from}]: {e}")
                    # ⛔ 如果是 429 配额耗尽，立刻向上抛出终止整个爬虫
                    if "配额耗尽" in str(e) or "429" in str(e):
                        raise
                finally:
                    time.sleep(self.config["query"]["delay"])  # 每次请求后等待（config: 6s）

            # 机场切换时额外多等 3s，让 RapidAPI 限速窗口充分冷却
            time.sleep(3)

            # 统计完成后计算已有的每日取消率
            dd = airport_totals[airport]["daily_data"]
            airport_totals[airport]["days_collected"] = len(dd)
            self.logger.info(
                f"  {airport}: {len(dd)} days collected, "
                f"total cancelled={sum(v['cancelled'] for v in dd.values())}, "
                f"total flights={sum(v['total'] for v in dd.values())}"
            )

        # 整合结果
        results = []
        for airport, t in airport_totals.items():
            dd = t["daily_data"]
            daily_cancel_rates = []
            daily_cancelled_counts = []
            daily_total_counts = []

            for date_str in sorted(dd.keys()):
                c = dd[date_str]["cancelled"]
                tot = dd[date_str]["total"]
                rate = round(c / tot * 100, 2) if tot > 0 else 0.0
                daily_cancel_rates.append(rate)
                daily_cancelled_counts.append(c)
                daily_total_counts.append(tot)

            avg_rate  = round(sum(daily_cancel_rates) / len(daily_cancel_rates), 2) if daily_cancel_rates else 0
            max_rate  = max(daily_cancel_rates) if daily_cancel_rates else 0
            total_cancelled = sum(daily_cancelled_counts)
            total_flights   = sum(daily_total_counts)

            results.append({
                "airport_iata":          airport,
                "airport_icao":          self.aerodatabox_client.iata_to_icao(airport),
                "analysis_days":         days,
                "days_collected":        t["days_collected"],
                "total_cancelled":       total_cancelled,
                "total_flights":         total_flights,
                "avg_cancel_rate_pct":   avg_rate,
                "max_cancel_rate_pct":   max_rate,
                "daily_cancel_rates":    daily_cancel_rates,     # 每天取消率（%）
                "daily_cancelled_counts": daily_cancelled_counts, # 每天取消架次
                "daily_total_counts":    daily_total_counts,      # 每天总出发架次
                "crawl_time":            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            self.logger.info(
                f"  {airport}: avg_cancel={avg_rate}%  max={max_rate}%  "
                f"total={total_cancelled}/{total_flights}  days={t['days_collected']}"
            )

        self.aerodatabox_delays = results
        self.logger.info(f"✓ Aerodatabox FIDS aggregated: {len(results)} airports")

    def analyze_capacity_changes(self, from_airports: List[str], to_airports: List[str]):
        """
        分析运力变化（班次统计）
        Args:
            from_airports: 出发机场列表
            to_airports: 目的机场列表
        """
        self.logger.info(f"Analyzing capacity changes: {len(from_airports)} -> {len(to_airports)}")

        capacity_data = []

        for dep in from_airports[:5]:  # 限制数量避免过多请求
            for arr in to_airports[:5]:
                self.logger.info(f"  Checking {dep} -> {arr}")

                try:
                    # 获取航线信息
                    routes = self.client.get_routes(
                        dep_iata=dep,
                        arr_iata=arr,
                        limit=50
                    )

                    weekly_flights = len(routes)  # AviationStack每个route代表一个航班
                    airlines = set()

                    for route in routes:
                        airlines.add(route.get('airline', {}).get('iata', ''))

                    capacity = {
                        'route': f"{dep}-{arr}",
                        'from_airport': dep,
                        'to_airport': arr,
                        'weekly_flights': weekly_flights,
                        'airlines_count': len(airlines),
                        'airlines': ','.join(sorted(airlines)),
                        'routes_found': len(routes),
                        'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }

                    capacity_data.append(capacity)
                    self.logger.info(f"    Weekly flights: {weekly_flights}, Airlines: {len(airlines)}")

                    time.sleep(self.config['query']['delay'])

                except Exception as e:
                    self.logger.error(f"    Error analyzing {dep}->{arr}: {e}")

        self.capacity_data = capacity_data
        self.logger.info(f"✓ Capacity analysis completed for {len(capacity_data)} routes")

    def crawl_routes(self, from_airports: List[str], to_airports: List[str]):
        """爬取航线数据"""
        self.logger.info(f"Starting route crawl: {len(from_airports)} -> {len(to_airports)}")

        total = len(from_airports) * len(to_airports)
        current = 0

        for dep_airport in from_airports:
            for arr_airport in to_airports:
                current += 1
                self.logger.info(f"[{current}/{total}] Querying {dep_airport} -> {arr_airport}")

                try:
                    # 查询航线数据
                    params = {
                        'dep_iata': dep_airport,
                        'arr_iata': arr_airport,
                        'limit': self.config['query']['limit']
                    }

                    routes = self.client.get_routes(**params)

                    # 标准化并保存数据
                    for route in routes:
                        normalized = self._normalize_route_data(route)
                        self.results.append(normalized)

                    self.logger.info(f"  Found {len(routes)} routes")

                    # 延迟避免触发速率限制
                    time.sleep(self.config['query']['delay'])

                except Exception as e:
                    self.logger.error(f"  Failed to query {dep_airport} -> {arr_airport}: {e}")
                    continue

        self.logger.info(f"Total routes collected: {len(self.results)}")

    def query_routes(self, from_airports: List[str], to_airports: List[str], label: str) -> List[Dict]:
        """查询出发地→目的地的现有航线列表（S1/R1 通用）。
        每对 (dep, arr) 调用一次 /routes，合计 len(from) × len(to) 次请求。
        """
        self.logger.info(f"  Querying routes [{label}]: {from_airports} → {to_airports}")
        results = []
        total = len(from_airports) * len(to_airports)
        idx = 0
        for dep in from_airports:
            for arr in to_airports:
                idx += 1
                try:
                    routes = self.client.get_routes(dep_iata=dep, arr_iata=arr, limit=50)
                    for r in routes:
                        results.append({
                            "label":        label,
                            "dep_iata":     dep,
                            "arr_iata":     arr,
                            "flight_iata":  r.get("flight_iata", ""),
                            "airline_iata": r.get("airline_iata", "") or r.get("airline", {}).get("iata", ""),
                            "duration_min": r.get("duration", 0),
                            "dep_time":     r.get("dep_time", ""),
                            "arr_time":     r.get("arr_time", ""),
                            "days":         ",".join(r.get("days", [])),
                            "crawl_time":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        })
                    self.logger.info(f"    [{idx}/{total}] {dep}→{arr}: {len(routes)} routes")
                except Exception as e:
                    self.logger.error(f"    [{idx}/{total}] {dep}→{arr} failed: {e}")
                    if "429" in str(e) or "Too Many" in str(e):
                        raise  # 429 直接终止，不浪费配额
                time.sleep(self.config["query"]["delay"])
        return results

    def run(self):
        """运行爬虫 — 精简版（仅 D1 + S1 + R1）"""
        self.logger.info("="*60)
        self.logger.info("Flight Data Crawler Started")
        self.logger.info("="*60)

        try:
            sample_days   = self.config.get("analysis", {}).get("sample_days", 15)
            departures    = (self.config["departure_airports"]["hong_kong"] +
                             self.config["departure_airports"]["china_mainland"])
            europe_dest   = self.config.get("european_dest", [])
            high_risk     = self.config.get("high_risk_airports", [])
            me_hubs       = self.config.get("middle_east_hubs", ["DXB","DOH","AUH","JED","RUH"])

            # ── D1: Aerodatabox 中东枢纽 15天取消率 ──────────────────────
            if getattr(self, "aerodatabox_client", None):
                self.logger.info("\n[D1] Aerodatabox disruption — Middle East hubs (15 days)")
                try:
                    self.analyze_aerodatabox_delays(me_hubs, days=sample_days)
                except Exception as e:
                    self.logger.error(f"  D1 failed: {e}")

            # ── S1: 替代度 — HKG/PEK/PVG → 欧洲 现有航线 ──────────────
            if departures and europe_dest:
                self.logger.info("\n[S1] Substitution routes — departures → Europe")
                try:
                    self.route_substitution = self.query_routes(departures, europe_dest, "S1_europe")
                    self.logger.info(f"  ✓ S1: {len(self.route_substitution)} routes collected")
                except Exception as e:
                    self.logger.error(f"  S1 failed: {e}")

            # ── R1: 连接韧性 — HKG/PEK/PVG → 高风险国家 现有航线 ───────
            if departures and high_risk:
                self.logger.info("\n[R1] Resilience routes — departures → high-risk countries")
                try:
                    self.route_resilience = self.query_routes(departures, high_risk, "R1_high_risk")
                    self.logger.info(f"  ✓ R1: {len(self.route_resilience)} routes collected")
                except Exception as e:
                    self.logger.error(f"  R1 failed: {e}")

            # ── 保存结果 ──────────────────────────────────────────────────
            self.save_results()
            self._print_summary()

        except Exception as e:
            self.logger.error(f"Crawler failed: {e}", exc_info=True)
            raise

    def _print_summary(self):
        """打印分析摘要"""
        self.logger.info("\n" + "="*60)
        self.logger.info("ANALYSIS SUMMARY")
        self.logger.info("="*60)

        # D1: 断裂度
        if self.aerodatabox_delays:
            self.logger.info("\n[D1] Middle East Hub Disruption (15 days):")
            for r in self.aerodatabox_delays:
                self.logger.info(
                    f"  {r['airport_iata']}: avg_cancel={r['avg_cancel_rate_pct']}%  "
                    f"max={r['max_cancel_rate_pct']}%  "
                    f"total={r.get('total_cancelled',0)}/{r.get('total_flights',0)} flights  "
                    f"days={r['days_collected']}"
                )

        # S1: 替代度
        if self.route_substitution:
            airlines = {r['airline_iata'] for r in self.route_substitution if r['airline_iata']}
            self.logger.info(f"\n[S1] Substitution routes (departures → Europe): {len(self.route_substitution)} routes, {len(airlines)} airlines")

        # R1: 连接韧性
        if self.route_resilience:
            dest_with_routes = {r['arr_iata'] for r in self.route_resilience}
            self.logger.info(f"\n[R1] Resilience routes (departures → high-risk): {len(self.route_resilience)} routes")
            self.logger.info(f"  Reachable destinations: {sorted(dest_with_routes)}")
        else:
            self.logger.info("\n[R1] Resilience routes: 0 routes — high-risk destinations unreachable")

        self.logger.info("="*60)

    def save_results(self):
        """保存所有结果到文件"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        prefix = self.config['output']['filename_prefix']
        output_dir = Path(self.config['output']['directory'])

        # D1: Aerodatabox 中东枢纽断裂度
        if self.aerodatabox_delays:
            self._save_dataset(self.aerodatabox_delays, output_dir, f"{prefix}D1_disruption_{timestamp}")

        # S1: 替代度航线
        if self.route_substitution:
            self._save_dataset(self.route_substitution, output_dir, f"{prefix}S1_substitution_{timestamp}")

        # R1: 连接韧性航线
        if self.route_resilience:
            self._save_dataset(self.route_resilience, output_dir, f"{prefix}R1_resilience_{timestamp}")

        self.logger.info(f"\n✓ All data saved to {output_dir}/")
        self.logger.info(f"  D1 disruption:   {len(self.aerodatabox_delays)} airports")
        self.logger.info(f"  S1 substitution: {len(self.route_substitution)} routes")
        self.logger.info(f"  R1 resilience:   {len(self.route_resilience)} routes")

    def _save_dataset(self, data: List[Dict], output_dir: Path, filename: str):
        """保存数据集（CSV和JSON）"""
        if not data:
            return

        # 保存JSON
        if 'json' in self.config['output']['formats']:
            json_file = output_dir / f"{filename}.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Saved JSON: {json_file}")

        # 保存CSV
        if 'csv' in self.config['output']['formats']:
            csv_file = output_dir / f"{filename}.csv"
            with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            self.logger.info(f"Saved CSV: {csv_file}")

    def _save_summary_report(self, output_dir: Path, filename: str):
        """保存汇总报告"""
        report = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'cancellation_stats': {
                'total_cancelled_flights': sum(s['cancelled_flights'] for s in self.cancellation_stats),
                'total_delayed_flights': sum(s['delayed_flights'] for s in self.cancellation_stats),
                'total_affected_passengers': sum(s['affected_passengers'] for s in self.cancellation_stats),
                'airports_analyzed': len(self.cancellation_stats),
                'trends': [
                    {
                        'airport': s['airport'],
                        'cancelled_change': s.get('trend', {}).get('cancelled_change', 0),
                        'cancelled_change_percent': s.get('trend', {}).get('cancelled_change_percent', 0),
                        'affected_change': s.get('trend', {}).get('affected_change', 0),
                        'affected_change_percent': s.get('trend', {}).get('affected_change_percent', 0),
                    }
                    for s in self.cancellation_stats if 'trend' in s
                ],
                'details': self.cancellation_stats
            },
            'duration_changes': {
                'avg_time_increase_minutes': sum(r['time_increase_minutes'] for r in self.route_comparison) / len(self.route_comparison) if self.route_comparison else 0,
                'routes_analyzed': len(self.route_comparison),
                'details': self.route_comparison
            },
            'capacity': {
                'total_weekly_flights': sum(c['weekly_flights'] for c in self.capacity_data),
                'routes_analyzed': len(self.capacity_data),
                'details': self.capacity_data
            }
        }

        report_file = output_dir / f"{filename}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        self.logger.info(f"Saved summary report: {report_file}")


def main():
    """主函数"""
    try:
        crawler = FlightDataCrawler()
        crawler.run()
        print("\n✓ Crawler completed successfully!")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()