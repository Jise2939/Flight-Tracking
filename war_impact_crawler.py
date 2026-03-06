#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AeroDataBox API 航班影响数据爬虫
用于获取伊朗-美国战争对航班的影响数据（7天数据）
"""

import os
import json
import csv
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict
import requests


class AeroDataBoxClient:
    """AeroDataBox API 客户端"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://aerodatabox.p.rapidapi.com"
        self.session = requests.Session()
        self.headers = {
            'X-RapidAPI-Key': api_key,
            'X-RapidAPI-Host': 'aerodatabox.p.rapidapi.com'
        }
        
    def get_airport_delays(self, airport: str, timestamp: str) -> dict:
        """
        获取机场延迟数据
        
        Args:
            airport: 机场IATA代码
            timestamp: 时间戳 (格式: YYYY-MM-DDTHH:MM)
        """
        url = f"{self.base_url}/airports/delays/{timestamp}"
        
        try:
            response = self.session.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error: {e}")
            return None


class WarImpactAnalyzer:
    """战争影响分析器"""
    
    # 高风险区域（伊朗、伊拉克、以色列等）
    HIGH_RISK_ZONES = {
        'IKA': '德黑兰',
        'IKA': '德黑兰',
        'IRK': '伊拉克',
        'IRK': '伊拉克',
        'IL': '以色列',
        'IL': '以色列',
        'BYB': '巴林',
        'BYB': '巴林',
        'LBN': '黎巴嫩',
        'SYR': '叙利亚',
        'SYR': '叙利亚',
    }
    
    # 中东枢纽机场
    MIDDLE_EAST_HUBS = ['DXB', 'DOH', 'AUH', 'KWI', 'JED', 'AMM', 'BEY']
    
    # 中国大陆主要机场
    CHINA_MAINLAND = ['PEK', 'PKX', 'PVG', 'SHA', 'CAN', 'SZX', 'CTU', 'HGH', 'XIY']
    
    # 香港机场
    HONG_KONG = ['HKG']
    
    # 欧洲主要机场（非战区）
    EUROPE_AIRPORTS = ['LHR', 'CDG', 'FRA', 'AMS', 'MUC', 'MAD', 'FCO', 'IST']
    
    # 机型容量（用于估算受影响旅客）
    AIRCRAFT_CAPACITY = {
        'A380': 525, 'B747': 416, 'A350': 325, 'B777': 365,
        'B787': 296, 'A330': 277, 'A321': 185, 'B737': 160
    }
    
    def __init__(self, api_key: str, days_back: int = 7):
        self.client = AeroDataBoxClient(api_key)
        self.days_back = days_back
        self.results = {
            '断裂度': [],
            '替代度': [],
            '连接性': []
        }
        
        # 计算7天时间范围
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=days_back)
        self.date_from = self.start_date.strftime('%Y-%m-%d')
        self.date_to = self.end_date.strftime('%Y-%m-%d')
        
    def analyze_中断度(self):
        """
        【断裂度】中东枢纽飞往高风险区域的取消航班
        """
        print(f"\n[1] 分析断裂度 - 中东枢纽→高风险区域")
        print(f"时间范围: {self.date_from} 至 {self.date_to}")
        print("-" * 60)
        
        for hub in self.MIDDLE_EAST_HUBS:
            print(f"\n  分析: {hub}")
            
            for zone_code, zone_name in self.HIGH_RISK_ZONES.items():
                try:
                    # 获取延误数据
                    delays_data = self.client.get_airport_delays(
                        airport=hub,
                        date_from=self.date_from,
                        date_to=self.date_to
                    )
                    
                    if not delays_data:
                        continue
                        
                    # 提取取消航班数
                    cancelled_count = delays_data.get('departures', {}).get('cancelled', 0)
                    total_count = delays_data.get('departures', {}).get('total', 0)
                    
                    # 估算受影响旅客
                    affected = cancelled_count * 200  # 平均200人/航班
                    
                    result = {
                        'hub_airport': hub,
                        'target_zone': zone_code,
                        'target_zone_name': zone_name,
                        'cancelled_flights': cancelled_count,
                        'total_flights': total_count,
                        'affected_passengers': affected,
                        'date_from': self.date_from,
                        'date_to': self.date_to,
                        'analysis_type': '断裂度',
                        'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    self.results['断裂度'].append(result)
                    print(f"    {zone_name}: 取消{cancelled_count}/{total_count}, 受影响约{affected}人")
                    
                    time.sleep(1)  # 避免过快请求
                    
                except Exception as e:
                    print(f"    {zone_name}: 错误 - {e}")
    
    def analyze_替代度(self):
        """
        【替代度】大陆/香港→欧洲的航线数量
        """
        print(f"\n[2] 分析替代度 - 大陆/香港→欧洲（非战区）")
        print(f"时间范围: {self.date_from} 至 {self.date_to}")
        print("-" * 60)
        
        # 使用AviationStack API获取航线数据（更准确）
        try:
            aviation_key = os.getenv('AVIATIONSTACK_API_KEY')
            if aviation_key and aviation_key != 'your_aviationstack_key_here':
                self._get_aviationstack_routes(aviation_key)
            else:
                print("    未配置AviationStack API，跳过")
        except:
            pass
    
    def _get_aviationstack_routes(self, api_key: str):
        """使用AviationStack获取航线数据"""
        url = "https://api.aviationstack.com/v1/routes"
        
        all_departures = self.CHINA_MAINLAND + self.HONG_KONG
        all_arrivals = self.EUROPE_AIRPORTS
        
        for dep in all_departures[:5]:  # 限制数量
            for arr in all_arrivals[:5]:
                print(f"  检查: {dep} -> {arr}")
                
                params = {
                    'access_key': api_key,
                    'dep_iata': dep,
                    'arr_iata': arr,
                    'limit': 50
                }
                
                try:
                    response = requests.get(url, params=params, timeout=30)
                    data = response.json()
                    
                    routes = data.get('data', [])
                    weekly_flights = len(routes)
                    
                    # 获取运营的航空公司
                    airlines = set()
                    for r in routes:
                        airlines.add(r.get('airline', {}).get('iata', ''))
                    
                    result = {
                        'departure': dep,
                        'arrival': arr,
                        'weekly_flights': weekly_flights,
                        'airlines': list(airlines),
                        'route_type': '替代度',
                        'date_from': self.date_from,
                        'date_to': self.date_to,
                        'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    self.results['替代度'].append(result)
                    print(f"    每周{weekly_flights}班, {len(airlines)}家航司")
                    
                    time.sleep(0.5)
                    
                except Exception as e:
                    print(f"    {dep}->{arr}: 错误 - {e}")
    
    def analyze_连接性(self):
        """
        【连接性】三地对高风险区域的航线保留率
        """
        print(f"\n[3] 分析连接性 - 飞往高风险区域的活跃航班")
        print(f"时间范围: {self.date_from} 至 {self.date_to}")
        print("-" * 60)
        
        for group_name, airports in [
            ('中东枢纽', self.MIDDLE_EAST_HUBS),
            ('中国大陆', self.CHINA_MAINLAND[:5]),
            ('香港', self.HONG_KONG)
        ]:
            print(f"\n  {group_name}:")
            
            for dep in airports:
                # 统计飞往每个高风险区域的航班
                for zone_code, zone_name in self.HIGH_RISK_ZONES.items():
                    try:
                        # 使用AviationStack获取当前航班
                        aviation_key = os.getenv('AVIATIONSTACK_API_KEY')
                        if not aviation_key or aviation_key == 'your_aviationstack_key_here':
                            continue
                            
                        url = "https://api.aviationstack.com/v1/flights"
                        params = {
                            'access_key': aviation_key,
                            'dep_iata': dep,
                            'arr_iata': zone_code,
                            'limit': 50
                        }
                        
                        response = requests.get(url, params=params, timeout=30)
                        data = response.json()
                        
                        flights = data.get('data', [])
                        
                        # 统计活跃航班
                        active = sum(1 for f in flights 
                                    if f.get('flight_status') in ['active', 'scheduled'])
                        cancelled = sum(1 for f in flights 
                                       if f.get('flight_status') == 'cancelled')
                        
                        result = {
                            'departure': dep,
                            'group': group_name,
                            'target_zone': zone_code,
                            'target_zone_name': zone_name,
                            'active_flights': active,
                            'cancelled_flights': cancelled,
                            'total_flights': len(flights),
                            'retention_rate': round((active / len(flights)) * 100, 2) if len(flights) > 0 else 0,
                            'date_from': self.date_from,
                            'date_to': self.date_to,
                            'analysis_type': '连接性',
                            'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        
                        self.results['连接性'].append(result)
                        print(f"    {dep} → {zone_name}: {active}个活跃, {cancelled}个取消, 保留率{result['retention_rate']}%")
                        
                        time.sleep(0.5)
                        
                    except Exception as e:
                        pass
    
    def run(self):
        """运行分析"""
        print("="*60)
        print("战争影响分析器 - 7天数据采集")
        print("="*60)
        print(f"分析周期: {self.days_back}天")
        print(f"数据源: AeroDataBox API + AviationStack API")
        
        # 1. 断裂度分析
        self.analyze_中断度()
        
        # 2. 替代度分析
        self.analyze_替代度()
        
        # 3. 连接性分析
        self.analyze_连接性()
        
        # 保存结果
        self.save_results()
        
        # 打印摘要
        self._print_summary()
    
    def save_results(self):
        """保存结果"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        # 保存JSON
        json_file = output_dir / f'war_impact_7days_{timestamp}.json'
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        print(f"\n✓ 数据已保存: {json_file}")
        
        # 保存CSV
        csv_file = output_dir / f'war_impact_7days_{timestamp}.csv'
        with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'analysis_type', 'departure', 'arrival', 'target_zone', 'target_zone_name',
                'cancelled_flights', 'active_flights', 'total_flights', 'affected_passengers',
                'retention_rate', 'weekly_flights', 'airlines', 'date_from', 'date_to', 'crawl_time'
            ])
            writer.writeheader()
            
            for category in self.results.values():
                for item in category:
                    writer.writerow(item)
        print(f"✓ CSV已保存: {csv_file}")
    
    def _print_summary(self):
        """打印分析摘要"""
        print("\n" + "="*60)
        print("分析摘要（7天数据）")
        print("="*60)
        
        # 断裂度汇总
        if self.results['断裂度']:
            cancelled_total = sum(r['cancelled_flights'] for r in self.results['断裂度'])
            affected_total = sum(r['affected_passengers'] for r in self.results['断裂度'])
            print(f"\n【断裂度】")
            print(f"  总取消航班: {cancelled_total}")
            print(f"  受影响旅客: {affected_total:,}")
            
            # 按机场汇总
            by_hub = {}
            for r in self.results['断裂度']:
                hub = r['hub_airport']
                if hub not in by_hub:
                    by_hub[hub] = {'cancelled': 0, 'affected': 0}
                by_hub[hub]['cancelled'] += r['cancelled_flights']
                by_hub[hub]['affected'] += r['affected_passengers']
            
            print(f"\n  按机场:")
            for hub, data in sorted(by_hub.items()):
                print(f"    {hub}: 取消{data['cancelled']}, 受影响{data['affected']:,}")
        
        # 替代度汇总
        if self.results['替代度']:
            total_flights = sum(r['weekly_flights'] for r in self.results['替代度'])
            print(f"\n【替代度】")
            print(f"总航班数: {total_flights}")
        
        # 连接性汇总
        if self.results['连接性']:
            print(f"\n【连接性】")
            
            # 中东枢纽 vs 大陆/香港
            middle_east = [r for r in self.results['连接性'] if r['group'] == '中东枢纽']
            mainland_hk = [r for r in self.results['连接性'] if r['group'] in ['中国大陆', '香港']]
            
            me_retention = sum(r['retention_rate'] for r in middle_east) / len(middle_east) if middle_east else 0
            hk_retention = sum(r['retention_rate'] for r in mainland_hk) / len(mainland_hk) if mainland_hk else 0
            
            print(f"  中东枢纽平均保留率: {me_retention:.1f}%")
            print(f"  大陆/香港平均保留率: {hk_retention:.1f}%")
            print(f"  差距: {abs(me_retention - hk_retention):.1f}%")
        
        print("\n" + "="*60)


def main():
    """主函数"""
    print("="*60)
    print("战争影响数据采集器 - 7天数据")
    print("="*60)
    
    # 检查API Key
    api_key = os.getenv('AERODATABOX_API_KEY')
    if not api_key or api_key == 'your_aerodatabox_key_here':
        print("\n请提供你的 AeroDataBox API Key:")
        print("  从 https://rapidapi.com/aedbx-aedbx/api/aerodatabox 获取")
        print("  或者编辑 .env 文件添加: AERODATABOX_API_KEY=your_key")
        return
    
    print(f"✓ API Key: {api_key[:10]}...{api_key[-4:]}")
    
    # 创建分析器（7天数据）
    analyzer = WarImpactAnalyzer(api_key, days_back=7)
    
    # 运行分析
    analyzer.run()


if __name__ == "__main__":
    main()