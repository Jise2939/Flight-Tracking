#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试AviationStack API新功能（需要API Key）
"""

import os
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))


def test_api_connection():
    """测试API连接"""
    print("Testing AviationStack API connection...")

    try:
        from aviationstack_crawler import AviationStackAPIClient

        api_key = os.getenv('AVIATIONSTACK_API_KEY')
        if not api_key or api_key == 'your_api_key_here':
            print("  ✗ AVIATIONSTACK_API_KEY not set or invalid")
            print("  Hint: Set your API key in .env file")
            return False

        client = AviationStackAPIClient(api_key)

        # 测试ping端点（AviationStack没有ping，用flights测试）
        result = client._make_request('flights', {'limit': 1})
        
        if 'data' in result or 'error' in result:
            print("  ✓ API connection successful")
            return True
        else:
            print("  ✗ Unexpected response")
            return False

    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False


def test_flight_data():
    """测试航班数据获取"""
    print("\nTesting flight data retrieval...")

    try:
        from aviationstack_crawler import AviationStackAPIClient

        api_key = os.getenv('AVIATIONSTACK_API_KEY')
        client = AviationStackAPIClient(api_key)

        # 测试获取航班数据
        flights = client.get_flights(dep_iata='DXB', limit=5)

        if flights:
            print(f"  ✓ Retrieved {len(flights)} flights")
            print(f"  Sample flight: {flights[0].get('flight', {}).get('iata', 'N/A')}")
            return True
        else:
            print("  ✗ No flights retrieved")
            return False

    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False


def test_cancellation_analysis():
    """测试取消分析功能"""
    print("\nTesting cancellation analysis...")

    try:
        from aviationstack_crawler import FlightDataCrawler

        crawler = FlightDataCrawler()

        # 测试单个机场的取消分析
        crawler.analyze_flight_cancellations(['DXB'], days_back=1)

        if crawler.cancellation_stats:
            stat = crawler.cancellation_stats[0]
            print(f"  ✓ Analysis completed")
            print(f"    Cancelled: {stat['cancelled_flights']}")
            print(f"    Delayed: {stat['delayed_flights']}")
            print(f"    Affected: {stat['affected_passengers']}")
            return True
        else:
            print("  ✗ No data collected")
            return False

    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False


def main():
    print("="*60)
    print("AviationStack API - New Features Test")
    print("="*60)

    # 检查.env文件
    env_file = Path('.env')
    if not env_file.exists():
        print("\n✗ .env file not found")
        print("Run: ./setup.sh")
        return 1

    # 加载环境变量
    with open(env_file, 'r') as f:
        for line in f:
            if line.startswith('AVIATIONSTACK_API_KEY='):
                key = line.split('=', 1)[1].strip()
                if key == 'your_api_key_here':
                    print("\n✗ Please set your AviationStack API key in .env file")
                    return 1
                os.environ['AVIATIONSTACK_API_KEY'] = key
                break
        else:
            print("\n✗ AVIATIONSTACK_API_KEY not found in .env")
            return 1

    results = []

    # 运行测试
    results.append(("API Connection", test_api_connection()))
    results.append(("Flight Data", test_flight_data()))
    results.append(("Cancellation Analysis", test_cancellation_analysis()))

    print("\n" + "="*60)
    print("Test Results")
    print("="*60)

    for name, passed in results:
        print(f"  {'✓' if passed else '✗'} {name}")

    if all(p for _, p in results):
        print("\n✓ All tests passed!")
        print("\nYou can now run:")
        print("  ./run_crawler.sh")
        return 0
    else:
        print("\n✗ Some tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())