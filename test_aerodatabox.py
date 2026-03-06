#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试AeroDataBox API连接
"""

import os
import json
from pathlib import Path

# 加载API Key
env_file = Path('.env')
has_key = False
api_key = None

if env_file.exists():
    with open(env_file, 'r') as f:
        for line in f:
            if 'AERODATABOX_API_KEY' in line and '=' in line:
                key = line.split('=')[1].strip()
                if key and key != 'your_aerodatabox_key_here':
                    api_key = key
                    has_key = True
                    break

if not has_key:
    print("未找到 AeroDataBox API Key")
    print("\n请编辑 .env 文件，添加你的 AeroDataBox API Key:")
    print("AERODATABOX_API_KEY=your_aerodatabox_key_here")
    print("\n获取方式:")
    print("1. 访问 https://rapidapi.com/aedbx-aedbx/api/aerodatabox")
    print("2. 注册并获取免费API Key")
    print("3. 将Key添加到 .env 文件")
    print("\nAeroDataBox API 优势:")
    print("- ✅ 支持历史延误数据（'right now or in the past'）")
    print("- ✅ 支持7-30天的时间范围查询")
    print("- ✅ 适合获取战争影响数据")
else:
    print(f"✓ 找到 AeroDataBox API Key: {api_key[:10]}...{api_key[-4:]}")
    
    # 测试API连接
    print("\n测试 AeroDataBox API 连接...")
    
    import requests
    
    url = "https://aerodatabox.p.rapidapi.com/airport/delays"
    headers = {
        'X-RapidAPI-Key': api_key,
        'X-RapidAPI-Host': 'aerodatabox.p.rapidapi.com'
    }
    
    # 测试1: 当前时刻的延迟数据
    print("\n[1] 测试当前时刻数据（DXB机场）")
    params = {'airport': 'DXB', 'limit': 5}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        print(f"  状态码: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"  ✓ 数据获取成功")
            print(f"  响应示例: {json.dumps(data, indent=2)[:300]}")
        else:
            print(f"  ✗ HTTP错误: {response.status_code}")
            print(f"  响应: {response.text[:200]}")
    except Exception as e:
        print(f"  ✗ 请求失败: {e}")
    
    # 测试2: 历史数据（7天前）
    print("\n[2] 测试历史数据（7天前，DXB机场）")
    
    from datetime import datetime, timedelta
    date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    date_to = datetime.now().strftime('%Y-%m-%d')
    
    params = {
        'airport': 'DXB',
        'date_from': date_from,
        'date_to': date_to
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        print(f"  状态码: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"  ✓ 历史数据获取成功")
            print(f"  日期范围: {date_from} 至 {date_to}")
            print(f"  响应示例: {json.dumps(data, indent=2)[:300]}")
            
            # 检查是否有历史数据
            if 'data' in data or 'departures' in data or 'arrivals' in data:
                print(f"  ✓ 确认支持历史数据查询！")
            else:
                print(f"  ⚠️ 响应中没有预期的时间序列数据，可能需要检查端点")
        else:
            print(f"  ✗ HTTP错误: {response.status_code}")
            print(f"  响应: {response.text[:200]}")
    except Exception as e:
        print(f"  ✗ 请求失败: {e}")
    
    print("\n" + "="*60)
    print("测试完成")
    print("="*60)
    print("\n如果测试成功，运行完整爬虫:")
    print("python3 war_impact_crawler.py")