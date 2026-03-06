#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证AviationStack API配置和连接
"""

import os
import sys
import requests
from pathlib import Path


def check_file_exists(filename, required=True):
    """检查文件是否存在"""
    exists = Path(filename).exists()
    status = "✓" if exists else "✗"
    print(f"  {status} {filename}")

    if required and not exists:
        if filename == ".env" and Path(".env.example").exists():
            print(f"    Hint: Copy .env.example to .env and fill in your API key")
        return False
    return True


def verify_api_key(api_key):
    """验证API Key"""
    if not api_key or api_key == "your_api_key_here":
        return False, "Invalid API key"

    try:
        url = "https://api.aviationstack.com/v1/flights"
        params = {'access_key': api_key, 'limit': 1}
        response = requests.get(url, params=params, timeout=10)
        data = response.json()

        if 'error' in data:
            return False, data['error'].get('message', 'Unknown error')

        if 'data' in data:
            return True, "API key valid"

        return False, "Unexpected response"

    except Exception as e:
        return False, str(e)


def main():
    print("="*60)
    print("AviationStack API Crawler - Configuration Verification")
    print("="*60)
    print()

    # Check files
    print("Checking files...")
    files_ok = True
    files_ok &= check_file_exists("aviationstack_crawler.py")
    files_ok &= check_file_exists("config.yaml")
    files_ok &= check_file_exists(".env.example")
    files_ok &= check_file_exists("requirements.txt")
    files_ok &= check_file_exists(".gitignore")
    print()

    # Load environment
    env_file = Path(".env")
    if env_file.exists():
        print("✓ .env file exists")

        # Try to load API key
        with open(env_file, 'r') as f:
            for line in f:
                if line.startswith('AVIATIONSTACK_API_KEY='):
                    api_key = line.split('=', 1)[1].strip()

                    print()
                    print("Verifying API key...")
                    valid, message = verify_api_key(api_key)

                    if valid:
                        print(f"  ✓ {message}")
                    else:
                        print(f"  ✗ {message}")
                        files_ok = False
                    break
            else:
                print("  ✗ AVIATIONSTACK_API_KEY not found in .env")
                files_ok = False
    else:
        print("✗ .env file not found")
        print("  Hint: Run setup.sh to create .env file")
        files_ok = False

    print()
    print("="*60)

    if files_ok:
        print("✓ Configuration verified successfully!")
        print()
        print("You can now run the crawler:")
        print("  ./run_crawler.sh")
        return 0
    else:
        print("✗ Configuration check failed")
        print()
        print("Please fix the issues above and run again")
        return 1


if __name__ == "__main__":
    sys.exit(main())