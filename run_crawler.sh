#!/bin/bash
# AviationStack API Crawler Runner

set -e

echo "=========================================="
echo "AviationStack Flight Data Crawler"
echo "=========================================="

# Check if .env exists
if [ ! -f .env ]; then
    echo "⚠️  .env file not found!"
    echo "Creating from .env.example..."
    cp .env.example .env
    echo ""
    echo "Please edit .env and add your AviationStack API key:"
    echo "  AVIATIONSTACK_API_KEY=your_api_key_here"
    echo ""
    echo "Get your free API key at: https://aviationstack.com/"
    echo ""
    exit 1
fi

# Load environment variables
export $(cat .env | grep -v '^#' | xargs)

# Check API key
if [ "$AVIATIONSTACK_API_KEY" = "your_api_key_here" ]; then
    echo "⚠️  Please set your AviationStack API key in .env file"
    exit 1
fi

# Create virtual environment if not exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -q -r requirements.txt

# Create output directory
mkdir -p output
mkdir -p logs

# Run crawler
echo ""
echo "Starting crawler..."
python3 aviationstack_crawler.py

echo ""
echo "=========================================="
echo "✓ Crawler completed!"
echo "=========================================="
echo "Results saved to: output/"
echo ""