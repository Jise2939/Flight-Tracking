#!/bin/bash
# Setup script for AviationStack API Crawler

set -e

echo "=========================================="
echo "AviationStack API Crawler Setup"
echo "=========================================="
echo ""

# Check Python version
PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1-2)
echo "Python version: $PYTHON_VERSION"

if [ "$(echo "$PYTHON_VERSION < 3.8" | bc)" -eq 1 ]; then
    echo "⚠️  Python 3.8 or higher required"
    exit 1
fi

# Create .env from example if not exists
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo "✓ Created .env file"
    echo ""
    echo "Please edit .env and add your AviationStack API key:"
    echo "  AVIATIONSTACK_API_KEY=your_api_key_here"
    echo ""
    echo "Get your free API key at: https://aviationstack.com/"
    echo ""
fi

# Create virtual environment
if [ ! -d "venv" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv venv
    echo "✓ Created venv/"
fi

# Activate virtual environment and install dependencies
echo "Installing dependencies..."
source venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements.txt -q

# Create output directories
mkdir -p output logs

echo ""
echo "=========================================="
echo "✓ Setup completed!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Edit .env and add your AviationStack API key"
echo "2. Run: ./run_crawler.sh"
echo "   or: source venv/bin/activate && python aviationstack_crawler.py"
echo ""