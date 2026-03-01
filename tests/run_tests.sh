#!/bin/bash
# Run Phase 1 Dashboard Tests

set -e

echo "🧪 NSE Momentum Lab - Phase 1 Dashboard Tests"
echo "================================================"
echo ""

# Check if services are running
echo "📡 Checking services..."

if ! curl -s http://localhost:8501 > /dev/null; then
    echo "❌ Dashboard not running at http://localhost:8501"
    echo "   Start it with: doppler run -- uv run nseml-dashboard"
    exit 1
fi

if ! curl -s http://localhost:8004/health > /dev/null; then
    echo "❌ API not running at http://localhost:8004"
    echo "   Start it with: doppler run -- uv run nseml-api"
    exit 1
fi

echo "✅ All services running"
echo ""

# Run tests
echo "🚀 Running Playwright tests..."
echo ""

cd "$(dirname "$0")/.."  # Go to project root

# Option 1: Using python directly
python tests/test_dashboard_phase1.py

# Option 2: Using pytest (uncomment if you prefer pytest)
# pytest tests/test_dashboard_phase1.py -v -s

echo ""
echo "✅ Tests complete!"
