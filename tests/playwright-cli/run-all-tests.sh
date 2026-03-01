#!/bin/bash
# Run all Phase 1 Playwright CLI tests

set -e

echo "🧪 NSE Momentum Lab - Phase 1 Dashboard Tests (Playwright CLI)"
echo "================================================================"
echo ""

# Check if services are running
echo "📡 Checking if services are running..."

if ! curl -s http://localhost:8501 > /dev/null 2>&1; then
    echo "❌ Dashboard not running at http://localhost:8501"
    echo "   Start it with: doppler run -- uv run nseml-dashboard"
    echo ""
    read -p "Start dashboard now? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Starting dashboard..."
        doppler run -- uv run nseml-dashboard &
        echo "Waiting 10 seconds for dashboard to start..."
        sleep 10
    else
        exit 1
    fi
fi

if ! curl -s http://localhost:8004/health > /dev/null 2>&1; then
    echo "❌ API not running at http://localhost:8004"
    echo "   Start it with: doppler run -- uv run nseml-api"
    echo ""
    read -p "Start API now? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Starting API..."
        doppler run -- uv run nseml-api &
        echo "Waiting 5 seconds for API to start..."
        sleep 5
    else
        exit 1
    fi
fi

echo "✅ Services are running!"
echo ""

# Run tests
echo "🚀 Running tests..."
echo ""

TESTS_DIR="$(dirname "$0")"
PASSED=0
FAILED=0

# Test 1: Home Page
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Test 1: Home Page Navigation Cards"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if playwright-cli code "$TESTS_DIR/test-home-page.mjs"; then
    ((PASSED++))
else
    ((FAILED++))
fi
echo ""

# Test 2: Experiments Page
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Test 2: Experiments Page Improvements"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if playwright-cli code "$TESTS_DIR/test-experiments-page.mjs"; then
    ((PASSED++))
else
    ((FAILED++))
fi
echo ""

# Test 3: Scans Page
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Test 3: Scans Page Improvements"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if playwright-cli code "$TESTS_DIR/test-scans-page.mjs"; then
    ((PASSED++))
else
    ((FAILED++))
fi
echo ""

# Test 4: Chat Page
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Test 4: Chat Page Confirmation Dialog"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if playwright-cli code "$TESTS_DIR/test-chat-page.mjs"; then
    ((PASSED++))
else
    ((FAILED++))
fi
echo ""

# Summary
echo "================================================================"
echo "📊 Test Results Summary"
echo "================================================================"
echo "✅ Passed: $PASSED"
echo "❌ Failed: $FAILED"
echo "📈 Total:  $((PASSED + FAILED))"
echo ""

if [ $FAILED -eq 0 ]; then
    echo "🎉 All tests passed!"
    exit 0
else
    echo "⚠️  Some tests failed - please review the output above"
    exit 1
fi
