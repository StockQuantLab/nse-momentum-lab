#!/bin/bash
# Test Home Page - Playwright CLI

echo "🔍 Testing: Home Page Navigation Cards"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Open dashboard
playwright-cli open http://localhost:8501
sleep 3

# Take snapshot and check for navigation cards
playwright-cli snapshot > /tmp/home-snapshot.json

# Check for cards in the page (count elements with border styling)
if grep -q "Chat Assistant" /tmp/home-snapshot.json && \
   grep -q "Momentum Scans" /tmp/home-snapshot.json && \
   grep -q "Backtest Experiments" /tmp/home-snapshot.json; then
    echo "✅ PASS: All navigation cards present"
else
    echo "❌ FAIL: Navigation cards missing"
fi

# Close browser
playwright-cli close

echo ""
