# Playwright CLI Tests for NSE Momentum Lab Dashboard

Automated tests using **Playwright CLI** - no Python required!

## What is Playwright CLI?

Playwright CLI (`playwright-cli`) lets you run browser automation tests directly from the command line using JavaScript. Perfect for coding agents and CI/CD!

## Prerequisites

You already have it installed! 👍
```bash
# Verify installation
playwright-cli version
```

## Running Tests

### Quick Start (One Command)

**Windows:**
```bash
tests\playwright-cli\run-all-tests.bat
```

**Linux/Mac:**
```bash
bash tests/playwright-cli/run-all-tests.sh
```

### Run Individual Tests

```bash
# Test 1: Home Page
playwright-cli code tests/playwright-cli/test-home-page.mjs

# Test 2: Navigation URLs
playwright-cli code tests/playwright-cli/test-navigation-urls.mjs

# Test 3: Experiments Page
playwright-cli code tests/playwright-cli/test-experiments-page.mjs

# Test 4: Scans Page
playwright-cli code tests/playwright-cli/test-scans-page.mjs

# Test 5: Chat Page
playwright-cli code tests/playwright-cli/test-chat-page.mjs
```

## Test Coverage

### ✅ Test 1: Navigation URL Cleanliness
- Verifies all 8 navigation cards use clean URLs
- Ensures no .py extensions in URLs
- Tests page load success for each navigation item
- Captures screenshots for documentation

### ✅ Test 2: Home Page Navigation Cards
- Verifies visual navigation cards render
- Checks for at least 6 cards
- Validates all expected navigation items present

### ✅ Test 2: Experiments Page
- Search box functionality
- Status filter multiselect
- Chart tabs (Equity, Drawdown, P&L Distribution)
- Pagination controls (Previous/Next)
- CSV download button

### ✅ Test 3: Scans Page
- Score distribution chart elements
- Passed/Failed tabs
- Pagination controls
- CSV download button
- Removed arbitrary limits

### ✅ Test 4: Chat Page
- Clear Chat button exists
- Confirmation dialog appears
- Yes/Cancel buttons work

## Services Required

Tests need these services running:

```bash
# Terminal 1: Infrastructure
docker compose up -d

# Terminal 2: API Server
doppler run -- uv run nseml-api

# Terminal 3: Dashboard
doppler run -- uv run nseml-dashboard
```

**The test script will auto-start services if not running!** (Just press 'y' when prompted)

## Understanding Test Output

- ✅ **PASS**: Feature working correctly
- ⚠️ **WARNING**: Feature exists but may need data
- ❌ **FAIL**: Feature missing or broken

## Example Output

```
🧪 NSE Momentum Lab - Phase 1 Dashboard Tests
================================================================

📡 Checking if services are running...
✅ Services are running!

🚀 Running tests...

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Test 1: Navigation URL Cleanliness
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔍 Testing: Chat Assistant navigation
✅ PASS: URL is clean: http://localhost:8501/chat-assistant
🔍 Testing: Momentum Scans navigation
✅ PASS: URL is clean: http://localhost:8501/Scans
...
✅ PASS: All 8 navigation cards use clean URLs
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Test 2: Home Page Navigation Cards
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔍 Testing: Home Page Navigation Cards
   Found 8 navigation cards
✅ PASS: Navigation cards render correctly
✅ PASS: All expected navigation items present

================================================================
📊 Test Results Summary
================================================================
✅ Passed: 5
❌ Failed: 0
📈 Total:  5

🎉 All tests passed!
```

## Troubleshooting

### Tests fail with "Dashboard not running"
```bash
# Start services manually
docker compose up -d
doppler run -- uv run nseml-api
doppler run -- uv run nseml-dashboard
```

### Tests timeout
- Check if services are actually running
- Look at browser console (F12) for errors
- Verify data exists in database

### Playwright CLI not found
```bash
# Reinstall playwright-cli
npm install -g @playwright/test-cli
```

## Adding New Tests

1. Create new `.mjs` file in `tests/playwright-cli/`
2. Use Playwright CLI syntax:
   ```javascript
   await page.goto("http://localhost:8501/your-page");
   await page.waitForTimeout(2000);
   // Your test code here
   console.log("✅ PASS: Test passed");
   ```
3. Add to `run-all-tests.sh` / `run-all-tests.bat`

## Why Playwright CLI?

✅ No Python setup needed
✅ Fast execution (native CLI)
✅ Perfect for automation/coding agents
✅ Easy to debug (JavaScript you can read)
✅ Works in CI/CD pipelines

## Next Steps

After Phase 1 tests pass, we can add:
- Phase 4 tests (Compare Experiments, Strategy Analysis, Trade Analytics)
- Additional URL validation tests
- Screenshot tests for visual regression
- Video recording for failed tests
- Performance metrics
- Accessibility tests
