@echo off
REM Run all Phase 1 Playwright CLI tests (Windows)

setlocal enabledelayedexpansion

echo 🧪 NSE Momentum Lab - Phase 1 Dashboard Tests (Playwright CLI)
echo ================================================================
echo.

REM Check if services are running
echo 📡 Checking if services are running...

curl -s http://localhost:8501 >nul 2>&1
if errorlevel 1 (
    echo ❌ Dashboard not running at http://localhost:8501
    echo    Start it with: doppler run -- uv run nseml-dashboard
    echo.
    set /p START_DASH="Start dashboard now? (y/n): "
    if /i "!START_DASH!"=="y" (
        echo Starting dashboard...
        start "" doppler run -- uv run nseml-dashboard
        echo Waiting 10 seconds for dashboard to start...
        timeout /t 10 /nobreak >nul
    ) else (
        exit /b 1
    )
)

curl -s http://localhost:8004/health >nul 2>&1
if errorlevel 1 (
    echo ❌ API not running at http://localhost:8004
    echo    Start it with: doppler run -- uv run nseml-api
    echo.
    set /p START_API="Start API now? (y/n): "
    if /i "!START_API!"=="y" (
        echo Starting API...
        start "" doppler run -- uv run nseml-api
        echo Waiting 5 seconds for API to start...
        timeout /t 5 /nobreak >nul
    ) else (
        exit /b 1
    )
)

echo ✅ Services are running!
echo.

REM Run tests
echo 🚀 Running tests...
echo.

set TESTS_DIR=%~dp0
set PASSED=0
set FAILED=0

REM Test 1: Home Page
echo ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo Test 1: Home Page Navigation Cards
echo ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
playwright-cli code "%TESTS_DIR%test-home-page.mjs"
if errorlevel 1 (
    set /a FAILED+=1
) else (
    set /a PASSED+=1
)
echo.

REM Test 2: Experiments Page
echo ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo Test 2: Experiments Page Improvements
echo ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
playwright-cli code "%TESTS_DIR%test-experiments-page.mjs"
if errorlevel 1 (
    set /a FAILED+=1
) else (
    set /a PASSED+=1
)
echo.

REM Test 3: Scans Page
echo ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo Test 3: Scans Page Improvements
echo ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
playwright-cli code "%TESTS_DIR%test-scans-page.mjs"
if errorlevel 1 (
    set /a FAILED+=1
) else (
    set /a PASSED+=1
)
echo.

REM Test 4: Chat Page
echo ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
echo Test 4: Chat Page Confirmation Dialog
echo ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
playwright-cli code "%TESTS_DIR%test-chat-page.mjs"
if errorlevel 1 (
    set /a FAILED+=1
) else (
    set /a PASSED+=1
)
echo.

REM Summary
echo ================================================================
echo 📊 Test Results Summary
echo ================================================================
echo ✅ Passed: %PASSED%
echo ❌ Failed: %FAILED%
echo 📈 Total:  %PASSED%
echo.

if %FAILED%==0 (
    echo 🎉 All tests passed!
    exit /b 0
) else (
    echo ⚠️  Some tests failed - please review the output above
    exit /b 1
)
