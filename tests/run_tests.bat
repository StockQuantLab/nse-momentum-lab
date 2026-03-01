@echo off
REM Run Phase 1 Dashboard Tests - Windows

echo 🧪 NSE Momentum Lab - Phase 1 Dashboard Tests
echo ================================================
echo.

REM Check if services are running
echo 📡 Checking services...

curl -s http://localhost:8501 >nul 2>&1
if errorlevel 1 (
    echo ❌ Dashboard not running at http://localhost:8501
    echo    Start it with: doppler run -- uv run nseml-dashboard
    exit /b 1
)

curl -s http://localhost:8004/health >nul 2>&1
if errorlevel 1 (
    echo ❌ API not running at http://localhost:8004
    echo    Start it with: doppler run -- uv run nseml-api
    exit /b 1
)

echo ✅ All services running
echo.

REM Run tests
echo 🚀 Running Playwright tests...
echo.

cd /d %~dp0\..

python tests\test_dashboard_phase1.py

echo.
echo ✅ Tests complete!
pause
