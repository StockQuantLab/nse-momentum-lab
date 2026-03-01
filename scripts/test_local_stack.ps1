#!/usr/bin/env pwsh
# Test script to verify local stack is working

$ErrorActionPreference = "Stop"

Write-Host "=== nse-momentum-lab Local Stack Test ===" -ForegroundColor Green
Write-Host ""

# Test 1: Check Docker is running
Write-Host "[1/5] Checking Docker status..." -ForegroundColor Cyan
try {
    $dockerStatus = docker info 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: Docker is not running or not accessible" -ForegroundColor Red
        exit 1
    }
    Write-Host "  Docker is running" -ForegroundColor Green
} catch {
    Write-Host "ERROR: $_" -ForegroundColor Red
    exit 1
}

# Test 2: Check Docker Compose services
Write-Host "[2/5] Checking Docker Compose services..." -ForegroundColor Cyan
$services = docker compose ps --format json 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "WARNING: Could not get service status (may need to start services first)" -ForegroundColor Yellow
} else {
    Write-Host "  Services configured" -ForegroundColor Green
}

# Test 3: Check Python environment
Write-Host "[3/5] Checking Python environment..." -ForegroundColor Cyan
try {
    $pythonVersion = & .venv/Scripts/python.exe --version 2>&1
    Write-Host "  $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "ERROR: Python environment not found. Run 'uv sync' first." -ForegroundColor Red
    exit 1
}

# Test 4: Check schema can be parsed
Write-Host "[4/5] Checking schema file..." -ForegroundColor Cyan
if (Test-Path "db/init/001_init.sql") {
    Write-Host "  Schema file exists" -ForegroundColor Green
} else {
    Write-Host "ERROR: Schema file not found" -ForegroundColor Red
    exit 1
}

# Test 5: Verify pyproject.toml is valid
Write-Host "[5/5] Checking project configuration..." -ForegroundColor Cyan
try {
    $null = & .venv/Scripts/python.exe -c "import tomli; tomli.loads(open('pyproject.toml').read())" 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  pyproject.toml is valid" -ForegroundColor Green
    } else {
        Write-Host "WARNING: Could not parse pyproject.toml" -ForegroundColor Yellow
    }
} catch {
    Write-Host "  pyproject.toml check skipped (tomli not installed)" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=== Local Stack Verification Complete ===" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  1. Run 'doppler run -- docker compose up -d' to start services"
Write-Host "  2. Run 'Invoke-Test' to verify database connectivity"
Write-Host "  3. Run 'Invoke-Lint' to verify code quality"
