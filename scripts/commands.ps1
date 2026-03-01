# nse-momentum-lab PowerShell Commands Module
# This file provides canonical commands for Windows development workflow.
# Usage: Import-Module ./scripts/commands.ps1 or dot-source: . ./scripts/commands.ps1

# =============================================================================
# Day-1 Setup Commands
# =============================================================================

function Invoke-Setup {
    <#
    .SYNOPSIS
        Installs dependencies and starts local Docker services.
    .DESCRIPTION
        Runs uv sync to create virtual environment and install dependencies,
        then starts Docker Compose services (Postgres, MinIO).
    .EXAMPLE
        Invoke-Setup
    #>
    Write-Host "Installing dependencies..." -ForegroundColor Cyan
    uv sync
    Write-Host "Starting Docker services..." -ForegroundColor Cyan
    doppler run -- docker compose up -d
}

function Invoke-InstallDeps {
    <#
    .SYNOPSIS
        Installs Python dependencies using uv.
    #>
    uv sync
}

function Invoke-StartDocker {
    <#
    .SYNOPSIS
        Starts local Docker services (Postgres, MinIO).
    #>
    doppler run -- docker compose up -d
}

function Invoke-StopDocker {
    <#
    .SYNOPSIS
        Stops all Docker services.
    #>
    docker compose down
}

function Invoke-RebuildDocker {
    <#
    .SYNOPSIS
        Stops Docker services, removes volumes, and restarts.
    .DESCRIPTION
        Use this for a clean database state. WARNING: This deletes all local data.
    #>
    docker compose down -v
    Invoke-StartDocker
}

# =============================================================================
# Test & Quality Commands
# =============================================================================

function Invoke-Test {
    <#
    .SYNOPSIS
        Runs all tests with Doppler-injected environment.
    #>
    doppler run -- uv run pytest -q
}

function Invoke-TestVerbose {
    <#
    .SYNOPSIS
        Runs tests with verbose output.
    #>
    doppler run -- uv run pytest -v
}

function Invoke-Lint {
    <#
    .SYNOPSIS
        Runs Ruff linter on the codebase.
    #>
    uv run ruff check .
}

function Invoke-LintFix {
    <#
    .SYNOPSIS
        Runs Ruff linter with auto-fix.
    #>
    uv run ruff check --fix .
}

function Invoke-Typecheck {
    <#
    .SYNOPSIS
        Runs mypy type checker.
    #>
    uv run mypy src tests
}

function Invoke-QualityCheck {
    <#
    .SYNOPSIS
        Runs all quality checks: lint, typecheck, and tests.
    #>
    Invoke-Lint
    Invoke-Typecheck
    Invoke-Test
}

# =============================================================================
# Application Commands
# =============================================================================

function Invoke-StartApi {
    <#
    .SYNOPSIS
        Starts the FastAPI server.
    #>
    doppler run -- uv run nseml-api
}

function Invoke-StartDashboard {
    <#
    .SYNOPSIS
        Starts the Streamlit dashboard.
    #>
    doppler run -- uv run nseml-dashboard
}

function Invoke-RunApi {
    <#
    .SYNOPSIS
        Runs API tests (health check).
    #>
    uv run pytest tests/test_health.py -v
}

# =============================================================================
# Documentation Commands
# =============================================================================

function Get-DevCommands {
    <#
    .SYNOPSIS
        Displays all available development commands.
    #>
    Write-Host "nse-momentum-lab Development Commands" -ForegroundColor Green
    Write-Host ""
    Write-Host "Setup:" -ForegroundColor Yellow
    Write-Host "  Invoke-Setup          - Full setup: install deps + start Docker"
    Write-Host "  Invoke-InstallDeps    - Install Python dependencies"
    Write-Host "  Invoke-StartDocker    - Start Postgres & MinIO"
    Write-Host "  Invoke-StopDocker     - Stop all Docker services"
    Write-Host "  Invoke-RebuildDocker  - Restart with fresh volumes (WARNING: deletes data)"
    Write-Host ""
    Write-Host "Quality:" -ForegroundColor Yellow
    Write-Host "  Invoke-Test           - Run tests"
    Write-Host "  Invoke-Lint           - Run Ruff linter"
    Write-Host "  Invoke-LintFix        - Run Ruff with auto-fix"
    Write-Host "  Invoke-Typecheck      - Run mypy type checker"
    Write-Host "  Invoke-QualityCheck   - Run lint, typecheck, and tests"
    Write-Host ""
    Write-Host "Applications:" -ForegroundColor Yellow
    Write-Host "  Invoke-StartApi       - Start FastAPI server"
    Write-Host "  Invoke-StartDashboard - Start Streamlit dashboard"
    Write-Host ""
    Write-Host "Direct uv commands:" -ForegroundColor Yellow
    Write-Host "  uv run pytest -q"
    Write-Host "  uv run ruff check ."
    Write-Host "  uv run mypy src tests"
    Write-Host "  doppler run -- uv run <command>"
}

# Export functions for module usage
Export-ModuleMember -Function @(
    'Invoke-Setup',
    'Invoke-InstallDeps',
    'Invoke-StartDocker',
    'Invoke-StopDocker',
    'Invoke-RebuildDocker',
    'Invoke-Test',
    'Invoke-TestVerbose',
    'Invoke-Lint',
    'Invoke-LintFix',
    'Invoke-Typecheck',
    'Invoke-QualityCheck',
    'Invoke-StartApi',
    'Invoke-StartDashboard',
    'Invoke-RunApi',
    'Get-DevCommands'
)
