param(
    [Parameter(Mandatory = $true)]
    [string]$SessionId,

    [Parameter(Mandatory = $true)]
    [string]$TradeDate,

    [Parameter(Mandatory = $true)]
    [string]$Strategy,

    [Parameter(Mandatory = $true)]
    [double]$BreakoutThreshold,

    [string]$OutLog = "",
    [string]$ErrLog = "",

    [switch]$AllSymbols,
    [switch]$Watchlist,
    [switch]$Run,
    [switch]$Foreground
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$doppler = (Get-Command doppler).Source
$thresholdJson = ('{{"breakout_threshold":{0}}}' -f $BreakoutThreshold.ToString([System.Globalization.CultureInfo]::InvariantCulture))

$cmdArgs = @(
    "run",
    "--",
    "uv",
    "run",
    "nseml-paper",
    "daily-live",
    "--session-id",
    $SessionId,
    "--trade-date",
    $TradeDate,
    "--strategy",
    $Strategy,
    "--strategy-params",
    $thresholdJson
)

if ($AllSymbols) {
    $cmdArgs += "--all-symbols"
}
if ($Watchlist) {
    $cmdArgs += "--watchlist"
}
if ($Run) {
    $cmdArgs += "--run"
}

if ($Foreground) {
    Push-Location $repoRoot
    try {
        & $doppler @cmdArgs
    }
    finally {
        Pop-Location
    }
    return
}

$startParams = @{
    FilePath = $doppler
    ArgumentList = $cmdArgs
    WorkingDirectory = $repoRoot
    PassThru = $true
}

if ($OutLog) {
    $startParams.RedirectStandardOutput = $OutLog
}
if ($ErrLog) {
    $startParams.RedirectStandardError = $ErrLog
}

$proc = Start-Process @startParams
[pscustomobject]@{
    pid = $proc.Id
    session_id = $SessionId
    strategy = $Strategy
    breakout_threshold = $BreakoutThreshold
    run = [bool]$Run
    out_log = $OutLog
    err_log = $ErrLog
}
