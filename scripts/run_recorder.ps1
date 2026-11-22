# Keeps the live market-data recorder running: restarts it after a crash or exit.
# Usage (repo root or anywhere):  .\scripts\run_recorder.ps1
# Stop: Ctrl+C in this window (stops the recorder cleanly and ends the loop).
# Health: uv run python -m binance_futures_recorder.health
param(
    [string[]]$Symbols = @('BTCUSDT', 'ETHUSDT'),
    [int]$RestartDelaySeconds = 10
)
$ErrorActionPreference = 'Continue'
Set-Location (Split-Path -Parent $PSScriptRoot)

if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    $uvDir = Join-Path $env:USERPROFILE '.local\bin'
    if (Test-Path (Join-Path $uvDir 'uv.exe')) { $env:Path = "$uvDir;$env:Path" }
    else { Write-Error 'uv not found on PATH; install it: https://docs.astral.sh/uv/'; exit 1 }
}

$logDir = 'data\live\logs'
New-Item -ItemType Directory -Force $logDir | Out-Null
$log = Join-Path $logDir 'run_recorder.log'

function Write-Log([string]$message) {
    $line = "$((Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')) $message"
    Add-Content -Path $log -Value $line -Encoding utf8
    Write-Host $line
}

$restarts = 0
while ($true) {
    Write-Log "start recorder (restart #$restarts) symbols=$($Symbols -join ',')"
    & uv run python -m binance_futures_recorder --symbols @Symbols
    Write-Log "recorder exited with code $LASTEXITCODE; restarting in $RestartDelaySeconds s"
    $restarts++
    Start-Sleep -Seconds $RestartDelaySeconds
}
