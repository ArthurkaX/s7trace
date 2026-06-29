$ErrorActionPreference = "Stop"

$PidFile = Join-Path (Resolve-Path ".").Path "target\snap7-sim.pid"
if (-not (Test-Path -LiteralPath $PidFile)) {
    Write-Host "No Snap7 simulation pid file found."
    exit 0
}

$pidText = (Get-Content -LiteralPath $PidFile -Raw).Trim()
if (-not $pidText) {
    Remove-Item -LiteralPath $PidFile -Force -ErrorAction SilentlyContinue
    exit 0
}

$proc = Get-Process -Id ([int]$pidText) -ErrorAction SilentlyContinue
if ($proc) {
    Write-Host "Stopping Snap7 simulation pid=$pidText"
    Stop-Process -Id $proc.Id -Force
}

Remove-Item -LiteralPath $PidFile -Force -ErrorAction SilentlyContinue
