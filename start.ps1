Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Always run from repo folder (avoids mapping/cache path issues)
Set-Location $PSScriptRoot

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " VR-TS Bonepile Statistics Dashboard" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Starting server..." -ForegroundColor Green
Write-Host ""

# Use py launcher if available; fall back to python
try {
  py app.py
} catch {
  python app.py
}

Write-Host ""
Write-Host "Press Enter to close..." -ForegroundColor DarkGray
[void] (Read-Host)

