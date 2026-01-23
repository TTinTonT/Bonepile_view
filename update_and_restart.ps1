param(
  [string]$Remote = "origin",
  [string]$Branch = "main",
  [string]$Entry = "daily_test_app.py",
  [switch]$ForceRestart
)

$ErrorActionPreference = "Stop"

function Write-Log([string]$msg) {
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  Write-Output "[$ts] $msg"
}

function Get-RepoRoot() {
  # Prefer built-in script path variables (more reliable than $MyInvocation in wrappers)
  if ($PSScriptRoot) { return $PSScriptRoot }
  if ($PSCommandPath) { return (Split-Path -Parent $PSCommandPath) }
  return (Get-Location).Path
}

function Get-TargetProcesses([string]$entryName) {
  # Only match processes that have the script name in the command line.
  # This avoids killing unrelated python sessions.
  try {
    return Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match [regex]::Escape($entryName) }
  } catch {
    return @()
  }
}

$repo = Get-RepoRoot
Set-Location $repo

Write-Log "Repo: $repo"
Write-Log "Remote/branch: $Remote/$Branch"
Write-Log "Entry: $Entry"

try {
  if (-not $ForceRestart) {
    Write-Log "Fetching updates..."
    git fetch $Remote | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "git fetch failed (exit=$LASTEXITCODE)" }

    $local  = (git rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0) { throw "git rev-parse HEAD failed (exit=$LASTEXITCODE)" }
    $remote = (git rev-parse "$Remote/$Branch").Trim()
    if ($LASTEXITCODE -ne 0) { throw "git rev-parse $Remote/$Branch failed (exit=$LASTEXITCODE)" }

    if ($local -eq $remote) {
      Write-Log "No new commit. Do nothing."
      exit 0
    }

    Write-Log "New commit detected. Updating..."
    git pull --ff-only $Remote $Branch | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "git pull failed (exit=$LASTEXITCODE)" }
  } else {
    Write-Log "ForceRestart enabled (skip git check)."
  }

  $procs = Get-TargetProcesses $Entry
  if ($procs.Count -gt 0) {
    foreach ($p in $procs) {
      Write-Log ("Stopping PID={0} Name={1}" -f $p.ProcessId, $p.Name)
      Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    }
  } else {
    Write-Log "No running process found for $Entry"
  }

  Write-Log "Starting: py $Entry"
  Start-Process -WorkingDirectory $repo -FilePath "py" -ArgumentList $Entry -WindowStyle Minimized | Out-Null

  Write-Log "Done."
  exit 0
} catch {
  Write-Log ("ERROR: {0}" -f $_.Exception.Message)
  exit 1
}

