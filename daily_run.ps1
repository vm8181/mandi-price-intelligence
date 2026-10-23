<#
  Mandi Price Intelligence: nightly run on this laptop.

  What it does, in order:
    1. Download: fills every missing day of the last 10 years, re-pulls the last week
       (mandis upload late) and continues the backfill if it isn't finished.
       Finished days are skipped in a fraction of a second.
    2. Upload: sends every newly finished day to the Databricks volume and verifies it.
    3. Writes everything it prints to a log file in .\logs, one file per run,
       and deletes logs older than 30 days.

  Started by Windows Task Scheduler (see setup_scheduled_task.ps1), or by hand:
    powershell -ExecutionPolicy Bypass -File .\daily_run.ps1

  Safe to stop at any time (shutdown, Ctrl+C): the next run continues where this one stopped.
#>

# Run from the folder this script lives in, so relative paths like data\raw\mandi work
# no matter where Task Scheduler starts it.
Set-Location -Path $PSScriptRoot

# Keep going if one step fails, so the upload still runs after a partly failed download.
$ErrorActionPreference = "Continue"

# Python settings: print output immediately (so the log fills as it runs, not at the end),
# and use UTF-8 so names with special characters don't break the log.
$env:PYTHONUNBUFFERED = "1"
$env:PYTHONIOENCODING = "utf-8"

# One log file per run, e.g. logs\daily_run_2026-10-23_2100.log
New-Item -ItemType Directory -Force -Path "logs" | Out-Null
$log = Join-Path "logs" ("daily_run_{0}.log" -f (Get-Date -Format "yyyy-MM-dd_HHmm"))

# Show a line on screen and append it to the log (UTF-8, so Notepad and VS Code read it cleanly).
# Write-Host rather than plain output: in PowerShell, anything a function outputs becomes
# part of its return value, which would mix log lines into the exit codes below.
function Write-Line([string]$text) {
    Write-Host $text
    Add-Content -Path $log -Value $text -Encoding UTF8
}

function Write-Log([string]$text) {
    Write-Line ("{0}  {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $text)
}

# Run a Python script, copying everything it prints into the log. Returns only its exit code.
function Invoke-Step([string]$name, [string[]]$arguments) {
    Write-Log "----- ${name}: python $($arguments -join ' ')"
    & python @arguments 2>&1 | ForEach-Object { Write-Line "$_" }
    $code = $LASTEXITCODE
    Write-Log "----- $name finished with exit code $code"
    return $code
}

Write-Log "Daily run started on $env:COMPUTERNAME"

# The API key comes from a Windows user environment variable, saved once by
# setup_scheduled_task.ps1. It is never written into this script or the log.
if (-not $env:DATAGOV_API_KEY) {
    $env:DATAGOV_API_KEY = [Environment]::GetEnvironmentVariable("DATAGOV_API_KEY", "User")
}
if (-not $env:DATAGOV_API_KEY) {
    Write-Log "STOPPED: DATAGOV_API_KEY is not set. Run setup_scheduled_task.ps1 to save it."
    exit 1
}

$download = Invoke-Step "Download" @("backfill_history.py", "--last-years", "10", "--all-commodities")
$upload   = Invoke-Step "Upload"   @("upload_to_databricks.py")

# Housekeeping: keep 30 days of logs.
Get-ChildItem "logs\daily_run_*.log" |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-30) } |
    Remove-Item -ErrorAction SilentlyContinue

if ($download -eq 0 -and $upload -eq 0) {
    Write-Log "Daily run finished: OK"
    exit 0
} else {
    Write-Log "Daily run finished WITH PROBLEMS (download exit $download, upload exit $upload). See above."
    exit 1
}
