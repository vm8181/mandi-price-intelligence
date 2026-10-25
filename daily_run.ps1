<#
  Mandi Price Intelligence: nightly run on this laptop.

  Steps (choose with -Step; the default, All, runs both in order):
    Download  fills every missing day of the history window (from $HistoryStart below to
              yesterday), re-pulls the last 30 days (mandis upload late), adds new days.
              Finished days are skipped in a fraction of a second.
    Upload    sends newly finished days, and new complete versions of recent days, to
              the Databricks volume and verifies them.

  Two scheduled tasks run the steps separately (see setup_scheduled_task.ps1):
  Download nightly, Upload every 2 hours. Separate, so a long backfill download never
  holds back the upload of days that are already finished.

  Everything printed goes to a log file in .\logs (one per run, named by step);
  logs older than 30 days are deleted.

  By hand:
    powershell -ExecutionPolicy Bypass -File .\daily_run.ps1                # both steps
    powershell -ExecutionPolicy Bypass -File .\daily_run.ps1 -Step Upload   # upload only

  Safe to stop at any time (shutdown, Ctrl+C): the next run continues where this one stopped.
#>

param(
    [ValidateSet("All", "Download", "Upload")]
    [string]$Step = "All"
)

# Run from the folder this script lives in, so relative paths like data\raw\mandi work
# no matter where Task Scheduler starts it.
Set-Location -Path $PSScriptRoot

# Keep going if one step fails, so the upload still runs after a partly failed download.
$ErrorActionPreference = "Continue"

# Python settings: print output immediately (so the log fills as it runs, not at the end),
# and use UTF-8 so names with special characters don't break the log.
$env:PYTHONUNBUFFERED = "1"
$env:PYTHONIOENCODING = "utf-8"

# One log file per run, e.g. logs\download_2026-10-23_2100.log
New-Item -ItemType Directory -Force -Path "logs" | Out-Null
$log = Join-Path "logs" ("{0}_{1}.log" -f $Step.ToLower(), (Get-Date -Format "yyyy-MM-dd_HHmm"))

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

Write-Log "Run started on $env:COMPUTERNAME (step: $Step)"
$download = 0
$upload = 0

if ($Step -in "All", "Download") {
    # The API key comes from a Windows user environment variable, saved once by
    # setup_scheduled_task.ps1. It is never written into this script or the log.
    if (-not $env:DATAGOV_API_KEY) {
        $env:DATAGOV_API_KEY = [Environment]::GetEnvironmentVariable("DATAGOV_API_KEY", "User")
    }
    if (-not $env:DATAGOV_API_KEY) {
        Write-Log "STOPPED: DATAGOV_API_KEY is not set. Run setup_scheduled_task.ps1 to save it."
        exit 1
    }
    # History window: from $HistoryStart to yesterday (decision 011). Each night fills any missing
    # day in the window, adds yesterday, and refreshes the last 30 days. To extend the history,
    # set an earlier date here and in status.py; finished days are skipped, so only new ones download.
    # --max-hours 20: a run started at 21:00 stops by 17:00, so the next night's run starts on time.
    $HistoryStart = "01-10-2025"
    $download = Invoke-Step "Download" @("backfill_history.py", "--start", $HistoryStart, "--all-commodities", "--max-hours", "20")
}

if ($Step -in "All", "Upload") {
    $upload = Invoke-Step "Upload" @("upload_to_databricks.py")
}

# Housekeeping: keep 30 days of logs.
Get-ChildItem "logs\*.log" |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-30) } |
    Remove-Item -ErrorAction SilentlyContinue

if ($download -eq 0 -and $upload -eq 0) {
    Write-Log "Run finished: OK"
    exit 0
} else {
    Write-Log "Run finished WITH PROBLEMS (download exit $download, upload exit $upload). See above."
    exit 1
}
