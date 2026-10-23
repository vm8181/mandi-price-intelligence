<#
  One command to see what's happening:
    powershell -ExecutionPolicy Bypass -File .\status.ps1

  Shows: download and upload progress, gaps and failures, whether a run is going now,
  the scheduled task's last and next run, and the end of the latest log.
#>
Set-Location -Path $PSScriptRoot
$TaskName = "Mandi Price Intelligence - daily run"

# 1. Data on this laptop (progress, gaps, failures, running now)
python status.py

# 2. The scheduled task
Write-Host ""
Write-Host "Scheduled task" -ForegroundColor Cyan
$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if (-not $task) {
    Write-Host "  Not set up yet. Run setup_scheduled_task.ps1."
} else {
    $info = Get-ScheduledTaskInfo -TaskName $TaskName
    # Task Scheduler reports results as numbers; translate the common ones.
    $result = switch ($info.LastTaskResult) {
        0        { "succeeded" }
        1        { "finished with problems (see the log)" }
        267009   { "running now" }
        267011   { "has never run" }
        3221225786 { "stopped before finishing (window closed or laptop shut down); the next run continues" }
        default  { "code $($info.LastTaskResult)" }
    }
    Write-Host ("  State:     {0}" -f $task.State)
    Write-Host ("  Last run:  {0}, {1}" -f $info.LastRunTime, $result)
    Write-Host ("  Next run:  {0}" -f $info.NextRunTime)
}

# 3. The end of the most recent log
Write-Host ""
$latest = Get-ChildItem "logs\daily_run_*.log" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime | Select-Object -Last 1
if ($latest) {
    # Call out known problems directly, so they aren't missed in the tail of the log.
    $text = Get-Content $latest.FullName -Raw
    if ($text -match "API key was refused") {
        Write-Host "PROBLEM: the latest run's data.gov.in API key was refused (HTTP 403), so nothing was downloaded." -ForegroundColor Red
        Write-Host "         Get your current key from data.gov.in > My Account, then run setup_scheduled_task.ps1 again." -ForegroundColor Red
        Write-Host ""
    }
    if ($text -match "Internet was down") {
        Write-Host "NOTE: the latest run stopped because the internet was down for 30 minutes." -ForegroundColor Yellow
        Write-Host ""
    }
    Write-Host "Latest log: $($latest.Name) (last 8 lines)" -ForegroundColor Cyan
    Get-Content $latest.FullName -Tail 8 | ForEach-Object { Write-Host "  $_" }
} else {
    Write-Host "No logs yet." -ForegroundColor Cyan
}
