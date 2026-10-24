<#
  One command to see what's happening:
    powershell -ExecutionPolicy Bypass -File .\status.ps1

  Shows: download and upload progress, gaps and failures, whether a run is going now,
  both scheduled tasks' last and next runs, and the end of the latest download and upload logs.
#>
Set-Location -Path $PSScriptRoot
$Tasks = @("Mandi Price Intelligence - daily run", "Mandi Price Intelligence - upload")

# 1. Data on this laptop (progress, gaps, failures, running now)
python status.py

# 2. The scheduled tasks
foreach ($TaskName in $Tasks) {
    Write-Host ""
    Write-Host "Scheduled task: $TaskName" -ForegroundColor Cyan
    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if (-not $task) {
        Write-Host "  Not set up yet. Run setup_scheduled_task.ps1."
        continue
    }
    $info = Get-ScheduledTaskInfo -TaskName $TaskName
    # Task Scheduler reports results as numbers; translate the common ones.
    $result = switch ($info.LastTaskResult) {
        0        { "succeeded" }
        1        { "finished with problems (see the log)" }
        267009   { "running now" }
        267011   { "has never run" }
        3221225786 { "stopped before finishing (window closed or laptop shut down); the next run continues" }
        2147946720 { "a start was skipped because the previous run was still going" }
        default  { "code $($info.LastTaskResult)" }
    }
    Write-Host ("  State:     {0}" -f $task.State)
    Write-Host ("  Last run:  {0}, {1}" -f $info.LastRunTime, $result)
    Write-Host ("  Next run:  {0}" -f $info.NextRunTime)
}

# 3. The end of the most recent download and upload logs
foreach ($kind in @("download", "upload")) {
    Write-Host ""
    $pattern = if ($kind -eq "download") { "logs\download_*.log", "logs\daily_run_*.log", "logs\all_*.log" } else { "logs\upload_*.log" }
    $latest = Get-ChildItem $pattern -ErrorAction SilentlyContinue | Sort-Object LastWriteTime | Select-Object -Last 1
    if (-not $latest) {
        Write-Host "No $kind log yet." -ForegroundColor Cyan
        continue
    }
    # Call out known problems directly, so they aren't missed in the tail of the log.
    $text = Get-Content $latest.FullName -Raw
    if ($text -match "API key was refused") {
        Write-Host "PROBLEM: the latest run's data.gov.in API key was refused (HTTP 403), so nothing was downloaded." -ForegroundColor Red
        Write-Host "         Get your current key from data.gov.in > My Account, then run setup_scheduled_task.ps1 again." -ForegroundColor Red
    }
    if ($text -match "Internet was down") {
        Write-Host "NOTE: the latest run stopped because the internet was down for 30 minutes." -ForegroundColor Yellow
    }
    Write-Host "Latest $kind log: $($latest.Name) (last 6 lines)" -ForegroundColor Cyan
    Get-Content $latest.FullName -Tail 6 | ForEach-Object { Write-Host "  $_" }
}
