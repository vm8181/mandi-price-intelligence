<#
  One-time setup for the nightly run. Run it once from the project folder:
    powershell -ExecutionPolicy Bypass -File .\setup_scheduled_task.ps1

  It does three things:
    1. Saves your data.gov.in API key as a Windows user environment variable, so the
       scheduled tasks can use it without you typing it. You paste it into a hidden
       prompt, so it never appears on screen or in PowerShell's command history.
    2. Creates (or updates) the download task: daily_run.ps1 -Step Download, every night.
    3. Creates (or updates) the upload task: daily_run.ps1 -Step Upload, every 2 hours.
       Separate from the download so a long backfill never holds back finished days.

  Run it again at any time to change the key or the times; it replaces the old settings.
  Answer "n" when asked to replace the key if you only want to update the tasks.
#>

$ProjectDir = $PSScriptRoot                          # the folder this script is in
$TaskName       = "Mandi Price Intelligence - daily run"     # the download
$RunAt          = "21:00"                                    # 9 pm every day
$UploadTaskName = "Mandi Price Intelligence - upload"
$UploadEvery    = New-TimeSpan -Hours 2

# ---------- 1. Save the API key ----------
$existing = [Environment]::GetEnvironmentVariable("DATAGOV_API_KEY", "User")
$answer = "y"
if ($existing) {
    $answer = Read-Host "An API key is already saved. Replace it? (y/n)"
}
if ($answer -eq "y") {
    # -AsSecureString hides what you paste and keeps it out of the command history.
    $secure = Read-Host "Paste your data.gov.in API key and press Enter" -AsSecureString
    $bstr   = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
    $key    = [Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr).Trim()   # Trim drops stray spaces or line breaks
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    # Basic checks, so the wrong secret can't be saved by mistake.
    if ($key.StartsWith("dapi")) {
        Write-Host "That's a Databricks token (it starts with 'dapi'), not a data.gov.in key. Nothing was saved." -ForegroundColor Red
        Write-Host "Copy the key from data.gov.in > My Account instead." -ForegroundColor Red
        exit 1
    }
    if ($key.Length -ne 56) {
        Write-Host "A data.gov.in key is normally 56 characters; this one is $($key.Length). Nothing was saved." -ForegroundColor Red
        Write-Host "Copy it again from data.gov.in > My Account." -ForegroundColor Red
        exit 1
    }
    # "User" scope: stored in your Windows profile, readable by your account only,
    # and available to every program you start from now on, including scheduled tasks.
    [Environment]::SetEnvironmentVariable("DATAGOV_API_KEY", $key, "User")
    Write-Host "API key saved ($($key.Length) characters)." -ForegroundColor Green
}

# ---------- 2. Create the download task ----------
# What to run: PowerShell, running daily_run.ps1 from the project folder.
#   -NoProfile                skip personal PowerShell settings, so runs behave the same every time
#   -ExecutionPolicy Bypass   allow this one script to run, without changing Windows' setting
#   -WindowStyle Minimized    the window appears minimised in the taskbar, so you can see it's running
$action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Minimized -File `"$ProjectDir\daily_run.ps1`" -Step Download" `
    -WorkingDirectory $ProjectDir

# When: every day at $RunAt.
$trigger = New-ScheduledTaskTrigger -Daily -At $RunAt

# How:
#   -StartWhenAvailable          if the laptop was off at 9 pm, run as soon as it's next on
#   -MultipleInstances IgnoreNew if last night's run is still going, don't start a second one
#   -ExecutionTimeLimit 3 days   long enough to finish the 10-year backfill, but not forever
#   battery settings             still run, and keep running, on battery
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Days 3) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries

# Who: you, only while you're logged in. This avoids storing your Windows password;
# a run missed while you were logged out happens at your next login (StartWhenAvailable).
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings `
    -Principal $principal -Force `
    -Description "Downloads mandi prices from data.gov.in (backfill and daily refresh). Project: $ProjectDir" | Out-Null

Write-Host "Scheduled task '$TaskName' created: download every day at $RunAt." -ForegroundColor Green

# ---------- 3. Create the upload task ----------
# Same program, upload step only. Starts at the next even hour, then every 2 hours.
#   -MultipleInstances IgnoreNew   if an upload is still going, skip this one (the uploader's
#                                  own lock would stop it anyway)
#   -ExecutionTimeLimit 6 hours    a large backlog can take a while; a stuck run is stopped
$uploadAction = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Minimized -File `"$ProjectDir\daily_run.ps1`" -Step Upload" `
    -WorkingDirectory $ProjectDir
$now = Get-Date
$firstRun = $now.Date.AddHours($now.Hour - ($now.Hour % 2) + 2)     # the next even hour
$uploadTrigger = New-ScheduledTaskTrigger -Once -At $firstRun `
    -RepetitionInterval $UploadEvery -RepetitionDuration (New-TimeSpan -Days 3650)
$uploadSettings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 6) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries

Register-ScheduledTask -TaskName $UploadTaskName -Action $uploadAction -Trigger $uploadTrigger `
    -Settings $uploadSettings -Principal $principal -Force `
    -Description "Uploads finished days and new versions of recent days to Databricks. Project: $ProjectDir" | Out-Null

Write-Host "Scheduled task '$UploadTaskName' created: upload every 2 hours, first at $($firstRun.ToString('dd MMM HH:mm'))." -ForegroundColor Green
Write-Host "Logs will appear in $ProjectDir\logs"
Write-Host "To run now:   Start-ScheduledTask -TaskName '$TaskName'   or   '$UploadTaskName'"
