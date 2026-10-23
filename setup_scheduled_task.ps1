<#
  One-time setup for the nightly run. Run it once from the project folder:
    powershell -ExecutionPolicy Bypass -File .\setup_scheduled_task.ps1

  It does two things:
    1. Saves your data.gov.in API key as a Windows user environment variable, so the
       scheduled task can use it without you typing it. You paste it into a hidden
       prompt, so it never appears on screen or in PowerShell's command history.
    2. Creates (or updates) a Task Scheduler task that runs daily_run.ps1 every night.

  Run it again at any time to change the key or the time; it replaces the old settings.
#>

$ProjectDir = $PSScriptRoot                          # the folder this script is in
$TaskName   = "Mandi Price Intelligence - daily run"
$RunAt      = "21:00"                                # 9 pm every day

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

# ---------- 2. Create the scheduled task ----------
# What to run: PowerShell, running daily_run.ps1 from the project folder.
#   -NoProfile                skip personal PowerShell settings, so runs behave the same every time
#   -ExecutionPolicy Bypass   allow this one script to run, without changing Windows' setting
#   -WindowStyle Minimized    the window appears minimised in the taskbar, so you can see it's running
$action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Minimized -File `"$ProjectDir\daily_run.ps1`"" `
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
    -Description "Downloads mandi prices from data.gov.in and uploads finished days to Databricks. Project: $ProjectDir" | Out-Null

Write-Host "Scheduled task '$TaskName' created: every day at $RunAt." -ForegroundColor Green
Write-Host "Logs will appear in $ProjectDir\logs"
Write-Host "To run it now:   Start-ScheduledTask -TaskName '$TaskName'"
