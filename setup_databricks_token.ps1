<#
  Save (or replace) the Databricks token used by the CLI and by upload_to_databricks.py.
    powershell -ExecutionPolicy Bypass -File .\setup_databricks_token.ps1

  Why a script instead of "databricks configure": pasting into a terminal can add an
  invisible line break or space, which breaks every request with
  "invalid header field value for Authorization". This script removes all such
  characters, checks the token really is a Databricks token, saves it, and tests it.
#>

$WorkspaceUrl = "https://dbc-4ffdcd96-75b3.cloud.databricks.com"   # base address only, no /browse or ?o=
$ConfigFile   = Join-Path $HOME ".databrickscfg"                   # where the CLI and SDK read it from

# Hidden prompt: the token isn't shown on screen or saved in PowerShell's history.
$secure = Read-Host "Paste your Databricks token (starts with dapi) and press Enter" -AsSecureString
$bstr   = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
$plain  = [Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr)
[Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)

# Remove every whitespace character anywhere: spaces, tabs, line breaks from pasting.
$token = $plain -replace '\s', ''

# Check it's the right kind of secret before saving anything.
if (-not $token.StartsWith("dapi")) {
    Write-Host "That isn't a Databricks token (they start with 'dapi'). Nothing was saved." -ForegroundColor Red
    if ($token.Length -eq 56) { Write-Host "It looks like the data.gov.in key; that one belongs in setup_scheduled_task.ps1." -ForegroundColor Red }
    exit 1
}
if ($token.Length -lt 30 -or $token.Length -gt 60) {
    Write-Host "A Databricks token is normally about 36 characters; this is $($token.Length). Nothing was saved." -ForegroundColor Red
    exit 1
}

# Save in the format the CLI and SDK expect. ASCII: no hidden byte-order mark at the start.
$content = "[DEFAULT]`r`nhost  = $WorkspaceUrl`r`ntoken = $token`r`n"
Set-Content -Path $ConfigFile -Value $content -Encoding ASCII -NoNewline
Write-Host "Token saved ($($token.Length) characters) to $ConfigFile" -ForegroundColor Green

# Test the connection straight away.
Write-Host "Testing the connection ..."
databricks current-user me
if ($LASTEXITCODE -eq 0) {
    Write-Host "Connected. Uploads will work." -ForegroundColor Green
} else {
    Write-Host "The token was saved but Databricks refused it. Check it's the newest token and has the scopes files, identity and unity-catalog." -ForegroundColor Red
}
