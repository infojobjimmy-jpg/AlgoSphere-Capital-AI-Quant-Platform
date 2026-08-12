#Requires -Version 5.1
<#
.SYNOPSIS
  Validate the existing AlgoSphere News Guard deployment before enabling live trading/n8n.

.EXAMPLE
  .\infra\scripts\news-guard-preflight.ps1 -ApiBase "http://localhost:8080" -AdminKey $env:API_ADMIN_KEY
#>
param(
  [string]$ApiBase = "http://localhost:8080",
  [string]$AdminKey = $env:API_ADMIN_KEY,
  [switch]$RefreshCalendar,
  [switch]$TestBriefDryRun
)

$ErrorActionPreference = "Stop"
$ApiBase = $ApiBase.TrimEnd("/")

function Invoke-JsonGet {
  param([string]$Url, [hashtable]$Headers = @{})
  return Invoke-RestMethod -Method Get -Uri $Url -Headers $Headers -TimeoutSec 20
}

function Invoke-JsonPost {
  param([string]$Url, [hashtable]$Headers = @{})
  return Invoke-RestMethod -Method Post -Uri $Url -Headers $Headers -TimeoutSec 30
}

Write-Host "==> AlgoSphere News Guard preflight"
Write-Host "API: $ApiBase"

Write-Host "==> Checking API health"
$health = Invoke-JsonGet "$ApiBase/news-guard/health"
$health | ConvertTo-Json -Depth 8 | Write-Host

if ($RefreshCalendar) {
  if ([string]::IsNullOrWhiteSpace($AdminKey)) {
    throw "API_ADMIN_KEY is required for -RefreshCalendar"
  }
  Write-Host "==> Refreshing economic calendar"
  $headers = @{ "x-admin-key" = $AdminKey }
  $refresh = Invoke-JsonPost "$ApiBase/news-guard/refresh" $headers
  $refresh | ConvertTo-Json -Depth 8 | Write-Host
  $health = Invoke-JsonGet "$ApiBase/news-guard/health"
}

if ($TestBriefDryRun) {
  if ([string]::IsNullOrWhiteSpace($AdminKey)) {
    throw "API_ADMIN_KEY is required for -TestBriefDryRun"
  }
  Write-Host "==> Testing brief formatter without sending Discord/SMS"
  $headers = @{ "x-admin-key" = $AdminKey }
  $dry = Invoke-JsonPost "$ApiBase/news-guard/notify?kind=brief&sms=true&dry_run=true" $headers
  $dry | ConvertTo-Json -Depth 8 | Write-Host
}

Write-Host "==> Checking XAUUSD guard state"
$xau = Invoke-JsonGet "$ApiBase/news-guard/status?symbol=XAUUSD"
Write-Host ("XAUUSD: status={0} risk={1} reason={2}" -f $xau.status, $xau.risk, $xau.reason)

Write-Host "==> Checking XAGUSD guard state"
$xag = Invoke-JsonGet "$ApiBase/news-guard/status?symbol=XAGUSD"
Write-Host ("XAGUSD: status={0} risk={1} reason={2}" -f $xag.status, $xag.risk, $xag.reason)

$warnings = @()
if (-not $health.enabled) { $warnings += "News Guard is disabled" }
if (-not $health.calendar.source_configured) { $warnings += "Economic calendar source is not configured" }
if (-not $health.calendar.last_refresh) { $warnings += "Economic calendar has never been refreshed" }
if (-not $health.notifications.discord_configured) { $warnings += "Discord webhook is not configured" }
if (-not $health.mt5.token_configured) { $warnings += "MT5_API_TOKEN is not configured" }
if (-not $health.mt5.bridge_url_configured) { $warnings += "MT5 bridge URL is not configured" }

if ($warnings.Count -gt 0) {
  Write-Host "==> PRE-FLIGHT NOT READY"
  foreach ($warning in $warnings) { Write-Host "  - $warning" }
  exit 2
}

if (-not $health.ready) {
  Write-Host "==> PRE-FLIGHT BLOCKED: News Guard reports not ready"
  exit 3
}

Write-Host "==> PRE-FLIGHT READY"
Write-Host "News Guard calendar, Discord and MT5 prerequisites are configured."
if (-not $health.notifications.sms_configured) {
  Write-Host "SMS remains optional and is currently not configured."
}
exit 0
