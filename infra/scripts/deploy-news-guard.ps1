#Requires -Version 5.1
<#
.SYNOPSIS
  Deploy the existing AlgoSphere stack with News Guard, then run production preflight.

.DESCRIPTION
  This script does NOT change TRADING_MODE and does NOT enable live trading.
  It validates the existing backend/.env, builds the current Docker Compose stack,
  starts the trading profile, waits for the API, refreshes the economic calendar,
  and runs the News Guard preflight. It exits non-zero if a required production
  prerequisite is missing.

.EXAMPLE
  .\infra\scripts\deploy-news-guard.ps1
#>
param(
  [string]$EnvFile = "backend/.env",
  [string]$ApiBase = "http://localhost:8080",
  [int]$ApiWaitAttempts = 30,
  [int]$ApiWaitSeconds = 4,
  [switch]$SkipBuild,
  [switch]$SkipTradingProfile
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root
$ApiBase = $ApiBase.TrimEnd("/")

function Read-DotEnv {
  param([string]$Path)
  $map = @{}
  foreach ($line in Get-Content $Path) {
    $trimmed = $line.Trim()
    if (-not $trimmed -or $trimmed.StartsWith("#") -or -not $trimmed.Contains("=")) { continue }
    $parts = $trimmed.Split("=", 2)
    $map[$parts[0].Trim()] = $parts[1].Trim()
  }
  return $map
}

function Require-EnvValue {
  param([hashtable]$Values, [string]$Name)
  if (-not $Values.ContainsKey($Name) -or [string]::IsNullOrWhiteSpace([string]$Values[$Name])) {
    throw "Missing required production value in ${EnvFile}: $Name"
  }
}

if (-not (Test-Path $EnvFile)) {
  throw "Production env file not found: $EnvFile"
}

Write-Host "==> Reading existing AlgoSphere production configuration"
$envValues = Read-DotEnv $EnvFile
$required = @(
  "API_ADMIN_KEY",
  "FINNHUB_API_KEY",
  "MT5_API_TOKEN",
  "MT5_BRIDGE_URL",
  "NEWS_GUARD_DISCORD_WEBHOOK_URL"
)
foreach ($name in $required) { Require-EnvValue $envValues $name }

if ($envValues.ContainsKey("TRADING_MODE")) {
  Write-Host ("TRADING_MODE remains unchanged: {0}" -f $envValues["TRADING_MODE"])
}
Write-Host "This deploy script will not switch paper/live mode."

$composeArgs = @("-f", "docker-compose.yml", "-f", "docker-compose.prod.yml")
if (-not $SkipTradingProfile) {
  $composeArgs += @("--profile", "trading")
}

if (-not $SkipBuild) {
  Write-Host "==> Building current AlgoSphere containers"
  & docker compose @composeArgs build
  if ($LASTEXITCODE -ne 0) { throw "docker compose build failed" }
}

Write-Host "==> Starting current AlgoSphere stack"
& docker compose @composeArgs up -d
if ($LASTEXITCODE -ne 0) { throw "docker compose up failed" }

Write-Host "==> Waiting for AlgoSphere API"
$apiReady = $false
for ($i = 1; $i -le $ApiWaitAttempts; $i++) {
  try {
    Invoke-RestMethod -Method Get -Uri "$ApiBase/news-guard/health" -TimeoutSec 10 | Out-Null
    $apiReady = $true
    break
  } catch {
    Write-Host "API not ready yet ($i/$ApiWaitAttempts)"
    Start-Sleep -Seconds $ApiWaitSeconds
  }
}
if (-not $apiReady) {
  & docker compose @composeArgs ps
  throw "AlgoSphere API did not become ready at $ApiBase"
}

$adminKey = [string]$envValues["API_ADMIN_KEY"]
Write-Host "==> Running News Guard production preflight with calendar refresh"
& "$PSScriptRoot\news-guard-preflight.ps1" `
  -ApiBase $ApiBase `
  -AdminKey $adminKey `
  -RefreshCalendar `
  -TestBriefDryRun
if ($LASTEXITCODE -ne 0) {
  Write-Host "==> News Guard deployment is running but PRE-FLIGHT FAILED"
  Write-Host "Trading mode was not changed. Fix the reported prerequisite before live use."
  exit $LASTEXITCODE
}

Write-Host "==> NEWS GUARD DEPLOYMENT READY"
Write-Host "Dashboard/API/Redis/calendar prerequisites passed preflight."
Write-Host "n8n workflow file: infra/n8n/algosphere-news-guard.json"
Write-Host "n8n must still be imported/activated on the existing n8n instance if it is external to this Compose stack."
Write-Host "TRADING_MODE was not modified by this script."
exit 0
