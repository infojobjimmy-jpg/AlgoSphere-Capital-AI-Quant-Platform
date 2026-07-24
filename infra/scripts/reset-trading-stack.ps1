#Requires -Version 5.1
<#
.SYNOPSIS
  Stop trading stack (trading, kafka, redis), remove Kafka/Redis named volumes (KRaft — no Zookeeper),
  start in order, clear trading Redis keys, start trading. Other compose services are not stopped.

  Run from repo root:
    .\infra\scripts\reset-trading-stack.ps1

  To stream logs only:
    docker compose logs -f --tail 200 trading
#>
param(
  [string]$ComposeFile = "docker-compose.yml",
  [int]$KafkaWaitAttempts = 30,
  [int]$KafkaWaitSeconds = 2,
  [switch]$SkipLogStream
)

# Docker writes progress to stderr; do not treat that as terminating errors.
$ErrorActionPreference = "Continue"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $root

function Invoke-Compose {
  param([string[]]$ComposeArgs)
  & docker compose -f $ComposeFile @ComposeArgs 2>&1 | ForEach-Object { Write-Host $_ }
  if ($LASTEXITCODE -ne 0) { throw "docker compose failed: $($ComposeArgs -join ' ')" }
}

Write-Host "==> Stopping trading, kafka, redis (other services untouched)"
& docker compose -f $ComposeFile stop trading kafka redis 2>$null
# tolerate missing services
$LASTEXITCODE = 0

Write-Host "==> Removing stopped containers for those services"
& docker compose -f $ComposeFile rm -f trading kafka redis 2>$null
$LASTEXITCODE = 0

$project = Split-Path $root -Leaf
$volumeNames = @(
  "${project}_kafka_data",
  "${project}_redis_data"
)

Write-Host "==> Removing named volumes for fresh Kafka (KRaft) / Redis (project=$project)"
foreach ($vol in $volumeNames) {
  docker volume rm $vol 2>$null | Out-Null
  if ($LASTEXITCODE -eq 0) { Write-Host "    removed: $vol" }
  else { Write-Host "    skip or missing: $vol" }
  $LASTEXITCODE = 0
}

Write-Host "==> Starting kafka (KRaft)"
Invoke-Compose @("up", "-d", "kafka")
Start-Sleep -Seconds 5

Write-Host "==> Waiting for Kafka broker inside container (max $KafkaWaitAttempts attempts)"
$ok = $false
for ($i = 1; $i -le $KafkaWaitAttempts; $i++) {
  Write-Host "Waiting for Kafka... attempt $i"
  & docker compose -f $ComposeFile exec -T kafka bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/9092" 2>$null | Out-Null
  if ($LASTEXITCODE -eq 0) {
    $ok = $true
    Write-Host "Kafka is ready (broker listener open inside container)."
    break
  }
  $LASTEXITCODE = 0
  if ($i -lt $KafkaWaitAttempts) {
    $delay = [Math]::Min(60, $KafkaWaitSeconds + 0.5 * [Math]::Max(0, $i - 1))
    Start-Sleep -Seconds $delay
  }
}
if (-not $ok) {
  Write-Error "Kafka did not become healthy inside the container within $KafkaWaitAttempts attempts."
  exit 1
}

Write-Host "==> Starting redis"
Invoke-Compose @("up", "-d", "redis")
Start-Sleep -Seconds 2

Write-Host "==> Deleting trading-related Redis keys (missing keys ignored)"
$keys = @(
  "acap:ledger",
  "acap:positions",
  "acap:risk",
  "acap:last_trade_mono",
  "acap:trading_agent:paper_ledger",
  "acap:paper:state",
  "acap:paper:recent",
  "acap:trading:kill",
  "acap:trading:mode",
  "acap:live:positions",
  "acap:live:account"
)
foreach ($k in $keys) {
  & docker compose -f $ComposeFile exec -T redis redis-cli DEL $k 2>$null | Out-Null
}

Write-Host "==> Starting trading"
Invoke-Compose @("up", "-d", "trading")

if ($SkipLogStream) {
  Write-Host "==> Skip log stream (-SkipLogStream). Check: docker compose logs -f trading"
  exit 0
}

Write-Host "==> Streaming trading logs (Ctrl+C to stop)"
Invoke-Compose @("logs", "-f", "--tail", "120", "trading")
