$ErrorActionPreference = "Stop"

$apiBase = "http://127.0.0.1:8000"
$updateUri = "$apiBase/bot/update"
$signalsUri = "$apiBase/control/signals"

$payload = @{
    name = "ctrader_first_live_bot"
    profit = -300
    drawdown = 600
    win_rate = 0.30
    trades = 10
} | ConvertTo-Json

Write-Host "POST $updateUri"
$updateResp = Invoke-RestMethod -Method Post -Uri $updateUri -ContentType "application/json" -Body $payload
Write-Host "\n/bot/update response:"
$updateResp | ConvertTo-Json -Depth 10

Write-Host "\nGET $signalsUri"
$signalsResp = Invoke-RestMethod -Method Get -Uri $signalsUri
Write-Host "\n/control/signals response:"
$signalsResp | ConvertTo-Json -Depth 10
