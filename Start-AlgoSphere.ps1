<#
.SYNOPSIS
    Start AlgoSphere API (and optionally Streamlit) with real-data + live pipeline env vars.

.DESCRIPTION
    Sets ALGO_SPHERE_* variables for this process and child processes, frees port 8000 (and
    optionally 8501), starts uvicorn on 127.0.0.1:8000, then optionally starts Streamlit
    if frontend\dashboard.py exists and port 8501 is free.

    Does not change trading logic - environment and process launch only.

.PARAMETER NoStreamlit
    Do not start the Streamlit dashboard.

.PARAMETER SkipPortCleanup
    Do not stop existing listeners on 8000 / 8501 (not recommended).

.EXAMPLE
    .\Start-AlgoSphere.ps1

.EXAMPLE
    .\Start-AlgoSphere.ps1 -NoStreamlit
#>
[CmdletBinding()]
param(
    [switch]$NoStreamlit,
    [switch]$SkipPortCleanup
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

$Py = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Py)) {
    Write-Error "Venv Python not found: $Py"
}

# --- Environment (real data + live pipeline) ---
$env:ALGO_SPHERE_NO_SYNTHETIC_HISTORY = "1"
$env:ALGO_SPHERE_LIVE_SYMBOLS = "XAUUSD,EURUSD,NAS100,US30,SPX500,USDJPY"
$env:ALGO_SPHERE_LIVE_TESTING = "1"
$env:ALGO_SPHERE_LIVE_PRIMARY_SYMBOL = "XAUUSD"

function Stop-ListenerOnPort {
    param([int]$Port)
    try {
        $conns = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
        foreach ($c in $conns) {
            $procId = $c.OwningProcess
            if ($procId -and $procId -gt 0) {
                Write-Host "Stopping PID $procId listening on port $Port..."
                Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
            }
        }
    } catch {
        # Fallback: parse netstat (works without Get-NetTCPConnection details on some hosts)
    }
    $pattern = ":$Port\s"
    netstat -ano | ForEach-Object {
        if ($_ -match $pattern -and $_ -match "\sLISTENING\s+(\d+)\s*$") {
            $p = $Matches[1]
            if ($p -and $p -ne "0") {
                Write-Host "Stopping PID $p (netstat) on port $Port..."
                & taskkill.exe /F /PID $p 2>$null | Out-Null
            }
        }
    }
    Start-Sleep -Milliseconds 500
}

if (-not $SkipPortCleanup) {
    Stop-ListenerOnPort -Port 8000
    if (-not $NoStreamlit) {
        Stop-ListenerOnPort -Port 8501
    }
}

# --- API (uvicorn) ---
Write-Host 'Starting FastAPI (uvicorn) on http://127.0.0.1:8000 ...'
Start-Process -FilePath $Py -ArgumentList @(
    "-m", "uvicorn", "backend.main:app",
    "--host", "127.0.0.1",
    "--port", "8000"
) -WorkingDirectory $Root -WindowStyle Hidden

Start-Sleep -Seconds 2

# --- Optional Streamlit ---
$Dash = Join-Path $Root "frontend\dashboard.py"
if (-not $NoStreamlit -and (Test-Path -LiteralPath $Dash)) {
    $port8501Busy = $false
    try {
        $x = Get-NetTCPConnection -LocalPort 8501 -State Listen -ErrorAction SilentlyContinue
        if ($x) { $port8501Busy = $true }
    } catch { }
    if (-not $port8501Busy) {
        Write-Host 'Starting Streamlit dashboard on http://127.0.0.1:8501 ...'
        Start-Process -FilePath $Py -ArgumentList @(
            "-m", "streamlit", "run",
            "frontend\dashboard.py",
            "--server.port", "8501",
            "--server.address", "127.0.0.1",
            "--browser.gatherUsageStats", "false"
        ) -WorkingDirectory $Root -WindowStyle Hidden
        Start-Sleep -Seconds 2
    } else {
        Write-Host 'Port 8501 already in use - skipping Streamlit (use -NoStreamlit to suppress this check).'
    }
} elseif (-not $NoStreamlit) {
    Write-Host 'No frontend\dashboard.py - skipping Streamlit.'
}

Write-Host ''
Write-Host '=== AlgoSphere URLs ===' -ForegroundColor Cyan
Write-Host '  API:       http://127.0.0.1:8000'
Write-Host '  Live:      http://127.0.0.1:8000/live/status'
Write-Host '  Pipeline:  http://127.0.0.1:8000/data/pipeline/validation'
if (-not $NoStreamlit -and (Test-Path -LiteralPath $Dash)) {
    Write-Host '  Dashboard: http://127.0.0.1:8501'
}
Write-Host ''
Write-Host 'Env: NO_SYNTHETIC_HISTORY=1 | LIVE_TESTING=1 | PRIMARY=XAUUSD | 6 symbols'
Write-Host 'Stop listeners: re-run this script (port cleanup) or taskkill the Python PIDs on 8000/8501.'
Write-Host ''
