# algo-sphere (Phase 1)

Local production-ready baseline with:
- FastAPI backend
- SQLite storage (`sqlite3`, no ORM)
- Streamlit dashboard
- Modular backend architecture

## Phase 1 Scope

Implemented endpoints:
- `GET /`
- `POST /account/update`
- `POST /bot/update`
- `GET /bots`
- `GET /brain`

Implemented behavior:
- Bot scoring
- Risk evaluation
- Capital allocation
- Execution decision logic
- Simple AI regime detection

## Run (PowerShell, Windows)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Start API:

```powershell
python -m uvicorn backend.main:app --host 127.0.0.1 --port 8000 --reload
```

Start dashboard in a second terminal:

```powershell
.\.venv\Scripts\Activate.ps1
streamlit run frontend/dashboard.py
```

## Quick Test Steps

1. Check API health:
```powershell
Invoke-RestMethod http://127.0.0.1:8000/
```

2. Update account:
```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/account/update `
  -ContentType "application/json" `
  -Body '{"balance":15000,"risk_limit":0.03}'
```

3. Add/update bots:
```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/bot/update `
  -ContentType "application/json" `
  -Body '{"name":"alpha_bot","profit":320.5,"drawdown":120.0,"win_rate":0.61,"trades":74}'

Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/bot/update `
  -ContentType "application/json" `
  -Body '{"name":"beta_bot","profit":-40.0,"drawdown":280.0,"win_rate":0.47,"trades":41}'
```

4. Retrieve bots and brain:
```powershell
Invoke-RestMethod http://127.0.0.1:8000/bots
Invoke-RestMethod http://127.0.0.1:8000/brain
```

5. Open dashboard:
- Visit the Streamlit URL shown in terminal (usually `http://localhost:8501`)

## Expected Output (Phase 1)

- `GET /` returns:
```json
{"status":"ok","service":"algo-sphere"}
```
- `GET /bots` returns:
  - `count` as integer
  - `total_profit` as float
  - `bots` array containing `score`, `risk_level`, `capital_alloc`, and `decision`
- `GET /brain` returns:
  - `regime` in `RISK_ON | NEUTRAL | RISK_OFF`
  - `message` with short explanation
- Streamlit dashboard shows:
  - Total profit
  - Active bots
  - Bots table with score and allocation
  - Brain regime summary
