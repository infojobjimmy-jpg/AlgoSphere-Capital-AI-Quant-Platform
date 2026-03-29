# First Demo Runner

Safe demo runner for Algo Sphere Runner Bridge.

## What It Does
- Polls `GET /runner/jobs?limit=5`
- Acknowledges jobs
- Starts jobs
- Simulates execution for 5-20 seconds
- Completes jobs with 80% probability
- Fails jobs with 20% probability

## Safety
- Demo-only simulation
- No broker execution
- No live trading
- No capital deployment

## Run
```powershell
python runner/demo_runner.py
```

Optional custom config:
```powershell
python runner/demo_runner.py --config runner/config.yaml
```

## Example Logs
```text
[RUNNER] Found job 397720f6
[RUNNER] ACK
[RUNNER] START
[RUNNER] Simulating...
[RUNNER] COMPLETE
```
