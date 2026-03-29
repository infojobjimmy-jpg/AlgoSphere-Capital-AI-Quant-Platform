import json
import urllib.request

API_BASE = "http://127.0.0.1:8000"
UPDATE_URL = f"{API_BASE}/bot/update"
SIGNALS_URL = f"{API_BASE}/control/signals"

payload = {
    "name": "ctrader_first_live_bot",
    "profit": -300,
    "drawdown": 600,
    "win_rate": 0.30,
    "trades": 10,
}

req = urllib.request.Request(
    UPDATE_URL,
    data=json.dumps(payload).encode("utf-8"),
    headers={"Content-Type": "application/json"},
    method="POST",
)

print(f"POST {UPDATE_URL}")
with urllib.request.urlopen(req, timeout=10) as resp:
    update_body = json.loads(resp.read().decode("utf-8"))
print("\n/bot/update response:")
print(json.dumps(update_body, indent=2))

print(f"\nGET {SIGNALS_URL}")
with urllib.request.urlopen(SIGNALS_URL, timeout=10) as resp:
    signals_body = json.loads(resp.read().decode("utf-8"))
print("\n/control/signals response:")
print(json.dumps(signals_body, indent=2))
