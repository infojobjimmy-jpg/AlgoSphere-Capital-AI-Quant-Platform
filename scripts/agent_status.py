#!/usr/bin/env python3
"""
agent_status.py — Écrit le statut d'une équipe IA dans Memurai/Redis
pour que Jimmy puisse "checker le live" depuis RedisInsight/Memurai GUI
sans lire le code.

Usage (depuis un subagent, via Bash tool):
    python3 agent_status.py <team> <status> "<résumé court>"

Exemple:
    python3 agent_status.py strategie "en_cours" "Backtest RIC en cours sur XAUUSD H1"
    python3 agent_status.py cyber "termine" "Audit JWT OK, 0 faille critique"
    python3 agent_status.py prediction "bloque" "Attente validation risk-auditor avant merge"

Clé Redis créée: team:<team>:status  (TTL 24h, JSON)
    {
      "status": "en_cours" | "termine" | "bloque" | "erreur",
      "resume": "texte court",
      "timestamp": "2026-07-12T14:32:00",
      "agent": "nom du subagent qui a écrit"
    }
"""
import sys
import json
import os
from datetime import datetime, timezone

try:
    import redis
except ImportError:
    print("Installe d'abord: pip install redis --break-system-packages", file=sys.stderr)
    sys.exit(1)

TTL_SECONDS = 24 * 60 * 60  # 24h — la clé disparaît si l'équipe est inactive

def main():
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(1)

    team = sys.argv[1]
    status = sys.argv[2]
    resume = sys.argv[3]
    agent_name = os.environ.get("CLAUDE_AGENT_NAME", "inconnu")

    valid_status = {"en_cours", "termine", "bloque", "erreur", "attente_validation"}
    if status not in valid_status:
        print(f"Statut invalide. Utilise un de: {valid_status}", file=sys.stderr)
        sys.exit(1)

    payload = {
        "status": status,
        "resume": resume,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "agent": agent_name,
    }

    r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)
    key = f"team:{team}:status"
    r.set(key, json.dumps(payload, ensure_ascii=False), ex=TTL_SECONDS)
    print(f"[OK] {key} -> {status} : {resume}")

if __name__ == "__main__":
    main()
