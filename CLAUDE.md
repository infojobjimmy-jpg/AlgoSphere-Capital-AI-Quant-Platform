# AlgoSphere Global — Instructions Claude Code

## Contexte du projet

**Produit :** Plateforme d'intelligence géospatiale, fusion de données, cartographie, analyse et alertes
**Site :** algosphereglobal.com
**Dépôt local :** `C:\Users\jlpat\algosphere-capital\`
**Remote :** `https://github.com/infojobjimmy-jpg/AlgoSphere-Capital-AI-Quant-Platform.git`
**Branche de travail :** `codex/algosphere-product-completion` (base : `e52bb16`)
**VPS prod :** `root@157.230.59.195`, `/opt/algosphere/`
**Clé SSH :** `~/.ssh/algosphere_production_ed25519`

Ne jamais utiliser `origin/main` — historique divergent (Streamlit), sans rapport avec la prod.
Ne jamais fusionner depuis `origin/main`.

---

## RÈGLE ABSOLUE — AUCUN TRADING

AlgoSphere Global est **exclusivement** une plateforme d'intelligence géospatiale.
Le trading a été retiré et ne doit **jamais** revenir sous quelque forme que ce soit.

**Interdit d'ajouter, restaurer ou documenter comme fonctionnalité active :**
- trading, forex, crypto-trading
- MetaTrader, MT5, EA
- broker, ordre BUY/SELL, exécution de transactions
- portefeuille financier, signaux de trading
- stratégie quantitative financière, prédictions de rendement
- automatisation de placements financiers

Le nom historique du dépôt contient "Capital", "AI", "Quant" — ces termes ne définissent plus le produit.

**Code trading existant dans le dépôt — ne pas réactiver :**

| Fichier | Statut | Action requise |
|---------|--------|----------------|
| `backend/app/agents/trading_agent.py` | Mort (service en profil Docker inactif) | Supprimer lors d'une phase dédiée |
| `backend/app/routers/trading.py` | Mort (gaté `public_geospatial_mode=True`) | Supprimer lors d'une phase dédiée |
| `backend/app/trading/` (module) | Mort (importé uniquement par trading_agent/router) | Supprimer lors d'une phase dédiée |
| `backend/app/ai/ai_trader.py` | Mort | Supprimer lors d'une phase dédiée |
| `backend/app/agents/strategic_agent.py` | Nettoyé — overlay trading retiré (commit phase 1) | OK |
| `backend/app/agents/observer_agent.py` | Nettoyé — patterns trading retirés (commit phase 1) | OK |
| `backend/app/agents/task_agent.py` | Nettoyé — commandes trading retirées (commit phase 1) | OK |
| `backend/app/config.py:82-89` | Paramètres trading inactifs | À supprimer avec trading_agent |
| `frontend/src/components/trading/*` | Orphelins — non importés dans App.tsx | Supprimer lors d'une phase dédiée |
| `frontend/src/components/ai/FloatingAiRobot.tsx` | Orphelin | Supprimer lors d'une phase dédiée |
| `frontend/src/components/globe/GlobeAdvisoryPanel.tsx` | Orphelin (appelle /api/trading/ai) | Supprimer lors d'une phase dédiée |

---

## RÈGLE ABSOLUE — AUCUNE FAUSSE DONNÉE

Ne jamais afficher de données fictives, synthétiques ou simulées.

Chaque information visible doit provenir d'une source publique légale, d'un fournisseur autorisé, d'une licence valide, d'une source appartenant à l'organisation, ou d'un utilisateur ayant consenti.

Si une source n'est pas disponible, afficher honnêtement :
- `"Données indisponibles — licence requise"`
- `"Source non configurée — autorisation légale requise"`
- `"Aucune donnée autorisée disponible pour cette zone et cette période"`

**Ne jamais créer de faux navires, avions, véhicules, visages, plaques, satellites, événements ou alertes.**

**Statuts de source autorisés :** `ACTIVE`, `PUBLIC_SOURCE`, `LICENSE_REQUIRED`, `LEGAL_REVIEW_REQUIRED`, `AUTHORIZATION_REQUIRED`, `CREDENTIALS_REQUIRED`, `CONFIGURATION_REQUIRED`, `UNAVAILABLE`, `OFFLINE`, `DEGRADED`

---

## RÈGLE ABSOLUE — PRÉSERVER LE STYLE ET LE GLOBE

**Ne jamais modifier la structure visuelle, la navigation ou le globe Cesium sans validation explicite.**

Éléments protégés :
- Globe Cesium 3D (requestRenderMode, flyToBoundingSphere, gestion des couches)
- Navigation GPS (watchPosition, camera follow, HUD, reroute logic)
- Palette de couleurs, polices, design global
- Structure des composants React (App.tsx, MarketingApp.tsx, index.css)
- Modes de carte (SAT / HYB / RTE) et leurs couches Esri
- Animations et transitions UI existantes

Les améliorations mobiles doivent **adapter** l'existant avec CSS responsive, panneaux repliables et contrôles tactiles — ne pas créer une deuxième interface.

---

## Architecture

### Services Docker actifs en prod (14 conteneurs)

| Service | Image | Rôle |
|---------|-------|------|
| `api` | `algosphere-api` | Backend FastAPI — routes, auth, websockets |
| `frontend` | `algosphere-frontend` | React + Cesium (Nginx:1.27-alpine) |
| `ingestion` | `algosphere-ingestion` | Worker ingestion flux live |
| `bridge` | `algosphere-bridge` | Bridge Kafka |
| `camera-worker` | `algosphere-camera-worker` | Worker caméras MTQ |
| `cortex` | `algosphere-cortex` | Agent Cortex (Kafka + ChromaDB) |
| `observer` | `algosphere-observer` | Agent Observer |
| `caddy` | `caddy:2.10-alpine` | Reverse proxy HTTPS |
| `db` | `timescale/timescaledb:latest-pg16` | TimescaleDB |
| `redis` | `redis:7-alpine` | Cache + pub/sub |
| `kafka` | `confluentinc/cp-kafka:7.5.0` | Bus d'événements |
| `chromadb` | `chromadb/chroma:0.6.3` | Mémoire vectorielle |
| `prometheus` | `prom/prometheus:v2.51.2` | Métriques |
| `grafana` | `grafana/grafana:11.2.0` | Dashboard métriques |

### Fichiers clés

```
algosphere-capital/
├── backend/
│   ├── Dockerfile                   ← labels GIT_COMMIT/GIT_BRANCH/BUILD_DATE
│   └── app/
│       ├── main.py                  ← FastAPI entry point, routers enregistrés
│       ├── config.py                ← Settings (public_geospatial_mode=True en prod)
│       ├── routers/                 ← auth, navigation, analytics, webhooks, account, layers...
│       ├── agents/                  ← strategic, observer, cortex, anomaly, investigator...
│       ├── intel/                   ← orchestrator.py (build_snapshot, run_strategic_agent)
│       └── workers/                 ← ingestion_runner, camera_worker, kafka_bridge
├── frontend/
│   ├── Dockerfile                   ← labels GIT_COMMIT/GIT_BRANCH/BUILD_DATE
│   └── src/
│       ├── App.tsx                  ← Globe Cesium, GPS, couches, auth UI (NE PAS REFORMATER)
│       ├── MarketingApp.tsx         ← Landing page publique
│       ├── index.css                ← Styles globaux
│       └── i18n.ts                  ← Traductions (FR/EN/ES/PT)
├── infra/
│   ├── Caddyfile                    ← Config reverse proxy prod
│   └── sql/                         ← Migrations DB (sauvegarde obligatoire avant toute modif)
├── ops/deploy/                      ← Scripts de déploiement (non suivis Git — NE PAS COMMITTER EN BLOC)
│   ├── deploy-service.sh            ← Déploiement par service avec SHA
│   ├── rollback-service.sh          ← Rollback par service
│   └── lib/                         ← common.sh, state.sh, images.sh, health.sh
├── docker-compose.yml               ← Config base
└── docker-compose.prod.yml          ← Overrides prod (Caddy, ports fermés, profils)
```

### Fichiers non suivis à préserver (NE PAS SUPPRIMER, NE PAS COMMITTER AUTOMATIQUEMENT)

- `backend/get_product_url.py`
- `backups/`
- `gps-test-screenshots/`
- `ops/` — scripts de déploiement (modifiés en session, à traiter séparément)
- `pw-test/` — tests Playwright

---

## Points critiques Cesium — ne jamais casser

1. `flyToBoundingSphere` + callback `complete: () => viewer.scene.requestRender()` dans `applyRoute`
2. `requestRenderMode=true` → appeler `requestRender()` après CHAQUE modification Cesium
3. `gpsMarkerRef.current.position = ...` — jamais `removeAll` dans la boucle watchPosition
4. `handlePositionUpdateRef` pattern pour éviter stale closures dans le callback watchPosition
5. Ordre des effects React : map style effect APRÈS viewer init effect

---

## Protocole de déploiement obligatoire

Avant tout déploiement :
1. Tester localement
2. Sauvegarde récupérable des fichiers VPS concernés via SSH
3. Inventaire des images Docker actives (commit, digest)
4. Préparer le rollback : `ops/deploy/rollback-service.sh --sha <sha-précédent> --services <svc>`
5. Ne jamais toucher aux bases de données (TimescaleDB, Redis, ChromaDB) sans sauvegarde
6. Déployer uniquement après validation complète

**Production actuelle :**
```
9c4aaa1a6cd2a6cee5e6bbf623cc72adaee28957  (2026-08-03 — trading cleanup, build propre)
```

**Rollback immédiat — images Docker (prioritaire) :**
Images buildées le 2026-08-03 matin, trading déjà retiré. `git.commit=unknown` (build sans args), mais contenu vérifié.
```bash
# Sur le VPS — /opt/algosphere
docker tag algosphere-api:rollback-20260803-134459 algosphere-api:latest
docker tag algosphere-frontend:rollback-20260803-134459 algosphere-frontend:latest
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d \
  --force-recreate --no-deps --no-build api frontend
```

Vérification après rollback :
```bash
# Confirmer les images réellement utilisées
docker inspect algosphere-api-1 --format '{{index .Config.Labels "com.algosphere.git.commit"}} {{.Image}}'
docker inspect algosphere-frontend-1 --format '{{index .Config.Labels "com.algosphere.git.commit"}} {{.Image}}'
# État healthy et health checks
docker ps --format 'table {{.Names}}\t{{.Status}}' | grep -E 'api|frontend'
curl -s https://algosphereglobal.com/api/system/health
```

**Rollback Git d'urgence (dernier recours) :**
> **AVERTISSEMENT :** SHA `e52bb16b0ee6d48db24009277b938d22891f3e03` précède le nettoyage trading.
> Un rollback vers ce point réintroduit `trading_agent.py`, `routers/trading.py`, `market_hub_bus.py` et tous les fichiers supprimés.
> Nécessite un rebuild complet depuis ce SHA — ne pas utiliser sauf si `rollback-20260803-134459` est indisponible.

---

## Identification des builds Docker

Depuis la branche `codex/algosphere-product-completion`, toutes les images embarquent :
- `com.algosphere.git.commit` — SHA40
- `com.algosphere.git.branch` — branche source
- `com.algosphere.build.date` — date ISO 8601 UTC

Vérification :
```bash
docker inspect algosphere-api-1 --format '{{json .Config.Labels}}' | python3 -m json.tool
```

---

## Commandes SSH VPS

```bash
ssh -i ~/.ssh/algosphere_production_ed25519 root@157.230.59.195
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.CreatedAt}}'
docker logs algosphere-api-1 --tail 50
curl -s http://localhost:8080/system/health
```
