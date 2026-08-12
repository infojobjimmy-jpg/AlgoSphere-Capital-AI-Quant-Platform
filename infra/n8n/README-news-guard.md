# AlgoSphere News Guard — n8n

Ce workflow complète l'architecture AlgoSphere existante. Il ne remplace ni le backend, ni le dashboard, ni le bridge MT5.

## Workflow

Importer `algosphere-news-guard.json` dans l'instance n8n existante.

Le workflow exécute :

- rafraîchissement du calendrier économique toutes les 5 minutes ;
- brief à 07:00 America/Montreal ;
- brief à 17:00 America/Montreal ;
- contrôle T-10 toutes les minutes pour les événements HIGH/CRITICAL.

## Variables n8n

- `ALGOSPHERE_API_BASE` : URL publique/interne du backend AlgoSphere, sans slash final ;
- `ALGOSPHERE_API_ADMIN_KEY` : même valeur que `API_ADMIN_KEY` du backend.

## Variables backend

- `FINNHUB_API_KEY` pour le calendrier économique ;
- `NEWS_GUARD_DISCORD_WEBHOOK_URL` pour Discord ;
- `NEWS_GUARD_SMS_TO` pour le numéro SMS destinataire ;
- `TWILIO_ACCOUNT_SID` ;
- `TWILIO_AUTH_TOKEN` ;
- `TWILIO_FROM_NUMBER` ;
- `MT5_API_TOKEN` pour l'endpoint de garde MT5.

Le SMS est optionnel : si les variables Twilio ne sont pas définies, Discord peut continuer seul. Les secrets ne doivent jamais être inscrits dans le JSON du workflow ou poussés dans GitHub.

## Activation

1. Importer le workflow dans l'instance n8n existante.
2. Définir les variables n8n et backend.
3. Tester `POST /news-guard/refresh`.
4. Tester `POST /news-guard/notify?kind=brief&sms=true&dry_run=true`.
5. Tester Discord/SMS réel seulement après validation du dry-run.
6. Activer le workflow n8n.

Le backend applique une déduplication Redis sur les alertes T-10 et passe en fail-safe OFF si le calendrier n'a jamais été initialisé ou devient trop ancien.
