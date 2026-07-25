#!/bin/sh
set -eu

PROJECT_DIR="${ALGOSPHERE_PROJECT_DIR:-/opt/algosphere}"
BACKUP_DIR="${ALGOSPHERE_BACKUP_DIR:-/var/backups/algosphere}"
RETENTION_DAYS="${ALGOSPHERE_BACKUP_RETENTION_DAYS:-14}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
TARGET="${BACKUP_DIR}/algosphere-${STAMP}.sql.gz"
TMP="${TARGET}.partial"

mkdir -p "$BACKUP_DIR"
chmod 700 "$BACKUP_DIR"
cd "$PROJECT_DIR"

docker compose -f docker-compose.yml -f docker-compose.prod.yml exec -T db \
  pg_dump -U gaios -d gaios --clean --if-exists --no-owner \
  | gzip -9 > "$TMP"

gzip -t "$TMP"
test "$(wc -c < "$TMP")" -gt 1024
mv "$TMP" "$TARGET"
chmod 600 "$TARGET"
find "$BACKUP_DIR" -type f -name 'algosphere-*.sql.gz' -mtime "+${RETENTION_DAYS}" -delete
printf '%s\n' "$TARGET"
