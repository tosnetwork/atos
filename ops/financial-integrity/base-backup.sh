#!/usr/bin/env bash
set -euo pipefail
: "${ATOS_BACKUP_PGHOST:?required}"
: "${ATOS_BACKUP_PGPORT:?required}"
: "${ATOS_BACKUP_PGUSER:?required}"
: "${ATOS_BACKUP_ROOT:?required absolute destination}"
case "$ATOS_BACKUP_ROOT" in /*) ;; *) echo "ATOS_BACKUP_ROOT must be absolute" >&2; exit 2;; esac
umask 077
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
target="$ATOS_BACKUP_ROOT/base-$stamp"
mkdir -p "$target"
pg_basebackup --host="$ATOS_BACKUP_PGHOST" --port="$ATOS_BACKUP_PGPORT" \
  --username="$ATOS_BACKUP_PGUSER" --pgdata="$target" --format=plain \
  --wal-method=stream --checkpoint=fast --manifest-checksums=SHA256 --progress
pg_verifybackup "$target"
chmod -R go-rwx "$target"
printf '%s\n' "$target"
