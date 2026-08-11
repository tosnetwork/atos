#!/usr/bin/env bash
set -euo pipefail
: "${ATOS_BACKUP_PGHOST:?required}"
: "${ATOS_BACKUP_PGPORT:?required}"
: "${ATOS_BACKUP_PGUSER:?required}"
: "${ATOS_BACKUP_ROOT:?required absolute destination}"
case "$ATOS_BACKUP_ROOT" in /*) ;; *) echo "ATOS_BACKUP_ROOT must be absolute" >&2; exit 2;; esac
umask 077
for command_name in pg_basebackup pg_verifybackup mktemp; do
  command -v "$command_name" >/dev/null 2>&1 || { echo "missing required command: $command_name" >&2; exit 69; }
done
install -d -m 0700 "$ATOS_BACKUP_ROOT"
lock_file="$ATOS_BACKUP_ROOT/.base-backup.lock"
if test "${ATOS_BACKUP_LOCK_HELD:-}" != 1; then
  if command -v flock >/dev/null 2>&1; then
    exec 9>"$lock_file"
    flock -n 9 || { echo "another base backup is already running" >&2; exit 75; }
  elif command -v lockf >/dev/null 2>&1; then
    exec lockf -t 0 "$lock_file" env ATOS_BACKUP_LOCK_HELD=1 "$0" "$@"
  else
    echo "flock or lockf is required for single-writer backup publication" >&2
    exit 69
  fi
fi
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
target="$ATOS_BACKUP_ROOT/base-$stamp"
test ! -e "$target" || { echo "backup target already exists: $target" >&2; exit 73; }
temporary="$(mktemp -d "$ATOS_BACKUP_ROOT/.base-$stamp.XXXXXX")"
cleanup() {
  if test -n "${temporary:-}" && test -d "$temporary"; then
    find "$temporary" -depth -delete
  fi
}
trap cleanup EXIT HUP INT TERM
pg_basebackup --host="$ATOS_BACKUP_PGHOST" --port="$ATOS_BACKUP_PGPORT" \
  --username="$ATOS_BACKUP_PGUSER" --pgdata="$temporary" --format=plain \
  --wal-method=stream --checkpoint=fast --manifest-checksums=SHA256 --progress
pg_verifybackup "$temporary"
test -s "$temporary/backup_manifest" || { echo "verified backup has no manifest" >&2; exit 65; }
chmod -R go-rwx "$temporary"
mv "$temporary" "$target"
temporary=""
trap - EXIT HUP INT TERM
printf '%s\n' "$target"
