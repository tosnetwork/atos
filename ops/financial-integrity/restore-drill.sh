#!/usr/bin/env bash
set -euo pipefail
: "${ATOS_RESTORE_BACKUP:?absolute pg_basebackup directory}"
: "${ATOS_RESTORE_WAL_ARCHIVE:?absolute WAL archive directory}"
: "${ATOS_RESTORE_PORT:?unused local port}"
case "$ATOS_RESTORE_BACKUP:$ATOS_RESTORE_WAL_ARCHIVE" in /*:/*) ;; *) echo "restore paths must be absolute" >&2; exit 2;; esac
drill_root="$(mktemp -d "${TMPDIR:-/tmp}/atos-restore-drill.XXXXXX")"
cleanup() {
  if test -f "$drill_root/data/postmaster.pid"; then pg_ctl -D "$drill_root/data" -m immediate stop >/dev/null 2>&1 || true; fi
  # drill_root is an exact mktemp-created path; depth-first deletion avoids
  # recursive broad-target removal if an operator supplies a bad environment.
  find "$drill_root" -depth -delete
}
trap cleanup EXIT
cp -a "$ATOS_RESTORE_BACKUP" "$drill_root/data"
chmod 0700 "$drill_root/data"
pg_verifybackup "$drill_root/data"
cat >>"$drill_root/data/postgresql.auto.conf" <<EOF
port = $ATOS_RESTORE_PORT
listen_addresses = '127.0.0.1'
restore_command = 'cp $ATOS_RESTORE_WAL_ARCHIVE/%f %p'
recovery_target_timeline = 'latest'
EOF
touch "$drill_root/data/recovery.signal"
pg_ctl -D "$drill_root/data" -l "$drill_root/postgres.log" -w start
psql -h 127.0.0.1 -p "$ATOS_RESTORE_PORT" -U postgres -d postgres -v ON_ERROR_STOP=1 \
  -c "SELECT NOT pg_is_in_recovery() AS replay_completed" \
  -c "SELECT count(*) AS databases_restored FROM pg_database"
pg_ctl -D "$drill_root/data" -m fast -w stop
echo "restore drill passed"
