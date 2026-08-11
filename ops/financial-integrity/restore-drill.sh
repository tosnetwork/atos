#!/usr/bin/env bash
set -euo pipefail
: "${ATOS_RESTORE_BACKUP:?absolute pg_basebackup directory}"
: "${ATOS_RESTORE_WAL_ARCHIVE:?absolute WAL archive directory}"
: "${ATOS_RESTORE_PORT:?unused local port}"
: "${ATOS_RESTORE_TARGET_TIME:?RFC3339 PITR target time after the base backup}"
case "$ATOS_RESTORE_BACKUP:$ATOS_RESTORE_WAL_ARCHIVE" in /*:/*) ;; *) echo "restore paths must be absolute" >&2; exit 2;; esac
for command_name in cp mktemp pg_ctl psql python3; do
  command -v "$command_name" >/dev/null 2>&1 || { echo "missing required command: $command_name" >&2; exit 69; }
done
if ! [[ "$ATOS_RESTORE_PORT" =~ ^[0-9]+$ ]] || (( ATOS_RESTORE_PORT < 1024 || ATOS_RESTORE_PORT > 65535 )); then
  echo "ATOS_RESTORE_PORT must be an unused non-privileged TCP port" >&2
  exit 2
fi
if ! [[ "$ATOS_RESTORE_BACKUP" =~ ^[A-Za-z0-9_./-]+$ && "$ATOS_RESTORE_WAL_ARCHIVE" =~ ^[A-Za-z0-9_./-]+$ && "$ATOS_RESTORE_TARGET_TIME" =~ ^[0-9T:Z+.-]+$ ]]; then
  echo "restore paths or target time contain unsafe recovery-command characters" >&2
  exit 2
fi
test -d "$ATOS_RESTORE_BACKUP" && test -d "$ATOS_RESTORE_WAL_ARCHIVE" || { echo "backup or WAL archive directory is missing" >&2; exit 66; }
normalized_target="$(python3 -c 'import datetime,sys; value=datetime.datetime.fromisoformat(sys.argv[1].replace("Z","+00:00")); assert value.tzinfo is not None; print(value.isoformat(sep=" "))' "$ATOS_RESTORE_TARGET_TIME")" || {
  echo "ATOS_RESTORE_TARGET_TIME must be a timezone-bound RFC3339 timestamp" >&2
  exit 2
}
drill_root="$(mktemp -d "${TMPDIR:-/tmp}/atos-restore-drill.XXXXXX")"
cleanup() {
	status=$?
	if test "$status" -ne 0 && test -f "$drill_root/postgres.log"; then
	  echo "restore drill PostgreSQL log:" >&2
	  tail -n 80 "$drill_root/postgres.log" >&2 || true
	fi
  if test -f "$drill_root/data/postmaster.pid"; then pg_ctl -D "$drill_root/data" -m immediate stop >/dev/null 2>&1 || true; fi
	if test "$status" -ne 0 && test "${ATOS_RESTORE_KEEP_FAILED:-}" = 1; then
	  echo "retaining failed restore drill at $drill_root" >&2
	  return "$status"
	fi
  # drill_root is an exact mktemp-created path; depth-first deletion avoids
  # recursive broad-target removal if an operator supplies a bad environment.
  find "$drill_root" -depth -delete
	return "$status"
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
recovery_target_time = '$normalized_target'
recovery_target_action = 'promote'
EOF
touch "$drill_root/data/recovery.signal"
pg_ctl -D "$drill_root/data" -l "$drill_root/postgres.log" -w start
replay_completed=f
for ((attempt=1; attempt<=60; attempt++)); do
  replay_completed="$(psql -h 127.0.0.1 -p "$ATOS_RESTORE_PORT" -U postgres -d postgres -Atqc 'SELECT NOT pg_is_in_recovery()')"
  test "$replay_completed" = t && break
  sleep 1
done
test "$replay_completed" = t || { echo "restore never reached and promoted the requested PITR target" >&2; exit 1; }
psql -h 127.0.0.1 -p "$ATOS_RESTORE_PORT" -U postgres -d postgres -v ON_ERROR_STOP=1 \
  -c "SELECT count(*) AS databases_restored FROM pg_database"
if test -n "${ATOS_RESTORE_VERIFY_SQL_FILE:-}"; then
  case "$ATOS_RESTORE_VERIFY_SQL_FILE" in /*) ;; *) echo "ATOS_RESTORE_VERIFY_SQL_FILE must be absolute" >&2; exit 2;; esac
  test -r "$ATOS_RESTORE_VERIFY_SQL_FILE" || { echo "restore verification SQL is unreadable" >&2; exit 66; }
  psql -h 127.0.0.1 -p "$ATOS_RESTORE_PORT" -U postgres -d postgres -v ON_ERROR_STOP=1 -f "$ATOS_RESTORE_VERIFY_SQL_FILE"
fi
pg_ctl -D "$drill_root/data" -m fast -w stop
echo "restore drill reached $ATOS_RESTORE_TARGET_TIME, promoted, and passed"
