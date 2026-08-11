#!/usr/bin/env bash
set -euo pipefail
: "${ATOS_CONTROL_DATABASE_URL:?required}"
psql "$ATOS_CONTROL_DATABASE_URL" -v ON_ERROR_STOP=1 -f "$(dirname "$0")/verify-roles.sql"
archive_mode="$(psql "$ATOS_CONTROL_DATABASE_URL" -Atqc 'show archive_mode')"
archive_command="$(psql "$ATOS_CONTROL_DATABASE_URL" -Atqc 'show archive_command')"
test "$archive_mode" = on
test -n "$archive_command" && test "$archive_command" != '(disabled)'
psql "$ATOS_CONTROL_DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT safe_mode,reason,incident_id FROM financial_integrity_state"
echo "database integrity controls verified"
