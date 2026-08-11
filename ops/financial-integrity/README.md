# Managed financial integrity operations

These executable controls are deployment inputs, not a production mutation.
Use a dedicated PostgreSQL 16 cluster for ATOS and a separate one/database for
Blnk. `postgresql.conf` enables WAL archive/PITR prerequisites;
`base-backup.sh` and `restore-drill.sh` create and actually boot a verified
restore; `provision-roles.sql` separates migration, runtime and audit authority;
`object-lock.sh` enables and verifies S3 Object Lock compliance retention.

Break-glass access is a time-limited, separately approved database role that is
normally `NOLOGIN`. Enabling it requires an incident ID, two operators, session
recording, credential rotation afterwards, a full Blnk chain/reconciliation run,
and independent verification of every already anchored batch. Safe mode is not
cleared merely because projections were edited: repair projections from Blnk,
retain the incident evidence, verify sealed history, then use a reviewed
migration to clear the incident.

Alert on: any safe-mode transition; reconciliation mismatches; pending intents
older than two scheduler periods; chain verification failure; batch/signature/
retention/anchor lag; WAL archive age; base-backup age; restore-drill failure;
and any runtime authorization denial against sealed tables.
