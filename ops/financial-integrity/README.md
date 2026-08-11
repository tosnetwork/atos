# Managed financial integrity operations

These executable controls are deployment inputs, not a production mutation.
Use a dedicated PostgreSQL 16 cluster for ATOS and a separate one/database for
Blnk. `postgresql.conf` enables WAL archive/PITR prerequisites;
`base-backup.sh` and `restore-drill.sh` create and actually boot a verified
restore; `provision-roles.sql` separates migration, runtime and audit authority;
`object-lock.sh` enables and verifies S3 Object Lock compliance retention.
Base-backup creation is single-writer and atomically publishes only after
`pg_verifybackup` succeeds. The Object Lock check creates a retained control
object, verifies its version/digest, and proves both create-only overwrite and
locked-version deletion fail; the retained probe is intentionally not removed.
The locked bucket must also enforce a customer-managed KMS key; the probe
verifies the exact key ARN and server-side encryption response.
`restore-drill.sh` requires an explicit post-backup RFC3339 recovery target and
fails unless PostgreSQL reaches it and promotes out of recovery. Set
`ATOS_RESTORE_VERIFY_SQL_FILE` to an absolute read-only SQL file to assert a
known marker written after the base backup and before the target time.
Install `wal-archive.sh` as `/usr/local/libexec/atos-wal-archive`. PostgreSQL
reports archive success only after the WAL has an exact local spool copy and an
independently versioned, compliance-locked, KMS-encrypted object plus receipt.
Retries compare content and converge on the existing stable WAL object key.

Break-glass access is a time-limited, separately approved database role that is
created as `NOLOGIN`. Enabling it requires an incident ID, two operators, session
recording, credential rotation afterwards, a full Blnk chain/reconciliation run,
and independent verification of every already anchored batch. Safe mode is not
cleared merely because projections were edited: repair projections from Blnk,
retain the incident evidence, verify sealed history, then use a reviewed
migration to clear the incident.
Normal runtime has no direct `UPDATE` on integrity state or incident resolution;
it can only call `enter_financial_safe_mode`, which is incapable of clearing the
flag. `atos_backup` is the separate physical-backup/replication credential.

Alert on: any safe-mode transition; reconciliation mismatches; pending intents
older than two scheduler periods; chain verification failure; batch/signature/
retention/anchor lag; WAL archive age; base-backup age; restore-drill failure;
and any runtime authorization denial against sealed tables.
