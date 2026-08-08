from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one target, found {count}")
    p.write_text(text.replace(old, new, 1))


path = "internal/store/postgres/postgres.go"
replace_once(
    path,
    'const escrowColumns = `id, quote_id, principal_id, provider_id, capability_id, reserved, status, created_at, expires_at, settled_at, payload`',
    'const escrowColumns = `id, quote_id, job_id, principal_id, provider_id, capability_id, reserved, status, created_at, expires_at, settled_at, payload`',
)
replace_once(
    path,
    '''\t\tINSERT INTO escrows (\n\t\t\tid, quote_id, principal_id, provider_id, capability_id, reserved,\n\t\t\tstatus, created_at, expires_at, settled_at, payload\n\t\t) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)\n\t\tON CONFLICT (id) DO UPDATE SET\n\t\t\treserved=$6, status=$7, expires_at=$9, settled_at=$10, payload=$11\n\t`, e.ID, e.QuoteID, e.PrincipalID, e.ProviderID, e.CapabilityID,\n\t\tmustMarshal(e.Reserved), string(e.Status), e.CreatedAt, e.ExpiresAt,\n\t\te.SettledAt, mustMarshal(e))\n''',
    '''\t\tINSERT INTO escrows (\n\t\t\tid, quote_id, job_id, principal_id, provider_id, capability_id, reserved,\n\t\t\tstatus, created_at, expires_at, settled_at, payload\n\t\t) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)\n\t\tON CONFLICT (id) DO UPDATE SET\n\t\t\tjob_id=$3, reserved=$7, status=$8, expires_at=$10, settled_at=$11, payload=$12\n\t`, e.ID, e.QuoteID, e.JobID, e.PrincipalID, e.ProviderID, e.CapabilityID,\n\t\tmustMarshal(e.Reserved), string(e.Status), e.CreatedAt, e.ExpiresAt,\n\t\te.SettledAt, mustMarshal(e))\n''',
)
replace_once(
    path,
    '''\tif err := row.Scan(\n\t\t&e.ID, &e.QuoteID, &e.PrincipalID, &e.ProviderID, &e.CapabilityID,\n\t\t&reserved, &status, &e.CreatedAt, &e.ExpiresAt, &e.SettledAt, &payload,\n''',
    '''\tif err := row.Scan(\n\t\t&e.ID, &e.QuoteID, &e.JobID, &e.PrincipalID, &e.ProviderID, &e.CapabilityID,\n\t\t&reserved, &status, &e.CreatedAt, &e.ExpiresAt, &e.SettledAt, &payload,\n''',
)

migration = Path("migrations/005_phase1_crash_safety.sql")
text = migration.read_text()
needle = "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS reconciliation_target TEXT NOT NULL DEFAULT '';\n\n"
if needle not in text:
    raise SystemExit("migration anchor not found")
text = text.replace(
    needle,
    needle + "ALTER TABLE escrows ADD COLUMN IF NOT EXISTS job_id TEXT NOT NULL DEFAULT '';\n\n",
    1,
)
migration.write_text(text)
print("stage1 escrow job-id schema fixed")
