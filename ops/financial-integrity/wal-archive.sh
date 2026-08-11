#!/usr/bin/env bash
set -euo pipefail

: "${1:?usage: wal-archive.sh <postgres-wal-path> <wal-file-name>}"
: "${2:?usage: wal-archive.sh <postgres-wal-path> <wal-file-name>}"
: "${ATOS_WAL_ARCHIVE_ROOT:?required independent local spool}"
: "${ATOS_WAL_BUCKET:?required independently administered Object Lock bucket}"
: "${ATOS_WAL_KMS_KEY_ARN:?required customer-managed KMS key ARN}"
: "${ATOS_WAL_RETENTION_DAYS:?required}"
source_file=$1
wal_name=$2
case "$ATOS_WAL_ARCHIVE_ROOT" in /*) ;; *) echo "ATOS_WAL_ARCHIVE_ROOT must be absolute" >&2; exit 2;; esac
if ! [[ "$wal_name" =~ ^[A-F0-9]{8,64}(\.[A-Za-z0-9]+)*$ ]] || ! [[ "$ATOS_WAL_RETENTION_DAYS" =~ ^[1-9][0-9]*$ ]]; then
  echo "invalid WAL name or retention" >&2
  exit 2
fi
case "$ATOS_WAL_KMS_KEY_ARN" in arn:aws:kms:*) ;; *) echo "ATOS_WAL_KMS_KEY_ARN must be a KMS key ARN" >&2; exit 2;; esac
test -f "$source_file" || { echo "WAL source is missing" >&2; exit 66; }
for command_name in aws cmp python3; do
  command -v "$command_name" >/dev/null 2>&1 || { echo "missing required command: $command_name" >&2; exit 69; }
done

umask 077
install -d -m 0700 "$ATOS_WAL_ARCHIVE_ROOT"
cleanup() { rm -f -- "${temporary:-}" "${receipt_tmp:-}"; }
trap cleanup EXIT HUP INT TERM
local_copy="$ATOS_WAL_ARCHIVE_ROOT/$wal_name"
if test -e "$local_copy"; then
  cmp -s "$source_file" "$local_copy" || { echo "existing WAL archive differs: $wal_name" >&2; exit 1; }
else
  temporary="$(mktemp "$ATOS_WAL_ARCHIVE_ROOT/.$wal_name.XXXXXX")"
  install -m 0600 "$source_file" "$temporary"
  mv "$temporary" "$local_copy"
  temporary=""
fi

digest="sha256:$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$local_copy")"
object_key="${ATOS_WAL_OBJECT_PREFIX:-atos-financial/wal}/$wal_name"
retain_until="$(python3 -c 'import datetime,sys; print((datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(days=int(sys.argv[1]))).isoformat().replace("+00:00","Z"))' "$ATOS_WAL_RETENTION_DAYS")"

if ! head_json="$(aws s3api head-object --bucket "$ATOS_WAL_BUCKET" --key "$object_key" --output json 2>/dev/null)"; then
  aws s3api put-object --bucket "$ATOS_WAL_BUCKET" --key "$object_key" --body "$local_copy" --if-none-match '*' \
    --object-lock-mode COMPLIANCE --object-lock-retain-until-date "$retain_until" \
    --server-side-encryption aws:kms --ssekms-key-id "$ATOS_WAL_KMS_KEY_ARN" \
    --metadata "content-sha256=$digest" >/dev/null || true
  head_json="$(aws s3api head-object --bucket "$ATOS_WAL_BUCKET" --key "$object_key" --output json)"
fi
python3 -c 'import json,sys; h=json.load(sys.stdin); assert h.get("VersionId"); assert h["Metadata"]["content-sha256"]==sys.argv[1]; assert h["ObjectLockMode"]=="COMPLIANCE"; assert h["ServerSideEncryption"]=="aws:kms"; assert h["SSEKMSKeyId"]==sys.argv[2]' "$digest" "$ATOS_WAL_KMS_KEY_ARN" <<<"$head_json"
receipt_tmp="${local_copy}.receipt.tmp"
printf '%s\n' "$head_json" >"$receipt_tmp"
chmod 0600 "$receipt_tmp"
mv "$receipt_tmp" "${local_copy}.receipt.json"
receipt_tmp=""
