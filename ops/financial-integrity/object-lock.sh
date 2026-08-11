#!/usr/bin/env bash
set -euo pipefail
: "${ATOS_WORM_BUCKET:?required}"
: "${ATOS_WORM_RETENTION_DAYS:?required}"
: "${ATOS_WORM_KMS_KEY_ARN:?required customer-managed KMS key ARN}"
if ! [[ "$ATOS_WORM_RETENTION_DAYS" =~ ^[1-9][0-9]*$ ]]; then echo "invalid retention days" >&2; exit 2; fi
case "$ATOS_WORM_KMS_KEY_ARN" in arn:aws:kms:*) ;; *) echo "ATOS_WORM_KMS_KEY_ARN must be a KMS key ARN" >&2; exit 2;; esac
for command_name in aws python3; do
  command -v "$command_name" >/dev/null 2>&1 || { echo "missing required command: $command_name" >&2; exit 69; }
done
versioning="$(aws s3api get-bucket-versioning --bucket "$ATOS_WORM_BUCKET" --query Status --output text)"
test "$versioning" = Enabled || { echo "bucket versioning is not enabled" >&2; exit 1; }
encryption="$(aws s3api get-bucket-encryption --bucket "$ATOS_WORM_BUCKET" --output json)"
python3 -c 'import json,sys; r=json.load(sys.stdin)["ServerSideEncryptionConfiguration"]["Rules"]; assert any(x["ApplyServerSideEncryptionByDefault"].get("SSEAlgorithm")=="aws:kms" and x["ApplyServerSideEncryptionByDefault"].get("KMSMasterKeyID")==sys.argv[1] for x in r)' "$ATOS_WORM_KMS_KEY_ARN" <<<"$encryption"
aws s3api get-object-lock-configuration --bucket "$ATOS_WORM_BUCKET" >/dev/null 2>&1 || \
  aws s3api put-object-lock-configuration --bucket "$ATOS_WORM_BUCKET" --object-lock-configuration \
  "ObjectLockEnabled=Enabled,Rule={DefaultRetention={Mode=COMPLIANCE,Days=$ATOS_WORM_RETENTION_DAYS}}"
configuration="$(aws s3api get-object-lock-configuration --bucket "$ATOS_WORM_BUCKET" --output json)"
python3 -c 'import json,sys; c=json.load(sys.stdin); assert c["ObjectLockConfiguration"]["ObjectLockEnabled"]=="Enabled"; assert c["ObjectLockConfiguration"]["Rule"]["DefaultRetention"]["Mode"]=="COMPLIANCE"' <<<"$configuration"

probe_file="$(mktemp "${TMPDIR:-/tmp}/atos-worm-probe.XXXXXX")"
download_file="${probe_file}.download"
cleanup() { rm -f -- "$probe_file" "$download_file"; }
trap cleanup EXIT HUP INT TERM
printf 'atos managed financial integrity object-lock probe\n' >"$probe_file"
probe_digest="sha256:$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$probe_file")"
probe_key="${ATOS_WORM_PROBE_PREFIX:-atos-financial/control-probes}/$(date -u +%Y%m%dT%H%M%SZ)-$$"
retain_until="$(python3 -c 'import datetime,sys; print((datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(days=int(sys.argv[1]))).isoformat().replace("+00:00","Z"))' "$ATOS_WORM_RETENTION_DAYS")"
version_id="$(aws s3api put-object --bucket "$ATOS_WORM_BUCKET" --key "$probe_key" --body "$probe_file" \
  --object-lock-mode COMPLIANCE --object-lock-retain-until-date "$retain_until" \
  --server-side-encryption aws:kms --ssekms-key-id "$ATOS_WORM_KMS_KEY_ARN" \
  --metadata "content-sha256=$probe_digest" --query VersionId --output text)"
test -n "$version_id" && test "$version_id" != None || { echo "probe upload returned no version ID" >&2; exit 1; }
head_json="$(aws s3api head-object --bucket "$ATOS_WORM_BUCKET" --key "$probe_key" --version-id "$version_id" --output json)"
python3 -c 'import json,sys; h=json.load(sys.stdin); assert h["VersionId"]==sys.argv[1]; assert h["ObjectLockMode"]=="COMPLIANCE"; assert h["Metadata"]["content-sha256"]==sys.argv[2]; assert h["ServerSideEncryption"]=="aws:kms"; assert h["SSEKMSKeyId"]==sys.argv[3]' "$version_id" "$probe_digest" "$ATOS_WORM_KMS_KEY_ARN" <<<"$head_json"
if aws s3api put-object --bucket "$ATOS_WORM_BUCKET" --key "$probe_key" --body "$probe_file" --if-none-match '*' >/dev/null 2>&1; then
  echo "create-only overwrite protection failed" >&2
  exit 1
fi
if aws s3api delete-object --bucket "$ATOS_WORM_BUCKET" --key "$probe_key" --version-id "$version_id" >/dev/null 2>&1; then
  echo "locked object version was deletable" >&2
  exit 1
fi
aws s3api get-object --bucket "$ATOS_WORM_BUCKET" --key "$probe_key" --version-id "$version_id" "$download_file" >/dev/null
download_digest="sha256:$(python3 -c 'import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],"rb").read()).hexdigest())' "$download_file")"
test "$download_digest" = "$probe_digest" || { echo "locked object content changed" >&2; exit 1; }
echo "Object Lock compliance retention and immutable version $version_id verified at $probe_key"
