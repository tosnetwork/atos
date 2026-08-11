#!/usr/bin/env bash
set -euo pipefail
: "${ATOS_WORM_BUCKET:?required}"
: "${ATOS_WORM_RETENTION_DAYS:?required}"
if ! [[ "$ATOS_WORM_RETENTION_DAYS" =~ ^[1-9][0-9]*$ ]]; then echo "invalid retention days" >&2; exit 2; fi
aws s3api get-object-lock-configuration --bucket "$ATOS_WORM_BUCKET" >/dev/null 2>&1 || \
  aws s3api put-object-lock-configuration --bucket "$ATOS_WORM_BUCKET" --object-lock-configuration \
  "ObjectLockEnabled=Enabled,Rule={DefaultRetention={Mode=COMPLIANCE,Days=$ATOS_WORM_RETENTION_DAYS}}"
configuration="$(aws s3api get-object-lock-configuration --bucket "$ATOS_WORM_BUCKET" --output json)"
python3 -c 'import json,sys; c=json.load(sys.stdin); assert c["ObjectLockConfiguration"]["ObjectLockEnabled"]=="Enabled"; assert c["ObjectLockConfiguration"]["Rule"]["DefaultRetention"]["Mode"]=="COMPLIANCE"' <<<"$configuration"
echo "Object Lock compliance retention verified"
