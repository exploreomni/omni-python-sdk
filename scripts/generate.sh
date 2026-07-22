#!/usr/bin/env bash
# Regenerate the omni_python_sdk package from the OpenAPI spec.
#
# Usage:
#   scripts/generate.sh                       # regenerate from the checked-in spec/openapi.json
#   scripts/generate.sh --source <path>       # sync spec from a local omni repo checkout first
#   scripts/generate.sh --url <instance-url>  # sync spec from a live instance (fetches <url>/openapi.json)
#
# Requires: openapi-python-client (pip install openapi-python-client), rsync.
# Hand-written files (omni_python_sdk/helpers.py) are preserved.

set -euo pipefail

# Enum value ordering in generated code depends on Python set iteration
# order; pin the hash seed so regeneration is deterministic.
export PYTHONHASHSEED=0

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPEC="$REPO_ROOT/spec/openapi.json"
PROCESSED="$REPO_ROOT/spec/openapi.processed.json"
CONFIG="$REPO_ROOT/generator/config.yaml"
PKG="$REPO_ROOT/omni_python_sdk"

# record_provenance <source-description>
# Only written when the spec is synced, so plain regeneration (and the CI
# drift check) stays deterministic.
record_provenance() {
  python - "$SPEC" "$1" <<'PYEOF'
import datetime, hashlib, json, sys
spec, source = sys.argv[1], sys.argv[2]
digest = hashlib.sha256(open(spec, "rb").read()).hexdigest()
provenance = {
    "source": source,
    "spec_sha256": digest,
    "synced_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
}
path = spec.rsplit("/", 1)[0] + "/provenance.json"
json.dump(provenance, open(path, "w"), indent=2)
print(f"wrote {path}")
PYEOF
}

if [[ "${1:-}" == "--source" ]]; then
  cp "$2" "$SPEC"
  OMNI_SHA="$(git -C "$(dirname "$2")" rev-parse HEAD 2>/dev/null || echo unknown)"
  record_provenance "omni repo @ $OMNI_SHA"
  echo "synced spec from $2"
elif [[ "${1:-}" == "--url" ]]; then
  curl -fsSL "${2%/}/openapi.json" -o "$SPEC"
  record_provenance "${2%/}/openapi.json"
  echo "synced spec from ${2%/}/openapi.json"
fi

python "$REPO_ROOT/scripts/preprocess_spec.py" "$SPEC" "$PROCESSED"

TMPDIR_GEN="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_GEN"' EXIT

openapi-python-client generate \
  --path "$PROCESSED" \
  --config "$CONFIG" \
  --output-path "$TMPDIR_GEN/out" \
  --overwrite

rsync -a --delete --exclude helpers.py "$TMPDIR_GEN/out/omni_python_sdk/" "$PKG/"

echo "regenerated $PKG"
