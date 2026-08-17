#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="${MANIFEST:-$ROOT/native/manifest.json}"
MAX_ZIP_BYTES="${MAX_ZIP_BYTES:-52428800}"
MAX_UNZIPPED_BYTES="${MAX_UNZIPPED_BYTES:-209715200}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "verify-release: $*" >&2; exit 1; }
command -v go >/dev/null || fail "go is required"
command -v python3 >/dev/null || fail "python3 is required"

[[ -f "$MANIFEST" ]] || fail "manifest not found: $MANIFEST (copy native/manifest.json.example for a release)"

python3 - "$MANIFEST" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
try:
    d = json.loads(p.read_text())
except Exception as e:
    raise SystemExit(f"invalid manifest JSON: {e}")
required = ("schema_version", "sdk_module", "sdk_version", "codec_abi_version", "artifacts")
missing = [k for k in required if k not in d]
if missing:
    raise SystemExit("manifest missing: " + ", ".join(missing))
if d["schema_version"] != 1:
    raise SystemExit("unsupported manifest schema_version")
if d["sdk_module"] != "github.com/easemob/go-im-sdk":
    raise SystemExit("unexpected sdk_module")
if not isinstance(d["artifacts"], list) or not d["artifacts"]:
    raise SystemExit("artifacts must be non-empty")
seen = set()
for a in d["artifacts"]:
    for k in ("goos", "goarch", "libc", "archive", "sha256"):
        if not a.get(k):
            raise SystemExit(f"artifact missing {k}")
    key = (a["goos"], a["goarch"], a["libc"])
    if key in seen:
        raise SystemExit(f"duplicate artifact {key}")
    seen.add(key)
PY

echo "[1/4] checking development tree"
for pattern in '*.proto' '*.pb.go'; do
  if find "$ROOT" -type f -name "$pattern" -print -quit | grep -q .; then
    echo "note: development source present ($pattern); release packaging must exclude it"
  fi
done

echo "[2/4] checking module zip size"
command -v zip >/dev/null || fail "zip is required"
ZIP="$TMP/go-im-sdk-module.zip"
PACKAGE_ROOT="${RELEASE_DIR:-$ROOT}"
(cd "$PACKAGE_ROOT" && zip -q -r "$ZIP" . \
  -x '.git/*' '.omx/*' 'bin/*' '*.log' '*.pid' 'native/manifest.json.example')
zip_bytes=$(wc -c < "$ZIP" | tr -d ' ')
(( zip_bytes <= MAX_ZIP_BYTES )) || fail "module zip ${zip_bytes} exceeds MAX_ZIP_BYTES=${MAX_ZIP_BYTES}"
unzip -q "$ZIP" -d "$TMP/unzip"
unzip_bytes=$(du -sk "$TMP/unzip" | awk '{print $1 * 1024}')
(( unzip_bytes <= MAX_UNZIPPED_BYTES )) || fail "unpacked module exceeds MAX_UNZIPPED_BYTES=${MAX_UNZIPPED_BYTES}"

echo "[3/4] checking release allowlist and protocol-source leakage"
if [[ -n "${RELEASE_DIR:-}" ]]; then
  [[ -d "$RELEASE_DIR" ]] || fail "RELEASE_DIR is not a directory"
  while IFS= read -r file; do
    rel="${file#"$RELEASE_DIR"/}"
    case "$rel" in
      go.mod|go.sum|README.md|LICENSE|THIRD_PARTY_NOTICES|config.example.yaml|start.sh|stop.sh|cmd/server/main.go|native/manifest.json|native/lib/*/*.a|native/include/*.h|sdk/*.go|internal/*) ;;
      *) fail "file outside release allowlist: $rel" ;;
    esac
  done < <(find "$RELEASE_DIR" -type f -print)
  if find "$RELEASE_DIR" -type f \( -name '*.proto' -o -name '*.pb.go' -o -name '*.pb.cc' -o -name '*.pb.h' -o -name '*.cpp' -o -name '*.cc' -o -name '*.hpp' \) -print -quit | grep -q .; then
    fail "protocol or native implementation source leaked into release"
  fi
  python3 - "$MANIFEST" "$RELEASE_DIR" <<'PY'
import hashlib, json, pathlib, sys
manifest = json.loads(pathlib.Path(sys.argv[1]).read_text())
root = pathlib.Path(sys.argv[2]).resolve()
for artifact in manifest["artifacts"]:
    expected = artifact["sha256"].lower()
    if len(expected) != 64 or any(c not in "0123456789abcdef" for c in expected):
        raise SystemExit(f"invalid SHA-256 for {artifact['archive']}")
    candidate = (root / artifact["archive"]).resolve()
    if root not in candidate.parents:
        raise SystemExit(f"artifact escapes release root: {artifact['archive']}")
    if not candidate.is_file():
        raise SystemExit(f"artifact missing: {artifact['archive']}")
    actual = hashlib.sha256(candidate.read_bytes()).hexdigest()
    if actual != expected:
        raise SystemExit(f"SHA-256 mismatch: {artifact['archive']}")
PY
fi

echo "[4/4] checking current Go module"
(cd "$ROOT" && GOCACHE="$TMP/go-build" go list ./...) >/dev/null
if [[ "${VERIFY_GO_TESTS:-0}" == "1" ]]; then
  (cd "$ROOT" && GOCACHE="$TMP/go-build" go test ./...)
else
  echo "note: set VERIFY_GO_TESTS=1 in an environment that permits local test listeners"
fi
echo "release verification passed (module zip: ${zip_bytes} bytes)"
echo "native targets: linux/amd64/glibc and linux/arm64/glibc; libc/compiler baselines remain release-time configuration"
