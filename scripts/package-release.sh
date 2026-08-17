#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/dist/go-im-sdk}"

rm -rf "$OUT"
mkdir -p "$OUT/sdk" "$OUT/internal/protocol/nativecodec" "$OUT/native/include" \
  "$OUT/native/lib/linux-amd64-glibc" "$OUT/native/lib/linux-arm64-glibc" "$OUT/cmd/server"

cp "$ROOT/go.mod" "$ROOT/go.sum" "$ROOT/README.md" "$ROOT/THIRD_PARTY_NOTICES" \
  "$ROOT/config.example.yaml" "$ROOT/start.sh" "$ROOT/stop.sh" "$OUT/"
cp "$ROOT/sdk/"*.go "$OUT/sdk/"
rm -f "$OUT/sdk/"*_test.go
cp "$ROOT/internal/protocol/codec.go" "$ROOT/internal/protocol/model.go" "$OUT/internal/protocol/"
cp "$ROOT/internal/protocol/nativecodec/codec.go" "$ROOT/internal/protocol/nativecodec/envelope.go" \
  "$ROOT/internal/protocol/nativecodec/unsupported.go" \
  "$OUT/internal/protocol/nativecodec/"
cp "$ROOT/native/include/em_msync_codec.h" "$OUT/native/include/"
cp "$ROOT/native/lib/linux-amd64-glibc/libem_msync_codec.a" "$OUT/native/lib/linux-amd64-glibc/"
cp "$ROOT/native/lib/linux-arm64-glibc/libem_msync_codec.a" "$OUT/native/lib/linux-arm64-glibc/"
cp "$ROOT/native/manifest.json" "$OUT/native/"
cp "$ROOT/cmd/server/main.go" "$OUT/cmd/server/"

# The public module uses only the native codec. Protocol schemas, generated Go
# protobuf, and the Go protobuf dependency are intentionally absent.
(cd "$OUT" && GOCACHE="${GOCACHE:-/tmp/go-im-sdk-release-cache}" GOWORK=off go mod tidy)

if find "$OUT" -type f \( -name '*.proto' -o -name '*.pb.go' -o -name '*.pb.cc' -o -name '*.pb.h' \) -print -quit | grep -q .; then
  echo "package-release: protocol source leaked into output" >&2
  exit 1
fi

echo "$OUT"
