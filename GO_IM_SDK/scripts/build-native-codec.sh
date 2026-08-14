#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
NATIVE="$ROOT/native"
BUILD_DIR=${BUILD_DIR:-"$NATIVE/build/$(uname -s | tr '[:upper:]' '[:lower:]')-$(uname -m)"}
CXX=${CXX:-c++}
AR=${AR:-ar}

case "$(uname -s)" in
  Linux) ;;
  Darwin)
    echo "note: building a macOS developer archive; release artifacts require Linux amd64/arm64 builders" >&2
    ;;
  *) echo "unsupported host: $(uname -s); use a Linux release builder" >&2; exit 2 ;;
esac

mkdir -p "$BUILD_DIR/obj"
INCLUDES="-I$NATIVE/include -I$NATIVE/private -I$NATIVE/private/generated -I$NATIVE/private/protobuf"
CXXFLAGS=${CXXFLAGS:-"-O2 -std=c++11 -fPIC -fvisibility=hidden -fvisibility-inlines-hidden"}

SOURCES="
$NATIVE/src/codec.cpp
$NATIVE/private/generated/jid.pb.cc
$NATIVE/private/generated/keyvalue.pb.cc
$NATIVE/private/generated/messagebody.pb.cc
$NATIVE/private/generated/msync.pb.cc
$NATIVE/private/generated/statisticsbody.pb.cc
$NATIVE/private/protobuf/google/protobuf/generated_message_util.cc
$NATIVE/private/protobuf/google/protobuf/message_lite.cc
$NATIVE/private/protobuf/google/protobuf/repeated_field.cc
$NATIVE/private/protobuf/google/protobuf/wire_format_lite.cc
$NATIVE/private/protobuf/google/protobuf/io/coded_stream.cc
$NATIVE/private/protobuf/google/protobuf/io/zero_copy_stream.cc
$NATIVE/private/protobuf/google/protobuf/io/zero_copy_stream_impl_lite.cc
$NATIVE/private/protobuf/google/protobuf/stubs/common.cc
$NATIVE/private/protobuf/google/protobuf/stubs/once.cc
"

case "$(uname -m)" in
  x86_64|amd64) SOURCES="$SOURCES
$NATIVE/private/protobuf/google/protobuf/stubs/atomicops_internals_x86_gcc.cc" ;;
esac

OBJECTS=""
for src in $SOURCES; do
  obj="$BUILD_DIR/obj/$(printf '%s' "$src" | sed 's|.*/||; s|\.cc$|.o|; s|\.cpp$|.o|')"
  "$CXX" $CXXFLAGS $INCLUDES -c "$src" -o "$obj"
  OBJECTS="$OBJECTS $obj"
done

OUT="$BUILD_DIR/libem_msync_codec.a"
rm -f "$OUT"
$AR rcs "$OUT" $OBJECTS
if [ "${RUN_SMOKE_TEST:-1}" = 1 ]; then
  "$CXX" $CXXFLAGS $INCLUDES "$NATIVE/tests/codec_smoke.cpp" "$OUT" -o "$BUILD_DIR/codec_smoke"
  "$BUILD_DIR/codec_smoke"
fi
echo "$OUT"
