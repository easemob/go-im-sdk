# Native MSync codec

This internal target packages the C-only codec ABI, the generated C++ protocol
implementation, and its namespaced protobuf-lite runtime. SDK consumers do not
install protobuf or protoc.

Developer build:

```sh
./scripts/build-native-codec.sh
```

macOS output is only for local ABI/codec validation. Release archives must be
built and tested on the intended Linux amd64/arm64 glibc baseline. The current
customer baseline is glibc 2.28 with GCC 8.5.0; Clang 18.1.8 is available as an
alternative compiler. Release archives must be built in an environment whose
glibc baseline is no newer than 2.28 and checked for GLIBC/GLIBCXX symbol
versions.

The current raw-frame API is the M2/M3 foundation. Higher-level semantic
Provision, Unread, Send, Queue Pull and Logout APIs are added without exposing
protobuf types through the public header.
