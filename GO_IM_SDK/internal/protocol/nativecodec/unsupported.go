//go:build !gopbcodec && ((linux && (!cgo || (!amd64 && !arm64))) || (nativecodecdev && (!cgo || !darwin || (!amd64 && !arm64))))

package nativecodec

// Native codec supports only cgo builds for declared release platforms.
// This intentionally fails at compile time instead of falling back to Go PB.
var _ unsupportedNativeCodecRequiresCGOLinuxOrDarwinAMD64OrARM64
