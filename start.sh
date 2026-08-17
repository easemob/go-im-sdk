#!/bin/sh
set -eu

usage() { echo "Usage: $0 -c CONFIG.yaml [-d]" >&2; exit 2; }

config=
daemon=0
while getopts "c:dh" opt; do
    case "$opt" in
        c) config=$OPTARG ;;
        d) daemon=1 ;;
        h|*) usage ;;
    esac
done
[ -n "$config" ] || usage
[ -f "$config" ] || { echo "Configuration file not found: $config" >&2; exit 1; }

base_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
config_dir=$(CDPATH= cd -- "$(dirname -- "$config")" && pwd)
config="$config_dir/$(basename -- "$config")"
binary=${GO_IM_SDK_BIN:-"$base_dir/bin/go-im-sdk-server"}
pidfile=${GO_IM_SDK_PIDFILE:-"$config.pid"}
logfile=${GO_IM_SDK_LOGFILE:-"$config.log"}

if [ -f "$pidfile" ]; then
    old_pid=$(sed -n '1p' "$pidfile")
    case "$old_pid" in *[!0-9]*|'') old_pid= ;; esac
    if [ -n "$old_pid" ] && kill -0 "$old_pid" 2>/dev/null; then
        echo "Already running with PID $old_pid (pidfile: $pidfile)" >&2
        exit 1
    fi
    rm -f -- "$pidfile"
fi

if [ ! -x "$binary" ]; then
    mkdir -p -- "$(dirname -- "$binary")"
    echo "Building $binary"
    (cd "$base_dir" && go build -o "$binary" ./cmd/server)
fi

if [ "$daemon" -eq 1 ]; then
    umask 077
    nohup "$binary" -config "$config" >>"$logfile" 2>&1 &
    pid=$!
    echo "$pid" >"$pidfile"
    sleep 1
    if ! kill -0 "$pid" 2>/dev/null; then
        rm -f -- "$pidfile"
        echo "Server failed to start; inspect $logfile" >&2
        exit 1
    fi
    echo "Started PID $pid (log: $logfile)"
    exit 0
fi

"$binary" -config "$config" &
pid=$!
echo "$pid" >"$pidfile"
trap 'kill -TERM "$pid" 2>/dev/null || true' HUP INT TERM
trap 'rm -f -- "$pidfile"' EXIT
wait "$pid"
