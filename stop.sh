#!/bin/sh
set -eu

usage() { echo "Usage: $0 -c CONFIG.yaml" >&2; exit 2; }

config=
while getopts "c:h" opt; do
    case "$opt" in c) config=$OPTARG ;; h|*) usage ;; esac
done
[ -n "$config" ] || usage
config_dir=$(CDPATH= cd -- "$(dirname -- "$config")" && pwd)
config="$config_dir/$(basename -- "$config")"
pidfile=${GO_IM_SDK_PIDFILE:-"$config.pid"}

[ -f "$pidfile" ] || { echo "Not running (pidfile not found: $pidfile)"; exit 0; }
pid=$(sed -n '1p' "$pidfile")
case "$pid" in *[!0-9]*|'') echo "Invalid pidfile: $pidfile" >&2; exit 1 ;; esac

if ! kill -0 "$pid" 2>/dev/null; then
    rm -f -- "$pidfile"
    echo "Process $pid is no longer running; removed stale pidfile"
    exit 0
fi

kill -TERM "$pid"
timeout=${GO_IM_SDK_STOP_TIMEOUT:-15}
case "$timeout" in *[!0-9]*|'') echo "GO_IM_SDK_STOP_TIMEOUT must be a non-negative integer" >&2; exit 2 ;; esac
elapsed=0
while kill -0 "$pid" 2>/dev/null; do
    if [ "$elapsed" -ge "$timeout" ]; then
        echo "Timed out waiting for PID $pid; process was not force-killed" >&2
        exit 1
    fi
    sleep 1
    elapsed=$((elapsed + 1))
done
rm -f -- "$pidfile"
echo "Stopped PID $pid"
