#!/bin/bash
# 单聊（1-1）消息端到端时序测试
#
# 时序保证：
#   1) 接收方 lxm2 先连接并保持在线（轮询日志等到 connection.ready）
#   2) 记录接收基线
#   3) 发送方 lxm 向 lxm2 发送一条单聊文本消息（携带消息级 Ext）
#   4) 校验 lxm2 收到该消息，且 is_group=false 并带上期望的 Ext
#
# 用法：
#   ./scripts/test-single-message.sh
#
# 可通过环境变量覆盖：
#   SENDER_CONFIG / RECEIVER_CONFIG / RECEIVER_LABEL / SEND_TEXT / SEND_EXT
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin/integration-demo"

SENDER_CONFIG="${SENDER_CONFIG:-$ROOT/prod.yaml}"
RECEIVER_CONFIG="${RECEIVER_CONFIG:-$ROOT/prod-lxm2.yaml}"
RECEIVER_LABEL="${RECEIVER_LABEL:-lxm2}"
SEND_TEXT="${SEND_TEXT:-single-chat hello from lxm}"
SEND_EXT="${SEND_EXT:-trace_id=single-demo}"

RECEIVER_LOG="$ROOT/test-single-receiver.log"
SENDER_LOG="$ROOT/test-single-send.log"

RECEIVER_PID=""
SENDER_PID=""

# ---- 日志工具 ----
log()      { echo "[$(date '+%H:%M:%S')] $*"; }
log_diag() { echo "        $*"; }

cleanup() {
    log "清理：终止后台 demo 进程"
    local p
    for p in "$RECEIVER_PID" "$SENDER_PID"; do
        [ -n "$p" ] && kill "$p" 2>/dev/null || true
    done
    sleep 1
    for p in "$RECEIVER_PID" "$SENDER_PID"; do
        [ -n "$p" ] && kill -9 "$p" 2>/dev/null || true
    done
}
trap cleanup EXIT

# The acceptance check derives its expected message-level Ext from the first
# configured key=value pair. The default is deliberately a simple string so the
# check has no jq/Python dependency and stays portable across Linux and macOS.
SEND_EXT_FIRST="${SEND_EXT%%,*}"
case "$SEND_EXT_FIRST" in
    *=*)
        SEND_EXT_KEY=$(printf '%s' "${SEND_EXT_FIRST%%=*}" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
        SEND_EXT_VALUE=$(printf '%s' "${SEND_EXT_FIRST#*=}" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
        ;;
    *)
        log "FAIL SEND_EXT 必须至少包含一个 key=value"
        exit 2
        ;;
esac
if [ -z "$SEND_EXT_KEY" ]; then
    log "FAIL SEND_EXT 的首个 key 不能为空"
    exit 2
fi

log "=============================================="
log "单聊（1-1）消息端到端测试"
log_diag "发送方 配置: $SENDER_CONFIG"
log_diag "接收方 配置: $RECEIVER_CONFIG  (label=$RECEIVER_LABEL)"
log_diag "文本: $SEND_TEXT"
log_diag "消息 Ext: key=$SEND_EXT_KEY  value_bytes=${#SEND_EXT_VALUE}"
log "=============================================="

log "构建 demo 二进制（native codec）"
mkdir -p "$ROOT/bin"
BUILD_TAGS="${GO_IM_SDK_BUILD_TAGS:-}"
if [ -z "$BUILD_TAGS" ] && [ "$(uname -s)" = "Darwin" ]; then
    BUILD_TAGS="nativecodecdev"
fi
if [ "$(uname -s)" = "Darwin" ]; then
    if [ -n "${GO_IM_SDK_GOARCH:-}" ]; then
        export GOARCH="$GO_IM_SDK_GOARCH"
    elif [ -z "${GOARCH:-}" ]; then
        log "FAIL macOS native 测试必须显式设置 GOARCH=arm64 或 GOARCH=amd64"
        exit 2
    fi
    log_diag "macOS native 架构: ${GOARCH}"
fi
if [ -n "$BUILD_TAGS" ]; then
    (cd "$ROOT" && CGO_ENABLED="${CGO_ENABLED:-1}" go build -tags "$BUILD_TAGS" -o "$BIN" ./cmd/integration-demo)
else
    (cd "$ROOT" && CGO_ENABLED="${CGO_ENABLED:-1}" go build -o "$BIN" ./cmd/integration-demo)
fi
log_diag "二进制: $BIN"

# wait_for <logfile> <pattern> <timeout_seconds> <描述>
wait_for() {
    local logfile="$1" pattern="$2" timeout="$3" what="$4"
    local i
    for ((i = 1; i <= timeout; i++)); do
        if grep -q "$pattern" "$logfile" 2>/dev/null; then
            log "OK   ${what}（约 ${i}s）"
            return 0
        fi
        # Fail fast on terminal demo errors so a DNS/auth/config failure does not
        # leave the script sleeping for the full timeout.
        if grep -Eq '"msg":"(connection\.failed|integration configuration rejected|integration SDK initialization failed)"' "$logfile" 2>/dev/null; then
            log "FAIL ${what}：demo 进程报告连接或初始化失败"
            log "----- $logfile 最近 10 行 -----"
            tail -n 10 "$logfile" 2>/dev/null || true
            log "----- $logfile 结束 -----"
            return 1
        fi
        if (( i % 15 == 0 )); then
            log "     仍在等待 ${what}（已 ${i}s / ${timeout}s）..."
        fi
        sleep 1
    done
    log "FAIL ${what}：${timeout}s 内未见 '$pattern'"
    log "----- $logfile 最近 10 行 -----"
    tail -n 10 "$logfile" 2>/dev/null || true
    log "----- $logfile 结束 -----"
    return 1
}

count_received() {
    local n
    n=$(grep -c "message.received" "$1" 2>/dev/null) || n=0
    echo "$n"
}

# wait_for_received_count <logfile> <baseline> <timeout_seconds> <描述>
wait_for_received_count() {
    local logfile="$1" baseline="$2" timeout="$3" what="$4"
    local current i
    for ((i = 1; i <= timeout; i++)); do
        current=$(count_received "$logfile")
        if (( current > baseline )); then
            log "OK   ${what}（约 ${i}s）"
            return 0
        fi
        if (( i % 15 == 0 )); then
            log "     仍在等待 ${what}（已 ${i}s / ${timeout}s）..."
        fi
        sleep 1
    done
    log "FAIL ${what}：${timeout}s 内未收到新消息"
    log "----- $logfile 最近 20 行 -----"
    tail -n 20 "$logfile" 2>/dev/null || true
    log "----- $logfile 结束 -----"
    return 1
}

# Escape one string for matching inside a JSON string. message_json is emitted as
# a JSON-encoded string by slog, so the inner Message JSON quotes appear as \" in
# the receiver log. Values are not printed by this assertion.
escape_json_string() {
    printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

# assert_received_single <logfile> <baseline> <key> <value> <描述>
# Only inspect message.received records after the baseline, and require the new
# message to be a single-chat message (is_group=false) carrying the expected Ext.
assert_received_single() {
    local logfile="$1" baseline="$2" key="$3" value="$4" what="$5"
    local key_json value_json ext_marker inner_fragment expected_fragment new_messages line ext_type
    key_json=$(escape_json_string "$key")
    value_json=$(escape_json_string "$value")
    ext_marker=$(escape_json_string '"ext":{')
    new_messages=$(grep "message.received" "$logfile" 2>/dev/null | tail -n "+$((baseline + 1))") || true
    while IFS= read -r line; do
        case "$line" in
            *'"is_group":false'*) ;;
            *) continue ;;
        esac
        for ext_type in string json_string; do
            inner_fragment="\"${key_json}\":{\"type\":\"${ext_type}\",\"value\":\"${value_json}\"}"
            expected_fragment=$(escape_json_string "$inner_fragment")
            case "$line" in
                *"$ext_marker"*"$expected_fragment"*)
                    log "OK   ${what}"
                    return 0
                    ;;
            esac
        done
    done <<EOF
$new_messages
EOF
    log "FAIL ${what}：未找到 is_group=false 且带期望 Ext key=$key 的单聊消息"
    return 1
}

# 提取 message.received 行的关键字段，方便排查（不打印完整的 message_json）
summarize_received() {
    local logfile="$1" label="$2"
    log "$label 收到的消息："
    if ! grep -q "message.received" "$logfile" 2>/dev/null; then
        return 0
    fi
    grep "message.received" "$logfile" 2>/dev/null | while IFS= read -r line; do
        local from to is_group meta_id
        meta_id=$(echo "$line" | grep -oE '"meta_id":[0-9]+' | head -1 | sed -E 's/"meta_id":([0-9]+)/\1/')
        from=$(echo "$line" | grep -oE '"from":"[^"]*"' | head -1 | sed -E 's/"from":"([^"]*)"/\1/')
        to=$(echo "$line" | grep -oE '"to":"[^"]*"' | head -1 | sed -E 's/"to":"([^"]*)"/\1/')
        is_group=$(echo "$line" | grep -oE '"is_group":(true|false)' | head -1 | sed -E 's/"is_group"://')
        log_diag "meta_id=$meta_id from=$from to=$to is_group=$is_group"
    done
}

stop_proc() {
    local pid="$1" name="$2"
    [ -n "$pid" ] || return 0
    log "停止 $name (pid=$pid)"
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    log_diag "$name 已退出"
}

# 1) 接收方先连上，保证"先在线、后发消息"的时序
log "==> [1/3] 启动接收端 $RECEIVER_LABEL 并等待连接"
"$BIN" -c "$RECEIVER_CONFIG" -debug >"$RECEIVER_LOG" 2>&1 &
RECEIVER_PID=$!
log_diag "接收进程 pid=${RECEIVER_PID}，日志 ${RECEIVER_LOG}"
wait_for "$RECEIVER_LOG" "connection.ready" 90 "$RECEIVER_LABEL 已连接"

# 2) 记录接收基线，避免历史消息造成假通过
BASE_RECEIVER=$(count_received "$RECEIVER_LOG")
log "接收基线：${RECEIVER_LABEL}=$BASE_RECEIVER 条"

# 3) 发送方发送单聊消息（不带 -group 即为 1-1 单聊）
log "==> [2/3] 发送方向 $RECEIVER_LABEL 发送单聊消息"
"$BIN" -c "$SENDER_CONFIG" -debug -send-to "$RECEIVER_LABEL" -send-ext "$SEND_EXT" -send-text "$SEND_TEXT" >"$SENDER_LOG" 2>&1 &
SENDER_PID=$!
log_diag "发送进程 pid=${SENDER_PID}，日志 ${SENDER_LOG}"
wait_for "$SENDER_LOG" "message.send_succeeded" 60 "单聊消息已发送"
stop_proc "$SENDER_PID" "发送端"
SENDER_PID=""

# 4) 等待接收端真正收到
wait_for_received_count "$RECEIVER_LOG" "$BASE_RECEIVER" 60 "${RECEIVER_LABEL} 收到单聊消息"

# 5) 校验
NOW_RECEIVER=$(count_received "$RECEIVER_LOG")
log ""
log "==> [3/3] 校验结果"
log "${RECEIVER_LABEL} 收到消息数: baseline=$BASE_RECEIVER -> now=$NOW_RECEIVER （期望 +1）"
EXT_OK=0
if assert_received_single "$RECEIVER_LOG" "$BASE_RECEIVER" "$SEND_EXT_KEY" "$SEND_EXT_VALUE" "${RECEIVER_LABEL} 收到的单聊消息为 is_group=false 且含消息级 Ext"; then
    EXT_OK=1
fi
summarize_received "$RECEIVER_LOG" "$RECEIVER_LABEL"
log ""
if [ "$NOW_RECEIVER" -gt "$BASE_RECEIVER" ] && [ "$EXT_OK" -eq 1 ]; then
    log "PASS：${RECEIVER_LABEL} 收到带消息级 Ext 的单聊消息"
else
    log "FAIL：单聊消息校验未通过"
    log "----- 发送日志 -----"
    tail -n 5 "$SENDER_LOG" 2>/dev/null || true
    exit 1
fi

log "完整日志："
log_diag "发送 : $SENDER_LOG"
log_diag "接收 : $RECEIVER_LOG"
