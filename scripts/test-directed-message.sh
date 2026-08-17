#!/bin/bash
# 群聊定向消息端到端时序测试
#
# 时序保证：
#   1) lxm2、xu 先连接并保持在线（轮询日志等到 connection.ready）
#   2) lxm 创建公开群（成员 lxm2、xu），从日志提取 groupid
#   3) lxm 发送定向给 DIRECTED_USER 的群消息
#   4) 校验目标收到、另一个成员未收到
#
# 用法：
#   ./scripts/test-directed-message.sh
#
# 可通过环境变量覆盖：
#   LXM_CONFIG / LXM2_CONFIG / XU_CONFIG / GROUP_NAME / DIRECTED_USER / DIRECTED_TEXT / DIRECTED_EXT
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin/integration-demo"

LXM_CONFIG="${LXM_CONFIG:-$ROOT/prod.yaml}"
LXM2_CONFIG="${LXM2_CONFIG:-$ROOT/prod-lxm2.yaml}"
XU_CONFIG="${XU_CONFIG:-$ROOT/prod-xu.yaml}"
GROUP_NAME="${GROUP_NAME:-go-sdk-directed-test}"
DIRECTED_USER="${DIRECTED_USER:-lxm2}"
DIRECTED_TEXT="${DIRECTED_TEXT:-directed hello from lxm}"
DIRECTED_EXT="${DIRECTED_EXT:-trace_id=directed-demo}"

LXM_LOG="$ROOT/test-lxm.log"
LXM_SEND_LOG="$ROOT/test-lxm-send.log"
LXM2_LOG="$ROOT/test-lxm2.log"
XU_LOG="$ROOT/test-xu.log"

LXM2_PID=""
XU_PID=""
LXM_PID=""

# ---- 日志工具 ----
log()    { echo "[$(date '+%H:%M:%S')] $*"; }
log_diag() { echo "        $*"; }

cleanup() {
    log "清理：终止后台 demo 进程"
    local p
    for p in "$LXM2_PID" "$XU_PID" "$LXM_PID"; do
        [ -n "$p" ] && kill "$p" 2>/dev/null || true
    done
    sleep 1
    for p in "$LXM2_PID" "$XU_PID" "$LXM_PID"; do
        [ -n "$p" ] && kill -9 "$p" 2>/dev/null || true
    done
}
trap cleanup EXIT

case "$DIRECTED_USER" in
    lxm2)
        TARGET_LABEL="lxm2"
        TARGET_LOG="$LXM2_LOG"
        OTHER_LABEL="xu"
        OTHER_LOG="$XU_LOG"
        ;;
    xu)
        TARGET_LABEL="xu"
        TARGET_LOG="$XU_LOG"
        OTHER_LABEL="lxm2"
        OTHER_LOG="$LXM2_LOG"
        ;;
    *)
        log "FAIL 当前脚本只启动 lxm2/xu 两个接收端，DIRECTED_USER 必须是 lxm2 或 xu（当前: $DIRECTED_USER）"
        exit 2
        ;;
esac

# The acceptance check derives its expected message-level Ext from the first
# configured key=value pair.  The default is deliberately a simple string so
# the check has no jq/Python dependency and remains portable across the Linux
# and macOS environments supported by this script.
DIRECTED_EXT_FIRST="${DIRECTED_EXT%%,*}"
case "$DIRECTED_EXT_FIRST" in
    *=*)
        DIRECTED_EXT_KEY=$(printf '%s' "${DIRECTED_EXT_FIRST%%=*}" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
        DIRECTED_EXT_VALUE=$(printf '%s' "${DIRECTED_EXT_FIRST#*=}" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')
        ;;
    *)
        log "FAIL DIRECTED_EXT 必须至少包含一个 key=value"
        exit 2
        ;;
esac
if [ -z "$DIRECTED_EXT_KEY" ]; then
    log "FAIL DIRECTED_EXT 的首个 key 不能为空"
    exit 2
fi

log "=============================================="
log "群聊定向消息端到端测试"
log_diag "lxm  配置: $LXM_CONFIG"
log_diag "lxm2 配置: $LXM2_CONFIG"
log_diag "xu   配置: $XU_CONFIG"
log_diag "群名: $GROUP_NAME  定向用户: $DIRECTED_USER  文本: $DIRECTED_TEXT"
log_diag "消息 Ext: key=$DIRECTED_EXT_KEY  value_bytes=${#DIRECTED_EXT_VALUE}"
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
    local log="$1" pattern="$2" timeout="$3" what="$4"
    local i
    for ((i = 1; i <= timeout; i++)); do
        if grep -q "$pattern" "$log" 2>/dev/null; then
            log "OK   ${what}（约 ${i}s）"
            return 0
        fi
        # Fail fast on terminal demo errors. Without this check a DNS/auth/
        # configuration failure leaves the script sleeping for the full
        # timeout even though the child process has already exited.
        if grep -Eq '"msg":"(connection\.failed|integration configuration rejected|integration SDK initialization failed)"' "$log" 2>/dev/null; then
            log "FAIL ${what}：demo 进程报告连接或初始化失败"
            log "----- $log 最近 10 行 -----"
            tail -n 10 "$log" 2>/dev/null || true
            log "----- $log 结束 -----"
            return 1
        fi
        if (( i % 15 == 0 )); then
            log "     仍在等待 ${what}（已 ${i}s / ${timeout}s）..."
        fi
        sleep 1
    done
    log "FAIL ${what}：${timeout}s 内未见 '$pattern'"
    log "----- $log 最近 10 行 -----"
    tail -n 10 "$log" 2>/dev/null || true
    log "----- $log 结束 -----"
    return 1
}

# Creating a group returns before every connected member has subscribed to the
# new group queue.  Do not send the directed message until both receivers have
# observed the NOTICE for this exact group; otherwise the send ACK can succeed
# while the test races membership/queue propagation and reports a false fail.
wait_for_group_queue() {
    local logfile="$1" group_id="$2" timeout="$3" what="$4"
    local queue_pattern="\"queue\":\"/${group_id}/conference.easemob.com/\""
    local i
    for ((i = 1; i <= timeout; i++)); do
        if grep -F '"msg":"wss.notice"' "$logfile" 2>/dev/null |
            grep -Fq "$queue_pattern"; then
            log "OK   ${what}（约 ${i}s）"
            return 0
        fi
        if (( i % 15 == 0 )); then
            log "     仍在等待 ${what}（已 ${i}s / ${timeout}s）..."
        fi
        sleep 1
    done
    log "FAIL ${what}：${timeout}s 内未见群队列 NOTICE"
    log "----- $logfile 最近 20 行 -----"
    tail -n 20 "$logfile" 2>/dev/null || true
    log "----- $logfile 结束 -----"
    return 1
}

count_received() {
    local n
    n=$(grep -c "message.received" "$1" 2>/dev/null) || n=0
    echo "$n"
}

# wait_for_received_count <logfile> <baseline> <timeout_seconds> <描述>
# 等待目标端真正收到一条消息，避免固定 sleep 造成慢网络下的假失败。
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

# Escape one string for matching inside a JSON string. message_json is emitted
# as a JSON-encoded string by slog, so the inner Message JSON quotes appear as
# \" in the receiver log. Values are not printed by this assertion.
escape_json_string() {
    printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

# assert_received_ext <logfile> <baseline> <key> <value> <描述>
# Only inspect message.received records after the baseline so an older message
# carrying the same extension cannot make this acceptance check pass.
assert_received_ext() {
    local logfile="$1" baseline="$2" key="$3" value="$4" what="$5"
    local key_json value_json ext_marker inner_fragment expected_fragment new_messages line ext_type
    key_json=$(escape_json_string "$key")
    value_json=$(escape_json_string "$value")
    ext_marker=$(escape_json_string '"ext":{')
    new_messages=$(grep "message.received" "$logfile" 2>/dev/null | tail -n "+$((baseline + 1))") || true
    while IFS= read -r line; do
        # integration-demo emits CLI Ext values as either string or
        # json_string. Match both while keeping the expected key/value bound
        # to one complete KeyValue object.
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
    log "FAIL ${what}：新消息的 message_json 中未找到期望的 Ext key=$key"
    return 1
}

# 提取 message.received 行的关键字段，方便排查（不打印完整的 message_json）
summarize_received() {
    local log="$1" label="$2"
    log "$label 收到的消息："
    # 没有消息是定向测试的合法结果（负向接收端必须为空）。在
    # set -euo pipefail 下，直接 grep 空结果会让整个脚本提前退出。
    if ! grep -q "message.received" "$log" 2>/dev/null; then
        return 0
    fi
    grep "message.received" "$log" 2>/dev/null | while IFS= read -r line; do
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
log "==> [1/4] 启动 lxm2 / xu 接收端并等待连接"
"$BIN" -c "$LXM2_CONFIG" -debug >"$LXM2_LOG" 2>&1 &
LXM2_PID=$!
log_diag "lxm2 接收进程 pid=${LXM2_PID}，日志 ${LXM2_LOG}"
"$BIN" -c "$XU_CONFIG" -debug >"$XU_LOG" 2>&1 &
XU_PID=$!
log_diag "xu   接收进程 pid=${XU_PID}，日志 ${XU_LOG}"
wait_for "$LXM2_LOG" "connection.ready" 90 "lxm2 已连接"
wait_for "$XU_LOG" "connection.ready" 90 "xu 已连接"

# 2) lxm 建群并提取 groupid
log "==> [2/4] lxm 创建公开群（成员 lxm2、xu）"
"$BIN" -c "$LXM_CONFIG" -debug -create-group "$GROUP_NAME" -group-members "lxm2,xu" >"$LXM_LOG" 2>&1 &
LXM_PID=$!
log_diag "lxm 建群进程 pid=${LXM_PID}，日志 ${LXM_LOG}"
wait_for "$LXM_LOG" "rest.create_group_succeeded" 90 "群已创建"
# 环信建群响应形如 "data":{"id":"322139974598659"}，字段名是 id 而非 groupid。
# 必须先定位到 rest.create_group_succeeded 行，否则会误匹配连接日志里
# session_id/trace_id 等字段名的 "id" 子串（其值多为 0）。
GROUP_ID=$(grep "rest.create_group_succeeded" "$LXM_LOG" | head -1 | grep -oE 'data[^0-9]*id[^0-9]*[0-9]+' | head -1 | grep -oE '[0-9]+$') || true
if [ -z "$GROUP_ID" ]; then
    log "FAIL 无法自动提取群 id"
    log "----- $LXM_LOG 中 rest.create_group_succeeded 原始行 -----"
    grep "rest.create_group_succeeded" "$LXM_LOG" || true
    log "若字段名/格式有变化，请调整脚本正则"
    exit 1
fi
log "GROUP_ID=$GROUP_ID"
stop_proc "$LXM_PID" "lxm建群"
LXM_PID=""

# REST 建群成功与接收端完成群队列订阅之间存在异步传播窗口。两端都
# 收到该群 NOTICE 后，才开始记录基线和发送定向消息。
wait_for_group_queue "$LXM2_LOG" "$GROUP_ID" 90 "lxm2 已订阅新群队列"
wait_for_group_queue "$XU_LOG" "$GROUP_ID" 90 "xu 已订阅新群队列"

# 3) 记录接收基线
BASE_TARGET=$(count_received "$TARGET_LOG")
BASE_OTHER=$(count_received "$OTHER_LOG")
log "接收基线：${TARGET_LABEL}=$BASE_TARGET 条，${OTHER_LABEL}=$BASE_OTHER 条"

# 4) lxm 发送定向消息给目标成员
log "==> [3/4] lxm 向 $GROUP_ID 发送定向给 $DIRECTED_USER 的消息"
"$BIN" -c "$LXM_CONFIG" -debug -send-to "$GROUP_ID" -group -directed-users "$DIRECTED_USER" -send-ext "$DIRECTED_EXT" -send-text "$DIRECTED_TEXT" >"$LXM_SEND_LOG" 2>&1 &
LXM_PID=$!
log_diag "lxm 发送进程 pid=${LXM_PID}，日志 ${LXM_SEND_LOG}"
wait_for "$LXM_SEND_LOG" "message.send_succeeded" 60 "定向消息已发送"
stop_proc "$LXM_PID" "lxm发送"
LXM_PID=""

# 5) 等待目标端真正收到消息；另一个成员再留出短暂窗口用于负向校验
wait_for_received_count "$TARGET_LOG" "$BASE_TARGET" 60 "${TARGET_LABEL} 收到定向消息"
log "等待 3 秒确认 ${OTHER_LABEL} 未收到..."
sleep 3

# 6) 校验
NOW_TARGET=$(count_received "$TARGET_LOG")
NOW_OTHER=$(count_received "$OTHER_LOG")
log ""
log "==> [4/4] 校验结果"
log "${TARGET_LABEL} 收到消息数: baseline=$BASE_TARGET -> now=$NOW_TARGET （期望 +1）"
log "${OTHER_LABEL} 收到消息数: baseline=$BASE_OTHER -> now=$NOW_OTHER （期望 0 增长）"
EXT_OK=0
if assert_received_ext "$TARGET_LOG" "$BASE_TARGET" "$DIRECTED_EXT_KEY" "$DIRECTED_EXT_VALUE" "${TARGET_LABEL} 收到的定向消息包含消息级 Ext"; then
    EXT_OK=1
fi
summarize_received "$LXM2_LOG" "lxm2"
summarize_received "$XU_LOG" "xu"
log ""
if [ "$NOW_TARGET" -gt "$BASE_TARGET" ] && [ "$NOW_OTHER" -eq "$BASE_OTHER" ] && [ "$EXT_OK" -eq 1 ]; then
	log "PASS：${TARGET_LABEL} 收到带消息级 Ext 的定向消息，${OTHER_LABEL} 未收到"
else
    log "FAIL：定向消息校验未通过"
    log "----- lxm 发送日志 -----"
    tail -n 5 "$LXM_SEND_LOG" 2>/dev/null || true
    exit 1
fi

log "完整日志："
log_diag "lxm 建群 : $LXM_LOG"
log_diag "lxm 发送 : $LXM_SEND_LOG"
log_diag "lxm2     : $LXM2_LOG"
log_diag "xu       : $XU_LOG"
