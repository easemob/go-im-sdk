#!/bin/bash
# 离线消息端到端测试
#
# 验证目标：接收方离线期间累积的消息，会在其重新登录后由 SDK 通过 UNREAD 下行
# 拉取（wss.unread_pull），并与在线消息一样经 MessageHandler 回调返回。
#
# 时序保证：
#   1) 接收方 lxm2 全程保持离线（本脚本在发送阶段绝不启动 lxm2）
#   2) 发送方 lxm 连续发送 COUNT 条单聊消息给离线的 lxm2，每条带唯一 RUN_TAG
#   3) 发送方退出后，才启动 lxm2 登录
#   4) 校验 lxm2 登录后：
#        a. 日志出现 wss.unread_pull（证明走了离线拉取路径）
#        b. 至少收到 COUNT 条带本次 RUN_TAG 的消息（排除历史积压干扰）
#
# 前置要求：
#   运行期间不得有其他进程以 lxm2 身份在线，否则消息会走在线投递而非离线积压。
#
# 用法：
#   ./scripts/test-offline-message.sh
#
# 可通过环境变量覆盖：
#   SENDER_CONFIG / RECEIVER_CONFIG / RECEIVER_LABEL / OFFLINE_COUNT / SEND_TEXT
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/bin/integration-demo"

SENDER_CONFIG="${SENDER_CONFIG:-$ROOT/prod.yaml}"
RECEIVER_CONFIG="${RECEIVER_CONFIG:-$ROOT/prod-lxm2.yaml}"
RECEIVER_LABEL="${RECEIVER_LABEL:-lxm2}"
OFFLINE_COUNT="${OFFLINE_COUNT:-3}"
SEND_TEXT="${SEND_TEXT:-offline hello from lxm}"

# 每次运行生成唯一标记，避免历史积压消息造成假通过。
RUN_TAG="offline-$(date '+%Y%m%d%H%M%S')-$$"

RECEIVER_LOG="$ROOT/test-offline-receiver.log"
SENDER_LOG="$ROOT/test-offline-send.log"

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

case "$OFFLINE_COUNT" in
    ''|*[!0-9]*)
        log "FAIL OFFLINE_COUNT 必须是正整数（当前: ${OFFLINE_COUNT}）"
        exit 2
        ;;
esac
if [ "$OFFLINE_COUNT" -lt 1 ]; then
    log "FAIL OFFLINE_COUNT 必须 >= 1"
    exit 2
fi

log "=============================================="
log "离线消息端到端测试"
log_diag "发送方 配置: $SENDER_CONFIG"
log_diag "接收方 配置: $RECEIVER_CONFIG  (label=$RECEIVER_LABEL)"
log_diag "离线消息条数: $OFFLINE_COUNT   RUN_TAG=$RUN_TAG"
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

# 统计包含本次 RUN_TAG 的 message.received 行数。RUN_TAG 同时出现在文本和 Ext
# 里，任一命中即计数；这样即便正文被脱敏，Ext 中的 trace_id 仍可作为凭据。
count_tagged_received() {
    local logfile="$1" n
    n=$(grep "message.received" "$logfile" 2>/dev/null | grep -c "$RUN_TAG") || n=0
    echo "$n"
}

# wait_for_tagged_count <logfile> <want> <timeout_seconds> <描述>
wait_for_tagged_count() {
    local logfile="$1" want="$2" timeout="$3" what="$4"
    local current i
    for ((i = 1; i <= timeout; i++)); do
        current=$(count_tagged_received "$logfile")
        if (( current >= want )); then
            log "OK   ${what}（约 ${i}s，已收 ${current}/${want}）"
            return 0
        fi
        if (( i % 15 == 0 )); then
            log "     仍在等待 ${what}（已 ${i}s / ${timeout}s，已收 ${current}/${want}）..."
        fi
        sleep 1
    done
    log "FAIL ${what}：${timeout}s 内带 RUN_TAG 的消息不足（当前 $(count_tagged_received "$logfile")/${want}）"
    log "----- $logfile 最近 20 行 -----"
    tail -n 20 "$logfile" 2>/dev/null || true
    log "----- $logfile 结束 -----"
    return 1
}

stop_proc() {
    local pid="$1" name="$2"
    [ -n "$pid" ] || return 0
    log "停止 $name (pid=$pid)"
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    log_diag "$name 已退出"
}

# 1) 接收方保持离线，发送方连续发送 COUNT 条单聊消息
log "==> [1/3] 接收端 $RECEIVER_LABEL 保持离线；发送方连发 $OFFLINE_COUNT 条离线消息"
: >"$SENDER_LOG"
for ((k = 1; k <= OFFLINE_COUNT; k++)); do
    MSG_TAG="${RUN_TAG}-${k}"
    ONE_LOG="${SENDER_LOG}.${k}"
    log_diag "发送第 ${k}/${OFFLINE_COUNT} 条（tag=${MSG_TAG}）"
    "$BIN" -c "$SENDER_CONFIG" -debug \
        -send-to "$RECEIVER_LABEL" \
        -send-ext "trace_id=${MSG_TAG}" \
        -send-text "${SEND_TEXT} #${k} ${MSG_TAG}" >"$ONE_LOG" 2>&1 &
    SENDER_PID=$!
    wait_for "$ONE_LOG" "message.send_succeeded" 60 "第 ${k} 条离线消息已发送"
    stop_proc "$SENDER_PID" "发送端#${k}"
    SENDER_PID=""
    cat "$ONE_LOG" >>"$SENDER_LOG" 2>/dev/null || true
    rm -f "$ONE_LOG" 2>/dev/null || true
done
log "OK   已发送 $OFFLINE_COUNT 条离线消息给离线的 $RECEIVER_LABEL"

# 给服务端留出把消息落为离线积压的短暂窗口
sleep 2

# 2) 现在才让接收方登录，触发 UNREAD 离线拉取
log "==> [2/3] 启动接收端 $RECEIVER_LABEL 登录并触发离线拉取"
: >"$RECEIVER_LOG"
"$BIN" -c "$RECEIVER_CONFIG" -debug >"$RECEIVER_LOG" 2>&1 &
RECEIVER_PID=$!
log_diag "接收进程 pid=${RECEIVER_PID}，日志 ${RECEIVER_LOG}"
wait_for "$RECEIVER_LOG" "connection.ready" 90 "$RECEIVER_LABEL 已连接"

# 3) 等待离线消息经 UNREAD 拉取到达
wait_for_tagged_count "$RECEIVER_LOG" "$OFFLINE_COUNT" 90 "$RECEIVER_LABEL 收到本次 RUN_TAG 的离线消息"

# 断言确实走了 UNREAD 离线拉取路径（-debug 下该日志在有未读队列时出现）
UNREAD_PULL_OK=0
if grep -Fq '"msg":"wss.unread_pull"' "$RECEIVER_LOG" 2>/dev/null; then
    UNREAD_PULL_OK=1
    log "OK   观察到 wss.unread_pull（离线拉取路径已触发）"
else
    log "WARN 未观察到 wss.unread_pull；消息可能通过其他路径到达（请确认 lxm2 发送期间确实离线，且已开启 -debug）"
fi

# 4) 校验
TAGGED=$(count_tagged_received "$RECEIVER_LOG")
log ""
log "==> [3/3] 校验结果"
log "带 RUN_TAG 的离线消息: 收到 ${TAGGED} 条（期望 >= ${OFFLINE_COUNT}）"
log "wss.unread_pull: $([ "$UNREAD_PULL_OK" -eq 1 ] && echo 命中 || echo 未命中)"
log ""
if [ "$TAGGED" -ge "$OFFLINE_COUNT" ] && [ "$UNREAD_PULL_OK" -eq 1 ]; then
    log "PASS：$RECEIVER_LABEL 登录后经 UNREAD 拉取到全部 $OFFLINE_COUNT 条离线消息"
else
    log "FAIL：离线消息校验未通过（tagged=${TAGGED}/want=${OFFLINE_COUNT}，unread_pull=${UNREAD_PULL_OK}）"
    log "----- 接收端最近 20 行 -----"
    tail -n 20 "$RECEIVER_LOG" 2>/dev/null || true
    exit 1
fi

log "完整日志："
log_diag "发送 : $SENDER_LOG"
log_diag "接收 : $RECEIVER_LOG"
