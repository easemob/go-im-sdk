#!/bin/bash
# 在 Docker 容器内运行 go-im-sdk 的 integration-demo。
#
# 用法：
#   ./run-docker-demo.sh [配置文件] [demo 额外参数...]      # 后台模式（默认）
#   FOREGROUND=1 ./run-docker-demo.sh [配置文件] [...]      # 前台模式，直接看日志，Ctrl+C 退出
#
# 容器名可用 NAME 环境变量覆盖，让接收端/发送端能同时共存：
#   NAME=go-sdk-recv ./run-docker-demo.sh prod-xu.yaml
#   NAME=go-sdk-send ./run-docker-demo.sh prod.yaml -send-to xu -send-text "hello"
set -euo pipefail
cd "$(dirname "$0")"

NAME="${NAME:-go-sdk-ebs-cli}"
IMAGE="golang:1.21-bookworm"
BIN="$PWD/bin/integration-demo-linux"
CFG="${1:-prod-xu.yaml}"
shift || true

# 清理同名旧容器（后台模式需要；前台 --rm 会自动清理）
if [ "${FOREGROUND:-0}" = "1" ]; then
  echo "前台运行（Ctrl+C 退出并清理容器）: $NAME"
  docker run --rm -it --name "$NAME" \
    -v "$BIN":/demo:ro \
    -v "$PWD/$CFG":/run/"$CFG":ro \
    "$IMAGE" /demo -c /run/"$CFG" "$@"
else
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run -d --name "$NAME" \
    -v "$BIN":/demo:ro \
    -v "$PWD/$CFG":/run/"$CFG":ro \
    "$IMAGE" /demo -c /run/"$CFG" "$@"
  echo "容器已启动(后台): $NAME"
  echo "查看日志:   docker logs -f $NAME"
  echo "停止容器:   docker stop $NAME"
fi
