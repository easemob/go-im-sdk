# go-im-sdk

环信 IM 服务端 Go SDK 仓库。SDK 模块位于 [`GO_IM_SDK/`](GO_IM_SDK/)，提供安全 WebSocket 长连接、实时消息收发、群组定向消息，以及用户属性和公开群 REST API。

## 快速开始

```bash
cd GO_IM_SDK
go version                 # Go 1.21+
go test -tags gopbcodec ./...
go get github.com/easemob/go-im-sdk/sdk@latest
```

Linux 客户发布构建默认使用 native codec，需要 `CGO_ENABLED=1` 和可用的 C/C++ 工具链：

```bash
cd GO_IM_SDK
CGO_ENABLED=1 go build ./cmd/your-service
```

`gopbcodec` 仅用于仓库内部回归和 macOS 本地联调，不属于客户正式发布构建。

## 文档导航

- [SDK 模块说明](GO_IM_SDK/README.md)：能力边界、构建约束、部署和安全注意事项。
- [Go 代码集成指南](GO_IM_SDK/GO_SDK_INTEGRATION_GUIDE.md)：导包、初始化、收发 Text/Custom/CMD、群组定向消息、REST API、错误处理和优雅退出。
- [Demo/API 验收说明](GO_IM_SDK/INTEGRATION_DEMO_README.md)：真实环境命令行测试、三终端群组定向验收和自动化脚本。

## 最小导包示例

业务工程中导入 SDK 子包：

```go
import imsdk "github.com/easemob/go-im-sdk/sdk"
```

初始化时至少提供 `MsyncHost`、`RestBase`、`AppKey`、`UserID`、`Token`、稳定唯一的 `Resource` 和 `MessageHandler`。完整可运行生命周期示例请直接查看 [Go 代码集成指南](GO_IM_SDK/GO_SDK_INTEGRATION_GUIDE.md)。

## 验收入口

在 `GO_IM_SDK` 目录准备 `prod.yaml` 后，可以运行：

```bash
cd GO_IM_SDK
cp config.example.yaml prod.yaml
chmod 600 prod.yaml
go build -tags gopbcodec -o ./bin/integration-demo ./cmd/integration-demo
./bin/integration-demo -c prod.yaml -debug
```

群组定向消息的三终端验收和 `test-directed-message.sh` 使用方法见 [Demo/API 验收说明](GO_IM_SDK/INTEGRATION_DEMO_README.md)。
