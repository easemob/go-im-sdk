// Command integration-demo exercises the customer-facing WSS lifecycle and
// optionally sends one safe test message. It is an acceptance aid, not a
// production message processor.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	imsdk "github.com/easemob/go-im-sdk/sdk"
)

func main() {
	config := flag.String("config", "", "flat YAML config path")
	flag.StringVar(config, "c", "", "flat YAML config path (shorthand)")
	sendTo := flag.String("send-to", "", "optional recipient for one test message")
	sendType := flag.String("send-type", "text", "message type: text, command or custom")
	sendText := flag.String("send-text", "integration-demo test", "text body for -send-type=text")
	sendAction := flag.String("send-action", "", "command action for -send-type=command")
	sendEvent := flag.String("send-event", "", "custom event for -send-type=custom")
	sendParams := flag.String("send-params", "", "comma-separated key=value pairs (command params / custom extensions)")
	sendExt := flag.String("send-ext", "", "comma-separated message extension key=value pairs")
	sendGroup := flag.Bool("group", false, "send to a group instead of a single user")
	directedUsers := flag.String("directed-users", "", "comma-separated user IDs for a directed group message")
	probeREST := flag.Bool("probe-rest", false, "run a safe user-info REST probe after connecting")
	setUser := flag.String("set-user", "", "set user properties: key=value,key2=value2")
	fetchUsers := flag.String("fetch-users", "", "fetch user properties for comma-separated user IDs")
	fetchProperties := flag.String("fetch-properties", "", "optional comma-separated properties for -fetch-users")
	createGroup := flag.String("create-group", "", "create a public group with this name")
	groupMembers := flag.String("group-members", "", "optional comma-separated members for -create-group")
	joinGroup := flag.String("join-group", "", "join a public group by ID")
	leaveGroup := flag.String("leave-group", "", "leave a group by ID")
	debug := flag.Bool("debug", false, "log WSS command/queue/ACK metadata without payloads or tokens")
	flag.Parse()
	if *config == "" {
		fmt.Fprintln(os.Stderr, "usage: go run ./cmd/integration-demo -c prod.yaml [-send-to USER [-send-type text|command|custom] [-send-action ACTION] [-send-event EVENT] [-send-params k=v,k2=v2] [-send-ext k=v] [-group] [-directed-users u1,u2]] [-probe-rest] [-set-user key=value] [-fetch-users user[,user]] [-create-group NAME] [-join-group ID] [-leave-group ID]")
		os.Exit(2)
	}

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	fc, err := loadConfig(*config)
	if err != nil {
		logger.Error("integration configuration rejected", "error", err)
		os.Exit(2)
	}
	var client *imsdk.Client
	client, err = imsdk.New(imsdk.Config{
		AppKey: fc.AppKey, Domain: fc.Domain, Resource: fc.Resource,
		HeartbeatInterval: seconds(fc.HeartbeatIntervalSeconds), HeartbeatTimeout: seconds(fc.HeartbeatTimeoutSeconds),
		ConnectTimeout: seconds(fc.ConnectTimeoutSeconds), SendTimeout: seconds(fc.SendTimeoutSeconds),
		LogoutTimeout: seconds(fc.LogoutTimeoutSeconds), DisableReconnect: fc.DisableReconnect,
		MaxRedirectHops: fc.MaxRedirectHops, MaxFrameBytes: fc.MaxFrameBytes, WriteQueueSize: fc.WriteQueueSize,
		HandlerTimeout: seconds(fc.HandlerTimeoutSeconds), HandlerMaxAttempts: fc.HandlerMaxAttempts,
		HandlerConcurrency: fc.HandlerConcurrency, TokenExpiryWarningBefore: seconds(fc.TokenExpiryWarningSeconds),
		Logger: logger, Debug: *debug,
		MessageHandler: func(_ context.Context, msg *imsdk.Message) error {
			full, marshalErr := marshalSafeMessage(msg)
			attrs := []any{"meta_id", msg.MetaID, "from", msg.From, "to", msg.To,
				"is_group", msg.IsGroup, "body_count", len(msg.Bodies), "body_bytes", messageBodyBytes(msg),
				"ext_count", len(msg.Ext)}
			if marshalErr == nil {
				attrs = append(attrs, "message_json", string(full))
			}
			for i, body := range msg.Bodies {
				attrs = append(attrs, fmt.Sprintf("body_%d_type", i), body.Type,
					fmt.Sprintf("body_%d_text_bytes", i), len(body.Text),
					fmt.Sprintf("body_%d_action", i), body.Action,
					fmt.Sprintf("body_%d_event", i), body.Event)
			}
			logger.Info("message.received", attrs...)
			return nil
		},
		OnConnectionStateChanged: func(state imsdk.ConnState) {
			h := client.Health()
			logger.Info("connection.state", "state", state.String(), "session_id", h.SessionID,
				"generation", h.ConnectionGeneration)
		},
		OnDisconnect: func(err error) {
			logger.Warn("connection.disconnected", "error", err, "health", client.Health())
		},
		OnTokenExpired:    func() { logger.Warn("token.expired") },
		OnTokenWillExpire: func(at time.Time) { logger.Warn("token.will_expire", "expires_at", at.UTC().Format(time.RFC3339)) },
	})
	if err != nil {
		logger.Error("integration SDK initialization failed", "error", err)
		os.Exit(1)
	}
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), durationOr(fc.ConnectTimeoutSeconds, 15))
	err = client.Login(ctx, fc.UserID, fc.Token)
	cancel()
	if err != nil {
		logger.Error("connection.failed", "error", err)
		return
	}
	h := client.Health()
	logger.Info("connection.ready", "session_id", h.SessionID, "generation", h.ConnectionGeneration)

	if *sendTo != "" {
		body, buildErr := buildSendBody(*sendType, *sendText, *sendAction, *sendEvent, *sendParams)
		ext, extErr := parseKeyValues(*sendExt)
		if buildErr != nil || extErr != nil {
			if buildErr == nil {
				buildErr = fmt.Errorf("message ext: %w", extErr)
			}
			logger.Error("message.send_failed", "from", fc.UserID, "to", *sendTo, "ext_count", len(ext), "error", buildErr)
		} else {
			req := imsdk.SendRequest{To: *sendTo, IsGroup: *sendGroup, DirectedUsers: splitCSV(*directedUsers), Ext: ext, Body: body}
			result, err := sendTestMessage(client, req)
			if err != nil {
				logger.Error("message.send_failed", "from", fc.UserID, "to", *sendTo, "ext_count", len(req.Ext), "error", err)
			} else {
				logger.Info("message.send_succeeded", "to", *sendTo, "message_id", result.MessageID,
					"client_message_id", result.ClientMessageID, "server_timestamp", result.ServerTimestamp,
					"ext_count", len(req.Ext))
			}
		}
	}
	if *probeREST {
		probeCtx, cancelProbe := context.WithTimeout(context.Background(), 15*time.Second)
		response, probeErr := client.FetchUserInfo(probeCtx, []string{fc.UserID}, nil)
		cancelProbe()
		if probeErr != nil {
			var apiErr *imsdk.APIError
			if errors.As(probeErr, &apiErr) {
				status := 0
				if apiErr.Response != nil {
					status = apiErr.Response.StatusCode
				}
				logger.Error("rest.probe_failed", "status", status, "request_id", apiErr.RequestID,
					"service_code", apiErr.ServiceCode, "error", probeErr)
			} else {
				logger.Error("rest.probe_failed", "error", probeErr)
			}
		} else {
			logger.Info("rest.probe_succeeded", "status", response.StatusCode, "request_id", response.Header.Get("X-Request-ID"))
		}
	}
	restCtx, cancelREST := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelREST()
	if *setUser != "" {
		attrs, parseErr := parseProperties(*setUser)
		if parseErr != nil {
			logger.Error("rest.set_user_failed", "error", parseErr)
		} else if response, callErr := client.UpdateOwnUserInfo(restCtx, attrs); callErr != nil {
			logRESTError(logger, "rest.set_user_failed", callErr)
		} else {
			logger.Info("rest.set_user_succeeded", "status", response.StatusCode, "body", string(response.Body))
		}
	}
	if *fetchUsers != "" {
		users := splitCSV(*fetchUsers)
		properties := splitCSV(*fetchProperties)
		if response, callErr := client.FetchUserInfo(restCtx, users, properties); callErr != nil {
			logRESTError(logger, "rest.fetch_users_failed", callErr)
		} else {
			logger.Info("rest.fetch_users_succeeded", "status", response.StatusCode, "body", string(response.Body))
		}
	}
	if *createGroup != "" {
		response, callErr := client.CreatePublicGroup(restCtx, *createGroup, imsdk.CreatePublicGroupOptions{Members: splitCSV(*groupMembers)})
		if callErr != nil {
			logRESTError(logger, "rest.create_group_failed", callErr)
		} else {
			logger.Info("rest.create_group_succeeded", "status", response.StatusCode, "body", string(response.Body))
		}
	}
	if *joinGroup != "" {
		if response, callErr := client.JoinPublicGroup(restCtx, *joinGroup); callErr != nil {
			logRESTError(logger, "rest.join_group_failed", callErr)
		} else {
			logger.Info("rest.join_group_succeeded", "status", response.StatusCode, "body", string(response.Body))
		}
	}
	if *leaveGroup != "" {
		if response, callErr := client.LeaveGroup(restCtx, *leaveGroup); callErr != nil {
			logRESTError(logger, "rest.leave_group_failed", callErr)
		} else {
			logger.Info("rest.leave_group_succeeded", "status", response.StatusCode, "body", string(response.Body))
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	signal.Stop(sigCh)
	logger.Info("shutdown.requested", "signal", sig.String())
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), durationOr(fc.LogoutTimeoutSeconds, 5))
	if err := client.Logout(shutdown); err != nil {
		logger.Warn("logout.failed", "error", err)
	}
	cancelShutdown()
	logger.Info("shutdown.complete")
}

func sendTestMessage(client *imsdk.Client, req imsdk.SendRequest) (*imsdk.SendResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if req.ClientMessageID == 0 {
		req.ClientMessageID = uint64(time.Now().UnixNano())
	}
	result, err := client.Send(ctx, req)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("send returned nil result")
	}
	return result, nil
}

func buildSendBody(bodyType, text, action, event, params string) (imsdk.MessageBody, error) {
	switch bodyType {
	case "text":
		return imsdk.MessageBody{Type: imsdk.MessageBodyText, Text: text}, nil
	case "command":
		kv, err := parseKeyValues(params)
		if err != nil {
			return imsdk.MessageBody{}, fmt.Errorf("command params: %w", err)
		}
		return imsdk.MessageBody{Type: imsdk.MessageBodyCommand, Action: action, Params: kv}, nil
	case "custom":
		kv, err := parseKeyValues(params)
		if err != nil {
			return imsdk.MessageBody{}, fmt.Errorf("custom extensions: %w", err)
		}
		return imsdk.MessageBody{Type: imsdk.MessageBodyCustom, Event: event, CustomExts: kv}, nil
	default:
		return imsdk.MessageBody{}, fmt.Errorf("unknown send type %q (want text, command or custom)", bodyType)
	}
}

// parseKeyValues parses "k1=v1,k2=v2". Values that look like JSON objects or
// arrays are sent as json_string; everything else is a plain string. JSON
// values containing commas are not supported via this flag; use Go code.
func parseKeyValues(value string) (map[string]imsdk.KeyValue, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	out := make(map[string]imsdk.KeyValue)
	for _, item := range splitCSV(value) {
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			return nil, fmt.Errorf("key value must use key=value: %q", item)
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		kv := imsdk.KeyValue{Type: imsdk.KeyValueString, Value: val}
		if (strings.HasPrefix(val, "{") && strings.HasSuffix(val, "}")) ||
			(strings.HasPrefix(val, "[") && strings.HasSuffix(val, "]")) {
			if json.Valid([]byte(val)) {
				kv = imsdk.KeyValue{Type: imsdk.KeyValueJSONString, Value: val}
			}
		}
		out[key] = kv
	}
	return out, nil
}

func marshalSafeMessage(msg *imsdk.Message) ([]byte, error) {
	type safeBody struct {
		Type   imsdk.MessageBodyType `json:"type"`
		Action string                `json:"action,omitempty"`
		Event  string                `json:"event,omitempty"`
	}
	type safeMessage struct {
		From      string                    `json:"from"`
		To        string                    `json:"to"`
		IsGroup   bool                      `json:"is_group"`
		MetaID    uint64                    `json:"meta_id"`
		Timestamp uint64                    `json:"timestamp"`
		Bodies    []safeBody                `json:"bodies"`
		Ext       map[string]imsdk.KeyValue `json:"ext,omitempty"`
	}
	view := safeMessage{
		From: msg.From, To: msg.To, IsGroup: msg.IsGroup, MetaID: msg.MetaID,
		Timestamp: msg.Timestamp, Ext: msg.Ext,
	}
	for _, body := range msg.Bodies {
		view.Bodies = append(view.Bodies, safeBody{Type: body.Type, Action: body.Action, Event: body.Event})
	}
	return json.Marshal(view)
}

func messageBodyBytes(msg *imsdk.Message) int {
	n := 0
	for _, body := range msg.Bodies {
		n += len(body.Text) + len(body.Action) + len(body.Event) + len(body.RawPayload)
	}
	return n
}

func splitCSV(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func parseProperties(value string) (map[string]string, error) {
	out := make(map[string]string)
	for _, item := range splitCSV(value) {
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			return nil, fmt.Errorf("property must use key=value: %q", item)
		}
		out[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	return out, nil
}

func logRESTError(logger *slog.Logger, msg string, err error) {
	var apiErr *imsdk.APIError
	if errors.As(err, &apiErr) {
		status := 0
		if apiErr.Response != nil {
			status = apiErr.Response.StatusCode
		}
		logger.Error(msg, "status", status, "request_id", apiErr.RequestID, "service_code", apiErr.ServiceCode, "error", err)
		return
	}
	logger.Error(msg, "error", err)
}

func seconds(v int) time.Duration {
	if v <= 0 {
		return 0
	}
	return time.Duration(v) * time.Second
}

func durationOr(v, fallback int) time.Duration {
	if v <= 0 {
		v = fallback
	}
	return time.Duration(v) * time.Second
}

// Reuse the deliberately strict deployment parser from cmd/server without a
// new dependency. The demo is copied into this package at release time.
func loadConfig(path string) (fileConfig, error) { return parseConfig(path) }

type fileConfig struct {
	AppKey, UserID, Token, Domain, Resource                                                  string
	HeartbeatIntervalSeconds, HeartbeatTimeoutSeconds, ConnectTimeoutSeconds                 int
	SendTimeoutSeconds, LogoutTimeoutSeconds, MaxRedirectHops, WriteQueueSize                int
	HandlerTimeoutSeconds, HandlerMaxAttempts, HandlerConcurrency, TokenExpiryWarningSeconds int
	MaxFrameBytes                                                                            int64
	DisableReconnect                                                                         bool
}

func parseConfig(path string) (fileConfig, error) {
	// Keep integration-demo self-contained and intentionally explicit. In
	// production, use cmd/server's parser and secret-file policy.
	b, err := os.ReadFile(path)
	if err != nil {
		return fileConfig{}, err
	}
	var c fileConfig
	var tokenFile string
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(stripYAMLComment(line))
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key, value := strings.TrimSpace(parts[0]), strings.Trim(strings.TrimSpace(parts[1]), "\"")
		switch key {
		case "app_key":
			c.AppKey = value
		case "user_id":
			c.UserID = value
		case "token":
			c.Token = value
		case "token_file":
			tokenFile = value
		case "domain":
			c.Domain = value
		case "resource":
			c.Resource = value
		case "heartbeat_interval_seconds":
			c.HeartbeatIntervalSeconds, _ = atoi(value)
		case "heartbeat_timeout_seconds":
			c.HeartbeatTimeoutSeconds, _ = atoi(value)
		case "connect_timeout_seconds":
			c.ConnectTimeoutSeconds, _ = atoi(value)
		case "send_timeout_seconds":
			c.SendTimeoutSeconds, _ = atoi(value)
		case "logout_timeout_seconds":
			c.LogoutTimeoutSeconds, _ = atoi(value)
		case "max_redirect_hops":
			c.MaxRedirectHops, _ = atoi(value)
		case "write_queue_size":
			c.WriteQueueSize, _ = atoi(value)
		case "handler_timeout_seconds":
			c.HandlerTimeoutSeconds, _ = atoi(value)
		case "handler_max_attempts":
			c.HandlerMaxAttempts, _ = atoi(value)
		case "handler_concurrency":
			c.HandlerConcurrency, _ = atoi(value)
		case "token_expiry_warning_seconds":
			c.TokenExpiryWarningSeconds, _ = atoi(value)
		case "max_frame_bytes":
			c.MaxFrameBytes, _ = atoi64(value)
		case "disable_reconnect":
			c.DisableReconnect = value == "true"
		}
	}
	if token := strings.TrimSpace(os.Getenv("GO_IM_SDK_TOKEN")); token != "" {
		c.Token = token
	} else if path := strings.TrimSpace(os.Getenv("GO_IM_SDK_TOKEN_FILE")); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return c, fmt.Errorf("read token file: %w", err)
		}
		c.Token = strings.TrimSpace(string(data))
	} else if tokenFile != "" {
		data, err := os.ReadFile(tokenFile)
		if err != nil {
			return c, fmt.Errorf("read token file: %w", err)
		}
		c.Token = strings.TrimSpace(string(data))
	}
	if c.Token == "" {
		return c, fmt.Errorf("token is required")
	}
	return c, nil
}

// YAML treats # inside a quoted scalar as data, not a comment. App keys use
// org#app, so a plain strings.Split on # corrupts otherwise valid config.
func stripYAMLComment(line string) string {
	var quote rune
	escaped := false
	for i, r := range line {
		if escaped {
			escaped = false
			continue
		}
		if quote == '"' && r == '\\' {
			escaped = true
			continue
		}
		if r == '\'' || r == '"' {
			if quote == 0 {
				quote = r
			} else if quote == r {
				quote = 0
			}
			continue
		}
		if r == '#' && quote == 0 && (i == 0 || line[i-1] == ' ' || line[i-1] == '\t') {
			return line[:i]
		}
	}
	return line
}

func atoi(v string) (int, error)     { var n int; _, e := fmt.Sscan(v, &n); return n, e }
func atoi64(v string) (int64, error) { var n int64; _, e := fmt.Sscan(v, &n); return n, e }
