// Command server is a minimal, production-oriented example for running one
// long-lived IM user session. Applications normally import sdk directly.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	imsdk "github.com/easemob/go-im-sdk/sdk"
)

const (
	tokenEnv     = "GO_IM_SDK_TOKEN"
	tokenFileEnv = "GO_IM_SDK_TOKEN_FILE"
)

type fileConfig struct {
	AppKey                    string
	UserID                    string
	Token                     string
	TokenFile                 string
	Domain                    string
	Resource                  string
	HeartbeatIntervalSeconds  int
	HeartbeatTimeoutSeconds   int
	DisableReconnect          bool
	ConnectTimeoutSeconds     int
	SendTimeoutSeconds        int
	LogoutTimeoutSeconds      int
	MaxRedirectHops           int
	MaxFrameBytes             int64
	WriteQueueSize            int
	HandlerTimeoutSeconds     int
	HandlerMaxAttempts        int
	HandlerConcurrency        int
	TokenExpiryWarningSeconds int
}

func main() {
	configPath := flag.String("config", "", "path to the YAML configuration file")
	flag.StringVar(configPath, "c", "", "path to the YAML configuration file (shorthand)")
	flag.Parse()
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "usage: go-im-sdk-server -config CONFIG.yaml")
		os.Exit(2)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	fc, err := loadConfig(*configPath)
	if err != nil {
		logger.Error("configuration rejected", "error", err)
		os.Exit(2)
	}

	client, err := imsdk.New(imsdk.Config{
		AppKey:                   fc.AppKey,
		Domain:                   fc.Domain,
		Resource:                 fc.Resource,
		HeartbeatInterval:        seconds(fc.HeartbeatIntervalSeconds),
		HeartbeatTimeout:         seconds(fc.HeartbeatTimeoutSeconds),
		ConnectTimeout:           seconds(fc.ConnectTimeoutSeconds),
		SendTimeout:              seconds(fc.SendTimeoutSeconds),
		LogoutTimeout:            seconds(fc.LogoutTimeoutSeconds),
		DisableReconnect:         fc.DisableReconnect,
		MaxRedirectHops:          fc.MaxRedirectHops,
		MaxFrameBytes:            fc.MaxFrameBytes,
		WriteQueueSize:           fc.WriteQueueSize,
		HandlerTimeout:           seconds(fc.HandlerTimeoutSeconds),
		HandlerMaxAttempts:       fc.HandlerMaxAttempts,
		HandlerConcurrency:       fc.HandlerConcurrency,
		TokenExpiryWarningBefore: seconds(fc.TokenExpiryWarningSeconds),
		Logger:                   logger,
		MessageHandler: func(_ context.Context, msg *imsdk.Message) error {
			// Persist or dispatch the message transactionally here. Returning nil
			// acknowledges the queue batch; return an error to prevent acknowledgement.
			logger.Info("message received", "meta_id", msg.MetaID, "from", msg.From, "is_group", msg.IsGroup)
			return nil
		},
		OnConnectionStateChanged: func(state imsdk.ConnState) {
			logger.Info("IM connection state changed", "state", state.String())
		},
		OnDisconnect:   func(err error) { logger.Warn("IM session disconnected", "error", err) },
		OnTokenExpired: func() { logger.Warn("IM token expired") },
		OnTokenWillExpire: func(expiresAt time.Time) {
			logger.Warn("IM token will expire", "expires_at", expiresAt.UTC().Format(time.RFC3339))
		},
	})
	if err != nil {
		logger.Error("SDK configuration rejected", "error", err)
		os.Exit(2)
	}

	connectCtx, cancelConnect := context.WithTimeout(context.Background(), durationOr(fc.ConnectTimeoutSeconds, 15))
	err = client.Login(connectCtx, fc.UserID, fc.Token)
	cancelConnect()
	if err != nil {
		logger.Error("login failed", "error", err)
		os.Exit(1)
	}
	logger.Info("IM SDK connected", "user_id", fc.UserID)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	signal.Stop(sigCh)
	logger.Info("shutdown requested", "signal", sig.String())

	shutdownTimeout := durationOr(fc.LogoutTimeoutSeconds, 5)
	logoutCtx, cancelLogout := context.WithTimeout(context.Background(), shutdownTimeout)
	logoutErr := client.Logout(logoutCtx)
	cancelLogout()
	if logoutErr != nil {
		logger.Warn("graceful logout did not complete", "error", logoutErr)
	}

	closeCtx, cancelClose := context.WithTimeout(context.Background(), shutdownTimeout)
	closeErr := client.Close(closeCtx)
	cancelClose()
	if closeErr != nil && !errors.Is(closeErr, context.Canceled) {
		logger.Error("SDK close failed", "error", closeErr)
		os.Exit(1)
	}
	logger.Info("shutdown complete")
}

func seconds(value int) time.Duration {
	if value == 0 {
		return 0
	}
	return time.Duration(value) * time.Second
}

func durationOr(value, fallback int) time.Duration {
	if value <= 0 {
		value = fallback
	}
	return time.Duration(value) * time.Second
}

// loadConfig parses the deliberately flat YAML schema used by the example.
// Keeping the deployment parser small avoids adding a YAML library to the SDK.
func loadConfig(path string) (fileConfig, error) {
	values, err := readFlatYAML(path)
	if err != nil {
		return fileConfig{}, err
	}
	known := map[string]bool{
		"app_key": true, "user_id": true,
		"token": true, "token_file": true, "domain": true, "resource": true,
		"heartbeat_interval_seconds": true, "heartbeat_timeout_seconds": true, "disable_reconnect": true,
		"connect_timeout_seconds": true, "send_timeout_seconds": true, "logout_timeout_seconds": true,
		"max_redirect_hops": true, "max_frame_bytes": true, "write_queue_size": true,
		"handler_timeout_seconds": true, "handler_max_attempts": true, "handler_concurrency": true,
		"token_expiry_warning_seconds": true,
	}
	for key := range values {
		if !known[key] {
			return fileConfig{}, fmt.Errorf("unknown configuration key %q", key)
		}
	}

	fc := fileConfig{
		AppKey: values["app_key"],
		UserID: values["user_id"], Token: values["token"], TokenFile: values["token_file"],
		Domain: values["domain"], Resource: values["resource"],
	}
	if envToken := strings.TrimSpace(os.Getenv(tokenEnv)); envToken != "" {
		fc.Token = envToken
	} else {
		secretPath := strings.TrimSpace(os.Getenv(tokenFileEnv))
		if secretPath == "" {
			secretPath = fc.TokenFile
		}
		if secretPath != "" {
			fc.Token, err = readSecret(secretPath)
			if err != nil {
				return fileConfig{}, err
			}
		} else if fc.Token != "" {
			if err := requirePrivateFile(path); err != nil {
				return fileConfig{}, fmt.Errorf("token in YAML: %w", err)
			}
		}
	}
	if fc.Token == "" {
		return fileConfig{}, fmt.Errorf("token is required: set %s, %s, token_file, or token", tokenEnv, tokenFileEnv)
	}

	if fc.HeartbeatIntervalSeconds, err = positiveInt(values, "heartbeat_interval_seconds"); err != nil {
		return fileConfig{}, err
	}
	if fc.HeartbeatTimeoutSeconds, err = positiveInt(values, "heartbeat_timeout_seconds"); err != nil {
		return fileConfig{}, err
	}
	if fc.ConnectTimeoutSeconds, err = positiveInt(values, "connect_timeout_seconds"); err != nil {
		return fileConfig{}, err
	}
	if fc.SendTimeoutSeconds, err = positiveInt(values, "send_timeout_seconds"); err != nil {
		return fileConfig{}, err
	}
	if fc.LogoutTimeoutSeconds, err = positiveInt(values, "logout_timeout_seconds"); err != nil {
		return fileConfig{}, err
	}
	if fc.MaxRedirectHops, err = positiveInt(values, "max_redirect_hops"); err != nil {
		return fileConfig{}, err
	}
	if fc.WriteQueueSize, err = positiveInt(values, "write_queue_size"); err != nil {
		return fileConfig{}, err
	}
	if fc.HandlerTimeoutSeconds, err = positiveInt(values, "handler_timeout_seconds"); err != nil {
		return fileConfig{}, err
	}
	if fc.HandlerMaxAttempts, err = positiveInt(values, "handler_max_attempts"); err != nil {
		return fileConfig{}, err
	}
	if fc.HandlerConcurrency, err = positiveInt(values, "handler_concurrency"); err != nil {
		return fileConfig{}, err
	}
	if fc.TokenExpiryWarningSeconds, err = positiveInt(values, "token_expiry_warning_seconds"); err != nil {
		return fileConfig{}, err
	}
	if raw := values["max_frame_bytes"]; raw != "" {
		fc.MaxFrameBytes, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || fc.MaxFrameBytes <= 0 {
			return fileConfig{}, fmt.Errorf("max_frame_bytes must be a positive integer")
		}
	}
	if raw := values["disable_reconnect"]; raw != "" {
		fc.DisableReconnect, err = strconv.ParseBool(raw)
		if err != nil {
			return fileConfig{}, fmt.Errorf("disable_reconnect must be true or false")
		}
	}
	return fc, nil
}

func positiveInt(values map[string]string, key string) (int, error) {
	raw := values[key]
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}

func readFlatYAML(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()
	values := make(map[string]string)
	scanner := bufio.NewScanner(f)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := strings.TrimSpace(stripComment(scanner.Text()))
		if line == "" {
			continue
		}
		key, raw, ok := strings.Cut(line, ":")
		key = strings.TrimSpace(key)
		if !ok || key == "" || strings.ContainsAny(key, " \t[]{}") {
			return nil, fmt.Errorf("config line %d: expected a flat key: value pair", lineNo)
		}
		if _, exists := values[key]; exists {
			return nil, fmt.Errorf("config line %d: duplicate key %q", lineNo, key)
		}
		value, err := parseScalar(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("config line %d: %w", lineNo, err)
		}
		values[key] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return values, nil
}

func stripComment(line string) string {
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

func parseScalar(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if raw[0] == '"' {
		value, err := strconv.Unquote(raw)
		if err != nil {
			return "", fmt.Errorf("invalid quoted string")
		}
		return value, nil
	}
	if raw[0] == '\'' {
		if len(raw) < 2 || raw[len(raw)-1] != '\'' {
			return "", fmt.Errorf("invalid quoted string")
		}
		return strings.ReplaceAll(raw[1:len(raw)-1], "''", "'"), nil
	}
	if strings.ContainsAny(raw, "[]{}") {
		return "", fmt.Errorf("only scalar values are supported")
	}
	return strings.TrimSpace(raw), nil
}

func requirePrivateFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("%s permissions are %04o; require 0600 or stricter", path, info.Mode().Perm())
	}
	return nil
}

func readSecret(path string) (string, error) {
	if err := requirePrivateFile(path); err != nil {
		return "", fmt.Errorf("token secret file: %w", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read token secret file: %w", err)
	}
	token := strings.TrimSpace(string(b))
	if token == "" {
		return "", fmt.Errorf("token secret file %s is empty", path)
	}
	return token, nil
}
