package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseConfigPreservesHashInsideQuotedAppKey(t *testing.T) {
	t.Setenv("GO_IM_SDK_TOKEN", "test-token")
	path := filepath.Join(t.TempDir(), "prod.yaml")
	data := []byte("msync_host: \"wss://example.test/websocket\"\nrest_base: \"https://example.test/org/app\"\napp_key: \"easemob#easeim\" # comment\nuser_id: user\n")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := parseConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AppKey != "easemob#easeim" {
		t.Fatalf("app key = %q", cfg.AppKey)
	}
}
