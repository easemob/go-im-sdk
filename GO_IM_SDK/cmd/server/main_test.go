package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigTokenPriority(t *testing.T) {
	dir := t.TempDir()
	secret := filepath.Join(dir, "token")
	if err := os.WriteFile(secret, []byte("file-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(dir, "config.yaml")
	body := "app_key: \"org#app\"\nuser_id: bot\nresource: server-01\ntoken_file: \"" + secret + "\"\ntoken: yaml-token\n"
	if err := os.WriteFile(config, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}

	t.Setenv(tokenEnv, "env-token")
	fc, err := loadConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	if fc.Token != "env-token" {
		t.Fatalf("token = %q, want environment token", fc.Token)
	}

	t.Setenv(tokenEnv, "")
	fc, err = loadConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	if fc.Token != "file-token" {
		t.Fatalf("token = %q, want secret file token", fc.Token)
	}
}

func TestLoadConfigRejectsInsecureInlineToken(t *testing.T) {
	t.Setenv(tokenEnv, "")
	t.Setenv(tokenFileEnv, "")
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("token: inline-secret\n"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := loadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "require 0600") {
		t.Fatalf("loadConfig error = %v", err)
	}
}

func TestReadFlatYAMLRejectsUnknownAndDuplicateKeys(t *testing.T) {
	for name, body := range map[string]string{
		"unknown":   "unexpected: value\n",
		"duplicate": "user_id: one\nuser_id: two\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadConfig(path); err == nil {
				t.Fatal("loadConfig unexpectedly succeeded")
			}
		})
	}
}
