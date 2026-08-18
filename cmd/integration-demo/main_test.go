package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	imsdk "github.com/easemob/go-im-sdk/sdk"
)

func TestParseConfigPreservesHashInsideQuotedAppKey(t *testing.T) {
	t.Setenv("GO_IM_SDK_TOKEN", "test-token")
	path := filepath.Join(t.TempDir(), "prod.yaml")
	data := []byte("app_key: \"easemob#easeim\" # comment\nuser_id: user\nresource: demo\n")
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

func TestParseKeyValuesForMessageExt(t *testing.T) {
	values, err := parseKeyValues(`trace_id=demo-123,payload={"source":"integration-demo"},items=[1;2]`)
	if err != nil {
		t.Fatal(err)
	}
	if values["trace_id"].Type != imsdk.KeyValueString || values["trace_id"].Value != "demo-123" {
		t.Fatalf("trace_id=%#v", values["trace_id"])
	}
	if values["payload"].Type != imsdk.KeyValueJSONString {
		t.Fatalf("payload=%#v", values["payload"])
	}
	// A syntactically invalid array remains a plain string.
	if values["items"].Type != imsdk.KeyValueString {
		t.Fatalf("items=%#v", values["items"])
	}
	if empty, err := parseKeyValues(""); err != nil || empty != nil {
		t.Fatalf("empty=%#v err=%v", empty, err)
	}
	if _, err := parseKeyValues("missing-separator"); err == nil {
		t.Fatal("expected key=value validation error")
	}
}

func TestMarshalSafeMessageIncludesExtButOmitsTextAndRawPayload(t *testing.T) {
	data, err := marshalSafeMessage(&imsdk.Message{
		From: "alice", To: "bob", MetaID: 7,
		Ext:  map[string]imsdk.KeyValue{"trace_id": {Type: imsdk.KeyValueString, Value: "demo-123"}},
		Body: &imsdk.MessageBody{Type: imsdk.MessageBodyText, Text: "sensitive text", RawPayload: []byte("sensitive raw")},
	})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["ext"] == nil {
		t.Fatalf("safe JSON has no ext: %s", data)
	}
	if string(data) == "" || containsAny(string(data), "sensitive text", "sensitive raw", "raw_payload", `"text":`) {
		t.Fatalf("safe JSON leaked body: %s", data)
	}
}

func containsAny(value string, candidates ...string) bool {
	for _, candidate := range candidates {
		if strings.Contains(value, candidate) {
			return true
		}
	}
	return false
}
