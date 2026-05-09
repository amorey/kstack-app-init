package logging_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/kubetail-org/kstack-app/sidecar/internal/logging"
)

func TestParseLevel(t *testing.T) {
	cases := []struct {
		in   string
		want slog.Level
	}{
		{"", slog.LevelInfo},
		{"info", slog.LevelInfo},
		{"INFO", slog.LevelInfo},
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"garbage", slog.LevelInfo}, // default-on-unknown rather than fail-closed
	}
	for _, c := range cases {
		if got := logging.ParseLevel(c.in); got != c.want {
			t.Errorf("ParseLevel(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestInitWritesJSONAtConfiguredLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.Init(&buf, slog.LevelInfo)
	logger.Info("hello", "k", "v")

	var out map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &out); err != nil {
		t.Fatalf("decode %q: %v", buf.String(), err)
	}
	if out["level"] != "INFO" || out["msg"] != "hello" || out["k"] != "v" {
		t.Errorf("unexpected log entry: %v", out)
	}
	if _, ok := out["time"]; !ok {
		t.Errorf("missing time field: %v", out)
	}
}

func TestInitSuppressesBelowLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.Init(&buf, slog.LevelInfo)
	logger.Debug("not shown")
	if buf.Len() != 0 {
		t.Errorf("expected no output, got %q", buf.String())
	}
}
