// Package logging configures the sidecar's structured logger.
//
// All output goes to stderr — stdout is reserved for the READY IPC line.
package logging

import (
	"io"
	"log/slog"
	"strings"
)

// Init returns a logger emitting one JSON line per record to w, typically os.Stderr.
// Everything it writes is rendered through internal/safe on the way out — see redact.go.
func Init(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(redactHandler{slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})})
}

// ParseLevel maps KSTACK_LOG_LEVEL to a slog.Level; an unknown value falls back to Info
// so a typo can't silence the logger.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
