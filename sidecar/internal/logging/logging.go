// Package logging configures the sidecar's structured logger.
//
// All output goes to stderr — stdout is reserved for the READY IPC line.
package logging

import (
	"io"
	"log/slog"
	"strings"
)

// Init returns a logger that emits one JSON line per record to w at the
// given level. Callers typically pass os.Stderr and install the result
// as the default via slog.SetDefault.
func Init(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}

// ParseLevel maps the value of the KSTACK_LOG_LEVEL env var to a slog.Level.
// Unknown values fall back to Info so a typo doesn't silence the logger.
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
