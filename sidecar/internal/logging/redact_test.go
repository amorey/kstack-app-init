// Copyright 2026 The Kstack Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logging_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/logging"
	"github.com/kubetail-org/kstack-app/sidecar/internal/safe"
)

// The boundary that matters: the bytes this handler writes are what leaves the process,
// and the host forwards whatever it is given. A rendered error must survive JSON encoding
// with the credential still gone — a Rust test in logs.rs would only prove the forwarder
// forwards.
func TestInitWritesNoCredentialFromARenderedError(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.Init(&buf, slog.LevelInfo)

	err := errors.New(`Post "https://issuer.example/token?client_secret=SEKRIT": 401 Unauthorized`)
	logger.Error("cloud sign-in failed", "err", safe.Safe(err))

	if strings.Contains(buf.String(), "SEKRIT") {
		t.Errorf("the credential reached the writer: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "401 Unauthorized") {
		t.Errorf("the diagnostic did not survive: %s", buf.String())
	}
}

// The backstop. Every call site of ours renders its own error, but the loggers we do not
// own — beehive's verdicts, client-go, oauth2 — log through this handler and pass theirs
// whole. Message, attr, With-attr and group are the four positions a record has, and the
// count is what proves the value was rendered rather than dropped.
func TestInitRedactsWhatACallerDidNotRender(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.Init(&buf, slog.LevelInfo).With("cache", "https://api.example.com/api?token=SEKRIT")

	logger.Error(`reconcile "https://api.example.com/x?token=SEKRIT" failed`,
		"err", errors.New(`Get "https://api.example.com/readyz?token=SEKRIT": 401 Unauthorized`),
		slog.Group("request", "url", "https://api.example.com/y?token=SEKRIT"))

	if strings.Contains(buf.String(), "SEKRIT") {
		t.Errorf("a raw error reached the writer: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "401 Unauthorized") {
		t.Errorf("the diagnostic did not survive: %s", buf.String())
	}
	if n := strings.Count(buf.String(), "api.example.com"); n != 4 {
		t.Errorf("want all four positions rendered and kept, got %d: %s", n, buf.String())
	}
}

// A group inside a group: only recursion reaches the leaf. WithGroup is not this case —
// the inner handler does that nesting, and the attr still arrives as a plain string.
func TestInitRedactsAGroupNestedInAGroup(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.Init(&buf, slog.LevelInfo)

	logger.Info("probe", slog.Group("outer",
		slog.Group("inner", "url", "https://api.example.com/z?token=SEKRIT")))

	if strings.Contains(buf.String(), "SEKRIT") {
		t.Errorf("a nested group reached the writer unrendered: %s", buf.String())
	}
	if !strings.Contains(buf.String(), `"outer":{"inner":{"url":`) {
		t.Errorf("the group structure was not preserved: %s", buf.String())
	}
}

// WithGroup's own path: the handler must hand the name to the inner handler rather than
// swallow it, or every field logged after one collapses into the record's top level.
func TestInitKeepsWithGroupNesting(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.Init(&buf, slog.LevelInfo).WithGroup("outer").WithGroup("inner")

	logger.Info("probe", "url", "https://api.example.com/z?token=SEKRIT")

	if strings.Contains(buf.String(), "SEKRIT") {
		t.Errorf("the credential reached the writer: %s", buf.String())
	}
	if !strings.Contains(buf.String(), `"outer":{"inner":{"url":`) {
		t.Errorf("the group structure was not preserved: %s", buf.String())
	}
}

// Only strings and errors are rendered. A structured field that arrives as a number, a
// bool or a duration must reach the writer as its own JSON type — a renderer that
// stringified everything would quietly turn every log field into text.
func TestInitLeavesNonTextValuesTyped(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.Init(&buf, slog.LevelInfo)

	logger.Info("sweep", "cacheID", 7, "stopped", true, "took", 250*time.Millisecond)

	var out map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &out); err != nil {
		t.Fatalf("decode %q: %v", buf.String(), err)
	}
	if out["cacheID"] != float64(7) || out["stopped"] != true || out["took"] != float64(250*time.Millisecond) {
		t.Errorf("a typed value was rewritten: %v", out)
	}
}

// A LogValuer produces its value after the handler chain would otherwise be done, so the
// renderer resolves first. Without that, the credential is encoded past it.
func TestInitRendersAResolvedLogValuer(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.Init(&buf, slog.LevelInfo)

	logger.Info("dial", "endpoint", lazyEndpoint{})

	if strings.Contains(buf.String(), "SEKRIT") {
		t.Errorf("a LogValuer's value reached the writer unrendered: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "api.example.com") {
		t.Errorf("the diagnostic did not survive: %s", buf.String())
	}
}

type lazyEndpoint struct{}

func (lazyEndpoint) LogValue() slog.Value {
	return slog.StringValue("https://api.example.com/api?token=SEKRIT")
}
