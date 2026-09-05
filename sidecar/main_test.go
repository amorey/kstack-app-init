package main

import (
	"strings"
	"testing"
)

// A bad flag exits 2 without binding anything: the flag package has already
// written the problem to stderr, so run only reports the code.
func TestRunRejectsABadFlag(t *testing.T) {
	var out strings.Builder
	if code := run([]string{"--nope"}, strings.NewReader(""), &out); code != 2 {
		t.Errorf("exit code = %d, want 2", code)
	}
	if out.String() != "" {
		t.Errorf("stdout = %q, want empty (nothing was bound)", out.String())
	}
}

// A config the composition root rejects exits 1. READY is announced only once
// the app is built, so the host never sees an endpoint that will not serve.
func TestRunFailsWhenTheAppCannotBeComposed(t *testing.T) {
	var out strings.Builder
	// No --data-dir: app.New rejects it.
	if code := run([]string{"--socket", socketPath(t)}, strings.NewReader(""), &out); code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if out.String() != "" {
		t.Errorf("stdout = %q, want empty (READY follows a composed app)", out.String())
	}
}

// An endpoint that cannot be bound exits 1 before anything is printed.
func TestRunFailsWhenTheEndpointCannotBeBound(t *testing.T) {
	var out strings.Builder
	code := run([]string{"--socket", unbindableEndpoint(t), "--data-dir", t.TempDir()}, strings.NewReader(""), &out)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if out.String() != "" {
		t.Errorf("stdout = %q, want empty (the READY line follows a successful bind)", out.String())
	}
}
