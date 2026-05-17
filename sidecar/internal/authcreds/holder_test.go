package authcreds_test

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/authcreds"
)

// A fresh Holder is empty: the always-on engine must read "" (and stay
// Offline) until the host pushes a token, never a stale/garbage value.
func TestNewHolderIsEmpty(t *testing.T) {
	h := authcreds.NewHolder()
	if got := h.Token(); got != "" {
		t.Fatalf("want empty token, got %q", got)
	}
	if c := h.Get(); c.Token != "" || !c.ExpiresAt.IsZero() {
		t.Fatalf("want zero Credentials, got %+v", c)
	}
}

func TestSetThenGet(t *testing.T) {
	h := authcreds.NewHolder()
	exp := time.Now().Add(time.Hour).Round(0)
	h.Set(authcreds.Credentials{Token: "tok-1", ExpiresAt: exp})

	if got := h.Token(); got != "tok-1" {
		t.Fatalf("Token() = %q", got)
	}
	c := h.Get()
	if c.Token != "tok-1" || !c.ExpiresAt.Equal(exp) {
		t.Fatalf("Get() = %+v", c)
	}
}

// A later Set fully replaces (refresh pushes a new token over the old).
func TestSetReplaces(t *testing.T) {
	h := authcreds.NewHolder()
	h.Set(authcreds.Credentials{Token: "old"})
	h.Set(authcreds.Credentials{Token: "new"})
	if got := h.Token(); got != "new" {
		t.Fatalf("want new, got %q", got)
	}
}

// Concurrent readers + writers must not race (run with -race). Final value
// is one of the written tokens — never torn.
func TestConcurrentSetGet(t *testing.T) {
	h := authcreds.NewHolder()
	tokens := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

	var wg sync.WaitGroup
	for _, tok := range tokens {
		wg.Add(1)
		go func(tok string) {
			defer wg.Done()
			h.Set(authcreds.Credentials{Token: tok})
		}(tok)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = h.Token()
		}()
	}
	wg.Wait()

	if got := h.Token(); !slices.Contains(tokens, got) {
		t.Fatalf("final token %q is not one of the written values", got)
	}
}
