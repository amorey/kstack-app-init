package server_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kubetail-org/kstack-app/sidecar/server"
)

// TestPingQuery is the canary: a fresh server should answer `{ ping }` with "pong".
// If this passes, the gqlgen wiring (schema -> resolver -> handler) is intact.
func TestPingQuery(t *testing.T) {
	h := server.NewHandler()
	ts := httptest.NewServer(h)
	defer ts.Close()

	body := strings.NewReader(`{"query":"{ ping }"}`)
	resp, err := http.Post(ts.URL+"/graphql", "application/json", body)
	if err != nil {
		t.Fatalf("POST /graphql: %v", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	var out struct {
		Data struct {
			Ping string `json:"ping"`
		} `json:"data"`
	}
	if err := json.NewDecoder(bytes.NewReader(raw)).Decode(&out); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if out.Data.Ping != "pong" {
		t.Fatalf("want ping=pong, got %q (raw=%s)", out.Data.Ping, raw)
	}
}
