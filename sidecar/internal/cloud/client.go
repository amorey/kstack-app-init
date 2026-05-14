// Package cloud is a minimal GraphQL-over-HTTP client for the kstack cloud
// API's Settings surface. Three operations, one transport file each:
//
//   - GetSettings    — POST { query }
//   - UpdateSettings — POST { query, variables }
//   - WatchSettings  — SSE stream of `next` events
//
// The client is stateless re: auth: every call takes a bearer token and
// attaches it as `Authorization: Bearer <token>`. Tokens come from whoever
// called the sidecar; this package never persists them.
package cloud

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/prefs"
)

// tokenFingerprint returns a short, log-safe summary of a bearer token:
// presence flag + length + first 8 chars. Enough to distinguish "no token"
// from "token but rejected" without spilling credentials into logs.
func tokenFingerprint(t string) string {
	if t == "" {
		return "<empty>"
	}
	head := t
	if len(head) > 8 {
		head = head[:8]
	}
	return fmt.Sprintf("len=%d prefix=%s…", len(t), head)
}

const (
	queryGetSettings  = `query { settings { placeholder } }`
	mutUpdateSettings = `mutation($input: UpdateSettingsInput!) {` +
		` updateSettings(input: $input) { placeholder } }`
	subWatchSettings = `subscription { settingsWatch { placeholder } }`
)

// UpdateInput mirrors the cloud's `UpdateSettingsInput`. Pointer fields so
// "field not present" is distinguishable from "field set to empty string"
// (the cloud uses deep-merge semantics on the JSONB row).
type UpdateInput struct {
	Placeholder *string `json:"placeholder,omitempty"`
}

// Client talks to the kstack cloud GraphQL endpoint.
type Client struct {
	endpoint string
	// Separate clients: the POST client has a sane timeout; the SSE
	// client must not, because the stream is intentionally long-lived.
	post *http.Client
	sse  *http.Client
}

// New returns a Client bound to the cloud API's base URL — e.g.
// `https://api.kstack.sh`. The `/graphql` path is appended here so callers
// (and the user-facing `KSTACK_CLOUD_URL` env var) only have to think about
// the host. A trailing slash on the base is tolerated.
func New(baseURL string) *Client {
	endpoint := strings.TrimRight(baseURL, "/") + "/graphql"
	return &Client{
		endpoint: endpoint,
		post:     &http.Client{Timeout: 15 * time.Second},
		sse:      &http.Client{}, // no timeout — long-lived stream
	}
}

// GetSettings fetches the user's current Settings from the cloud.
func (c *Client) GetSettings(ctx context.Context, token string) (prefs.Settings, error) {
	var resp struct {
		Settings prefs.Settings `json:"settings"`
	}
	if err := c.do(ctx, token, queryGetSettings, nil, &resp); err != nil {
		return prefs.Settings{}, err
	}
	return resp.Settings, nil
}

// UpdateSettings posts the deep-merge mutation and returns the cloud's view
// of the merged Settings.
func (c *Client) UpdateSettings(ctx context.Context, token string, input UpdateInput) (prefs.Settings, error) {
	var resp struct {
		UpdateSettings prefs.Settings `json:"updateSettings"`
	}
	vars := map[string]any{"input": input}
	if err := c.do(ctx, token, mutUpdateSettings, vars, &resp); err != nil {
		return prefs.Settings{}, err
	}
	return resp.UpdateSettings, nil
}

// gqlEnvelope is the shared response shape (data + errors). We decode `data`
// into a caller-supplied struct so the per-op types stay close to their
// resolvers.
type gqlEnvelope struct {
	Data   json.RawMessage `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

func (c *Client) do(ctx context.Context, token, query string, variables map[string]any, into any) error {
	body, err := json.Marshal(map[string]any{
		"query":     query,
		"variables": variables,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	slog.DebugContext(ctx, "cloud POST",
		"endpoint", c.endpoint,
		"bearer", tokenFingerprint(token),
	)

	resp, err := c.post.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("cloud responded %d: %s", resp.StatusCode, raw)
	}
	var env gqlEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("decode envelope: %w (raw=%s)", err, raw)
	}
	if len(env.Errors) > 0 {
		return fmt.Errorf("graphql error: %s", env.Errors[0].Message)
	}
	if into != nil {
		if err := json.Unmarshal(env.Data, into); err != nil {
			return fmt.Errorf("decode data: %w", err)
		}
	}
	return nil
}

// WatchSettings opens a gqlgen-style SSE subscription and returns a channel
// of Settings events. The channel closes when:
//   - ctx is cancelled (the request is aborted),
//   - the server sends `event: complete`,
//   - or the underlying connection errors.
//
// The returned error is only the synchronous "could not open the stream"
// failure; per-event errors during the stream just close the channel.
func (c *Client) WatchSettings(ctx context.Context, token string) (<-chan prefs.Settings, error) {
	body, err := json.Marshal(map[string]any{"query": subWatchSettings})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Authorization", "Bearer "+token)

	slog.DebugContext(ctx, "cloud SSE open",
		"endpoint", c.endpoint,
		"bearer", tokenFingerprint(token),
	)

	resp, err := c.sse.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("sse open failed %d: %s", resp.StatusCode, raw)
	}

	out := make(chan prefs.Settings)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		parseSSE(ctx, resp.Body, "settingsWatch", out)
	}()
	return out, nil
}

// parseSSE reads a gqlgen SSE stream and pushes decoded `next` events onto
// out. Frame format: lines of `field: value`, frames separated by blank
// lines. Recognized event types: `next` (data envelope) and `complete`
// (end of stream).
func parseSSE(ctx context.Context, r io.Reader, dataField string, out chan<- prefs.Settings) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var (
		eventType string
		dataBuf   strings.Builder
	)
	flush := func() bool {
		defer func() {
			eventType = ""
			dataBuf.Reset()
		}()
		switch eventType {
		case "next":
			var env gqlEnvelope
			if err := json.Unmarshal([]byte(dataBuf.String()), &env); err != nil {
				return true // skip malformed frame; keep stream open
			}
			var payload map[string]prefs.Settings
			if err := json.Unmarshal(env.Data, &payload); err != nil {
				return true
			}
			s := payload[dataField]
			select {
			case out <- s:
			case <-ctx.Done():
				return false
			}
		case "complete":
			return false
		}
		return true
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if !flush() {
				return
			}
			continue
		}
		// `field: value` (the single space after the colon is optional in
		// the SSE spec; trim one if present).
		field, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value := strings.TrimPrefix(rest, " ")
		switch field {
		case "event":
			eventType = value
		case "data":
			dataBuf.WriteString(value)
		}
	}
	// Trailing frame without a blank line terminator.
	flush()
}
