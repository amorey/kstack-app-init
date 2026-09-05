// Package api is a minimal GraphQL-over-HTTP client for the kstack cloud API, covering
// the Settings surface. It holds a token-source factory and pulls a fresh bearer per
// request, so callers never thread tokens through. GetSettings/UpdateSettings are POSTs;
// WatchSettings is an SSE stream of `next` events.
package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/oauth2"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/prefs"
)

// Settings re-exports prefs.Settings for callers importing only this package.
type Settings = prefs.Settings

const (
	queryGetSettings  = `query { settings { theme locale } }`
	mutUpdateSettings = `mutation($input: UpdateSettingsInput!) {` +
		` updateSettings(input: $input) { theme locale } }`
	subWatchSettings = `subscription { settingsWatch { theme locale } }`
)

// Client talks to the cloud GraphQL endpoint, authenticating each request from a fresh
// per-request TokenSource.
type Client struct {
	endpoint    string
	tokenSource func(context.Context) oauth2.TokenSource
	// Separate clients: post has a timeout; sse must not, its stream being
	// intentionally long-lived.
	post *http.Client
	sse  *http.Client
}

// sseHeaderTimeout bounds the SSE open handshake ONLY (the wait for headers) — a
// whole-request timeout would cap the long-lived stream body.
var sseHeaderTimeout = 30 * time.Second

// tokenFetchTimeout bounds acquiring the bearer (which may trigger a refresh); the engine
// drives requests with long-lived contexts, so a stalled token endpoint would otherwise
// hang sync and sign-out.
var tokenFetchTimeout = 15 * time.Second

// maxErrBody caps the diagnostic read of a non-2xx body.
const maxErrBody = 4 << 10

// maxRespBody caps a 2xx body, so a misconfigured URL returning a huge 200 can't allocate
// unbounded memory.
const maxRespBody = 1 << 20

// New binds a Client to the cloud API base URL (trailing slash tolerated; /graphql is
// appended). tokenSource builds a TokenSource bound to a per-request context.
func New(baseURL string, tokenSource func(context.Context) oauth2.TokenSource) *Client {
	// Clone the default transport (keeping proxy/dial settings) and add only a
	// response-header deadline — no Client.Timeout, which would cap the stream body.
	sseTransport := http.DefaultTransport.(*http.Transport).Clone()
	sseTransport.ResponseHeaderTimeout = sseHeaderTimeout

	return &Client{
		endpoint:    strings.TrimRight(baseURL, "/") + "/graphql",
		tokenSource: tokenSource,
		post:        &http.Client{Timeout: 15 * time.Second},
		sse:         &http.Client{Transport: sseTransport},
	}
}

// GetSettings fetches the user's current Settings from the cloud.
func (c *Client) GetSettings(ctx context.Context) (Settings, error) {
	var resp struct {
		Settings Settings `json:"settings"`
	}
	if err := c.do(ctx, queryGetSettings, nil, &resp); err != nil {
		return Settings{}, err
	}
	return resp.Settings, nil
}

// UpdateSettings posts the deep-merge mutation and returns the merged Settings.
func (c *Client) UpdateSettings(ctx context.Context, input Settings) (Settings, error) {
	var resp struct {
		UpdateSettings Settings `json:"updateSettings"`
	}
	vars := map[string]any{"input": input}
	if err := c.do(ctx, mutUpdateSettings, vars, &resp); err != nil {
		return Settings{}, err
	}
	return resp.UpdateSettings, nil
}

// gqlEnvelope is the shared GraphQL response shape (data + errors).
type gqlEnvelope struct {
	Data   json.RawMessage `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// token fetches the bearer under a bounded context — see tokenFetchTimeout.
func (c *Client) token(ctx context.Context) (string, error) {
	tctx, cancel := context.WithTimeout(ctx, tokenFetchTimeout)
	defer cancel()
	tok, err := c.tokenSource(tctx).Token()
	if err != nil {
		return "", err
	}
	return tok.AccessToken, nil
}

// newRequest builds an authenticated JSON POST to the GraphQL endpoint.
func (c *Client) newRequest(ctx context.Context, body []byte) (*http.Request, error) {
	token, err := c.token(ctx)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	return req, nil
}

func (c *Client) do(ctx context.Context, query string, variables map[string]any, into any) error {
	body, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		return err
	}
	req, err := c.newRequest(ctx, body)
	if err != nil {
		return err
	}

	resp, err := c.post.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrBody))
		return fmt.Errorf("cloud responded %d: %s", resp.StatusCode, body)
	}
	// One byte past the cap, so an over-limit body is detected rather than silently
	// truncated into a decode error.
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxRespBody+1))
	if err != nil {
		return err
	}
	if len(raw) > maxRespBody {
		return fmt.Errorf("cloud response exceeded %d bytes", maxRespBody)
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

// WatchSettings opens a gqlgen-style SSE subscription, returning the events channel, a
// buffered channel with the stream's terminal error (nil for a clean complete/cancel),
// and a synchronous open error. The events channel closes on cancel, `complete`, or a
// connection error.
func (c *Client) WatchSettings(ctx context.Context) (<-chan Settings, <-chan error, error) {
	body, err := json.Marshal(map[string]any{"query": subWatchSettings})
	if err != nil {
		return nil, nil, err
	}
	req, err := c.newRequest(ctx, body)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.sse.Do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		msg := readErrBody(resp.Body)
		resp.Body.Close()
		return nil, nil, fmt.Errorf("sse open failed %d: %s", resp.StatusCode, msg)
	}

	out := make(chan Settings)
	errCh := make(chan error, 1)
	go func() {
		err := parseSSE(ctx, resp.Body, "settingsWatch", out)
		resp.Body.Close()
		// Publish before closing out (errCh is buffered), so a reader observing the
		// close can still read the terminal error.
		errCh <- err
		close(out)
	}()
	return out, errCh, nil
}

// readErrBody reads a bounded, deadlined slice of a non-2xx body, so a server that sends
// headers then stalls can't hang the timeout-less SSE client. The read runs in a
// goroutine the caller unblocks by closing the body; the buffered send never blocks.
func readErrBody(body io.Reader) string {
	ch := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(io.LimitReader(body, maxErrBody))
		ch <- b
	}()
	select {
	case b := <-ch:
		return string(b)
	case <-time.After(sseHeaderTimeout):
		return "(error body read timed out)"
	}
}

// parseSSE pushes decoded `next` events onto out (frames are `field: value` lines
// separated by blank lines; `complete` ends the stream). Returns nil on a clean end,
// else the error the engine surfaces as Status.LastError.
func parseSSE(ctx context.Context, r io.Reader, dataField string, out chan<- Settings) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var (
		eventType string
		dataBuf   strings.Builder
		streamErr error // set by a GraphQL error frame; returned as terminal error
	)
	// flush processes one frame; false ends the stream, cleanly or (streamErr set) on
	// a GraphQL error frame.
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
			// An error frame (typically data:null) must never publish a zero Settings
			// that would wipe local prefs, and must not be silently skipped either —
			// end the stream so the engine backs off and reconnects.
			if len(env.Errors) > 0 {
				streamErr = fmt.Errorf("graphql error: %s", env.Errors[0].Message)
				return false
			}
			var payload map[string]Settings
			if err := json.Unmarshal(env.Data, &payload); err != nil {
				return true
			}
			s, ok := payload[dataField]
			if !ok {
				return true // nothing to publish
			}
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
				return terminalErr(ctx, streamErr)
			}
			continue
		}
		field, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value := strings.TrimPrefix(rest, " ")
		switch field {
		case "event":
			eventType = value
		case "data":
			// The scanner bounds a line, but a frame may contain arbitrarily many
			// data lines without a blank terminator. Bound their aggregate too.
			if len(value) > maxRespBody-dataBuf.Len() {
				return fmt.Errorf("cloud SSE frame exceeds %d bytes", maxRespBody)
			}
			dataBuf.WriteString(value)
		}
	}
	flush() // trailing frame without a blank-line terminator
	// A cancelled ctx is a clean shutdown, winning over both a captured GraphQL error
	// and the context.Canceled scanner.Err() surfaces.
	if ctx.Err() != nil {
		return nil
	}
	if streamErr != nil {
		return streamErr
	}
	return scanner.Err()
}

// terminalErr resolves a frame-driven stop: a cancelled ctx is a clean shutdown and wins
// over any captured error.
func terminalErr(ctx context.Context, streamErr error) error {
	if ctx.Err() != nil {
		return nil
	}
	return streamErr
}
