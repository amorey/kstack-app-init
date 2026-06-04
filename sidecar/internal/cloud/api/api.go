// Package api is a minimal GraphQL-over-HTTP client for the kstack cloud
// API. Today it covers the Settings surface (get / update / watch); profile
// and AI-model routing land here later.
//
// Unlike the removed first cut (which took a bearer token per call), this
// client holds a token-source factory and pulls a fresh bearer for every
// request — so auth lives in-process and callers never thread tokens through.
// Three operations, one transport shape: GetSettings (POST), UpdateSettings
// (POST), WatchSettings (SSE stream of `next` events).
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

// Settings is the cloud-synced preferences payload (alias of prefs.Settings,
// re-exported for callers that only import this package).
type Settings = prefs.Settings

const (
	queryGetSettings  = `query { settings { theme locale } }`
	mutUpdateSettings = `mutation($input: UpdateSettingsInput!) {` +
		` updateSettings(input: $input) { theme locale } }`
	subWatchSettings = `subscription { settingsWatch { theme locale } }`
)

// Client talks to the kstack cloud GraphQL endpoint, authenticating every
// request from a fresh oauth2.TokenSource built per request from the request
// context (so the bounded token-fetch timeout governs each refresh).
type Client struct {
	endpoint    string
	tokenSource func(context.Context) oauth2.TokenSource
	// Separate clients: the POST client has a sane timeout; the SSE client
	// must not, because the stream is intentionally long-lived.
	post *http.Client
	sse  *http.Client
}

// sseHeaderTimeout bounds the SSE open handshake — the wait for response
// headers after the request is sent. It must NOT be a whole-request timeout
// (the stream body is intentionally long-lived); it only stops a cloud/proxy
// that accepts the connection but stalls before replying from hanging the
// engine's open call forever (so it can back off and retry).
var sseHeaderTimeout = 30 * time.Second

// tokenFetchTimeout bounds acquiring the bearer token for a request (which may
// trigger an OAuth refresh). The engine drives requests with long-lived
// contexts, so without this a stalled token endpoint would hang sync (and
// sign-out) indefinitely.
var tokenFetchTimeout = 15 * time.Second

// maxErrBody caps how much of a non-2xx response body we read for the error
// message — enough for diagnostics, bounded so a huge/stalled body can't blow
// memory or hang.
const maxErrBody = 4 << 10

// maxRespBody caps a successful (2xx) response body. Settings payloads are tiny,
// so 1 MiB is ample headroom; the cap exists only so a misconfigured cloud URL
// or proxy returning a huge 200 can't allocate unbounded memory.
const maxRespBody = 1 << 20

// New returns a Client bound to the cloud API's base URL (e.g.
// https://api.kstack.sh); the /graphql path is appended here. A trailing
// slash on the base is tolerated. tokenSource builds an oauth2.TokenSource
// bound to the given (per-request) context — in production auth.Service's
// TokenSource method.
func New(baseURL string, tokenSource func(context.Context) oauth2.TokenSource) *Client {
	// Clone the default transport (preserving proxy/dial settings) and add a
	// response-header deadline for the SSE open handshake. No Client.Timeout —
	// that would cap the long-lived stream body.
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

// UpdateSettings posts the deep-merge mutation and returns the cloud's view
// of the merged Settings.
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

// token fetches the bearer under a bounded context so a stalled token/refresh
// endpoint can't hang a request driven by a long-lived caller context.
func (c *Client) token(ctx context.Context) (string, error) {
	tctx, cancel := context.WithTimeout(ctx, tokenFetchTimeout)
	defer cancel()
	tok, err := c.tokenSource(tctx).Token()
	if err != nil {
		return "", err
	}
	return tok.AccessToken, nil
}

// newRequest builds an authenticated JSON POST to the GraphQL endpoint, fetching
// the bearer under a bounded context.
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

	// Check the status before reading: a non-2xx (mis)configured cloud/proxy can
	// return an arbitrarily large error body, so cap the diagnostic read.
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrBody))
		return fmt.Errorf("cloud responded %d: %s", resp.StatusCode, body)
	}
	// Cap the successful body too: a misconfigured cloud URL/proxy returning a
	// huge 200 could otherwise allocate unbounded memory. Read one byte past the
	// cap so an over-limit body is detected rather than silently truncated into a
	// decode error.
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

// WatchSettings opens a gqlgen-style SSE subscription. It returns a channel of
// Settings events, a buffered channel carrying the stream's terminal error
// (nil for a clean `event: complete` / ctx cancel, non-nil for a read failure),
// and a synchronous error if the stream couldn't be opened. The events channel
// closes when ctx is cancelled, the server sends `event: complete`, or the
// connection errors.
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
		// Publish the terminal error before closing out (errCh is buffered, so
		// this never blocks), so a reader observing the close can read it.
		errCh <- err
		close(out)
	}()
	return out, errCh, nil
}

// readErrBody reads up to maxErrBody bytes of a non-2xx response body for use
// in an error message, bounded by a deadline so a server that returns headers
// then stalls the body can't hang the (deliberately no-overall-timeout) SSE
// client. The read runs in a goroutine; the caller closes the body right after,
// which unblocks a stalled read so the goroutine can't leak (the channel is
// buffered, so its send never blocks).
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

// parseSSE reads a gqlgen SSE stream and pushes decoded `next` events onto
// out. Frame format: lines of `field: value`, frames separated by blank
// lines. Recognized events: `next` (data envelope) and `complete` (end). It
// returns nil on a clean end (`complete` or ctx cancel); a non-nil error
// (surfaced to the engine as Status.LastError) when the stream ends on a
// GraphQL error frame or a read/transport failure.
func parseSSE(ctx context.Context, r io.Reader, dataField string, out chan<- Settings) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var (
		eventType string
		dataBuf   strings.Builder
		streamErr error // set by a GraphQL error frame; returned as terminal error
	)
	// flush processes a completed frame and reports whether to keep reading.
	// Returning false ends the stream: cleanly (`complete`/ctx cancel, streamErr
	// nil) or on a GraphQL error frame (streamErr set).
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
			// An error frame (typically data:null) is not a settings update. End
			// the stream with the error rather than skipping it: we still never
			// publish a (zero) Settings that could wipe local prefs, but the
			// failure now surfaces in Status.LastError and triggers a backoff +
			// reconnect (which re-authenticates) instead of staying silently live.
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
				return true // no settingsWatch field present; nothing to publish
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
			dataBuf.WriteString(value)
		}
	}
	flush() // trailing frame without a blank-line terminator
	// A cancelled ctx is a clean shutdown (Poke/sign-out), not a failure — it
	// wins over both a captured GraphQL error and the context.Canceled the
	// blocked body read surfaces via scanner.Err().
	if ctx.Err() != nil {
		return nil
	}
	if streamErr != nil {
		return streamErr
	}
	return scanner.Err()
}

// terminalErr resolves the terminal error for a frame-driven stop (`complete`,
// ctx cancel, or a GraphQL error frame). A cancelled ctx is a clean shutdown
// (Poke/sign-out), not a failure, so it wins over any captured error; otherwise
// a GraphQL error frame's error (streamErr) is returned.
func terminalErr(ctx context.Context, streamErr error) error {
	if ctx.Err() != nil {
		return nil
	}
	return streamErr
}
