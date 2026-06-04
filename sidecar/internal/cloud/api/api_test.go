package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

// SetSSEHeaderTimeoutForTest overrides the SSE open-handshake timeout (used when
// New builds the SSE transport) and returns a restore func. Test-only.
func SetSSEHeaderTimeoutForTest(d time.Duration) func() {
	prev := sseHeaderTimeout
	sseHeaderTimeout = d
	return func() { sseHeaderTimeout = prev }
}

// fakeSource is a stub oauth2.TokenSource. srcFn wraps it (or any source) into
// the per-request factory func New expects.
type fakeSource struct {
	tok string
	err error
}

func (f fakeSource) Token() (*oauth2.Token, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &oauth2.Token{AccessToken: f.tok}, nil
}

// srcFn adapts a static oauth2.TokenSource into the func(ctx) factory New takes.
func srcFn(s oauth2.TokenSource) func(context.Context) oauth2.TokenSource {
	return func(context.Context) oauth2.TokenSource { return s }
}

// C21: GetSettings POSTs the query with the bearer from the TokenSource.
func TestGetSettingsSendsBearer(t *testing.T) {
	var gotAuth, gotMethod string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotMethod = r.Method
		_, _ = io.WriteString(w, `{"data":{"settings":{"theme":"dark"}}}`)
	}))
	defer ts.Close()

	c := New(ts.URL, srcFn(fakeSource{tok: "tok-abc"}))
	got, err := c.GetSettings(context.Background())
	if err != nil {
		t.Fatalf("GetSettings: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %q", gotMethod)
	}
	if gotAuth != "Bearer tok-abc" {
		t.Errorf("auth = %q", gotAuth)
	}
	if got.Theme == nil || *got.Theme != "dark" {
		t.Errorf("settings = %+v", got)
	}
}

// C22: UpdateSettings sends the mutation + variables and returns the merged
// settings the cloud reports.
func TestUpdateSettingsSendsMutation(t *testing.T) {
	var body map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		_, _ = io.WriteString(w, `{"data":{"updateSettings":{"theme":"light"}}}`)
	}))
	defer ts.Close()

	c := New(ts.URL, srcFn(fakeSource{tok: "tok"}))
	theme := "light"
	got, err := c.UpdateSettings(context.Background(), Settings{Theme: &theme})
	if err != nil {
		t.Fatalf("UpdateSettings: %v", err)
	}
	if q, _ := body["query"].(string); !strings.Contains(q, "updateSettings") {
		t.Errorf("query missing updateSettings: %q", q)
	}
	if _, ok := body["variables"].(map[string]any)["input"]; !ok {
		t.Errorf("variables.input missing: %+v", body["variables"])
	}
	if got.Theme == nil || *got.Theme != "light" {
		t.Errorf("settings = %+v", got)
	}
}

// A cloud/proxy that accepts the connection but stalls before sending response
// headers must not hang WatchSettings forever: the SSE transport's
// response-header timeout bounds the open handshake so the engine can back off.
func TestWatchSettingsBoundsOpenHandshake(t *testing.T) {
	restore := SetSSEHeaderTimeoutForTest(100 * time.Millisecond)
	defer restore()

	block := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-block // accept the connection but never write response headers
	}))
	defer ts.Close()
	defer close(block) // released before ts.Close (LIFO) so the handler returns

	c := New(ts.URL, srcFn(fakeSource{tok: "tok"}))

	done := make(chan error, 1)
	go func() {
		_, _, err := c.WatchSettings(context.Background())
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want error when the open handshake stalls, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WatchSettings did not bound the stalled open handshake")
	}
}

// A token fetch (which may trigger an OAuth refresh) is bounded, so a stalled
// token endpoint can't hang a request driven by a long-lived caller context.
func TestTokenFetchBounded(t *testing.T) {
	prev := tokenFetchTimeout
	tokenFetchTimeout = 100 * time.Millisecond
	defer func() { tokenFetchTimeout = prev }()

	// blockingSource.Token blocks until its (bounded) context expires. The
	// factory captures the per-request context New passes it (the one bounded by
	// tokenFetchTimeout).
	c := New("http://example.invalid", func(ctx context.Context) oauth2.TokenSource {
		return blockingSource{ctx: ctx}
	})

	done := make(chan error, 1)
	go func() {
		_, err := c.GetSettings(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want error when token fetch stalls, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GetSettings did not bound the token fetch")
	}
}

// blockingSource is an oauth2.TokenSource whose Token blocks until the context
// it captured (the per-request, tokenFetchTimeout-bounded one) is cancelled.
type blockingSource struct{ ctx context.Context }

func (b blockingSource) Token() (*oauth2.Token, error) {
	<-b.ctx.Done()
	return nil, b.ctx.Err()
}

// On a non-200 SSE open, reading the error body is bounded, so a server that
// returns headers then stalls the body can't hang the no-timeout SSE client.
func TestWatchSettingsBoundsErrorBody(t *testing.T) {
	restore := SetSSEHeaderTimeoutForTest(100 * time.Millisecond)
	defer restore()

	block := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.(http.Flusher).Flush() // send headers, then stall the body
		<-block
	}))
	defer ts.Close()
	defer close(block)

	c := New(ts.URL, srcFn(fakeSource{tok: "tok"}))

	done := make(chan error, 1)
	go func() {
		_, _, err := c.WatchSettings(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want error on non-200 SSE open, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WatchSettings hung reading a stalled error body")
	}
}

// A GraphQL POST that fails with a huge error body is read only up to maxErrBody
// for the diagnostic, not in full.
func TestDoCapsErrorBody(t *testing.T) {
	big := strings.Repeat("x", 1<<20) // 1 MiB
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, big)
	}))
	defer ts.Close()

	c := New(ts.URL, srcFn(fakeSource{tok: "tok"}))
	_, err := c.GetSettings(context.Background())
	if err == nil {
		t.Fatal("want error on 500, got nil")
	}
	// The message is "cloud responded 500: " + at most maxErrBody bytes; without
	// the cap it would carry the whole 1 MiB body.
	if len(err.Error()) > maxErrBody+64 {
		t.Fatalf("error body not capped: message len=%d", len(err.Error()))
	}
}

// sseServer streams frames provided over emit; closing emit ends the stream
// with `complete`.
func sseServer(emit <-chan string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		fl.Flush()
		for {
			select {
			case <-r.Context().Done():
				return
			case frame, ok := <-emit:
				if !ok {
					fmt.Fprint(w, "event: complete\ndata: \n\n")
					fl.Flush()
					return
				}
				fmt.Fprint(w, frame)
				fl.Flush()
			}
		}
	}))
}

// C23: WatchSettings emits a value on an `event: next` frame.
func TestWatchSettingsEmitsNext(t *testing.T) {
	emit := make(chan string, 1)
	ts := sseServer(emit)
	defer ts.Close()

	// Cancel before ts.Close (LIFO defers) so the client disconnects and the
	// streaming handler returns — otherwise httptest.Close blocks on it.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := New(ts.URL, srcFn(fakeSource{tok: "tok"}))
	ch, _, err := c.WatchSettings(ctx)
	if err != nil {
		t.Fatalf("WatchSettings: %v", err)
	}
	emit <- "event: next\ndata: {\"data\":{\"settingsWatch\":{\"theme\":\"blue\"}}}\n\n"

	select {
	case got, ok := <-ch:
		if !ok {
			t.Fatal("channel closed early")
		}
		if got.Theme == nil || *got.Theme != "blue" {
			t.Fatalf("got %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for next event")
	}
}

// An `event: next` frame carrying GraphQL errors (data:null) must NOT publish a
// (zero) Settings value — an upstream error must not masquerade as an empty
// update that could wipe local prefs. Instead it ends the stream with the error
// as the terminal error, so the engine surfaces it in Status.LastError and
// backs off + reconnects rather than staying silently live.
func TestWatchSettingsSurfacesErrorFrame(t *testing.T) {
	emit := make(chan string, 1)
	ts := sseServer(emit)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := New(ts.URL, srcFn(fakeSource{tok: "tok"}))
	ch, errCh, err := c.WatchSettings(ctx)
	if err != nil {
		t.Fatalf("WatchSettings: %v", err)
	}
	// Error frame (data:null): no Settings is published, and the channel closes.
	emit <- "event: next\ndata: {\"errors\":[{\"message\":\"boom\"}],\"data\":null}\n\n"

	select {
	case got, ok := <-ch:
		if ok {
			t.Fatalf("error frame published a Settings value: %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not close on the error frame")
	}
	select {
	case e := <-errCh:
		if e == nil || !strings.Contains(e.Error(), "boom") {
			t.Fatalf("terminal error = %v, want one carrying the GraphQL error \"boom\"", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no terminal error published for the error frame")
	}
}

// A successful (2xx) response body is bounded too: a misconfigured cloud
// URL/proxy returning a huge 200 must error rather than allocate unbounded
// memory (and not be silently truncated into a misleading decode error).
func TestDoCapsSuccessBody(t *testing.T) {
	big := strings.Repeat("x", (1<<20)+1024) // just over maxRespBody
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, big)
	}))
	defer ts.Close()

	c := New(ts.URL, srcFn(fakeSource{tok: "tok"}))
	_, err := c.GetSettings(context.Background())
	if err == nil || !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("want an over-size error, got %v", err)
	}
}

// C24a: an `event: complete` frame closes the channel.
func TestWatchSettingsCompleteCloses(t *testing.T) {
	emit := make(chan string)
	ts := sseServer(emit)
	defer ts.Close()

	c := New(ts.URL, srcFn(fakeSource{tok: "tok"}))
	ch, _, err := c.WatchSettings(context.Background())
	if err != nil {
		t.Fatalf("WatchSettings: %v", err)
	}
	close(emit) // server sends `complete`

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("want closed channel on complete")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for close")
	}
}

// C24b: cancelling the context closes the channel.
func TestWatchSettingsContextCancelCloses(t *testing.T) {
	emit := make(chan string)
	ts := sseServer(emit)
	defer ts.Close()

	c := New(ts.URL, srcFn(fakeSource{tok: "tok"}))
	ctx, cancel := context.WithCancel(context.Background())
	ch, _, err := c.WatchSettings(ctx)
	if err != nil {
		t.Fatalf("WatchSettings: %v", err)
	}
	cancel()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("want closed channel on ctx cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for close")
	}
}

// Cancelling the context is a CLEAN shutdown (Poke/sign-out), so the terminal
// error must be nil — not the context.Canceled the blocked body read surfaces.
func TestWatchSettingsContextCancelIsCleanError(t *testing.T) {
	emit := make(chan string)
	ts := sseServer(emit)
	defer ts.Close()

	c := New(ts.URL, srcFn(fakeSource{tok: "tok"}))
	ctx, cancel := context.WithCancel(context.Background())
	ch, errCh, err := c.WatchSettings(ctx)
	if err != nil {
		t.Fatalf("WatchSettings: %v", err)
	}
	cancel()

	for range ch { // drain until close
	}
	select {
	case e := <-errCh:
		if e != nil {
			t.Fatalf("ctx cancel must be a clean close, got terminal error %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no terminal error published")
	}
}
