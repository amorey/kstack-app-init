package auth

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// fakeOAuth is a stand-in for the oauth protocol client the login flow drives,
// recording what it was asked and returning canned tokens/identity.
type fakeOAuth struct {
	authURL  string
	token    Token
	identity Identity

	gotState    string
	gotVerifier string
	gotRedirect string

	exchangeErr error
	verifyErr   error
}

func (f *fakeOAuth) AuthCodeURL(state, verifier, redirect string) string {
	f.gotState, f.gotVerifier, f.gotRedirect = state, verifier, redirect
	return f.authURL
}

func (f *fakeOAuth) Exchange(_ context.Context, _, _, _ string) (Token, error) {
	return f.token, f.exchangeErr
}

func (f *fakeOAuth) Verify(_ context.Context, _ string) (Identity, error) {
	return f.identity, f.verifyErr
}

// fakeLoopback is a stand-in for the loopback redirect listener, handing back a
// canned authorization code without binding a real socket.
type fakeLoopback struct {
	code    string
	closed  bool
	waitErr error
}

func (l *fakeLoopback) RedirectURL() string                  { return "http://127.0.0.1:55555/callback" }
func (l *fakeLoopback) Wait(context.Context) (string, error) { return l.code, l.waitErr }
func (l *fakeLoopback) Close() error                         { l.closed = true; return nil }

// start runs the synchronous setup — bind loopback, open the browser at the
// authorize URL (passing the loopback redirect + a non-empty state + PKCE
// verifier) — and returns a pendingLogin tail. Driving that tail waits for the
// loopback code, exchanges + verifies it, returns the minted token + verified
// identity, and tears the listener down.
func TestLoginFlowRunsEndToEnd(t *testing.T) {
	var openedURL string
	lb := &fakeLoopback{code: "auth-code-xyz"}
	oa := &fakeOAuth{
		authURL:  "https://oauth.kstack.sh/oauth2/auth?x=1",
		token:    Token{AccessToken: "a", RefreshToken: "r", IDToken: "id", Expiry: time.Now().Add(time.Hour)},
		identity: Identity{UserID: "u1", Email: "a@x.com", Name: "Ada"},
	}
	flow := &loginFlow{
		oauth:       oa,
		newLoopback: func(string) (Loopback, error) { return lb, nil },
		openBrowser: func(u string) error { openedURL = u; return nil },
	}

	// Setup is synchronous: it opens the browser and returns the tail.
	finish, err := flow.start(context.Background())
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// The browser was opened at the authorize URL during the synchronous setup.
	if openedURL != oa.authURL {
		t.Fatalf("opened browser at %q, want %q", openedURL, oa.authURL)
	}
	// The authorize request carried the loopback redirect and a non-empty state +
	// PKCE verifier.
	if oa.gotRedirect != lb.RedirectURL() {
		t.Fatalf("authorize redirect = %q, want loopback %q", oa.gotRedirect, lb.RedirectURL())
	}
	if oa.gotState == "" || oa.gotVerifier == "" {
		t.Fatalf("authorize must carry state+verifier, got state=%q verifier=%q", oa.gotState, oa.gotVerifier)
	}
	// The listener is still open until the tail completes.
	if lb.closed {
		t.Fatal("loopback should stay open until the tail finishes")
	}

	tok, id, err := finish(context.Background())
	if err != nil {
		t.Fatalf("finish: %v", err)
	}

	// The minted token + verified identity are returned to the caller.
	if tok.RefreshToken != "r" {
		t.Fatalf("returned token = %+v, want refresh \"r\"", tok)
	}
	if id.UserID != "u1" || id.Email != "a@x.com" {
		t.Fatalf("returned identity = %+v", id)
	}
	// The loopback listener was torn down after the tail finished.
	if !lb.closed {
		t.Fatal("loopback was not closed after the flow")
	}
}

// A loopback build failure aborts setup before the browser is opened, surfacing
// the error synchronously from start.
func TestLoginFlowLoopbackBuildError(t *testing.T) {
	opened := false
	flow := &loginFlow{
		oauth:       &fakeOAuth{},
		newLoopback: func(string) (Loopback, error) { return nil, errors.New("bind failed") },
		openBrowser: func(string) error { opened = true; return nil },
	}
	if _, err := flow.start(context.Background()); err == nil {
		t.Fatal("want an error when the loopback can't be built")
	}
	if opened {
		t.Fatal("browser should not open when the loopback failed to bind")
	}
}

// A browser-open failure aborts setup synchronously and still tears the listener
// down (no tail is returned to finish it).
func TestLoginFlowBrowserError(t *testing.T) {
	lb := &fakeLoopback{code: "c"}
	flow := &loginFlow{
		oauth:       &fakeOAuth{authURL: "https://x/auth"},
		newLoopback: func(string) (Loopback, error) { return lb, nil },
		openBrowser: func(string) error { return errors.New("no browser") },
	}
	if _, err := flow.start(context.Background()); err == nil {
		t.Fatal("want an error when the browser can't open")
	}
	if !lb.closed {
		t.Fatal("loopback should be closed even when the browser open fails")
	}
}

const lbState = "test-state-123"

// newTestLoopback binds a real 127.0.0.1 loopback listener via the Loopback seam
// and registers its shutdown.
func newTestLoopback(t *testing.T) Loopback {
	t.Helper()
	lb, err := defaultNewLoopback(lbState)
	if err != nil {
		t.Fatalf("defaultNewLoopback: %v", err)
	}
	t.Cleanup(func() { _ = lb.Close() })
	return lb
}

// callbackURL is the loopback's redirect URL carrying the given query.
func callbackURL(t *testing.T, lb Loopback, q url.Values) string {
	t.Helper()
	u, err := url.Parse(lb.RedirectURL())
	if err != nil {
		t.Fatalf("parse redirect url: %v", err)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

// hitCallback fires a GET at the loopback's redirect URL with the given query.
func hitCallback(t *testing.T, lb Loopback, q url.Values) *http.Response {
	t.Helper()
	u := callbackURL(t, lb, q)
	resp, err := http.Get(u)
	if err != nil {
		t.Fatalf("GET %s: %v", u, err)
	}
	return resp
}

// The happy path: a redirect carrying the expected state + a code lands on the
// "signed in" page and Wait surfaces the code.
func TestLoopbackDeliversCode(t *testing.T) {
	lb := newTestLoopback(t)

	resp := hitCallback(t, lb, url.Values{"state": {lbState}, "code": {"the-code"}})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "Signed in") {
		t.Fatalf("callback body missing landing page: %q", body)
	}

	code, err := lb.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if code != "the-code" {
		t.Fatalf("code = %q, want the-code", code)
	}
}

// RedirectURL points at the bound 127.0.0.1 listener and the callback route, so
// it can be registered as the OAuth redirect_uri.
func TestLoopbackRedirectURL(t *testing.T) {
	lb := newTestLoopback(t)
	u, err := url.Parse(lb.RedirectURL())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if u.Scheme != "http" || u.Path != "/oauth/callback" {
		t.Fatalf("redirect url = %q, want http …/oauth/callback", u)
	}
	if !strings.HasPrefix(u.Host, "127.0.0.1:") {
		t.Fatalf("host = %q, want 127.0.0.1:<port>", u.Host)
	}
}

// Invalid callbacks are rejected (400) and must neither consume nor abort the
// one-shot flow: state is validated before anything else, so a stray request —
// wrong state, missing code, or an ?error= without the matching state — can't
// kill it. Each case is proven by a following legit redirect that still delivers.
func TestLoopbackRejectsInvalidCallbackWithoutConsuming(t *testing.T) {
	cases := []struct {
		name string
		q    url.Values
	}{
		{"bad state", url.Values{"state": {"wrong"}, "code": {"stray"}}},
		{"missing code", url.Values{"state": {lbState}}},
		{"error without matching state", url.Values{"error": {"access_denied"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lb := newTestLoopback(t)

			bad := hitCallback(t, lb, tc.q)
			bad.Body.Close()
			if bad.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", bad.StatusCode)
			}

			good := hitCallback(t, lb, url.Values{"state": {lbState}, "code": {"real-code"}})
			good.Body.Close()
			code, err := lb.Wait(context.Background())
			if err != nil || code != "real-code" {
				t.Fatalf("Wait = (%q, %v), want (real-code, nil) — stray request must not consume the flow", code, err)
			}
		})
	}
}

// A redirect carrying ?error= WITH the expected state surfaces as a Wait error
// (the authorization was declined) rather than a code.
func TestLoopbackSurfacesAuthorizationError(t *testing.T) {
	lb := newTestLoopback(t)

	resp := hitCallback(t, lb, url.Values{"state": {lbState}, "error": {"access_denied"}})
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}

	if _, err := lb.Wait(context.Background()); err == nil || !strings.Contains(err.Error(), "access_denied") {
		t.Fatalf("Wait err = %v, want one mentioning access_denied", err)
	}
}

// A second declined authorization must still get a response. Wait consumes at most one
// error and nothing drains the channel behind it, so a blocking send would park the
// handler goroutine for the life of the process.
func TestLoopbackSurvivesRepeatedAuthorizationErrors(t *testing.T) {
	lb := newTestLoopback(t)
	q := url.Values{"state": {lbState}, "error": {"access_denied"}}

	first := hitCallback(t, lb, q)
	first.Body.Close()

	// A wedged handler never answers, so the response is the only event to wait on.
	u := callbackURL(t, lb, q)
	testutil.WaitReturn(t, func() {
		if resp, err := http.Get(u); err == nil {
			resp.Body.Close()
		}
	}, "the second authorization-error callback to answer")

	if _, err := lb.Wait(context.Background()); err == nil || !strings.Contains(err.Error(), "access_denied") {
		t.Fatalf("Wait err = %v, want one mentioning access_denied", err)
	}
}

// The callback listens on a port any local process can reach, so a peer that opens a
// connection and then stalls must not hold a slot until the login times out.
func TestLoopbackServerBoundsItsReads(t *testing.T) {
	lb, err := newLoopbackServer(lbState)
	if err != nil {
		t.Fatalf("newLoopbackServer: %v", err)
	}
	t.Cleanup(func() { _ = lb.Close() })

	for _, tc := range []struct {
		name string
		got  time.Duration
	}{
		{"ReadHeaderTimeout", lb.srv.ReadHeaderTimeout},
		{"ReadTimeout", lb.srv.ReadTimeout},
		{"IdleTimeout", lb.srv.IdleTimeout},
	} {
		if tc.got <= 0 {
			t.Errorf("%s = %v, want a bound", tc.name, tc.got)
		}
	}
}

// The Serve goroutine's whole job is to surface a listener failure to whoever is
// waiting, so a listener that dies under it reaches Wait rather than stranding the
// caller until the login times out. Closing lb.ln rather than lb.Close() is what makes
// Serve fail: Close is the shutdown path Serve reports as ErrServerClosed and swallows.
func TestLoopbackSurfacesAServeFailure(t *testing.T) {
	lb, err := newLoopbackServer(lbState)
	if err != nil {
		t.Fatalf("newLoopbackServer: %v", err)
	}
	t.Cleanup(func() { _ = lb.Close() })

	if err := lb.ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	// Bounded: a Serve failure that never reaches the channel leaves Wait with nothing
	// to return, and the test has to fail rather than hang until the suite's own timeout.
	ctx, cancel := context.WithTimeout(context.Background(), testutil.Timeout)
	defer cancel()
	if _, err := lb.Wait(ctx); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Wait err = %v, want one wrapping net.ErrClosed", err)
	}
}

// Each step of the tail — the redirect, the code exchange, the ID-token check — reports
// its own failure and stops there, and the loopback is torn down whichever one fails.
func TestLoginFlowTailFailures(t *testing.T) {
	boom := errors.New("step failed")
	for _, tc := range []struct {
		name  string
		wire  func(*fakeOAuth, *fakeLoopback)
		after func(*testing.T, *fakeOAuth)
	}{
		{
			name: "redirect",
			wire: func(_ *fakeOAuth, lb *fakeLoopback) { lb.waitErr = boom },
			after: func(t *testing.T, o *fakeOAuth) {
				if o.gotRedirect == "" {
					t.Fatal("the authorize URL should still have been built")
				}
			},
		},
		{
			name: "exchange",
			wire: func(o *fakeOAuth, _ *fakeLoopback) { o.exchangeErr = boom },
		},
		{
			name: "verify",
			wire: func(o *fakeOAuth, _ *fakeLoopback) { o.verifyErr = boom },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oauth := &fakeOAuth{authURL: "https://issuer/authorize"}
			lb := &fakeLoopback{code: "the-code"}
			tc.wire(oauth, lb)
			flow := &loginFlow{
				oauth:       oauth,
				newLoopback: func(string) (Loopback, error) { return lb, nil },
				openBrowser: func(string) error { return nil },
			}

			finish, err := flow.start(context.Background())
			if err != nil {
				t.Fatalf("start: %v", err)
			}
			if _, _, err := finish(context.Background()); !errors.Is(err, boom) {
				t.Fatalf("finish err = %v, want %v", err, boom)
			}
			if !lb.closed {
				t.Fatal("the loopback must be torn down even when the tail fails")
			}
			if tc.after != nil {
				tc.after(t, oauth)
			}
		})
	}
}

// The callback is one-shot: a redirect replayed after the code has been accepted is
// answered and dropped, never queued behind the first.
func TestLoopbackKeepsOnlyTheFirstCode(t *testing.T) {
	lb := newTestLoopback(t)

	for _, code := range []string{"first", "second"} {
		resp := hitCallback(t, lb, url.Values{"state": {lbState}, "code": {code}})
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status for %q = %d, want 200", code, resp.StatusCode)
		}
	}

	got, err := lb.Wait(context.Background())
	if err != nil || got != "first" {
		t.Fatalf("Wait = (%q, %v), want (first, nil)", got, err)
	}
}

// The opener is chosen per platform, and every arm is checked from whichever one runs the
// suite: the mapping takes the OS as an argument precisely so none of it is unreachable.
func TestBrowserCommand(t *testing.T) {
	const u = "https://issuer/authorize?state=x"
	for _, tc := range []struct {
		goos string
		name string
		args []string
	}{
		{"darwin", "open", []string{u}},
		{"windows", "rundll32", []string{"url.dll,FileProtocolHandler", u}},
		{"linux", "xdg-open", []string{u}},
		{"freebsd", "xdg-open", []string{u}},
	} {
		t.Run(tc.goos, func(t *testing.T) {
			name, args := browserCommand(tc.goos, u)
			if name != tc.name || !slices.Equal(args, tc.args) {
				t.Fatalf("browserCommand(%q) = (%q, %v), want (%q, %v)", tc.goos, name, args, tc.name, tc.args)
			}
		})
	}
}

// Wait honors context cancellation when no redirect arrives.
func TestLoopbackWaitRespectsContext(t *testing.T) {
	lb := newTestLoopback(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := lb.Wait(ctx); err == nil {
		t.Fatal("Wait should return the context error when cancelled before a redirect")
	}
}
