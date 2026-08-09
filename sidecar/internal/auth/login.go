package auth

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"runtime"

	"golang.org/x/oauth2"
)

// generateVerifier returns a PKCE code verifier; the protocol layer treats it as
// caller-supplied.
func generateVerifier() string { return oauth2.GenerateVerifier() }

// generateState returns the CSRF state the loopback callback must echo back.
func generateState() string { return oauth2.GenerateVerifier() }

// defaultOpenBrowser opens rawURL in the default browser. The sidecar inherits the host's
// GUI session env, so the per-OS opener behaves as it would from the host.
func defaultOpenBrowser(rawURL string) error {
	var name string
	var args []string
	switch runtime.GOOS {
	case "darwin":
		name, args = "open", []string{rawURL}
	case "windows":
		name, args = "rundll32", []string{"url.dll,FileProtocolHandler", rawURL}
	default: // linux, *bsd, etc.
		name, args = "xdg-open", []string{rawURL}
	}
	return exec.Command(name, args...).Start()
}

// OAuthFlow is the subset of the oauth client the login flow drives; tests fake it to
// exercise the flow without Hydra.
type OAuthFlow interface {
	AuthCodeURL(state, verifier, redirect string) string
	Exchange(ctx context.Context, code, verifier, redirect string) (Token, error)
	Verify(ctx context.Context, rawIDToken string) (Identity, error)
}

// Loopback is the redirect listener the login flow waits on; tests fake it with a canned
// code and no socket.
type Loopback interface {
	RedirectURL() string
	Wait(ctx context.Context) (string, error)
	Close() error
}

// LoopbackFunc builds a Loopback bound to the given OAuth state.
type LoopbackFunc func(state string) (Loopback, error)

// pendingLogin is the slow tail of a started login: wait for the redirect's code, then
// exchange + verify it.
type pendingLogin func(ctx context.Context) (Token, Identity, error)

// loginStarter runs the synchronous setup (bind loopback, open browser) and returns the
// tail; its error is the pre-browser failure surfaced to the login mutation. Production
// is (*loginFlow).start; the service holds it behind this func as a test seam.
type loginStarter func(ctx context.Context) (pendingLogin, error)

// loginFlow orchestrates the interactive desktop login over three seams: the OAuth
// operations, the loopback factory, and the browser opener. The latter two stay out of
// oauth.Client — an HTTP listener and a browser launch aren't protocol concerns — which
// also keeps the per-concern test split.
type loginFlow struct {
	oauth       OAuthFlow
	newLoopback LoopbackFunc
	openBrowser func(url string) error
}

// start generates the verifier + state, binds the loopback listener and opens the
// browser, returning the tail that finishes the round-trip. Setup failures surface
// synchronously (tearing the listener down); otherwise the tail closes it.
func (f *loginFlow) start(_ context.Context) (pendingLogin, error) {
	verifier := generateVerifier()
	state := generateState()

	lb, err := f.newLoopback(state)
	if err != nil {
		return nil, err
	}
	redirect := lb.RedirectURL()

	if err := f.openBrowser(f.oauth.AuthCodeURL(state, verifier, redirect)); err != nil {
		lb.Close()
		return nil, err
	}

	return func(ctx context.Context) (Token, Identity, error) {
		defer lb.Close()

		code, err := lb.Wait(ctx)
		if err != nil {
			return Token{}, Identity{}, err
		}
		tok, err := f.oauth.Exchange(ctx, code, verifier, redirect)
		if err != nil {
			return Token{}, Identity{}, err
		}
		id, err := f.oauth.Verify(ctx, tok.IDToken)
		if err != nil {
			return Token{}, Identity{}, err
		}
		return tok, id, nil
	}, nil
}

// loopbackServer is the 127.0.0.1 listener catching the redirect and surfacing the
// authorization code. Bind it before building AuthCodeURL so the redirect_uri matches.
type loopbackServer struct {
	ln    net.Listener
	srv   *http.Server
	state string // the redirect must echo this back

	codeCh chan string
	errCh  chan error
}

// newLoopbackServer binds an ephemeral port and serves /oauth/callback. state must match
// what the caller passed to AuthCodeURL, so a stray local request can't satisfy the
// one-shot callback.
func newLoopbackServer(state string) (*loopbackServer, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	lb := &loopbackServer{
		ln:     ln,
		state:  state,
		codeCh: make(chan string, 1),
		errCh:  make(chan error, 1),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/oauth/callback", lb.handleCallback)
	lb.srv = &http.Server{Handler: mux}
	go func() {
		if err := lb.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			lb.errCh <- err
		}
	}()
	return lb, nil
}

// callbackHTML is the page the browser lands on after a successful redirect.
const callbackHTML = "<!doctype html>" +
	"<html lang=en>" +
	"<head>" +
	"<meta charset=utf-8>" +
	"<title>kstack — signed in</title>" +
	"<style>body{font:16px system-ui,sans-serif;text-align:center;padding:4em;color:#222}h1{font-weight:600;margin:0 0 .25em}p{color:#666}</style>" +
	"</head>" +
	"<body>" +
	"<h1>Signed in</h1>" +
	"<p>You can close this tab and return to the app.</p>" +
	"</body>" +
	"</html>"

func (lb *loopbackServer) handleCallback(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	// State first, before consuming an error or a code: a stray local request must not
	// be able to abort (via ?error=) or consume the one-shot flow.
	if q.Get("state") != lb.state {
		http.Error(w, "invalid state", http.StatusBadRequest)
		return
	}
	if e := q.Get("error"); e != "" {
		lb.errCh <- fmt.Errorf("oauth: authorization failed: %s", e)
		http.Error(w, e, http.StatusBadRequest)
		return
	}
	code := q.Get("code")
	if code == "" {
		http.Error(w, "missing code", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	_, _ = w.Write([]byte(callbackHTML))
	select {
	case lb.codeCh <- code:
	default:
	}
}

// RedirectURL is the redirect_uri to register with the authorize request.
func (lb *loopbackServer) RedirectURL() string {
	return (&url.URL{Scheme: "http", Host: lb.ln.Addr().String(), Path: "/oauth/callback"}).String()
}

// Wait blocks until the redirect delivers a code, the flow errors, or ctx is
// cancelled.
func (lb *loopbackServer) Wait(ctx context.Context) (string, error) {
	select {
	case code := <-lb.codeCh:
		return code, nil
	case err := <-lb.errCh:
		return "", err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// Close shuts down the loopback server.
func (lb *loopbackServer) Close() error {
	return lb.srv.Close()
}

// defaultNewLoopback adapts newLoopbackServer to the Loopback seam.
func defaultNewLoopback(state string) (Loopback, error) {
	return newLoopbackServer(state)
}
