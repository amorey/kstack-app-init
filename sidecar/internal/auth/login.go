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

// generateVerifier returns a high-entropy PKCE code verifier. The verifier and
// state are inputs the login flow generates and hands to the oauth client's
// AuthCodeURL — the protocol layer treats them as caller-supplied.
func generateVerifier() string { return oauth2.GenerateVerifier() }

// generateState returns a high-entropy, URL-safe OAuth state value — the CSRF
// guard the loopback callback must echo back.
func generateState() string { return oauth2.GenerateVerifier() }

// defaultOpenBrowser opens rawURL in the user's default browser. The sidecar
// inherits the host's GUI session env (it's a child process), so the per-OS
// opener works the same as it would from the host.
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

// OAuthFlow is the subset of the oauth client the login flow drives.
// *oauth.Client satisfies it; tests inject a fake so the flow is exercised
// without Hydra.
type OAuthFlow interface {
	AuthCodeURL(state, verifier, redirect string) string
	Exchange(ctx context.Context, code, verifier, redirect string) (Token, error)
	Verify(ctx context.Context, rawIDToken string) (Identity, error)
}

// Loopback is the redirect listener the login flow waits on. *loopbackServer
// satisfies it; tests inject a fake that returns a canned code without binding a
// socket.
type Loopback interface {
	RedirectURL() string
	Wait(ctx context.Context) (string, error)
	Close() error
}

// LoopbackFunc builds a Loopback bound to the given OAuth state.
type LoopbackFunc func(state string) (Loopback, error)

// pendingLogin completes the slow tail of an already-started login: it waits for
// the loopback redirect to deliver the authorization code, then exchanges +
// verifies it into the minted token and verified identity. It is returned by a
// loginStarter once the synchronous setup has succeeded.
type pendingLogin func(ctx context.Context) (Token, Identity, error)

// loginStarter runs the synchronous setup of the interactive login — bind the
// loopback listener and open the system browser at the authorize URL — and
// returns the pendingLogin tail to finish asynchronously. The returned error is
// the pre-browser setup failure, surfaced synchronously to the login mutation.
// (*loginFlow).start is the production implementation; the service holds the
// login step behind this func so a test can inject a canned result without
// standing up the flow's loopback/browser/oauth seams — those are the
// loginFlow's own concern, tested directly against it.
type loginStarter func(ctx context.Context) (pendingLogin, error)

// loginFlow orchestrates the interactive desktop login. It owns the three
// injectable seams of that flow — the OAuth protocol operations, the loopback
// redirect-listener factory, and the system-browser opener. These three are
// deliberately kept out of the protocol-level oauth.Client: a loopback HTTP
// listener and a browser launch aren't OAuth-protocol concerns, and keeping them
// here preserves the per-concern test split (oauth_test against httptest Hydra,
// loopback_test in isolation, login_test with everything faked).
type loginFlow struct {
	oauth       OAuthFlow
	newLoopback LoopbackFunc
	openBrowser func(url string) error
}

// start runs the synchronous setup of the login flow — generate the PKCE
// verifier + CSRF state, spin up the loopback redirect listener, and open the
// system browser at the PKCE authorize URL — then returns a pendingLogin tail
// that finishes the slow browser round-trip (wait for the redirect, exchange +
// verify the code into the minted token and verified identity). The setup steps
// are fast and deterministic, so their failures (loopback bind, browser launch)
// are surfaced synchronously to the caller. On a browser-open failure the
// listener is torn down before returning (no tail will run to close it); on
// success the returned tail closes it when it completes, errors, or ctx is
// cancelled.
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

// loopbackServer is the 127.0.0.1 HTTP listener that catches Hydra's redirect
// and surfaces the authorization code. Bind it before building AuthCodeURL so
// the redirect_uri matches. It satisfies the Loopback seam.
type loopbackServer struct {
	ln    net.Listener
	srv   *http.Server
	state string // expected OAuth state; the redirect must echo it back

	codeCh chan string
	errCh  chan error
}

// newLoopbackServer binds an ephemeral 127.0.0.1 port and starts serving the
// /oauth/callback route. state is the OAuth state the redirect must echo back;
// the caller passes the same value to AuthCodeURL so a stray local request can't
// satisfy the one-shot callback.
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

// callbackHTML is the page the browser lands on after a successful redirect to
// our loopback listener.
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
	// Validate state first, before consuming either an error or a code: a
	// stray/malicious local request without the expected state must not be
	// able to abort (via ?error=) or consume the one-shot flow.
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
