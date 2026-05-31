package app

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/authcreds"
)

// controlWake is the host-only OS-wake hook. Like /control/credentials it's
// deliberately off the GraphQL surface — a host→sidecar control signal, not
// data. Generic name: today it triggers an engine resync via Engine.Poke, but
// future wake-responses can be dispatched from the same endpoint without a wire
// rename.
func controlWake(poke func()) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		poke()
		w.WriteHeader(http.StatusNoContent)
	})
}

// controlCredentials accepts the host's token push. Kept off the GraphQL
// surface deliberately: setting process credentials is host-only, and the
// UDS is already user-restricted (0600). A malformed or empty-token push
// is rejected and leaves the existing credentials intact, so a bad push
// can never blank a working token.
func controlCredentials(creds *authcreds.Holder) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Token     string    `json:"token"`
			ExpiresAt time.Time `json:"expiresAt"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Token == "" {
			http.Error(w, "bad credentials payload", http.StatusBadRequest)
			return
		}
		creds.Set(authcreds.Credentials{Token: body.Token, ExpiresAt: body.ExpiresAt})
		w.WriteHeader(http.StatusNoContent)
	})
}
