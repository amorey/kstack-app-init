// Copyright 2026 The Kstack Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kubeconn

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
)

// The three paths one probe reads. selfsubjectreviews is authentication.k8s.io/v1 from
// 1.28 on, so an older server answers it with a 404.
const (
	versionPath  = "/version"
	identityPath = "/api/v1/namespaces/kube-system"
	reviewPath   = "/apis/authentication.k8s.io/v1/selfsubjectreviews"
)

// reviewBody is the SelfSubjectReview posted to reviewPath. A create endpoint decodes a
// resource from the request body, so an empty POST is a 400 and the username silently
// comes back missing; the server fills in the status itself. Sent raw rather than
// through a typed client, which would link the API types this binary keeps out.
var reviewBody = []byte(`{"apiVersion":"authentication.k8s.io/v1","kind":"SelfSubjectReview"}`)

// Identity is what one probe learns. Every field is optional: a probe that reaches the
// API server succeeds, and reports whatever this user was allowed to read.
//
// The error field makes this uncomparable — == on an error panics for a dynamic type
// that is itself uncomparable — so compare the fields, never the struct.
type Identity struct {
	ServerUID     string
	ServerVersion string
	Username      string
	// UIDErr is why ServerUID is empty, when a response came back saying so. It survives
	// as an error rather than a bool because the caller tells the user apart: no RBAC on
	// kube-system (403) and a server that has no such namespace (404) are different news.
	UIDErr error
}

// sameAs reports whether two identities say the same thing. Field by field, because
// UIDErr makes Identity uncomparable.
//
// UIDErr compares by text, unlike the transport failures on Result: a probe that reached
// the server carries a status line here ("…/kube-system: 403 Forbidden"), which is stable
// across probes and is the whole content of the report — so 403 turning into 404 is news,
// and comparing presence alone would swallow the distinction this field exists to keep.
func (id Identity) sameAs(other Identity) bool {
	return id.ServerUID == other.ServerUID &&
		id.ServerVersion == other.ServerVersion &&
		id.Username == other.Username &&
		errText(id.UIDErr) == errText(other.UIDErr)
}

// errText is err's message, or "" for no error.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Probe reads a cluster's identity over conn.
//
// A request that got an HTTP response reached the API server, which is the thing being
// probed, so a refusal leaves its field empty rather than failing the probe: one error for
// three requests would let a namespace-scoped user's missing RBAC read as a cluster that is
// down. Two things do fail it — a transport failure, and every request being refused, which
// is credentials that do not work rather than a user who may not read much.
//
// The three requests go out concurrently — they share one connection, so the probe costs
// one round trip rather than three.
func Probe(ctx context.Context, conn *Connection) (Identity, error) {
	var (
		id         Identity
		versionErr error
		reviewErr  error
	)

	var wg sync.WaitGroup
	wg.Go(func() {
		var body struct {
			GitVersion string `json:"gitVersion"`
		}
		versionErr = get(ctx, conn, versionPath, &body)
		id.ServerVersion = body.GitVersion
	})
	wg.Go(func() {
		var body struct {
			Metadata struct {
				UID string `json:"uid"`
			} `json:"metadata"`
		}
		id.UIDErr = get(ctx, conn, identityPath, &body)
		id.ServerUID = body.Metadata.UID
	})
	wg.Go(func() {
		var body struct {
			Status struct {
				UserInfo struct {
					Username string `json:"username"`
				} `json:"userInfo"`
			} `json:"status"`
		}
		reviewErr = post(ctx, conn, reviewPath, reviewBody, &body)
		id.Username = body.Status.UserInfo.Username
	})
	wg.Wait()

	// A transport failure hits all three, so any one of them names it.
	for _, err := range []error{id.UIDErr, versionErr, reviewErr} {
		if errors.Is(err, errTransport) {
			return Identity{}, err
		}
	}

	// Every request refused is not partial RBAC — it is credentials that do not work at
	// all, which expired or revoked ones look like (401 on every path). Reported as a
	// failure, because an Identity with nothing in it is what a caller cannot tell from a
	// healthy cluster whose contents this user may not read.
	if id.UIDErr != nil && versionErr != nil && reviewErr != nil {
		return Identity{}, reviewErr
	}
	return id, nil
}

// get reads one probe path and decodes the response into out.
func get(ctx context.Context, conn *Connection, path string, out any) error {
	return do(ctx, conn, http.MethodGet, path, nil, out)
}

// post creates one resource and decodes the answer into out.
func post(ctx context.Context, conn *Connection, path string, body []byte, out any) error {
	return do(ctx, conn, http.MethodPost, path, body, out)
}

// do performs one probe request and decodes its response into out.
// maxDiscard bounds what is read off a response nobody wants, which is enough for an error
// page and short of anything a hostile endpoint could stream.
const maxDiscard = 4 << 10

func do(ctx context.Context, conn *Connection, method, path string, body []byte, out any) error {
	var payload io.Reader
	if body != nil {
		payload = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, conn.BaseURL.JoinPath(path).String(), payload)
	if err != nil {
		// Nothing reached the API server, which is what this failure class means.
		return fmt.Errorf("probe %s: %w: %w", path, errTransport, err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := conn.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("probe %s: %w: %w", path, errTransport, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// Drained so the connection can be reused: an HTTP/1.1 fallback will not take it
		// back with a body outstanding, and a user without RBAC on kube-system gets one of
		// these every cadence. Bounded, since the body is an error page we do not read.
		_, _ = io.CopyN(io.Discard, resp.Body, maxDiscard)
		return fmt.Errorf("%s: %s", path, resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

// errTransport marks the one failure that means nothing reached the API server. A
// status code does not: the server answered.
var errTransport = errors.New("transport failure")
