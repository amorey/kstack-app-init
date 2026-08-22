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

// Package kubeconn keeps one validated connection per set of credentials, so everything
// that talks to a cluster — discovery, the sync workers, log tailing — rides the same
// TCP/TLS connection instead of opening its own, and none of them has to work out whether
// it still functions.
//
// Credentials are the unit throughout, named by a key the caller computes. Two contexts
// aimed at one server as one user are one entry, one socket and one probe — the identity
// they would each learn is the same answer. What a caller keeps is a Lease, and the probe
// runs only while something holds one or has asked for a single answer.
//
// It never decides which clusters exist, nor what a probe result means for one: it is
// handed credentials and reports what they reach.
package kubeconn

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/amorey/gobus/conflate"
	"github.com/amorey/gobus/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
)

// The tuning every pooled connection carries. The pool key describes credentials only,
// so two callers under one key with different tuning would silently share whichever
// connection built first — the tuning is this package's to set.
const (
	defaultQPS   float32 = 20
	defaultBurst int     = 40
	userAgent            = "kstack-app"
)

// ErrClosed reports a Get against a pool that has shut down.
var ErrClosed = errors.New("kubeconn: pool is closed")

// Connection is one set of credentials and the clients built over them. The clients
// share one http.Client, so they share one connection pool — with HTTP/2 that is a
// single TCP connection carrying every concurrent request to that API server.
//
// It never changes after it is built. Credentials change by arriving under a new key,
// which builds a new one.
type Connection struct {
	Config *rest.Config
	// BaseURL is Config.Host resolved to an absolute URL, carrying the scheme and any
	// path prefix; APIPath is the versioned path beside it. Raw paths join onto them.
	BaseURL *url.URL
	APIPath string
	// HTTPClient is authenticated and pooled. Raw API paths go through it directly, and
	// unthrottled: the QPS bucket lives in rest.RESTClient, so it reaches Dynamic alone.
	HTTPClient *http.Client
	Dynamic    dynamic.Interface
}

// Service is the pool, and the probe that keeps what it hands out validated. One entry
// per set of credentials carries both: the connection built here, and the probe state
// loop.go maintains beside it.
type Service struct {
	mu sync.Mutex
	// closed latches, so a Get racing shutdown cannot repopulate the pool with a live
	// transport that nothing is left to close.
	closed bool
	conns  map[string]*entry

	// budget is fixed at construction, since the loops read it without the mutex.
	budget Budget

	// slots bounds concurrent probes: a caller can declare its whole fleet in one pass,
	// and each first probe may run a credential helper.
	slots chan struct{}

	// resultsHub wakes whoever is parked in Lease.Conn: a caller registers under the
	// mutex, having just read the current result, and hears the next one. It carries
	// results and stores none — the store is entry.result.
	resultsHub *watch.Hub[string, *Result]

	// newsHub carries which credentials moved, for a reader that holds no claim. Separate
	// from resultsHub: that one is a claim waiting for its own key's next result, this one
	// is every key, coalesced, at the reader's pace.
	newsHub *conflate.Hub[string, struct{}]

	// ctx bounds every loop and stopLoops ends them. Live from New, so demand arriving
	// before Start still gets a loop.
	ctx       context.Context
	stopLoops context.CancelFunc
	// stopped closes the door on new loops, set under the mutex before the drain begins.
	// A loop starts on demand, so without this a claim arriving mid-shutdown would add a
	// goroutine to a WaitGroup already being waited on.
	stopped bool
	wg      sync.WaitGroup

	// newDynamic builds the dynamic client; a build seam, since the case worth testing
	// is client-go refusing one, which valid credentials cannot ask for.
	newDynamic func(*rest.Config, *http.Client) (dynamic.Interface, error)
	// probe is a build seam, overridden only by this package's tests, so they never touch
	// the network.
	probe func(ctx context.Context, conn *Connection) (Identity, error)
}

// Budget is the pacing, taken from the caller rather than read from a constant here, so
// a test never has to outwait production's numbers.
type Budget struct {
	// Cadence is how often a leased connection re-probes.
	Cadence time.Duration
	// RetryBase and RetryMax bound the ladder a failing probe backs off along.
	RetryBase time.Duration
	RetryMax  time.Duration
	// Timeout bounds one probe: a server that completes the handshake and then never
	// answers would otherwise hold its slot until the process ended.
	Timeout time.Duration
	// Concurrency bounds how many credentials probe at once. A caller can declare its
	// whole fleet in one pass, and each first probe may run a credential helper.
	Concurrency int
}

// DefaultBudget is the pacing production runs at. A caller passes it explicitly, so a
// test picks its own timescale by passing another.
var DefaultBudget = Budget{
	Cadence:     30 * time.Second,
	RetryBase:   2 * time.Second,
	RetryMax:    5 * time.Minute,
	Timeout:     10 * time.Second,
	Concurrency: 4,
}

// New returns an empty pool, probing at the pace budget sets.
//
// It also tightens the process-wide HTTP/2 keepalive (see keepalive.go), where the
// operator has not set a value. apimachinery reads those env vars each time a transport
// is built, so setting them here covers every connection this pool goes on to build.
func New(budget Budget) *Service {
	configureHTTP2Keepalive()
	// An unbuffered slots channel deadlocks probeOnce against itself, and a partly-filled
	// Budget literal is an easy way to ask for one. One at a time is the honest reading of
	// "no concurrency", and it fails nothing.
	budget.Concurrency = max(budget.Concurrency, 1)
	// Not Start's context, which bounds startup: this one bounds every loop, so it lives
	// until the stop func cancels it.
	ctx, stopLoops := context.WithCancel(context.Background())

	return &Service{
		conns:      map[string]*entry{},
		budget:     budget,
		slots:      make(chan struct{}, budget.Concurrency),
		resultsHub: watch.New[string, *Result](),
		// Latest-wins, the default: the value is empty, so two probes coalescing must leave
		// the slot pending. A merge that annihilated it would leave a subscriber one behind
		// hearing about neither.
		newsHub:   conflate.New[string, struct{}](),
		ctx:       ctx,
		stopLoops: stopLoops,
		probe:     Probe,
		// NewForConfigAndClient, never NewForConfig: the latter builds a fresh client
		// and a fresh pool, which is the whole thing this package exists to avoid.
		newDynamic: func(cfg *rest.Config, c *http.Client) (dynamic.Interface, error) {
			return dynamic.NewForConfigAndClient(cfg, c)
		},
	}
}

// entry is one key's connection, built exactly once however many callers arrive, plus
// what the probe behind it knows. The probe fields are guarded by the Service's mutex;
// conn and err are written under once and read only after it.
type entry struct {
	once sync.Once
	conn *Connection
	err  error

	// wake asks the loop to probe now.
	wake chan struct{}
	// idle asks the loop to re-check whether it is still wanted, which a dropped claim
	// does. Separate from wake because that one means "probe", and a claim going away is
	// the opposite request.
	idle chan struct{}

	// leases counts the callers that need these credentials connected, and is what arms
	// the cadence: with none held the loop probes when asked and ends.
	leases int
	// running says a loop goroutine exists. A loop lives as long as there is work — a
	// claim, or a probe asked for and not yet run — and no longer.
	running bool
	// probing says a probe is asked for and unanswered. Set when it is asked for rather
	// than when it starts, so time spent queued behind the semaphore reads as what it is.
	probing bool
	// result is nil until the first probe lands, and is this package's own store of it:
	// resultsHub carries a value to whoever is parked and keeps none.
	result *Result
}

// kick asks the loop to probe now. Never blocks: the request is only ever "run again", so
// a second while one is pending adds nothing.
func (e *entry) kick() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// nudge asks the loop to re-check whether anything still wants it. Never blocks, for the
// same reason kick does not.
func (e *entry) nudge() {
	select {
	case e.idle <- struct{}{}:
	default:
	}
}

// entryFor is the one way in: the shared entry for these credentials, built on first use.
// key identifies the credentials; the caller supplies it, because only the caller knows
// what makes them different. Concurrent callers for one key get the same entry.
//
// Unexported, so every connection this pool hands out is held through a Lease — which is
// what lets it know when nothing needs one any more.
func (s *Service) entryFor(cfg *rest.Config, key string) (*entry, error) {
	s.mu.Lock()
	if s.shuttingDown() {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	e, ok := s.conns[key]
	if !ok {
		e = &entry{wake: make(chan struct{}, 1), idle: make(chan struct{}, 1)}
		s.conns[key] = e
	}
	s.mu.Unlock()

	// Outside the pool's lock: building reads the credentials' TLS material from disk,
	// and holding the lock across that would make every other key — and every plain
	// cache hit — queue behind it, flattening the probe fan-out to one at a time.
	e.once.Do(func() {
		conn, err := s.build(cfg)

		// Published under the lock rather than written in place: the Once orders this
		// against other builders and against anyone waiting on it, but Close never calls
		// Do — and it reads e.conn to decide what to shut down.
		s.mu.Lock()
		e.conn, e.err = conn, err
		if err != nil {
			// A failed build leaves nothing behind, or the next caller inherits it.
			delete(s.conns, key)
		}
		s.mu.Unlock()
	})
	if e.err != nil {
		return nil, e.err
	}

	// Re-checked because the build ran outside the lock: a shutdown landing during it took
	// this entry with it, and found nothing to close because the transport did not exist
	// yet. Nothing else will close it now, so close it here and refuse the caller.
	s.mu.Lock()
	done := s.shuttingDown()
	s.mu.Unlock()
	if done {
		e.conn.HTTPClient.CloseIdleConnections()
		return nil, ErrClosed
	}
	return e, nil
}

// shuttingDown reports whether the pool has begun winding down: the stop func sets
// stopped, Close sets closed, and from the first of those nothing new is handed out. A
// claim taken after it could never be satisfied — no loop would start to probe it, so its
// Conn would wait on a result nothing was going to produce. Callers hold the mutex.
func (s *Service) shuttingDown() bool { return s.closed || s.stopped }

// Start returns the func that ends every probe loop. The loops themselves start with the
// demand that needs them, which can be before this. Nothing here can fail.
func (s *Service) Start(context.Context) (func(context.Context) error, error) {
	return func(ctx context.Context) error {
		s.mu.Lock()
		s.stopped = true
		s.mu.Unlock()

		s.stopLoops()
		return drain.WithContext(ctx, s.wg.Wait)
	}, nil
}

// Close drops every pooled connection and both buses. Only idle sockets close, so a stream
// in flight is never cut.
//
// The stop func is what waits for the loops; this only cancels them, so that a Close on its
// own still leaves nothing running. A loop unwinding past this finds the buses closed and
// its sends refused, which is what makes the order between the two safe either way.
func (s *Service) Close() error {
	// Latched first, and on its own: everything below leaves something unusable behind —
	// a cancelled loop context, a closed bus — and a caller that got past shuttingDown in
	// the meantime would be handed a claim nothing can answer.
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	s.stopLoops()
	s.resultsHub.Close()
	s.newsHub.Close()

	s.mu.Lock()
	defer s.mu.Unlock()

	for key, e := range s.conns {
		if e.conn != nil {
			e.conn.HTTPClient.CloseIdleConnections()
		}
		delete(s.conns, key)
	}
	return nil
}

// build materializes the clients for one set of credentials.
func (s *Service) build(cfg *rest.Config) (*Connection, error) {
	// A copy, because the tuning below is ours and the caller's config is not.
	own := rest.CopyConfig(cfg)
	own.QPS = defaultQPS
	own.Burst = defaultBurst
	own.UserAgent = userAgent

	// DefaultServerUrlFor rather than DefaultServerURL: it derives the scheme from
	// whether the config actually carries CA or client-cert data, so a scheme-less
	// plain-HTTP endpoint (a port-forward) stays HTTP instead of failing at a handshake.
	baseURL, apiPath, err := rest.DefaultServerUrlFor(own)
	if err != nil {
		return nil, fmt.Errorf("resolve server URL: %w", err)
	}

	httpClient, err := rest.HTTPClientFor(own)
	if err != nil {
		return nil, fmt.Errorf("build http client: %w", err)
	}

	dyn, err := s.newDynamic(own, httpClient)
	if err != nil {
		return nil, fmt.Errorf("build dynamic client: %w", err)
	}

	return &Connection{
		Config:     own,
		BaseURL:    baseURL,
		APIPath:    apiPath,
		HTTPClient: httpClient,
		Dynamic:    dyn,
	}, nil
}
