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

package kubesync

import "context"

// ListLimiter bounds how many workers are inside their LIST phase at once, shared by
// every worker competing for one budget (in this app, all of one cache's kinds).
//
// A worker's cost is uneven over its life: the watch phase is indefinite but nearly free,
// while a LIST holds a page of decoded bodies and hammers the API server — and a cache's
// hundred-plus workers all start at once on a cold sync or resume poke.
//
// The slot covers the list-heavy work ONLY, released before re-entering the watch;
// holding it across the watch would deadlock the cache behind its first N kinds. A nil
// limiter is unbounded.
type ListLimiter chan struct{}

// NewListLimiter returns a limiter admitting n concurrent LIST phases. A non-positive n
// returns nil (unbounded), matching the zero value.
func NewListLimiter(n int) ListLimiter {
	if n <= 0 {
		return nil
	}
	return make(ListLimiter, n)
}

// acquire takes a slot, blocking until one frees or ctx ends. release is safe to call
// unconditionally (a no-op on a nil limiter or cancelled acquire), so callers can
// `defer release()` before checking the error.
func (l ListLimiter) acquire(ctx context.Context) (release func(), err error) {
	if l == nil {
		return func() {}, nil
	}
	select {
	case <-ctx.Done():
		return func() {}, ctx.Err()
	case l <- struct{}{}:
		return func() { <-l }, nil
	}
}
