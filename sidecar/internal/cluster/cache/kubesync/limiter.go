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

// ListLimiter bounds how many workers may be inside their LIST phase at once. One is
// shared by every worker that should compete for the same budget — in this app, all of
// one cache's kinds.
//
// It exists because a worker's cost is wildly uneven across its own lifetime: the watch
// phase is indefinite but nearly free (one idle connection), while the LIST phase holds a
// page of decoded bodies and hammers the API server. A cache has one worker per served
// kind — a hundred or more — and they all start at once on a cold sync or after a resume
// poke, so without a bound the peak memory and API burst scale with the kind count rather
// than staying flat.
//
// The slot is held across the list-heavy work ONLY and released before the worker re-enters
// its watch — holding it across the watch would deadlock the whole cache behind its first
// N kinds. A nil ListLimiter imposes no bound, which is what a driver built directly in a
// unit test gets.
type ListLimiter chan struct{}

// NewListLimiter returns a limiter admitting n concurrent LIST phases. A non-positive n
// returns nil (unbounded), matching the zero value.
func NewListLimiter(n int) ListLimiter {
	if n <= 0 {
		return nil
	}
	return make(ListLimiter, n)
}

// acquire takes a slot, blocking until one frees or ctx ends. The returned release func is
// safe to call unconditionally — it is a no-op on a nil limiter or a cancelled acquire —
// so callers can `defer release()` before checking the error.
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
