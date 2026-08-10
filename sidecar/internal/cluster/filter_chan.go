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

package cluster

import (
	"context"
)

// filterChan forwards what decide returns for each value of in — zero or MORE,
// because a filter may not be able to decide a frame yet, and a dropped watch
// frame is gone for good (beehive re-emits only on change); holding undecided
// frames keeps that from being permanent. Closes out when in closes or ctx ends.
func filterChan[T any](ctx context.Context, in <-chan T, decide func(T) []T) <-chan T {
	out := make(chan T, 1)
	go func() {
		defer close(out)
		for v := range in {
			// send, not a bare write: a consumer that stops draining (closed sync
			// dialog) would otherwise park this goroutine forever.
			for _, o := range decide(v) {
				if !send(ctx, out, o) {
					return
				}
			}
		}
	}()
	return out
}
