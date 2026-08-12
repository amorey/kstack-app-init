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

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

func TestListLimiterAdmitsUpToItsBound(t *testing.T) {
	l := NewListLimiter(2)
	ctx := context.Background()

	r1, err := l.acquire(ctx)
	require.NoError(t, err)
	r2, err := l.acquire(ctx)
	require.NoError(t, err)

	// The third caller waits: the bound is the whole point, so this must not admit.
	admitted := make(chan struct{})
	go func() {
		r3, aerr := l.acquire(ctx)
		if aerr == nil {
			defer r3()
			close(admitted)
		}
	}()
	select {
	case <-admitted:
		t.Fatal("a third LIST was admitted past a bound of 2")
	case <-time.After(50 * time.Millisecond):
	}

	// Releasing one lets it through, which is what keeps a cold sync progressing rather
	// than deadlocking behind its first N kinds.
	r1()
	testutil.Wait(t, admitted, "releasing a slot to admit the waiter")
	r2()
}

// A cancelled acquire must still hand back a usable release func: callers `defer release()`
// before checking the error, so a nil func would panic on every shutdown-during-LIST.
func TestListLimiterAcquireHonorsContext(t *testing.T) {
	l := NewListLimiter(1)
	release, err := l.acquire(context.Background())
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r, err := l.acquire(ctx)
	require.Error(t, err)
	require.NotNil(t, r)
	assert.NotPanics(t, r)
}

// A nil limiter is what a driver built directly in a unit test gets, and it must impose no
// bound rather than block forever.
func TestNilListLimiterIsUnbounded(t *testing.T) {
	var l ListLimiter
	assert.Nil(t, NewListLimiter(0))

	for range 100 {
		release, err := l.acquire(context.Background())
		require.NoError(t, err)
		release()
	}
}
