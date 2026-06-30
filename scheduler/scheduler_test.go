package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitForResult polls cond() up to the given timeout, failing t if it never
// returns true. Keeps tests free of arbitrary sleeps.
func waitForResult(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

// --------------------------------------------------------------------------- //
// Delayed jobs (After)                                                        //
// --------------------------------------------------------------------------- //

func TestAfter(t *testing.T) {
	s := New()
	defer func() { _ = s.Shutdown(context.Background()) }()

	var ran atomic.Bool
	start := time.Now()
	id := s.After(context.Background(), 50*time.Millisecond, func(ctx context.Context) error {
		ran.Store(true)
		return nil
	})

	if id == "" {
		t.Fatal("expected non-empty job ID")
	}

	waitForResult(t, time.Second, ran.Load)
	elapsed := time.Since(start)
	if elapsed < 40*time.Millisecond {
		t.Fatalf("job ran too early: %v", elapsed)
	}
}

func TestAt(t *testing.T) {
	s := New()
	defer func() { _ = s.Shutdown(context.Background()) }()

	var ran atomic.Bool
	when := time.Now().Add(60 * time.Millisecond)
	s.At(context.Background(), when, func(ctx context.Context) error {
		ran.Store(true)
		return nil
	})

	waitForResult(t, time.Second, ran.Load)
}

func TestAtPastTime(t *testing.T) {
	s := New()
	defer func() { _ = s.Shutdown(context.Background()) }()

	var ran atomic.Bool
	s.At(context.Background(), time.Now().Add(-time.Hour), func(ctx context.Context) error {
		ran.Store(true)
		return nil
	})

	waitForResult(t, 500*time.Millisecond, ran.Load)
}

// --------------------------------------------------------------------------- //
// Recurring jobs (Every)                                                      //
// --------------------------------------------------------------------------- //

func TestEvery(t *testing.T) {
	s := New()
	defer func() { _ = s.Shutdown(context.Background()) }()

	var count atomic.Int32
	s.Every(context.Background(), 30*time.Millisecond, func(ctx context.Context) error {
		count.Add(1)
		return nil
	})

	waitForResult(t, 2*time.Second, func() bool { return count.Load() >= 3 })
}

func TestEveryNoOverlap(t *testing.T) {
	s := New(WithMaxConcurrency(1))
	defer func() { _ = s.Shutdown(context.Background()) }()

	var concurrent, maxConcurrent atomic.Int32
	var total atomic.Int32

	s.Every(context.Background(), 20*time.Millisecond, func(ctx context.Context) error {
		cur := concurrent.Add(1)
		for {
			old := maxConcurrent.Load()
			if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
				break
			}
		}
		total.Add(1)
		time.Sleep(40 * time.Millisecond) // slow handler
		concurrent.Add(-1)
		return nil
	})

	waitForResult(t, 2*time.Second, func() bool { return total.Load() >= 3 })
	if maxConcurrent.Load() > 1 {
		t.Fatalf("recurring jobs overlapped: max concurrency = %d", maxConcurrent.Load())
	}
}

// --------------------------------------------------------------------------- //
// Cancellation                                                                //
// --------------------------------------------------------------------------- //

func TestCancel(t *testing.T) {
	s := New()
	defer func() { _ = s.Shutdown(context.Background()) }()

	var ran atomic.Bool
	id := s.After(context.Background(), 100*time.Millisecond, func(ctx context.Context) error {
		ran.Store(true)
		return nil
	})

	if !s.Cancel(id) {
		t.Fatal("Cancel returned false for existing job")
	}
	if s.Cancel(id) {
		t.Fatal("Cancel returned true for already-cancelled job")
	}

	time.Sleep(200 * time.Millisecond)
	if ran.Load() {
		t.Fatal("cancelled job still ran")
	}
}

func TestCancelRecurring(t *testing.T) {
	s := New()
	defer func() { _ = s.Shutdown(context.Background()) }()

	var count atomic.Int32
	id := s.Every(context.Background(), 20*time.Millisecond, func(ctx context.Context) error {
		count.Add(1)
		return nil
	})

	waitForResult(t, time.Second, func() bool { return count.Load() >= 2 })

	s.Cancel(id)
	before := count.Load()
	time.Sleep(100 * time.Millisecond)
	after := count.Load()

	if after != before {
		t.Fatalf("recurring job ran after cancel: before=%d after=%d", before, after)
	}
}

// --------------------------------------------------------------------------- //
// Graceful shutdown                                                           //
// --------------------------------------------------------------------------- //

func TestShutdownWaits(t *testing.T) {
	s := New()

	var completed atomic.Bool
	s.After(context.Background(), 10*time.Millisecond, func(ctx context.Context) error {
		time.Sleep(100 * time.Millisecond)
		completed.Store(true)
		return nil
	})

	// Give the job time to start.
	time.Sleep(30 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
	if !completed.Load() {
		t.Fatal("in-flight job did not complete before shutdown")
	}
}

func TestShutdownTimeout(t *testing.T) {
	s := New()

	block := make(chan struct{})
	s.After(context.Background(), 0, func(ctx context.Context) error {
		<-block
		return nil
	})

	time.Sleep(30 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := s.Shutdown(ctx)
	if err == nil {
		close(block)
		t.Fatal("expected shutdown timeout error")
	}
	close(block)
}

func TestShutdownBlocksNewJobs(t *testing.T) {
	s := New()
	_ = s.Shutdown(context.Background())

	id := s.After(context.Background(), 10*time.Millisecond, func(ctx context.Context) error {
		return nil
	})
	if id != "" {
		t.Fatal("expected empty ID after shutdown")
	}
}

func TestShutdownTwice(t *testing.T) {
	s := New()
	_ = s.Shutdown(context.Background())
	if err := s.Shutdown(context.Background()); err != ErrAlreadyShutdown {
		t.Fatalf("expected ErrAlreadyShutdown, got %v", err)
	}
}

// --------------------------------------------------------------------------- //
// Panic recovery                                                              //
// --------------------------------------------------------------------------- //

func TestPanicRecovery(t *testing.T) {
	var errMu sync.Mutex
	var gotErr error
	var gotID string

	s := New(WithErrorHandler(func(jobID string, err error) {
		errMu.Lock()
		defer errMu.Unlock()
		gotID = jobID
		gotErr = err
	}))
	defer func() { _ = s.Shutdown(context.Background()) }()

	var ran atomic.Bool
	s.After(context.Background(), 10*time.Millisecond, func(ctx context.Context) error {
		ran.Store(true)
		panic("boom")
	})

	waitForResult(t, time.Second, ran.Load)
	waitForResult(t, time.Second, func() bool {
		errMu.Lock()
		defer errMu.Unlock()
		return gotErr != nil
	})

	if gotID == "" {
		t.Fatal("error handler did not receive job ID")
	}
	if gotErr == nil {
		t.Fatal("error handler did not receive error")
	}
}

func TestPanicRecoveryRecurring(t *testing.T) {
	s := New()
	defer func() { _ = s.Shutdown(context.Background()) }()

	var count atomic.Int32
	s.Every(context.Background(), 20*time.Millisecond, func(ctx context.Context) error {
		count.Add(1)
		if count.Load() == 1 {
			panic("first run fails")
		}
		return nil
	})

	waitForResult(t, time.Second, func() bool { return count.Load() >= 3 })
}

// --------------------------------------------------------------------------- //
// Retry / backoff                                                             //
// --------------------------------------------------------------------------- //

func TestRetry(t *testing.T) {
	s := New()
	defer func() { _ = s.Shutdown(context.Background()) }()

	var attempts atomic.Int32
	s.After(context.Background(), 0, func(ctx context.Context) error {
		n := attempts.Add(1)
		if n < 3 {
			return errors.New("transient")
		}
		return nil
	}, WithRetry(RetryPolicy{
		MaxAttempts:  5,
		InitialDelay: 5 * time.Millisecond,
		MaxDelay:     20 * time.Millisecond,
		Multiplier:   2,
	}))

	waitForResult(t, 2*time.Second, func() bool { return attempts.Load() == 3 })
}

func TestRetryExhausted(t *testing.T) {
	var errMu sync.Mutex
	var gotErr error

	s := New(WithErrorHandler(func(_ string, err error) {
		errMu.Lock()
		defer errMu.Unlock()
		gotErr = err
	}))
	defer func() { _ = s.Shutdown(context.Background()) }()

	var attempts atomic.Int32
	s.After(context.Background(), 0, func(ctx context.Context) error {
		attempts.Add(1)
		return errors.New("always fails")
	}, WithRetry(RetryPolicy{
		MaxAttempts:  3,
		InitialDelay: 5 * time.Millisecond,
		MaxDelay:     10 * time.Millisecond,
		Multiplier:   2,
	}))

	waitForResult(t, 2*time.Second, func() bool { return attempts.Load() == 3 })
	waitForResult(t, time.Second, func() bool {
		errMu.Lock()
		defer errMu.Unlock()
		return gotErr != nil
	})
}

func TestNoRetryByDefault(t *testing.T) {
	s := New()
	defer func() { _ = s.Shutdown(context.Background()) }()

	var attempts atomic.Int32
	s.After(context.Background(), 0, func(ctx context.Context) error {
		attempts.Add(1)
		return errors.New("fail")
	})

	waitForResult(t, 500*time.Millisecond, func() bool { return attempts.Load() == 1 })
	time.Sleep(100 * time.Millisecond)
	if attempts.Load() != 1 {
		t.Fatalf("expected no retry, got %d attempts", attempts.Load())
	}
}

// --------------------------------------------------------------------------- //
// Bounded concurrency                                                         //
// --------------------------------------------------------------------------- //

func TestBoundedConcurrency(t *testing.T) {
	s := New(WithMaxConcurrency(2))
	defer func() { _ = s.Shutdown(context.Background()) }()

	var active, maxActive atomic.Int32

	run := func(ctx context.Context) error {
		cur := active.Add(1)
		for {
			old := maxActive.Load()
			if cur <= old || maxActive.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		active.Add(-1)
		return nil
	}

	for range 6 {
		s.After(context.Background(), 0, run)
	}

	waitForResult(t, 3*time.Second, func() bool { return active.Load() == 0 && maxActive.Load() > 0 })
	if maxActive.Load() > 2 {
		t.Fatalf("concurrency exceeded limit: max=%d (limit=2)", maxActive.Load())
	}
}

// --------------------------------------------------------------------------- //
// Context cancellation                                                        //
// --------------------------------------------------------------------------- //

func TestContextCancellation(t *testing.T) {
	s := New()
	defer func() { _ = s.Shutdown(context.Background()) }()

	parentCtx, parentCancel := context.WithCancel(context.Background())

	var ran atomic.Bool
	s.After(parentCtx, 50*time.Millisecond, func(ctx context.Context) error {
		ran.Store(true)
		return nil
	})

	parentCancel()
	time.Sleep(150 * time.Millisecond)
	if ran.Load() {
		t.Fatal("job ran despite parent context cancellation")
	}
}

// --------------------------------------------------------------------------- //
// Misc                                                                        //
// --------------------------------------------------------------------------- //

func TestNilHandler(t *testing.T) {
	s := New()
	defer func() { _ = s.Shutdown(context.Background()) }()

	if id := s.After(context.Background(), 0, nil); id != "" {
		t.Fatal("expected empty ID for nil handler")
	}
}

func TestEveryZeroInterval(t *testing.T) {
	s := New()
	defer func() { _ = s.Shutdown(context.Background()) }()

	if id := s.Every(context.Background(), 0, func(ctx context.Context) error { return nil }); id != "" {
		t.Fatal("expected empty ID for zero interval")
	}
}

func TestPending(t *testing.T) {
	s := New()
	defer func() { _ = s.Shutdown(context.Background()) }()

	id1 := s.After(context.Background(), time.Hour, func(ctx context.Context) error { return nil })
	id2 := s.Every(context.Background(), time.Hour, func(ctx context.Context) error { return nil })

	pending := s.Pending()
	if len(pending) != 2 {
		t.Fatalf("expected 2 pending jobs, got %d", len(pending))
	}

	s.Cancel(id1)
	pending = s.Pending()
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending job after cancel, got %d", len(pending))
	}
	if pending[0] != id2 {
		t.Fatalf("expected remaining job %s, got %s", id2, pending[0])
	}
}
