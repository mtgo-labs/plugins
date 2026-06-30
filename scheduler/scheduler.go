// Package scheduler provides an in-memory job scheduler plugin for
// [mtgo](https://github.com/mtgo-labs/mtgo) Telegram bots.
//
// It supports one-time, delayed, and recurring jobs with optional retry/backoff,
// bounded concurrency, panic recovery, context cancellation, and graceful
// shutdown — all without busy-loops (jobs are driven by [time.AfterFunc]).
//
// # Basic usage
//
//	sched := scheduler.New()
//	client.Use(sched)
//
//	// Run once after 30 seconds.
//	sched.After(ctx, 30*time.Second, sendReminder)
//
//	// Run at a specific time.
//	sched.At(ctx, time.Date(2025, 12, 31, 23, 59, 0, 0, time.UTC), newYearMsg)
//
//	// Run every 5 minutes.
//	sched.Every(ctx, 5*time.Minute, pollAPI)
//
//	// Cancel a scheduled job.
//	id := sched.Every(ctx, time.Minute, heartbeat)
//	sched.Cancel(id)
//
// The scheduler is safe for concurrent use. Jobs run in separate goroutines,
// bounded by a configurable concurrency limit (default 10).
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	tg "github.com/mtgo-labs/mtgo/telegram"
)

// HandlerFunc is the function executed by a scheduled job.
// Returning a non-nil error triggers retry when a RetryPolicy is configured.
type HandlerFunc func(ctx context.Context) error

// RetryPolicy configures exponential backoff for failed jobs.
//
// MaxAttempts is the total number of attempts including the first run.
// A value of 1 means no retries (run once, fail on error).
type RetryPolicy struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}

// DefaultRetry is a sensible retry policy: up to 3 attempts, starting at
// 100ms, doubling each time, capped at 10s.
var DefaultRetry = RetryPolicy{
	MaxAttempts:  3,
	InitialDelay: 100 * time.Millisecond,
	MaxDelay:     10 * time.Second,
	Multiplier:   2.0,
}

// Option configures a [Scheduler] at construction time.
type Option func(*Scheduler)

// WithMaxConcurrency sets the maximum number of jobs that may execute
// simultaneously. Must be greater than 0.
func WithMaxConcurrency(n int) Option {
	return func(s *Scheduler) {
		if n > 0 {
			s.sem = make(chan struct{}, n)
		}
	}
}

// WithErrorHandler installs a callback invoked when a job exhausts all retry
// attempts or completes with an error. Panics are reported here as well.
func WithErrorHandler(fn func(jobID string, err error)) Option {
	return func(s *Scheduler) {
		s.onError = fn
	}
}

// WithLogger sets the slog logger used for internal diagnostics. Defaults to
// [slog.Default].
func WithLogger(l *slog.Logger) Option {
	return func(s *Scheduler) {
		if l != nil {
			s.log = l
		}
	}
}

// JobOption configures an individual job.
type JobOption func(*job)

// WithRetry attaches a RetryPolicy to the job. Without this, jobs run once and
// errors are reported via the error handler (if set) but never retried.
func WithRetry(p RetryPolicy) JobOption {
	return func(j *job) {
		j.retry = &p
	}
}

type job struct {
	id        string
	handler   HandlerFunc
	interval  time.Duration // >0 for recurring; 0 for one-shot
	recurring bool
	retry     *RetryPolicy
	ctx       context.Context
	cancel    context.CancelFunc
	timer     *time.Timer // protected by Scheduler.mu
}

// ErrAlreadyShutdown is returned by [Scheduler.Shutdown] when called more than
// once.
var ErrAlreadyShutdown = fmt.Errorf("scheduler: already shutdown")

// Scheduler is the core job scheduler. It implements [tg.Plugin] so it can be
// registered with client.Use; it also works standalone via [New].
//
// A Scheduler is safe for concurrent use by multiple goroutines.
type Scheduler struct {
	mu      sync.Mutex
	jobs    map[string]*job
	sem     chan struct{}
	wg      sync.WaitGroup
	stopCh  chan struct{}
	closed  atomic.Bool
	onError func(jobID string, err error)
	log     *slog.Logger
	seq     atomic.Uint64
}

// New creates a scheduler with sensible defaults: 10 concurrent jobs and
// slog.Default for logging. Pass [Option] values to customize.
func New(opts ...Option) *Scheduler {
	s := &Scheduler{
		jobs:   make(map[string]*job),
		sem:    make(chan struct{}, 10),
		stopCh: make(chan struct{}),
		log:    slog.Default(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// --- Plugin interface -------------------------------------------------------

func (s *Scheduler) Name() string { return "scheduler" }

// Start satisfies the [tg.Plugin] interface. It is a no-op — the scheduler is
// ready to use immediately after [New].
func (s *Scheduler) Start(_ context.Context, _ *tg.Client) error {
	return nil
}

// Stop satisfies the [tg.Plugin] interface and triggers graceful shutdown.
// It waits up to 10 seconds for running jobs to finish.
func (s *Scheduler) Stop(ctx context.Context) error {
	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return s.Shutdown(shutdownCtx)
}

// --- Scheduling API ---------------------------------------------------------

// After schedules handler to run once after delay. Returns the job ID (usable
// with [Scheduler.Cancel]) or "" if the handler is nil or the scheduler is
// shutting down.
func (s *Scheduler) After(ctx context.Context, delay time.Duration, handler HandlerFunc, opts ...JobOption) string {
	return s.schedule(ctx, delay, false, handler, opts)
}

// At schedules handler to run once at the given time. A time in the past fires
// immediately. Returns the job ID or "" if the handler is nil or the scheduler
// is shutting down.
func (s *Scheduler) At(ctx context.Context, t time.Time, handler HandlerFunc, opts ...JobOption) string {
	return s.schedule(ctx, time.Until(t), false, handler, opts)
}

// Every schedules handler to run repeatedly at the given interval. The first
// execution occurs after one interval; the next is scheduled only after the
// previous handler returns (fixed-delay, no overlap). Returns the job ID or ""
// if the interval is ≤0, the handler is nil, or the scheduler is shutting down.
func (s *Scheduler) Every(ctx context.Context, interval time.Duration, handler HandlerFunc, opts ...JobOption) string {
	if interval <= 0 {
		return ""
	}
	return s.schedule(ctx, interval, true, handler, opts)
}

func (s *Scheduler) schedule(ctx context.Context, firstDelay time.Duration, recurring bool, handler HandlerFunc, opts []JobOption) string {
	if handler == nil {
		return ""
	}

	j := &job{
		id:        s.newID(),
		handler:   handler,
		interval:  firstDelay,
		recurring: recurring,
	}
	for _, opt := range opts {
		opt(j)
	}
	j.ctx, j.cancel = context.WithCancel(ctx)

	delay := max(firstDelay, 0)

	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		j.cancel()
		return ""
	}
	s.jobs[j.id] = j
	// wg.Add must happen under s.mu (before timer creation) to satisfy the
	// WaitGroup happens-before guarantee relative to Shutdown's wg.Wait.
	s.wg.Add(1)
	j.timer = time.AfterFunc(delay, func() { s.execute(j) })
	s.mu.Unlock()

	return j.id
}

// Cancel removes the job with the given ID and cancels its context. If the job
// is currently running it is allowed to finish (the context is cancelled so the
// handler should observe ctx.Done). Returns true if a job was found and
// cancelled.
func (s *Scheduler) Cancel(jobID string) bool {
	s.mu.Lock()
	j, ok := s.jobs[jobID]
	if !ok {
		s.mu.Unlock()
		return false
	}
	delete(s.jobs, jobID)

	var stopped bool
	if j.timer != nil {
		stopped = j.timer.Stop()
		j.timer = nil
	}
	s.mu.Unlock()

	j.cancel()

	// If the timer was stopped before firing, the callback will never run, so
	// we undo the wg.Add from schedule. If it already fired (or is running),
	// execute's defer wg.Done handles it.
	if stopped {
		s.wg.Done()
	}
	return true
}

// Pending returns the IDs of all scheduled but not-yet-cancelled jobs. This
// includes jobs whose handler is currently executing (recurring jobs remain in
// the map between executions).
func (s *Scheduler) Pending() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.jobs))
	for id := range s.jobs {
		ids = append(ids, id)
	}
	return ids
}

// Shutdown stops all timers, cancels all job contexts, and waits for currently
// running jobs to finish. If ctx has a deadline it is respected; otherwise the
// call blocks indefinitely until all jobs complete. Calling Shutdown more than
// once returns [ErrAlreadyShutdown].
func (s *Scheduler) Shutdown(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return ErrAlreadyShutdown
	}

	close(s.stopCh)

	// Stop all timers and cancel contexts under s.mu.
	s.mu.Lock()
	for _, j := range s.jobs {
		if j.timer != nil {
			if j.timer.Stop() {
				s.wg.Done() // timer didn't fire; undo wg.Add
			}
			j.timer = nil
		}
		j.cancel()
	}
	clear(s.jobs)
	s.mu.Unlock()

	// Wait for in-flight handlers, respecting ctx.
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// --- Internals --------------------------------------------------------------

func (s *Scheduler) newID() string {
	return fmt.Sprintf("job-%d", s.seq.Add(1))
}

// execute is the timer callback. It acquires a concurrency slot, runs the
// handler (with retry and panic recovery), and reschedules recurring jobs.
//
// Each execute invocation is paired with exactly one wg.Done (via defer). The
// matching wg.Add was performed by schedule (for the first execution) or by a
// prior execute's reschedule block (for subsequent executions).
func (s *Scheduler) execute(j *job) {
	defer s.wg.Done()

	// Bail out if the scheduler is shutting down or the job's context was
	// cancelled before the timer fired.
	if s.closed.Load() || j.ctx.Err() != nil {
		return
	}

	// Acquire a concurrency slot; bail out on shutdown.
	select {
	case s.sem <- struct{}{}:
	case <-s.stopCh:
		return
	}
	defer func() { <-s.sem }()

	s.runWithRetry(j)

	// Reschedule recurring jobs.
	if j.recurring && j.ctx.Err() == nil {
		s.mu.Lock()
		if !s.closed.Load() && s.jobs[j.id] == j {
			s.wg.Add(1) // count the next timer
			j.timer = time.AfterFunc(j.interval, func() { s.execute(j) })
		}
		s.mu.Unlock()
	}
}

func (s *Scheduler) runWithRetry(j *job) {
	ctx := j.ctx
	var attempt int
	for {
		attempt++
		err := s.runProtected(ctx, j.handler)
		if err == nil {
			return
		}

		maxAttempts := 1
		if j.retry != nil && j.retry.MaxAttempts > 0 {
			maxAttempts = j.retry.MaxAttempts
		}
		if attempt >= maxAttempts {
			s.reportError(j.id, err)
			return
		}

		delay := backoffDelay(j.retry, attempt)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		}
	}
}

func (s *Scheduler) runProtected(ctx context.Context, handler HandlerFunc) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("scheduler: job panic recovered",
				"panic", r,
				"stack", string(debug.Stack()))
			err = fmt.Errorf("scheduler: panic: %v", r)
		}
	}()
	return handler(ctx)
}

func (s *Scheduler) reportError(jobID string, err error) {
	if s.onError != nil {
		s.onError(jobID, err)
		return
	}
	s.log.Error("scheduler: job failed", "job", jobID, "err", err)
}

func backoffDelay(p *RetryPolicy, attempt int) time.Duration {
	if p == nil || p.InitialDelay <= 0 {
		return 100 * time.Millisecond
	}
	delay := p.InitialDelay
	for i := 1; i < attempt; i++ {
		delay = time.Duration(float64(delay) * p.Multiplier)
	}
	if p.MaxDelay > 0 && delay > p.MaxDelay {
		delay = p.MaxDelay
	}
	if delay < 0 {
		delay = p.MaxDelay
	}
	return delay
}
