# mtgo plugin: scheduler

In-memory job scheduler for [mtgo](https://github.com/mtgo-labs/mtgo) Telegram bots. Schedule one-time, delayed, and recurring jobs with retry/backoff, bounded concurrency, panic recovery, and graceful shutdown — no external dependencies beyond mtgo.

## Features

- **One-time jobs** — `After` (delay) and `At` (absolute time)
- **Recurring jobs** — `Every` with fixed-delay scheduling (no overlap)
- **Cancellation** — cancel any job by ID
- **Retry & backoff** — configurable exponential backoff per job
- **Bounded concurrency** — semaphore-limited worker pool (default 10)
- **Panic recovery** — panics are caught, logged, and reported to an error handler
- **Context cancellation** — jobs honour their context's cancellation
- **Graceful shutdown** — wait for in-flight jobs to finish, with optional timeout
- **No busy loops** — all scheduling is driven by `time.AfterFunc`
- **Standalone** — works without a Telegram client; the `tg.Plugin` interface is a thin lifecycle wrapper

## Install

```bash
go get github.com/mtgo-labs/plugins/scheduler
```

## Quick start

```go
import (
    tg "github.com/mtgo-labs/mtgo/telegram"
    "github.com/mtgo-labs/plugins/scheduler"
)

func main() {
    client, _ := tg.NewClient(apiID, apiHash, &tg.Config{
        BotToken:    botToken,
        SessionName: "my_bot",
    })

    sched := scheduler.New()
    client.Use(sched) // Start/Stop wired automatically

    // Run once after 30 seconds.
    sched.After(ctx, 30*time.Second, func(ctx context.Context) error {
        return client.SendMessage(chatID, "⏰ Reminder!")
    })

    // Run at a specific wall-clock time.
    sched.At(ctx, time.Date(2025, 12, 31, 23, 59, 0, 0, time.UTC),
        func(ctx context.Context) error {
            return client.SendMessage(chatID, "🎆 Happy New Year!")
        })

    // Poll an API every 5 minutes.
    sched.Every(ctx, 5*time.Minute, func(ctx context.Context) error {
        return pollAndNotify(client)
    })
}
```

## Standalone usage (without mtgo)

```go
sched := scheduler.New()

id := sched.After(context.Background(), 2*time.Second, func(ctx context.Context) error {
    fmt.Println("hello")
    return nil
})

// Later: cancel or shut down.
sched.Cancel(id)
_ = sched.Shutdown(context.Background())
```

## API

### `After(ctx, delay, handler, opts...) string`

Schedule `handler` to run once after `delay`. Returns a job ID for cancellation.

```go
id := sched.After(ctx, 10*time.Second, func(ctx context.Context) error {
    return doWork(ctx)
})
```

### `At(ctx, time, handler, opts...) string`

Schedule `handler` to run at the given `time.Time`. A time in the past fires immediately.

```go
sched.At(ctx, time.Now().Add(1*time.Hour), func(ctx context.Context) error {
    return doWork(ctx)
})
```

### `Every(ctx, interval, handler, opts...) string`

Schedule `handler` to run repeatedly at `interval`. The first execution occurs after one interval; subsequent executions are scheduled only after the previous handler returns (fixed-delay). An interval ≤ 0 is rejected (returns `""`).

```go
sched.Every(ctx, 30*time.Second, func(ctx context.Context) error {
    return heartbeat(ctx)
})
```

### `Cancel(jobID) bool`

Cancel the job with the given ID. Returns `true` if the job was found and cancelled. If the handler is currently running it is allowed to finish, but its context is cancelled.

```go
if sched.Cancel(id) {
    fmt.Println("cancelled")
}
```

### `Shutdown(ctx) error`

Stop all timers, cancel all job contexts, and wait for in-flight handlers to complete. Respects the context deadline. Calling `Shutdown` twice returns `ErrAlreadyShutdown`.

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := sched.Shutdown(ctx); err != nil {
    log.Printf("shutdown: %v", err)
}
```

## Options

### Scheduler options

```go
sched := scheduler.New(
    scheduler.WithMaxConcurrency(4),
    scheduler.WithErrorHandler(func(jobID string, err error) {
        log.Printf("job %s failed: %v", jobID, err)
    }),
    scheduler.WithLogger(slog.With("component", "scheduler")),
)
```

| Option | Description | Default |
|--------|-------------|---------|
| `WithMaxConcurrency(n)` | Max simultaneous job executions | `10` |
| `WithErrorHandler(fn)` | Called when a job fails after retries | logs via slog |
| `WithLogger(l)` | slog logger for internal diagnostics | `slog.Default()` |

### Job options

```go
id := sched.After(ctx, 0, func(ctx context.Context) error {
    return callExternalAPI(ctx)
}, scheduler.WithRetry(scheduler.RetryPolicy{
    MaxAttempts:  5,
    InitialDelay: 100 * time.Millisecond,
    MaxDelay:     5 * time.Second,
    Multiplier:   2.0,
}))
```

| Option | Description |
|--------|-------------|
| `WithRetry(policy)` | Retry on error with exponential backoff |

When no `WithRetry` is configured, jobs run exactly once and errors are reported via the error handler.

## Design notes

- **No busy loops.** Every job uses a `time.Timer` via `time.AfterFunc`. There is no polling goroutine.
- **Fixed-delay recurring jobs.** `Every` schedules the next execution *after* the handler returns, so a slow handler never causes overlapping executions.
- **Bounded concurrency.** A buffered channel semaphore limits simultaneous executions. If all slots are occupied, a job's timer callback blocks until a slot frees — providing natural backpressure.
- **Graceful shutdown.** `Shutdown` stops timers, cancels all job contexts, and waits for in-flight handlers. The `WaitGroup` ensures no handler is abandoned.

## License

MIT
