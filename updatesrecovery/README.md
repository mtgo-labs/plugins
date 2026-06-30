# updatesrecovery

Updates recovery plugin for [mtgo](https://github.com/mtgo-labs/mtgo) — persists and restores Telegram update state (pts, qts, date, seq) across restarts, reconnects, and network failures.

## Why a plugin?

mtgo's core stays lightweight: it receives updates and dispatches them to handlers, nothing more. Update recovery — state persistence, gap detection, and `updates.getDifference` calls — is **opt-in** via this plugin. Users who do not need durable update tracking pay zero cost.

This is **not** part of the core by default because:

- **Not every bot needs it.** Webhook-style processors, fire-and-forget bots, and testing harnesses don't require PTS gap recovery.
- **It adds I/O overhead.** State persistence writes to storage on every update (debounced). For high-throughput bots where occasional gaps are acceptable, this is unnecessary.
- **It couples to a storage backend.** The core should not assume any particular storage is available. The plugin takes a `Store` interface, keeping the choice in user hands.

## When to enable

Enable this plugin when your bot **must not miss updates** across:

- **App restart** — state is persisted to storage and restored on startup, then `getDifference` fills any gap.
- **Reconnect** — after a network drop, `getDifference` fetches updates missed while disconnected.
- **Temporary network failure** — brief outages are recovered transparently.
- **Updates gap** — if a PTS gap is detected in the live update stream, `getDifference` is called to fill it (debounced by 500ms by default to avoid unnecessary RPC calls for self-resolving gaps).

## Usage

```go
import (
    "github.com/mtgo-labs/plugins/updatesrecovery"
    "github.com/mtgo-labs/storage/sqlite"
    telegram "github.com/mtgo-labs/mtgo/telegram"
)

store, _ := sqlite.Open("bot.db")
client, _ := telegram.NewClient(apiID, apiHash, &telegram.Config{
    Storage:    store,
    SessionName: "my_bot",
})

// Enable update state persistence and gap recovery.
client.Use(updatesrecovery.New(updatesrecovery.Storage(store, "my_bot")))
```

### Options

```go
// Immediate gap recovery (no debounce).
client.Use(updatesrecovery.New(
    updatesrecovery.Storage(store, "my_bot"),
    updatesrecovery.WithGapBuffer(0),
    updatesrecovery.WithSaveInterval(5 * time.Second),
    updatesrecovery.WithLogger(slog.Default()),
))
```

| Option | Default | Description |
|--------|---------|-------------|
| `WithSaveInterval(d)` | 2s | Debounce interval for persisting state. Set to 0 for immediate saves. |
| `WithGapBuffer(d)` | 500ms | Delay before calling `getDifference` after a gap. Set to 0 for immediate recovery. |
| `WithLogger(l)` | `slog.Default()` | Structured logger for diagnostics. |

### In-memory store (testing / ephemeral)

```go
client.Use(updatesrecovery.New(updatesrecovery.NewMemoryStore()))
```

### Disabled plugin (feature flag)

Pass `nil` to create a no-op plugin — all hooks are safe no-ops:

```go
recoveryEnabled := false
store := updatesrecovery.Storage(dbStore, "bot")
if !recoveryEnabled {
    store = nil
}
client.Use(updatesrecovery.New(store))
```

## How it works

```
┌──────────────────────────────────────────────────────────┐
│  mtgo core (telegram.Client)                             │
│                                                          │
│  Session ──► processRawUpdate ──► fireUpdateReceived ──┐ │
│                                  (lifecycle hook)       │ │
│                                                       │ │
│  postConnect ──► fireSessionLoaded / fireConnected    │ │
│  reconnectOnce ──► fireReconnect                      │ │
└───────────────────────────────────────────────────────┼─┘
                                                         │
┌────────────────────────────────────────────────────────▼──┐
│  updatesrecovery.Plugin                                    │
│                                                            │
│  onUpdateReceived:                                         │
│    1. Extract pts/qts/date/seq from batch                  │
│    2. Classify: in-sequence / duplicate / gap              │
│    3. Advance in-memory state (O(1), non-blocking)         │
│    4. Signal debounced save (async)                        │
│    5. If gap → trigger getDifference (async, single-flight)│
│                                                            │
│  onReconnect:                                              │
│    → getDifference to fill gap from disconnect             │
│                                                            │
│  Start:                                                    │
│    → Load saved state from Store                           │
│    → If state exists → initial getDifference (restart)     │
│                                                            │
│  Stop:                                                     │
│    → Flush final state to Store                            │
└────────────────────────────────────────────────────────────┘
```

### Concurrency safety

- State tracking uses `sync.RWMutex` — the update hook takes a read-lock for classification and a write-lock for advancement.
- Persistence is debounced to a background goroutine via a buffered channel — the update hook never blocks on I/O.
- Gap recovery uses `atomic.Bool` single-flight — concurrent gap triggers produce at most one `getDifference` call.

### Storage interface

The plugin defines a minimal `Store` interface:

```go
type Store interface {
    SaveState(state *State) error
    LoadState() (*State, error)
}
```

The `Storage()` adapter wraps any `storage.UpdateStateStore` from `github.com/mtgo-labs/storage` (SQLite, PostgreSQL, MongoDB, or custom). No storage backend is hardcoded.

## Core lifecycle hooks

This plugin uses four lifecycle hooks exposed by the mtgo core:

| Hook | Fires | Plugin use |
|------|-------|------------|
| `OnUpdateReceived` | Every incoming update batch | State tracking + gap detection |
| `OnSessionLoaded` | Session restored from storage | (available for other plugins) |
| `OnConnected` | After connect + auth | (available for other plugins) |
| `OnReconnect` | After successful reconnect | Gap recovery for missed updates |

These hooks are generic — no recovery-specific logic lives in the core.

## License

MIT
