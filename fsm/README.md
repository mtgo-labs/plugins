# mtgo plugin: fsm

Finite-state-machine and key-value state management for [mtgo](https://github.com/mtgo-labs/mtgo) Telegram bots. Track per-user, per-chat, or per-chat+user conversation state across updates with TTL support, step/flow tracking, and automatic persistence.

## Features

- **Scoped state** — per chat+user (default), per user, or per chat
- **Key-value API** — `Set`, `Get`, `GetString`, `GetInt`, `Delete`, `Clear`, `ClearAll`
- **Step & Flow** — convenience fields for multi-step form/flow tracking
- **TTL expiration** — `SetWithTTL` with `Has`, `Expired`, and background GC cleanup
- **Persistent by default** — auto-detects the client's storage adapter (SQLite, PostgreSQL, MongoDB)
- **In-memory fallback** — `MemoryStore` for testing and single-process bots
- **Concurrency-safe** — all operations are safe for concurrent use
- **Pluggable storage** — `Store` interface for custom backends

## Install

```bash
go get github.com/mtgo-labs/plugins/fsm
```

## Quick Start

```go
import (
    tg "github.com/mtgo-labs/mtgo/telegram"
    "github.com/mtgo-labs/plugins/fsm"
)

func main() {
    client, _ := tg.NewClient(apiID, apiHash, &tg.Config{
        BotToken:    botToken,
        SessionName: "my_bot",
        Storage:     sqlite.New("bot.db"), // state persists here automatically
    })

    fsmPlugin := fsm.New()
    client.Use(fsmPlugin)

    client.OnMessage(func(ctx *tg.Context) {
        st := fsm.FromContext(ctx)
        st.SetStep("waiting_for_name")
        ctx.Reply("What is your name?")
    }, tg.Command("start"))

    client.OnMessage(func(ctx *tg.Context) {
        st := fsm.FromContext(ctx)
        switch st.Step() {
        case "waiting_for_name":
            st.Set("name", ctx.Message.Text)
            st.SetStep("waiting_for_phone")
            ctx.Reply("Now send your phone number.")
        case "waiting_for_phone":
            st.Set("phone", ctx.Message.Text)
            st.SetStep("")
            ctx.Reply("Registration completed.")
        }
    })

    client.Connect(0)
    client.Idle()
}
```

> **Note:** Use `fsm.FromContext(ctx)` to access the state handle inside handlers. This reads the `*fsm.State` that the plugin middleware attached to the context.

## State Scoping

State is isolated by scope. The default is `ScopeChatAndUser`.

| Scope | Isolation | Use case |
|-------|-----------|----------|
| `ScopeChatAndUser` (default) | Per (chat, user) pair | Private chat flows, per-user group interactions |
| `ScopeUser` | Per user, shared across chats | Global user preferences, cross-chat registration |
| `ScopeChat` | Per chat, shared by all users | Admin panels, chat-wide configuration |

```go
// Per-user state (shared across all chats)
fsm.New(fsm.WithScope(fsm.ScopeUser))

// Chat-wide state (shared by all users in a chat)
fsm.New(fsm.WithScope(fsm.ScopeChat))
```

## API

### State Handle

| Method | Description |
|--------|-------------|
| `Set(key, value)` | Store a value |
| `SetWithTTL(key, value, ttl)` | Store a value with expiration |
| `Get(key)` | Retrieve value (`any`), nil if missing/expired |
| `GetString(key)` | Retrieve as string |
| `GetInt(key)` | Retrieve as int |
| `Has(key)` | Check existence (not expired) |
| `Expired(key)` | Check if TTL has passed |
| `Delete(key)` | Remove a single key |
| `Clear(key)` | Alias for `Delete` |
| `ClearAll()` | Remove all keys for this scope |
| `SetStep(step)` | Set the "step" field |
| `Step()` | Get the "step" field |
| `SetFlow(flow)` | Set the "flow" field |
| `Flow()` | Get the "flow" field |

### Plugin Options

| Option | Description |
|--------|-------------|
| `WithStore(store)` | Use a custom store |
| `WithScope(scope)` | Set default scope |
| `WithLogger(logger)` | Set slog logger |
| `WithGCInterval(d)` | Set GC interval (0 = disable) |

## Examples

### Registration Flow

```go
fsmPlugin := fsm.New()
client.Use(fsmPlugin)

client.OnMessage(func(ctx *tg.Context) {
    st := fsm.FromContext(ctx)
    st.SetFlow("registration")
    st.SetStep("name")
    ctx.Reply("Welcome! What is your name?")
}, tg.Command("register"))

client.OnMessage(func(ctx *tg.Context) {
    st := fsm.FromContext(ctx)
    if st.Flow() != "registration" {
        return
    }
    switch st.Step() {
    case "name":
        st.Set("name", ctx.Message.Text)
        st.SetStep("email")
        ctx.Reply("What is your email?")
    case "email":
        st.Set("email", ctx.Message.Text)
        st.SetStep("confirm")
        ctx.Reply("Confirm? (yes/no)")
    case "confirm":
        if ctx.Message.Text == "yes" {
            name := st.GetString("name")
            email := st.GetString("email")
            st.ClearAll()
            ctx.Reply(fmt.Sprintf("Registered: %s <%s>", name, email))
        } else {
            st.ClearAll()
            ctx.Reply("Registration cancelled.")
        }
    }
})
```

### Order / Payment Flow

```go
client.OnCallbackQuery(func(ctx *tg.Context) {
    st := fsm.FromContext(ctx)
    if ctx.CallbackQuery.Data == "order_start" {
        st.SetFlow("order")
        st.SetStep("awaiting_payment")
        st.SetWithTTL("order_id", generateOrderID(), 10*time.Minute)
        ctx.Reply("Order created. You have 10 minutes to pay.")
    }
})

client.OnMessage(func(ctx *tg.Context) {
    st := fsm.FromContext(ctx)
    if st.Flow() != "order" || st.Step() != "awaiting_payment" {
        return
    }
    if st.Expired("order_id") {
        st.ClearAll()
        ctx.Reply("Order expired. Please start again.")
        return
    }
    // Process payment...
    orderID := st.GetString("order_id")
    st.ClearAll()
    ctx.Reply(fmt.Sprintf("Payment received for order %s!", orderID))
})
```

### Admin Broadcast Flow

```go
// Use chat-wide scope so any admin can contribute to the broadcast draft.
fsmPlugin := fsm.New(fsm.WithScope(fsm.ScopeChat))
client.Use(fsmPlugin)

client.OnMessage(func(ctx *tg.Context) {
    st := fsm.FromContext(ctx)
    st.SetStep("composing")
    st.Set("draft", "")
    ctx.Reply("Send the broadcast message. Use /done when finished, /cancel to abort.")
}, tg.Command("broadcast"), isAdmin)

client.OnMessage(func(ctx *tg.Context) {
    st := fsm.FromContext(ctx)
    if st.Step() != "composing" {
        return
    }
    switch ctx.Message.Text {
    case "/done":
        msg := st.GetString("draft")
        st.ClearAll()
        broadcastToAllUsers(ctx.Client, msg)
    case "/cancel":
        st.ClearAll()
        ctx.Reply("Broadcast cancelled.")
    default:
        draft := st.GetString("draft")
        st.Set("draft", draft+"\n"+ctx.Message.Text)
    }
})
```

### State Expiration

```go
st := fsm.FromContext(ctx)

// Set a value that expires in 5 minutes.
st.SetWithTTL("verification_code", "123456", 5*time.Minute)

// Check later:
if st.Expired("verification_code") {
    ctx.Reply("Code expired. Request a new one.")
    return
}
code := st.GetString("verification_code")
```

The plugin runs a background GC goroutine (default: every 1 minute) that removes expired entries. Configure or disable it:

```go
// Check every 30 seconds.
fsm.New(fsm.WithGCInterval(30 * time.Second))

// Disable GC (expired entries still ignored on read).
fsm.New(fsm.WithGCInterval(0))
```

### Clearing State

```go
st := fsm.FromContext(ctx)

// Remove a single field.
st.Clear("temp_data")

// Remove the step (exit the current flow).
st.Clear("step")

// Remove everything for this scope.
st.ClearAll()
```

## Storage

### Auto-Detection (Default)

The plugin checks the client's storage adapter at startup. All built-in adapters (SQLite, PostgreSQL, MongoDB) implement `storage.StateStore` and persist state to a `plugin_state` table created lazily on first use:

```go
// State persists to bot.db alongside sessions and peers.
client, _ := tg.NewClient(apiID, apiHash, &tg.Config{
    BotToken:    botToken,
    SessionName: "my_bot",
    Storage:     sqlite.New("bot.db"),
})
client.Use(fsm.New())
```

### In-Memory Store

For testing or single-process bots that don't need persistence:

```go
fsm.New(fsm.WithStore(fsm.NewMemoryStore()))
```

### Custom Store

Implement the `Store` interface:

```go
type Store interface {
    Set(key Key, field string, entry Entry) error
    Get(key Key, field string) (Entry, bool, error)
    Delete(key Key, field string) error
    Clear(key Key) error
}
```

Pass it with `WithStore`:

```go
fsm.New(fsm.WithStore(myRedisStore))
```

## How It Works

1. `client.Use(fsmPlugin)` registers a handler middleware
2. The middleware extracts chat/user IDs from each update and creates a scoped `*State` handle
3. The handle is stored in `ctx.PluginData["fsm_state"]`
4. `fsm.FromContext(ctx)` retrieves it in any handler
5. State operations delegate to the store (SQLite/Postgres/MongoDB or in-memory)
6. A background GC goroutine periodically removes expired entries

## License

MIT
