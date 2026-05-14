# mtgo plugin: conversations

Multi-step conversation management for [mtgo](https://github.com/mtgo-labs/mtgo) Telegram bots. Track per-user state across multiple messages with a simple callback-based API, inspired by [python-telegram-bot](https://github.com/python-telegram-bot/python-telegram-bot)'s ConversationHandler.

## Features

- **Named conversations** — register multiple independent conversation flows
- **Per-user state** — tracks active conversations by chat+user pair
- **Wait helpers** — `Wait()`, `WaitMessage()`, `WaitCallback()`, `WaitUntil()`, `WaitFor()`
- **Persistent state** — survive restarts via storage backends (SQLite, PostgreSQL, MongoDB)
- **In-memory fallback** — built-in `MemoryStore` for simple bots
- **Auto-restore** — resumes active conversations after client restart
- **`/cancel` support** — users can exit conversations at any time
- **Data persistence** — attach arbitrary JSON data to conversation state with `SetData`/`GetData`

## Install

```bash
go get github.com/mtgo-labs/plugins/conversations
```

## Usage

```go
import (
    tg "github.com/mtgo-labs/mtgo/telegram"
    "github.com/mtgo-labs/plugins/conversations"
)

func main() {
    client, _ := tg.NewClient(apiID, apiHash, &tg.Config{
        BotToken:    botToken,
        SessionName: "my_bot",
    })

    p := conversations.New()

    p.Register("signup", func(cc *conversations.ConversationContext) error {
        cc.SendMessage("What is your name?")

        ctx, err := cc.WaitMessage()
        if err != nil {
            return err
        }
        name := ctx.Message.Text

        cc.SendMessage("What is your email?")
        ctx, err = cc.WaitMessage()
        if err != nil {
            return err
        }
        email := ctx.Message.Text

        cc.SendMessage(fmt.Sprintf("Welcome, %s! (%s)", name, email))
        return nil
    })

    client.Use(p)

    client.OnMessage(func(client *tg.Client, msg *types.Message) {
        p.Enter("signup", ctx)
    }, tg.Command("start"))

    client.Connect(0)
    client.Idle()
}
```

## API

### Plugin

| Method | Description |
|--------|-------------|
| `New(store ...ConversationStore)` | Create plugin with optional persistent store |
| `Register(name, ConversationFunc)` | Register a named conversation handler |
| `Enter(name, ctx)` | Start a conversation for the current chat+user |
| `Exit(ctx)` | Cancel the active conversation |
| `Active(chatID, userID)` | Check if a conversation is running |

### ConversationContext

| Method | Description |
|--------|-------------|
| `Wait()` | Block until next update, returns `*tg.Context` |
| `WaitMessage()` | Wait for a text message |
| `WaitCallback()` | Wait for a callback query (inline button press) |
| `WaitUntil(filter)` | Wait until an update matches a filter |
| `WaitFor(filter, timeout)` | Wait with a timeout |
| `SendMessage(text, opts...)` | Send a message to the conversation's chat |
| `SetData(v)` | Persist arbitrary data as JSON |
| `GetData(v)` | Retrieve previously persisted data |
| `Step()` | Get current step number |

### Error Sentinels

| Sentinel | Meaning |
|----------|---------|
| `ErrConversationDone` | Conversation was cancelled (user sent `/cancel` or `Exit` called) |
| `ErrConversationSkip` | Skip the current update without advancing |
| `ErrConversationHalt` | Immediately terminate and remove state |

## Persistent Storage

Pass a `ConversationStore` to `New()` for state that survives restarts:

```go
import (
    "github.com/mtgo-labs/storage/sqlite"
    "github.com/mtgo-labs/plugins/conversations"
)

ext, _ := sqlite.Open("bot.db")
defer ext.Close()

// The plugin auto-detects if the storage adapter implements ConversationStore
// when using storage.NewAdapter(ext). Or wrap explicitly:
p := conversations.New(conversations.Storage(ext))
```

## How It Works

1. `client.Use(p)` registers a handler middleware that intercepts all updates
2. If an update belongs to an active conversation (matched by chat+user), it is routed to the conversation's channel
3. The conversation function receives updates via `Wait`/`WaitMessage`/`WaitCallback` calls
4. `/cancel` at any time exits the active conversation and resumes normal handler dispatch
5. On restart, active conversations are restored from the persistent store

## License

MIT
