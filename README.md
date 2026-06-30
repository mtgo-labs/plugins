# mtgo plugins

Official plugin collection for [mtgo](https://github.com/mtgo-labs/mtgo) — a Go MTProto client for Telegram bots and userbots.

## Available Plugins

| Plugin | Description |
|--------|-------------|
| [**conversations**](./conversations) | Multi-step conversation state management with persistent storage |
| [**i18n**](./i18n) | Internationalization with YAML/FTL formats and 25+ plural rules |
| [**scheduler**](./scheduler) | In-memory job scheduler with retry/backoff, bounded concurrency, and graceful shutdown |
| [**fsm**](./fsm) | Finite-state-machine and key-value state management with TTL, scoping, and persistent storage |
| [**throttle**](./throttle) | Local anti-spam rate limiting with global, per-chat, per-user, per-method, and custom-key scopes |
| [**updatesrecovery**](./updatesrecovery) | Persist and restore Telegram update state (pts/qts/date/seq) with gap detection and `getDifference` recovery |
| [**rawdebug**](./rawdebug) | Optional raw MTProto traffic inspector for development and debugging (RPC, updates, errors, redaction) |

## Usage

Plugins implement the `tg.Plugin` interface and are registered with `client.Use()`:

```go
client, _ := tg.NewClient(apiID, apiHash, &tg.Config{
    BotToken:    botToken,
    SessionName: "my_bot",
})

client.Use(myPlugin)
```

Multiple plugins can be chained — they form a middleware stack where each plugin intercepts updates before passing them along.

## Writing a Plugin

A plugin is any type that implements the `Plugin` interface:

```go
type Plugin interface {
    Name() string
    HandleUpdate(ctx *Context, update tg.TLObject) error
}
```

Register handlers inside `HandleUpdate`, inspect updates, and call `ctx.Next()` to pass control to the next plugin or handler in the chain.

## License

MIT
