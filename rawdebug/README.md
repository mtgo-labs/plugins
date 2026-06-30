# mtgo plugin: rawdebug

Opt-in plugin for inspecting raw MTProto traffic during local development and debugging of [mtgo](https://github.com/mtgo-labs/mtgo) clients.

It hooks into mtgo's existing observability surfaces — **invoker middleware** (RPC requests, responses, errors), the **update-received hook**, and the **connect/reconnect lifecycle hooks** — and emits structured debug records **without adding any logging to the mtgo core**.

> **⚠️ For local development and debugging only.**
> Do **not** enable this plugin in production unless logs are properly redacted and the output destination is access-controlled. Redaction is on by default, but no automated scrubber is foolproof — treat all output as potentially sensitive.

## Features

- **RPC logging** — method name, request/response type, latency, DC, and a monotonic trace ID correlating request ↔ response
- **Error logging** — RPC error code, type, message, and flood-wait duration
- **Update logging** — resolved update type (e.g. `updateNewMessage`) and DC
- **Transport logging** — connect / reconnect lifecycle events
- **Filtering** — by method name, update type, errors-only, and slow-request threshold
- **Output formats** — human-readable text (default) or JSON Lines (NDJSON)
- **Configurable writer** — stdout, stderr, or any `io.Writer` (default: stderr)
- **Redaction** — auth keys, phone numbers, session strings, and tokens are scrubbed from all output by default
- **Safe by default** — request/response/update bodies are never logged unless explicitly enabled; binary payload logging is off by default

## Install

```bash
go get github.com/mtgo-labs/plugins/rawdebug
```

> **Note:** This plugin depends on the update/transport hooks (`OnUpdateReceived`,
> `OnConnected`, `OnReconnect`) which land in the next mtgo release after v0.11.0.
> The `go.mod` ships a temporary `replace` directive pointing at the local mtgo
> working copy; remove it and bump the require once those hooks are released.

## Quick start

```go
import (
    tg "github.com/mtgo-labs/mtgo/telegram"
    "github.com/mtgo-labs/plugins/rawdebug"
)

func main() {
    client, _ := tg.NewClient(apiID, apiHash, &tg.Config{
        BotToken:    botToken,
        SessionName: "my_bot",
    })

    client.Use(rawdebug.New(rawdebug.Config{
        LogRequests:  true,
        LogResponses: true,
        LogUpdates:   true,
        LogErrors:    true,
        Format:       rawdebug.FormatText,
    }))

    client.Start()
}
```

### Sample output (text)

```
[rawdebug] → #1 help.getConfig req=HelpGetConfigRequest dc=2
[rawdebug] ← #1 help.getConfig resp=Config 42.13ms dc=2
[rawdebug] ✗ #2 messages.sendMessage resp=Updates 18.40ms dc=2 err=400 MESSAGE_TOO_LONG msg="MESSAGE_TOO_LONG"
[rawdebug] ⟳ update=updateNewMessage dc=2
[rawdebug] ⚡ connected dc=2
```

### Sample output (JSON)

Each record is one compact JSON object on its own line:

```json
{"kind":"response","trace":1,"method":"help.getConfig","resp_type":"*tg.Config","dc":2,"duration_ms":42.13}
{"kind":"error","trace":2,"method":"messages.sendMessage","dc":2,"duration_ms":18.4,"err_code":400,"err_type":"FLOOD_WAIT","err_message":"FLOOD_WAIT_60","is_flood":true,"flood_wait_ms":60000}
```

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `LogRequests` | `bool` | `false` | Emit a record before each RPC call (method, type, DC, trace ID) |
| `LogResponses` | `bool` | `false` | Emit a record after each successful RPC (response type, duration) |
| `LogErrors` | `bool` | `false` | Emit a record after each failed RPC (code, type, message) |
| `LogUpdates` | `bool` | `false` | Emit a record for each incoming update batch |
| `LogTransport` | `bool` | `false` | Emit records for connect/reconnect events |
| `Format` | `Format` | `FormatText` | `FormatText` or `FormatJSON` |
| `Writer` | `io.Writer` | `os.Stderr` | Output destination |
| `Methods` | `[]string` | `nil` (all) | RPC method allow-list (qualified names, case-insensitive) |
| `UpdateTypes` | `[]string` | `nil` (all) | Update type allow-list |
| `ErrorsOnly` | `bool` | `false` | Suppress success records; log only failed RPCs |
| `SlowThreshold` | `time.Duration` | `0` (all) | Only log RPCs at least this slow |
| `LogBodies` | `bool` | `false` | **Dangerous.** Log request/response/update bodies as JSON |

### Options

| Option | Effect |
|--------|--------|
| `rawdebug.WithoutRedaction()` | **Dangerous.** Disable scrubbing of secrets. Only for trusted local debugging. |

## Safety model

1. **Bodies are off by default.** Only method names, type names, error info, timing, DC, and trace IDs are logged. These do not carry secrets.
2. **Redaction is on by default.** When bodies *are* enabled (`LogBodies: true`), known-secret patterns are scrubbed:
   - Bot tokens (`id:AA…`)
   - API hashes (32-hex)
   - Auth keys (256+ hex chars)
   - Long opaque base64 blobs (session strings)
   - Phone numbers (`+digits`)
   - JSON fields named `auth_key`, `phone`, `session`, `token`, `api_hash`
3. **Redaction can be disabled** with `WithoutRedaction()` — use this **only** for trusted local debugging.

```go
// Maximum verbosity for local debugging (secrets still scrubbed):
client.Use(rawdebug.New(rawdebug.Config{
    LogRequests:  true,
    LogResponses: true,
    LogErrors:    true,
    LogUpdates:   true,
    LogBodies:    true,
    Format:       rawdebug.FormatJSON,
}))

// Fully unredacted — DANGEROUS, trusted local only:
client.Use(rawdebug.New(rawdebug.Config{
    LogBodies: true,
}, rawdebug.WithoutRedaction()))
```

## What gets logged

| Event | Hook | Fields |
|-------|------|--------|
| RPC request | invoker middleware (before dispatch) | method, request type, DC, trace ID |
| RPC response | invoker middleware (after success) | method, response type, duration, DC, trace ID |
| RPC error | invoker middleware (after failure) | method, error code/type/message, duration, DC, flood wait, trace ID |
| Update | `OnUpdateReceived` | update type, DC |
| Transport | `OnConnected` / `OnReconnect` | event name, DC |

> **Trace IDs** are plugin-internal monotonic counters that correlate a request
> with its response. They are **not** raw MTProto message IDs, which are assigned
> deeper in the session layer and are not exposed at the invoker level.

## Tests

```bash
cd plugins/rawdebug
GOWORK=off go test -race ./...
```

## License

MIT
