# mtgo i18n plugin

Internationalization plugin for [mtgo](https://github.com/mtgo-labs/mtgo), modeled after [@grammyjs/i18n](https://grammy.dev/plugins/i18n). Supports YAML and FTL formats with built-in plural rules for 25+ languages (extensible via `AddRule`).

## Install

```bash
go get github.com/mtgo-labs/plugins/i18n
```

## Usage

```go
import (
    tg "github.com/mtgo-labs/mtgo/telegram"
    "github.com/mtgo-labs/plugins/i18n"
    "golang.org/x/text/language"
)

func main() {
    client, _ := tg.NewClient(apiID, apiHash, &tg.Config{
        BotToken:    botToken,
        SessionName: "i18n_bot",
    })

    tr := i18n.NewTranslator(&i18n.Config{
        DefaultLang: language.English,
        Format:      i18n.FormatYAML,
        EmbedFS:     locales,
        LocaleDir:   "locales",
    })

    client.Use(tr)

    client.OnMessage(func(ctx *tg.Context) {
        // context-aware translation — resolves locale from user automatically
        ctx.Message.Reply(tr.Translate(ctx, "welcome", "World"))
    }, tg.Command("start"))

    // use hears filter for localized keyboard text
    client.OnMessage(func(ctx *tg.Context) {
        ctx.Message.Reply(tr.Translate(ctx, "menu_pressed"))
    }, tg.Create(tr.Hears("menu_btn")))
}
```

## API

| Method | Description |
|--------|-------------|
| `tr.Translate(ctx, key, args...)` | Translate with auto locale resolution (supports `*i18n.Args` for pluralization) |
| `tr.T(locale, key, args...)` | Translate for a specific locale |
| `tr.Tctx(locale, key, &i18n.Args{Count: 5})` | Translate with pluralization/gender |
| `tr.Hears("key")` | Filter matching translated text (for keyboards) |
| `tr.ResolveLocale(ctx)` | Get resolved locale for context |
| `tr.SetLang(userID, lang)` | Persist locale via SessionStore |
| `tr.SetGender(userID, gender)` | Set gender for variant selection |

## Locale Resolution

1. Custom negotiator (`WithNegotiator`)
2. Session store (`WithSession`)
3. `user.Language` from Telegram
4. `DefaultLang` fallback

## YAML Format

```yaml
welcome: "Hello, {0}!"
items:
  one: "You have {count} item."
  other: "You have {count} items."
menu:
  home: "Home"
  settings: "Settings"
```


## FTL Format

The FTL parser supports a subset of [Fluent](https://projectfluent.org/): simple `key = value` pairs with multiline continuations. Variables (`{ $var }`), selectors, and terms are not yet supported.

```ftl
welcome = Hello, {0}!
menu_btn = Menu
```
## License

MIT
