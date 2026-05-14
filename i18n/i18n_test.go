package i18n

import (
	"context"
	"sync"
	"testing"

	tg "github.com/mtgo-labs/mtgo/telegram"
	"github.com/mtgo-labs/mtgo/telegram/types"
	"golang.org/x/text/language"
)

func TestYAMLLoadAndT(t *testing.T) {
	tr := NewTranslator(&Config{
		DefaultLang: language.English,
		Format:      FormatYAML,
	})
	_ = tr.LoadYAML(language.English, []byte(`
welcome: "Hello, {0}!"
help: "Send me a message."
items_count:
  one: "You have {count} item."
  other: "You have {count} items."
greeting: "Welcome, {name}!"
`))
	_ = tr.LoadYAML(language.German, []byte(`
welcome: "Hallo, {0}!"
help: "Schick mir eine Nachricht."
items_count:
  one: "Du hast {count} Element."
  other: "Du hast {count} Elemente."
`))

	if got := tr.T(language.English, "welcome", "World"); got != "Hello, World!" {
		t.Errorf("en welcome = %q", got)
	}
	if got := tr.T(language.German, "welcome", "Welt"); got != "Hallo, Welt!" {
		t.Errorf("de welcome = %q", got)
	}
	if got := tr.T(language.English, "help"); got != "Send me a message." {
		t.Errorf("en help = %q", got)
	}
	if got := tr.T(language.English, "missing"); got != "missing" {
		t.Errorf("missing = %q", got)
	}
	if got := tr.T(language.Chinese, "welcome", "test"); got != "Hello, test!" {
		t.Errorf("fallback = %q", got)
	}
}

func TestPluralization(t *testing.T) {
	tr := NewTranslator(&Config{DefaultLang: language.English, Format: FormatYAML})
	_ = tr.LoadYAML(language.English, []byte(`
items_count:
  one: "You have {count} item."
  other: "You have {count} items."
`))

	if got := tr.Tctx(language.English, "items_count", &Args{Count: 1}); got != "You have 1 item." {
		t.Errorf("one = %q", got)
	}
	if got := tr.Tctx(language.English, "items_count", &Args{Count: 5}); got != "You have 5 items." {
		t.Errorf("other = %q", got)
	}
}

func TestNestedYAML(t *testing.T) {
	tr := NewTranslator(&Config{DefaultLang: language.English, Format: FormatYAML})
	_ = tr.LoadYAML(language.English, []byte(`
menu:
  home: "Home"
  settings: "Settings"
`))

	if got := tr.T(language.English, "menu.home"); got != "Home" {
		t.Errorf("nested = %q", got)
	}
}

func TestResolveLocaleFromUser(t *testing.T) {
	tr := NewTranslator(&Config{DefaultLang: language.English})
	_ = tr.LoadYAML(language.English, []byte(`hi: "Hi"`))
	_ = tr.LoadYAML(language.German, []byte(`hi: "Hallo"`))
	_ = tr.Start(context.Background(), &tg.Client{})

	ctx := &tg.Context{
		Update: &tg.Update{
			Users: map[int64]*types.User{1: {ID: 1, Language: "de"}},
		},
		Message: &types.Message{FromID: 1},
	}
	if got := tr.ResolveLocale(ctx); got != language.German {
		t.Errorf("expected de, got %v", got)
	}
}

func TestResolveLocaleFallback(t *testing.T) {
	tr := NewTranslator(&Config{DefaultLang: language.English})
	_ = tr.LoadYAML(language.English, []byte(`hi: "Hi"`))
	_ = tr.Start(context.Background(), &tg.Client{})

	ctx := &tg.Context{
		Update: &tg.Update{
			Users: map[int64]*types.User{1: {ID: 1, Language: "ja"}},
		},
		Message: &types.Message{FromID: 1},
	}
	if got := tr.ResolveLocale(ctx); got != language.English {
		t.Errorf("expected en fallback, got %v", got)
	}
}

func TestCustomNegotiator(t *testing.T) {
	tr := NewTranslator(&Config{DefaultLang: language.English})
	_ = tr.LoadYAML(language.English, []byte(`hi: "Hi"`))
	_ = tr.LoadYAML(language.French, []byte(`hi: "Salut"`))
	tr.WithNegotiator(func(ctx *tg.Context) language.Tag { return language.French })
	_ = tr.Start(context.Background(), &tg.Client{})

	ctx := &tg.Context{Update: &tg.Update{}}
	if got := tr.ResolveLocale(ctx); got != language.French {
		t.Errorf("expected fr, got %v", got)
	}
}

type mockSession struct {
	mu      sync.Mutex
	locales map[int64]string
}

func (m *mockSession) GetLocale(userID int64) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.locales[userID]
}

func (m *mockSession) SetLocale(userID int64, locale string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.locales[userID] = locale
	return nil
}

func TestSessionStore(t *testing.T) {
	tr := NewTranslator(&Config{DefaultLang: language.English})
	_ = tr.LoadYAML(language.English, []byte(`hi: "Hi"`))
	_ = tr.LoadYAML(language.German, []byte(`hi: "Hallo"`))
	tr.WithSession(&mockSession{locales: map[int64]string{1: "de"}})
	_ = tr.Start(context.Background(), &tg.Client{})

	ctx := &tg.Context{
		Update: &tg.Update{
			Users: map[int64]*types.User{1: {ID: 1, Language: "en"}},
		},
		Message: &types.Message{FromID: 1},
	}
	if got := tr.ResolveLocale(ctx); got != language.German {
		t.Errorf("expected de from session, got %v", got)
	}
}

func TestTranslate(t *testing.T) {
	tr := NewTranslator(&Config{DefaultLang: language.English})
	_ = tr.LoadYAML(language.English, []byte(`hi: "Hi {0}!"`))
	_ = tr.LoadYAML(language.German, []byte(`hi: "Hallo {0}!"`))
	_ = tr.Start(context.Background(), &tg.Client{})

	ctx := &tg.Context{
		Update: &tg.Update{
			Users: map[int64]*types.User{1: {ID: 1, Language: "de"}},
		},
		Message: &types.Message{FromID: 1},
	}
	if got := tr.Translate(ctx, "hi", "World"); got != "Hallo World!" {
		t.Errorf("Translate = %q", got)
	}
}

func TestHears(t *testing.T) {
	tr := NewTranslator(&Config{DefaultLang: language.English})
	_ = tr.LoadYAML(language.English, []byte(`btn: "Submit"`))
	_ = tr.LoadYAML(language.German, []byte(`btn: "Absenden"`))
	_ = tr.Start(context.Background(), &tg.Client{})

	enCtx := &tg.Context{
		Update: &tg.Update{Users: map[int64]*types.User{1: {ID: 1, Language: "en"}}},
		Message: &types.Message{FromID: 1, Text: "Submit"},
	}
	if !tr.Hears("btn")(enCtx) {
		t.Error("should hear 'Submit'")
	}

	deCtx := &tg.Context{
		Update: &tg.Update{Users: map[int64]*types.User{1: {ID: 1, Language: "de"}}},
		Message: &types.Message{FromID: 1, Text: "Absenden"},
	}
	if !tr.Hears("btn")(deCtx) {
		t.Error("should hear 'Absenden'")
	}

	wrongCtx := &tg.Context{
		Update: &tg.Update{Users: map[int64]*types.User{1: {ID: 1, Language: "en"}}},
		Message: &types.Message{FromID: 1, Text: "Other"},
	}
	if tr.Hears("btn")(wrongCtx) {
		t.Error("should not hear 'Other'")
	}
}

func TestPluginInterface(t *testing.T) {
	tr := NewTranslator(&Config{DefaultLang: language.English})
	if tr.Name() != "i18n" {
		t.Errorf("name = %q", tr.Name())
	}
	if err := tr.Start(context.Background(), &tg.Client{}); err != nil {
		t.Fatal(err)
	}
	if err := tr.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}
