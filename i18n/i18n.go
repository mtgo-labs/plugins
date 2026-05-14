// Package i18n provides a plugin for internationalizing Telegram bots built on
// the mtgo framework.
//
// It loads locale files (YAML or Fluent/FTL format) from an embed.FS, negotiates
// the user's language from their Telegram client settings or a custom
// LocaleNegotiator, and injects a translation function into every handler context
// via PluginData.
//
// The translator supports pluralization with language-aware rules (CLDR-based),
// gender-aware message variants, and global template context. A session store
// can be plugged in to persist per-user language preferences.
//
// Basic usage:
//
//	tr, err := i18n.NewTranslator(&i18n.Config{
//	    DefaultLang: language.English,
//	    Format:      i18n.FormatYAML,
//	    EmbedFS:     localeFS,
//	    LocaleDir:   "locales",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	client.Use(tr)
//
// Translating inside a handler:
//
//	_ = ctx.PluginData["_t"].(types.TranslatorFunc)
//	text := tr.T(language.English, "welcome", "World")
//	text := tr.Translate(ctx, "greeting")
package i18n

import (
	"context"
	"embed"
	"fmt"
	
	"sort"
	"strings"
	"sync"

	tg "github.com/mtgo-labs/mtgo/telegram"
	"github.com/mtgo-labs/mtgo/telegram/types"
	"golang.org/x/text/language"
)

// LocaleFormat specifies the file format of locale files.
type LocaleFormat string

const (
	// FormatYAML indicates locale files are in YAML format.
	FormatYAML LocaleFormat = "yaml"
	// FormatFTL indicates locale files are in Fluent (FTL) format.
	FormatFTL LocaleFormat = "ftl"
)

// Locale holds all translated messages for a single language.
type Locale struct {
	Lang     language.Tag
	Messages map[string]*Message
}

// Message represents a single translatable string, optionally with plural or
// gender variants.
type Message struct {
	Key      string
	Value    string
	Variants map[string]string
}

// Args carries template variables for translation calls, including count
// (for pluralization), gender (for gendered variants), and named parameters.
type Args struct {
	Count   int
	Gender  string
	Params  map[string]any
}

// LocaleNegotiator determines the language tag to use for a given handler
// context. It is consulted before session and Telegram client settings.
type LocaleNegotiator func(ctx *tg.Context) language.Tag

// Translator is the core i18n engine. It implements tg.Plugin to inject a
// translation function into every handler context.
type Translator struct {
	mu            sync.RWMutex
	locales       map[language.Tag]*Locale
	defaultLang   language.Tag
	format        LocaleFormat
	pluralizer    *Pluralizer
	genderContext genderCache
	client        *tg.Client
	negotiator    LocaleNegotiator
	session       SessionStore
	globalCtx     GlobalContextFunc
	started       bool
}

// SessionStore persists per-user language preferences across bot restarts.
type SessionStore interface {
	GetLocale(userID int64) string
	SetLocale(userID int64, locale string) error
}

// GlobalContextFunc returns extra template variables that are merged into every
// translation call. Use it for values that should be available in all messages
// (e.g. bot name, support URL).
type GlobalContextFunc func(ctx *tg.Context) map[string]any

// Config holds the translator configuration passed to NewTranslator.
type Config struct {
	DefaultLang    language.Tag
	Format         LocaleFormat
	EmbedFS        embed.FS
	LocaleDir      string
	SupportedLangs []language.Tag
	GlobalContext  GlobalContextFunc
}

// genderCache stores per-user gender strings with bounded size and LRU eviction.
type genderCache struct {
	mu    sync.Mutex
	data  map[int64]genderEntry
	order []int64 // tracks insertion order for LRU eviction
}

type genderEntry struct {
	gender string
	index  int // position in order slice
}

const maxGenderCacheSize = 10000

func newGenderCache() genderCache {
	return genderCache{
		data:  make(map[int64]genderEntry),
		order: make([]int64, 0, maxGenderCacheSize),
	}
}

func (c *genderCache) set(userID int64, gender string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ent, ok := c.data[userID]; ok {
		ent.gender = gender
		c.data[userID] = ent
		return
	}

	if len(c.order) >= maxGenderCacheSize {
		// Evict oldest entry (LRU)
		evictID := c.order[0]
		delete(c.data, evictID)
		c.order = c.order[1:]
	}

	c.data[userID] = genderEntry{gender: gender, index: len(c.order)}
	c.order = append(c.order, userID)
}

func (c *genderCache) get(userID int64) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ent, ok := c.data[userID]; ok {
		return ent.gender
	}
	return "other"
}

// NewTranslator creates a Translator from config. If EmbedFS and LocaleDir
// are set, locale files are loaded automatically at construction time.
// Returns an error if the configuration is invalid or locale loading fails.
func NewTranslator(config *Config) (*Translator, error) {
	if config == nil {
		return nil, fmt.Errorf("i18n: config is nil")
	}
	if config.DefaultLang == (language.Tag{}) {
		return nil, fmt.Errorf("i18n: DefaultLang is required")
	}

	t := &Translator{
		locales:       make(map[language.Tag]*Locale),
		defaultLang:   config.DefaultLang,
		format:        config.Format,
		pluralizer:    NewPluralizer(),
		genderContext: newGenderCache(),
		globalCtx:     config.GlobalContext,
	}

	if t.format == "" {
		t.format = FormatYAML
	}

	if config.EmbedFS != (embed.FS{}) {
		t.loadEmbeddedLocales(config.EmbedFS, config.LocaleDir, config.SupportedLangs)
	}

	return t, nil
}

func (t *Translator) Name() string {
	return "i18n"
}

func (t *Translator) Start(ctx context.Context, client *tg.Client) error {
	t.client = client
	client.UseMiddleware(func(next tg.Handler) tg.Handler {
		return &i18nMiddleware{inner: next, tr: t}
	})
	t.started = true
	return nil
}

func (t *Translator) Stop(ctx context.Context) error {
	return nil
}

// WithNegotiator sets a custom locale negotiation function. Must be called
// before Start. Panics if called after the plugin has started.
func (t *Translator) WithNegotiator(fn LocaleNegotiator) *Translator {
	if t.started {
		panic("i18n: WithNegotiator must be called before Start")
	}
	t.negotiator = fn
	return t
}

// WithSession sets the session store for persisting per-user language
// preferences. Must be called before Start.
func (t *Translator) WithSession(store SessionStore) *Translator {
	if t.started {
		panic("i18n: WithSession must be called before Start")
	}
	t.session = store
	return t
}

// WithGlobalContext sets a function that provides extra template variables
// available in all translations. Must be called before Start.
func (t *Translator) WithGlobalContext(fn GlobalContextFunc) *Translator {
	if t.started {
		panic("i18n: WithGlobalContext must be called before Start")
	}
	t.globalCtx = fn
	return t
}

type i18nMiddleware struct {
	inner tg.Handler
	tr    *Translator
}

func (m *i18nMiddleware) Check(update *tg.Update) bool {
	return m.inner.Check(update)
}

func (m *i18nMiddleware) Handle(ctx *tg.Context) {
	tfn := types.TranslatorFunc(func(key string, args ...any) string {
		return m.tr.Translate(ctx, key, args...)
	})
	if ctx.PluginData == nil {
		ctx.PluginData = make(map[string]interface{})
	}
	ctx.PluginData["_t"] = tfn
	if ctx.Message != nil {
		ctx.Message.SetTranslator(tfn)
	}
	if ctx.EditedMessage != nil {
		ctx.EditedMessage.SetTranslator(tfn)
	}
	m.inner.Handle(ctx)
}

// T translates key into the given locale using positional args ({0}, {1}, …).
// Returns the key unchanged if no message is found.
func (t *Translator) T(locale language.Tag, key string, args ...any) string {
	t.mu.RLock()
	locale = t.resolveLocaleTag(locale)
	msg := t.getMessage(locale, key)
	t.mu.RUnlock()

	if msg == nil {
		return key
	}
	return t.formatSimple(msg.Value, args...)
}

// Tctx translates key into the given locale using an Args struct for
// pluralization and named parameters.
func (t *Translator) Tctx(locale language.Tag, key string, ctx *Args) string {
	t.mu.RLock()
	locale = t.resolveLocaleTag(locale)
	msg := t.getMessage(locale, key)
	t.mu.RUnlock()

	if msg == nil {
		return key
	}
	userID := int64(0)
	return t.formatMessage(msg, locale, userID, ctx)
}

// Translate resolves the locale from ctx and translates key. Supports both
// positional args and a single *Args for plural/gender-aware formatting.
func (t *Translator) Translate(ctx *tg.Context, key string, args ...any) string {
	locale := t.ResolveLocale(ctx)

	t.mu.RLock()
	locale = t.resolveLocaleTag(locale)
	msg := t.getMessage(locale, key)
	t.mu.RUnlock()

	if msg == nil {
		return key
	}

	// If any arg is *Args and the message has variants, use variant-aware formatting.
	var a *Args
	var simpleArgs []any
	for _, arg := range args {
		if casted, ok := arg.(*Args); ok {
			a = casted
		} else {
			simpleArgs = append(simpleArgs, arg)
		}
	}

	if a != nil && len(msg.Variants) > 0 {
		template := t.formatMessage(msg, locale, t.userID(ctx), a)
		if t.globalCtx != nil {
			for k, v := range t.globalCtx(ctx) {
				template = strings.ReplaceAll(template, "{"+k+"}", fmt.Sprintf("%v", v))
			}
		}
		return template
	}

	template := msg.Value
	// Replace positional args first ({0}, {1}, …), then global context.
	// This prevents collisions where a global key overlaps a positional placeholder.
	for i, arg := range simpleArgs {
		template = strings.ReplaceAll(template, fmt.Sprintf("{%d}", i), fmt.Sprintf("%v", arg))
	}
	globals := make(map[string]any)
	if t.globalCtx != nil {
		for k, v := range t.globalCtx(ctx) {
			globals[k] = v
		}
	}
	for k, v := range globals {
		template = strings.ReplaceAll(template, "{"+k+"}", fmt.Sprintf("%v", v))
	}
	return template
}

// TranslateCtx is a convenience wrapper that resolves the locale and calls Tctx.
func (t *Translator) TranslateCtx(ctx *tg.Context, key string, a *Args) string {
	locale := t.ResolveLocale(ctx)
	return t.Tctx(locale, key, a)
}

// GetLang returns the stored language preference for the given user, or the
// default language if no session store is configured or no preference is set.
func (t *Translator) GetLang(userID int64) language.Tag {
	if t.session != nil {
		if code := t.session.GetLocale(userID); code != "" {
			if lang, err := language.Parse(code); err == nil && t.HasLang(lang) {
				return lang
			}
		}
	}
	return t.defaultLang
}

// SetLang persists a language preference for the given user. Returns an error
// if no session store is configured.
func (t *Translator) SetLang(userID int64, lang language.Tag) error {
	if t.session == nil {
		return fmt.Errorf("i18n: no session store configured")
	}
	return t.session.SetLocale(userID, lang.String())
}

// HasLang reports whether locale data is loaded for the given language.
func (t *Translator) HasLang(lang language.Tag) bool {
	t.mu.RLock()
	_, ok := t.locales[lang]
	t.mu.RUnlock()
	return ok
}

// Locales returns all loaded language tags sorted alphabetically.
func (t *Translator) Locales() []language.Tag {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]language.Tag, 0, len(t.locales))
	for l := range t.locales {
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].String() < out[j].String()
	})
	return out
}

// ResolveLocale determines the best language for the given context by checking
// the custom negotiator, session store, and the user's Telegram client language
// setting (with base-tag fallback), in that order.
func (t *Translator) ResolveLocale(ctx *tg.Context) language.Tag {
	if t.negotiator != nil {
		if lang := t.negotiator(ctx); t.HasLang(lang) {
			return lang
		}
	}
	if t.session != nil {
		if userID := t.userID(ctx); userID != 0 {
			if lang := t.GetLang(userID); t.HasLang(lang) {
				return lang
			}
		}
	}
	if ctx.Update != nil {
		if userID := t.userID(ctx); userID != 0 {
			if u, ok := ctx.Update.Users[userID]; ok && u.Language != "" {
				if parsed, err := language.Parse(u.Language); err == nil && t.HasLang(parsed) {
					return parsed
				}
				base, _ := language.Parse(u.Language)
				b, _ := base.Base()
				baseTag := language.Make(b.String())
				if t.HasLang(baseTag) {
					return baseTag
				}
			}
		}
	}
	return t.defaultLang
}

// Hears returns a filter function that matches when the incoming message text
// equals the translated value of key for the resolved locale. Useful with
// handler filter chains.
func (t *Translator) Hears(key string) func(ctx *tg.Context) bool {
	return func(ctx *tg.Context) bool {
		text := ""
		if ctx.Message != nil {
			text = ctx.Message.Text
		}
		if text == "" {
			return false
		}
		locale := t.ResolveLocale(ctx)
		translated := t.T(locale, key)
		return text == translated
	}
}

// SetGender stores a gender string for the given user, used in gender-aware
// message variant selection. The cache is bounded to 10 000 entries with
// LRU eviction.
func (t *Translator) SetGender(userID int64, gender string) {
	t.genderContext.set(userID, gender)
}

// GetGender returns the stored gender for the given user, or "other" if unset.
func (t *Translator) GetGender(userID int64) string {
	return t.genderContext.get(userID)
}

func (t *Translator) userID(ctx *tg.Context) int64 {
	switch {
	case ctx.Message != nil:
		return ctx.Message.FromID
	case ctx.EditedMessage != nil:
		return ctx.EditedMessage.FromID
	case ctx.CallbackQuery != nil:
		return ctx.CallbackQuery.UserID
	}
	return 0
}

func (t *Translator) resolveLocaleTag(lang language.Tag) language.Tag {
	if _, ok := t.locales[lang]; ok {
		return lang
	}
	base, _ := lang.Base()
	baseTag := language.Make(base.String())
	if _, ok := t.locales[baseTag]; ok {
		return baseTag
	}
	return t.defaultLang
}

func (t *Translator) getMessage(locale language.Tag, key string) *Message {
	loc, ok := t.locales[locale]
	if !ok {
		loc = t.locales[t.defaultLang]
	}
	if loc == nil {
		return nil
	}
	msg, ok := loc.Messages[key]
	if !ok && locale != t.defaultLang {
		if def, ok := t.locales[t.defaultLang]; ok {
			msg = def.Messages[key]
		}
	}
	return msg
}

func (t *Translator) formatSimple(template string, args ...any) string {
	result := template
	for i, arg := range args {
		result = strings.ReplaceAll(result, fmt.Sprintf("{%d}", i), fmt.Sprintf("%v", arg))
	}
	return result
}

func (t *Translator) formatMessage(msg *Message, lang language.Tag, userID int64, ctx *Args) string {
	if len(msg.Variants) > 0 && ctx != nil {
		variantKey := t.selectVariant(lang, userID, ctx)
		if v, ok := msg.Variants[variantKey]; ok {
			return t.applyArgs(v, ctx)
		}
	}
	return t.applyArgs(msg.Value, ctx)
}

func (t *Translator) selectVariant(lang language.Tag, userID int64, ctx *Args) string {
	if ctx.Gender != "" {
		return ctx.Gender
	}
	// Fall back to stored gender for this user.
	gender := t.GetGender(userID)
	if gender != "other" {
		return gender
	}
	return t.pluralizer.GetVariant(lang, ctx.Count)
}

func (t *Translator) applyArgs(template string, ctx *Args) string {
	result := template
	if ctx == nil {
		return result
	}
	if ctx.Params != nil {
		for k, v := range ctx.Params {
			result = strings.ReplaceAll(result, "{"+k+"}", fmt.Sprintf("%v", v))
		}
	}
	if ctx.Count != 0 {
		result = strings.ReplaceAll(result, "{count}", fmt.Sprintf("%d", ctx.Count))
	}
	return result
}

func (t *Translator) getOrCreateLocale(lang language.Tag) *Locale {
	locale, ok := t.locales[lang]
	if !ok {
		locale = &Locale{Lang: lang, Messages: make(map[string]*Message)}
		t.locales[lang] = locale
	}
	return locale
}