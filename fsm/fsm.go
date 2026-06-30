package fsm

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	tg "github.com/mtgo-labs/mtgo/telegram"
	"github.com/mtgo-labs/storage"
)

// Option configures a [Plugin] at construction time.
type Option func(*Plugin)

// WithStore sets a custom store. By default the plugin auto-detects the
// client's storage adapter at startup; use this to override with an in-memory
// store or a custom backend.
func WithStore(s Store) Option {
	return func(p *Plugin) { p.store = s }
}

// WithScope sets the default state scope. The default is [ScopeChatAndUser].
func WithScope(scope Scope) Option {
	return func(p *Plugin) { p.scope = scope }
}

// WithLogger sets the slog logger for internal diagnostics. Defaults to
// [slog.Default].
func WithLogger(l *slog.Logger) Option {
	return func(p *Plugin) {
		if l != nil {
			p.log = l
		}
	}
}

// WithGCInterval sets the interval for periodic expired-entry cleanup.
// A value of 0 disables the background GC goroutine (expired entries are still
// removed lazily on read). The default is 1 minute.
func WithGCInterval(d time.Duration) Option {
	return func(p *Plugin) { p.gcInterval = d }
}

// Plugin provides FSM/state management for mtgo bots. It implements
// [tg.Plugin] and attaches a [State] handle to every handler context via
// middleware.
//
// At startup, the plugin checks whether the client's storage adapter
// implements [storage.StateStore]. If it does, state is persisted to the same
// database as sessions and peers. Otherwise, an in-memory store is used.
type Plugin struct {
	store      Store
	scope      Scope
	log        *slog.Logger
	gcInterval time.Duration
	cancelGC   context.CancelFunc
}

// New creates an FSM plugin. By default it uses [ScopeChatAndUser] and a
// 1-minute GC interval. The store is resolved at [Plugin.Start] from the
// client's adapter unless [WithStore] is provided.
func New(opts ...Option) *Plugin {
	p := &Plugin{
		scope:      ScopeChatAndUser,
		log:        slog.Default(),
		gcInterval: time.Minute,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *Plugin) Name() string { return "fsm" }

// Start registers the middleware and resolves the store from the client's
// adapter if one was not explicitly configured.
func (p *Plugin) Start(ctx context.Context, client *tg.Client) error {
	// Auto-detect store from client adapter.
	if p.store == nil {
		if adapter := client.Adapter(); adapter != nil {
			if ss, ok := adapter.(storage.StateStore); ok {
				p.store = Storage(ss)
			}
		}
	}
	// Fallback to in-memory if no storage adapter is available.
	if p.store == nil {
		p.store = NewMemoryStore()
		p.log.Debug("fsm: no storage adapter detected, using in-memory store")
	}

	client.UseMiddleware(func(next tg.Handler) tg.Handler {
		return &middleware{inner: next, plugin: p}
	})

	// Start background GC for expired entries.
	if p.gcInterval > 0 {
		gcCtx, cancel := context.WithCancel(ctx)
		p.cancelGC = cancel
		go p.gcLoop(gcCtx)
	}

	return nil
}

// Stop cancels the GC goroutine.
func (p *Plugin) Stop(_ context.Context) error {
	if p.cancelGC != nil {
		p.cancelGC()
	}
	return nil
}

func (p *Plugin) gcLoop(ctx context.Context) {
	t := time.NewTicker(p.gcInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if c, ok := p.store.(Cleaner); ok {
				if n := c.Cleanup(); n > 0 {
					p.log.Debug("fsm: cleaned up expired state entries", "removed", n)
				}
			}
		}
	}
}

// resolveKey builds the state Key for the given context using the plugin's
// configured scope.
func (p *Plugin) resolveKey(ctx *tg.Context) Key {
	chatID, userID := extractIDs(ctx)
	return normalizeKey(Key{Scope: p.scope, ChatID: chatID, UserID: userID})
}

// StateFor returns a [State] handle for the given chat and user, bypassing the
// middleware. Useful for sending proactive messages or testing.
func (p *Plugin) StateFor(chatID, userID int64) *State {
	return &State{store: p.store, key: Key{Scope: p.scope, ChatID: chatID, UserID: userID}}
}

// Store returns the underlying store. Exposed for advanced use cases such as
// manual GC or custom queries.
func (p *Plugin) Store() Store { return p.store }

// String returns a human-readable description for diagnostics.
func (p *Plugin) String() string {
	return fmt.Sprintf("fsm.Plugin(scope=%d)", p.scope)
}
