// Package fsm provides a finite-state-machine and key-value state plugin for
// [mtgo](https://github.com/mtgo-labs/mtgo) Telegram bots.
//
// It manages per-user, per-chat, or per-chat+user conversation state across
// updates. State is scoped, TTL-aware, and persisted through the client's
// storage adapter (SQLite, PostgreSQL, or MongoDB) with automatic table
// creation. An in-memory store is available as a fallback for testing.
//
// # Basic usage
//
//	fsmPlugin := fsm.New()
//	client.Use(fsmPlugin)
//
//	client.OnMessage(func(ctx *tg.Context) {
//	    st := fsm.State(ctx)
//	    st.Set("step", "waiting_for_name")
//	    ctx.Reply("What is your name?")
//	}, tg.Command("start"))
//
//	client.OnMessage(func(ctx *tg.Context) {
//	    st := fsm.State(ctx)
//	    switch st.Step() {
//	    case "waiting_for_name":
//	        st.Set("name", ctx.Message.Text)
//	        st.SetStep("waiting_for_phone")
//	        ctx.Reply("Now send your phone number.")
//	    case "waiting_for_phone":
//	        st.Set("phone", ctx.Message.Text)
//	        st.Clear("step")
//	        ctx.Reply("Registration completed.")
//	    }
//	})
//
// # State scoping
//
// State is scoped by user, chat, or both. The default is [ScopeChatAndUser],
// which isolates state per (chat, user) pair — ideal for private chats and
// per-user flows in groups.
//
// Use [ScopeUser] for global per-user state (shared across all chats), or
// [ScopeChat] for chat-wide state (shared by all users in a chat, useful for
// admin panels).
//
//	fsm.New(fsm.WithScope(fsm.ScopeUser))
//
// # Storage
//
// By default the plugin auto-detects the client's storage adapter at startup.
// If the adapter implements [storage.StateStore] (all built-in SQLite,
// PostgreSQL, and MongoDB adapters do), state is persisted to the same
// database as sessions and peers. Otherwise an in-memory store is used.
//
// Pass a custom store with [WithStore]:
//
//	fsm.New(fsm.WithStore(fsm.NewMemoryStore()))
package fsm

import "time"

// Scope determines the granularity of state isolation.
type Scope int

const (
	// ScopeChatAndUser isolates state per (chat, user) pair. This is the
	// default and the right choice for most private-chat flows and per-user
	// group interactions.
	ScopeChatAndUser Scope = iota
	// ScopeUser isolates state per user across all chats. Useful for
	// cross-chat user preferences or global registration state.
	ScopeUser
	// ScopeChat isolates state per chat, shared by all users in that chat.
	// Useful for admin panels or chat-wide configuration.
	ScopeChat
)

// Key uniquely identifies a state scope.
type Key struct {
	Scope  Scope
	ChatID int64
	UserID int64
}

// Entry holds a single state value with optional expiration.
type Entry struct {
	Value     any
	ExpiresAt time.Time // zero = never expires
}

// Expired reports whether the entry has passed its TTL.
func (e Entry) Expired() bool {
	return !e.ExpiresAt.IsZero() && time.Now().After(e.ExpiresAt)
}

// Store persists state entries. All methods must be safe for concurrent use.
//
// The in-memory implementation ([MemoryStore]) is provided for testing and
// simple single-process bots. For production use, the client's storage adapter
// is auto-detected at plugin startup.
type Store interface {
	// Set stores entry under the given key and field, overwriting any
	// existing value. A zero Entry.ExpiresAt means the value never expires.
	Set(key Key, field string, entry Entry) error

	// Get retrieves the raw entry for the given key and field. Returns
	// (Entry{}, false, nil) when the field does not exist or has expired.
	Get(key Key, field string) (Entry, bool, error)

	// Delete removes a single field.
	Delete(key Key, field string) error

	// Clear removes all fields for the given key.
	Clear(key Key) error
}

// Cleaner is an optional interface that stores may implement to support
// periodic garbage collection of expired entries. The GC loop calls Cleanup
// at the configured interval; expired entries are also removed lazily on read.
type Cleaner interface {
	Cleanup() int
}

// Reserved field names for step/flow convenience methods.
const (
	fieldStep = "step"
	fieldFlow = "flow"
)
