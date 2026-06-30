package fsm

import (
	"fmt"
	"time"
)

// State is the per-scope state handle attached to handler contexts by the
// plugin middleware. It provides typed accessors for key-value state, step/flow
// tracking, and TTL-aware expiration.
//
// Obtain it inside a handler via [State] (the package-level function) or create
// one directly with [ForContext] for manual scope control or testing.
type State struct {
	store Store
	key   Key
}

// ForContext creates a State handle for the given store, scope, and IDs.
// Use this in tests or when you want manual scope control without the plugin
// middleware.
func ForContext(store Store, scope Scope, chatID, userID int64) *State {
	return &State{
		store: store,
		key:   normalizeKey(Key{Scope: scope, ChatID: chatID, UserID: userID}),
	}
}

// normalizeKey zeroes the ID that is irrelevant for the given scope so that
// entries are shared correctly:
//   - ScopeUser: ChatID is ignored (state shared across all chats for a user).
//   - ScopeChat: UserID is ignored (state shared across all users in a chat).
//   - ScopeChatAndUser: both IDs are used (per chat+user isolation).
func normalizeKey(k Key) Key {
	switch k.Scope {
	case ScopeUser:
		k.ChatID = 0
	case ScopeChat:
		k.UserID = 0
	}
	return k
}

// Set stores value under field with no expiration.
func (s *State) Set(field string, value any) {
	_ = s.store.Set(s.key, field, Entry{Value: value})
}

// SetWithTTL stores value under field, expiring after ttl.
func (s *State) SetWithTTL(field string, value any, ttl time.Duration) {
	_ = s.store.Set(s.key, field, Entry{
		Value:     value,
		ExpiresAt: time.Now().Add(ttl),
	})
}

// Get returns the value stored under field, or nil if the field does not
// exist or has expired.
func (s *State) Get(field string) any {
	e, ok := s.get(field)
	if !ok {
		return nil
	}
	return e.Value
}

// GetString returns the value as a string. Returns "" if not found, expired,
// or not convertible.
func (s *State) GetString(field string) string {
	v := s.Get(field)
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case fmt.Stringer:
		return val.String()
	default:
		return fmt.Sprint(val)
	}
}

// GetInt returns the value as an int. Returns 0 if not found, expired, or not
// convertible.
func (s *State) GetInt(field string) int {
	v := s.Get(field)
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int:
		return val
	case int32:
		return int(val)
	case int64:
		return int(val)
	case float64:
		return int(val)
	default:
		return 0
	}
}

// Has reports whether field exists and has not expired.
func (s *State) Has(field string) bool {
	_, ok := s.get(field)
	return ok
}

// Expired reports whether field exists but has passed its TTL. Returns false
// if the field was never set, has no TTL, or is still valid.
func (s *State) Expired(field string) bool {
	e, ok := s.raw(field)
	return ok && e.Expired()
}

// Delete removes a single field. It is a no-op if the field does not exist.
func (s *State) Delete(field string) {
	_ = s.store.Delete(s.key, field)
}

// Clear removes a single field. It is an alias for [State.Delete].
func (s *State) Clear(field string) {
	_ = s.store.Delete(s.key, field)
}

// ClearAll removes all fields for this state's scope.
func (s *State) ClearAll() {
	_ = s.store.Clear(s.key)
}

// --- Step / Flow convenience ---

// SetStep stores the current step/flow name under the reserved "step" field.
func (s *State) SetStep(step string) {
	s.Set(fieldStep, step)
}

// Step returns the current step, or "" if none is set.
func (s *State) Step() string {
	return s.GetString(fieldStep)
}

// SetFlow stores the current flow name under the reserved "flow" field.
func (s *State) SetFlow(flow string) {
	s.Set(fieldFlow, flow)
}

// Flow returns the current flow, or "" if none is set.
func (s *State) Flow() string {
	return s.GetString(fieldFlow)
}

// --- internal helpers ---

// get returns the entry if it exists and has not expired. Expired entries are
// treated as not-found but NOT lazily deleted — the GC loop reclaims them.
func (s *State) get(field string) (Entry, bool) {
	e, ok, err := s.store.Get(s.key, field)
	if err != nil || !ok {
		return Entry{}, false
	}
	if e.Expired() {
		return Entry{}, false
	}
	return e, true
}

// raw returns the stored entry without checking expiry. Used by Expired().
func (s *State) raw(field string) (Entry, bool) {
	e, ok, err := s.store.Get(s.key, field)
	if err != nil || !ok {
		return Entry{}, false
	}
	return e, true
}
