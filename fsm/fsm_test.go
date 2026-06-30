package fsm

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tg "github.com/mtgo-labs/mtgo/telegram"
	"github.com/mtgo-labs/mtgo/telegram/types"
)

// helper: create a State backed by a fresh MemoryStore.
func newState(t *testing.T) (*State, *MemoryStore) {
	t.Helper()
	store := NewMemoryStore()
	st := ForContext(store, ScopeChatAndUser, 100, 200)
	return st, store
}

// --------------------------------------------------------------------------- //
// Set / Get / Has                                                              //
// --------------------------------------------------------------------------- //

func TestSetAndGet(t *testing.T) {
	st, _ := newState(t)

	st.Set("name", "Alice")
	st.Set("count", 42)

	if got := st.Get("name"); got != "Alice" {
		t.Fatalf("Get(\"name\") = %v, want Alice", got)
	}
	if got := st.GetInt("count"); got != 42 {
		t.Fatalf("GetInt(\"count\") = %d, want 42", got)
	}
}

func TestGetString(t *testing.T) {
	st, _ := newState(t)

	st.Set("s", "hello")
	st.Set("n", 123)

	if got := st.GetString("s"); got != "hello" {
		t.Fatalf("GetString(\"s\") = %q, want hello", got)
	}
	// Non-string value should be stringified.
	if got := st.GetString("n"); got != "123" {
		t.Fatalf("GetString(\"n\") = %q, want 123", got)
	}
	// Missing key returns "".
	if got := st.GetString("missing"); got != "" {
		t.Fatalf("GetString(\"missing\") = %q, want empty", got)
	}
}

func TestGetInt(t *testing.T) {
	st, _ := newState(t)

	st.Set("i", 99)
	st.Set("i64", int64(77))
	st.Set("f", float64(55))

	if got := st.GetInt("i"); got != 99 {
		t.Fatalf("GetInt(\"i\") = %d, want 99", got)
	}
	if got := st.GetInt("i64"); got != 77 {
		t.Fatalf("GetInt(\"i64\") = %d, want 77", got)
	}
	if got := st.GetInt("f"); got != 55 {
		t.Fatalf("GetInt(\"f\") = %d, want 55", got)
	}
	if got := st.GetInt("missing"); got != 0 {
		t.Fatalf("GetInt(\"missing\") = %d, want 0", got)
	}
}

func TestHas(t *testing.T) {
	st, _ := newState(t)

	if st.Has("x") {
		t.Fatal("Has(\"x\") = true before Set")
	}
	st.Set("x", 1)
	if !st.Has("x") {
		t.Fatal("Has(\"x\") = false after Set")
	}
	st.Delete("x")
	if st.Has("x") {
		t.Fatal("Has(\"x\") = true after Delete")
	}
}

// --------------------------------------------------------------------------- //
// Delete / Clear / ClearAll                                                   //
// --------------------------------------------------------------------------- //

func TestDelete(t *testing.T) {
	st, _ := newState(t)

	st.Set("a", 1)
	st.Set("b", 2)
	st.Delete("a")

	if st.Has("a") {
		t.Fatal("a should be deleted")
	}
	if !st.Has("b") {
		t.Fatal("b should still exist")
	}
}

func TestClearIsAliasForDelete(t *testing.T) {
	st, _ := newState(t)

	st.Set("a", 1)
	st.Clear("a")
	if st.Has("a") {
		t.Fatal("Clear should remove the field")
	}
}

func TestClearAll(t *testing.T) {
	st, _ := newState(t)

	st.Set("a", 1)
	st.Set("b", 2)
	st.Set("c", 3)
	st.ClearAll()

	for _, k := range []string{"a", "b", "c"} {
		if st.Has(k) {
			t.Fatalf("%q should be cleared", k)
		}
	}
}

// --------------------------------------------------------------------------- //
// Step / Flow                                                                  //
// --------------------------------------------------------------------------- //

func TestStepAndFlow(t *testing.T) {
	st, _ := newState(t)

	if st.Step() != "" {
		t.Fatal("Step should be empty initially")
	}
	st.SetStep("waiting_for_name")
	if got := st.Step(); got != "waiting_for_name" {
		t.Fatalf("Step() = %q, want waiting_for_name", got)
	}

	st.SetFlow("registration")
	if got := st.Flow(); got != "registration" {
		t.Fatalf("Flow() = %q, want registration", got)
	}

	st.Clear(fieldStep)
	if st.Step() != "" {
		t.Fatal("Step should be empty after Clear")
	}
}

// --------------------------------------------------------------------------- //
// TTL expiration                                                               //
// --------------------------------------------------------------------------- //

func TestSetWithTTL(t *testing.T) {
	st, _ := newState(t)

	st.SetWithTTL("temp", "data", 50*time.Millisecond)

	// Immediately available.
	if got := st.Get("temp"); got != "data" {
		t.Fatalf("Get(\"temp\") = %v before expiry", got)
	}
	if !st.Has("temp") {
		t.Fatal("Has(\"temp\") should be true before expiry")
	}
	if st.Expired("temp") {
		t.Fatal("Expired(\"temp\") should be false before expiry")
	}

	// After TTL.
	time.Sleep(80 * time.Millisecond)
	if st.Has("temp") {
		t.Fatal("Has(\"temp\") should be false after expiry")
	}
	if got := st.Get("temp"); got != nil {
		t.Fatalf("Get(\"temp\") = %v after expiry, want nil", got)
	}
	if !st.Expired("temp") {
		t.Fatal("Expired(\"temp\") should be true after expiry")
	}
}

func TestExpiredNoTTL(t *testing.T) {
	st, _ := newState(t)
	st.Set("permanent", "val")

	if st.Expired("permanent") {
		t.Fatal("Expired should be false for non-TTL entry")
	}
	if st.Expired("missing") {
		t.Fatal("Expired should be false for missing entry")
	}
}

func TestTTLExpiredSurvivesRead(t *testing.T) {
	store := NewMemoryStore()
	st := ForContext(store, ScopeChatAndUser, 1, 2)

	st.SetWithTTL("ephemeral", 1, 30*time.Millisecond)
	time.Sleep(60 * time.Millisecond)

	// Reading an expired entry does not delete it — Expired() still works.
	_ = st.Get("ephemeral")
	if !st.Expired("ephemeral") {
		t.Fatal("Expired should be true even after a read of the expired entry")
	}

	// GC cleanup removes it.
	store.Cleanup()
	if st.Expired("ephemeral") {
		t.Fatal("Expired should be false after Cleanup removed the entry")
	}
}

func TestMemoryStoreCleanup(t *testing.T) {
	store := NewMemoryStore()
	st := ForContext(store, ScopeChatAndUser, 1, 2)

	st.SetWithTTL("a", 1, 20*time.Millisecond)
	st.SetWithTTL("b", 2, 20*time.Millisecond)
	st.Set("c", 3) // no TTL

	time.Sleep(40 * time.Millisecond)
	removed := store.Cleanup()
	if removed != 2 {
		t.Fatalf("Cleanup removed %d, want 2", removed)
	}
	// Non-expired entry should survive.
	if !st.Has("c") {
		t.Fatal("non-expired entry should survive Cleanup")
	}
}

// --------------------------------------------------------------------------- //
// Scope isolation                                                              //
// --------------------------------------------------------------------------- //

func TestPerUserIsolation(t *testing.T) {
	store := NewMemoryStore()

	// User 1 in chat 100.
	st1 := ForContext(store, ScopeChatAndUser, 100, 1)
	st1.Set("name", "Alice")

	// User 2 in same chat.
	st2 := ForContext(store, ScopeChatAndUser, 100, 2)
	st2.Set("name", "Bob")

	if got := st1.Get("name"); got != "Alice" {
		t.Fatalf("user 1 name = %v, want Alice", got)
	}
	if got := st2.Get("name"); got != "Bob" {
		t.Fatalf("user 2 name = %v, want Bob", got)
	}
}

func TestPerChatIsolation(t *testing.T) {
	store := NewMemoryStore()

	// Same user in chat 100.
	st1 := ForContext(store, ScopeChatAndUser, 100, 1)
	st1.Set("data", "chat100")

	// Same user in chat 200.
	st2 := ForContext(store, ScopeChatAndUser, 200, 1)
	st2.Set("data", "chat200")

	if got := st1.Get("data"); got != "chat100" {
		t.Fatalf("chat 100 data = %v, want chat100", got)
	}
	if got := st2.Get("data"); got != "chat200" {
		t.Fatalf("chat 200 data = %v, want chat200", got)
	}
}

func TestChatAndUserScope(t *testing.T) {
	store := NewMemoryStore()

	// (chat=100, user=1) and (chat=100, user=2) are distinct.
	stA := ForContext(store, ScopeChatAndUser, 100, 1)
	stB := ForContext(store, ScopeChatAndUser, 100, 2)
	stA.Set("x", "a")
	stB.Set("x", "b")

	if stA.Get("x") == stB.Get("x") {
		t.Fatal("same chat, different users should have isolated state")
	}

	// ClearAll on stA should not affect stB.
	stA.ClearAll()
	if stA.Has("x") {
		t.Fatal("stA should be cleared")
	}
	if !stB.Has("x") {
		t.Fatal("stB should be unaffected")
	}
}

func TestUserScopeSharedAcrossChats(t *testing.T) {
	store := NewMemoryStore()

	// ScopeUser: state shared across chats for the same user.
	st1 := ForContext(store, ScopeUser, 100, 42)
	st1.Set("lang", "en")

	// Different chat, same user.
	st2 := ForContext(store, ScopeUser, 200, 42)
	if got := st2.Get("lang"); got != "en" {
		t.Fatalf("ScopeUser: lang = %v, want en (shared across chats)", got)
	}
}

func TestChatScopeSharedAcrossUsers(t *testing.T) {
	store := NewMemoryStore()

	// ScopeChat: state shared across users in the same chat.
	st1 := ForContext(store, ScopeChat, 500, 1)
	st1.Set("mode", "admin")

	// Different user, same chat.
	st2 := ForContext(store, ScopeChat, 500, 2)
	if got := st2.Get("mode"); got != "admin" {
		t.Fatalf("ScopeChat: mode = %v, want admin (shared across users)", got)
	}
}

// --------------------------------------------------------------------------- //
// Concurrency                                                                  //
// --------------------------------------------------------------------------- //

func TestConcurrentAccess(t *testing.T) {
	store := NewMemoryStore()
	st := ForContext(store, ScopeChatAndUser, 1, 1)

	const goroutines = 100
	var wg sync.WaitGroup
	var errors atomic.Int64

	wg.Add(goroutines * 2)

	// Writers.
	for i := 0; i < goroutines; i++ {
		go func(n int) {
			defer wg.Done()
			st.Set("counter", n)
		}(i)
	}

	// Readers.
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_ = st.Get("counter")
			_ = st.Has("counter")
		}()
	}

	wg.Wait()
	if errors.Load() > 0 {
		t.Fatalf("concurrent access produced %d errors", errors.Load())
	}
}

func TestConcurrentSetDifferentFields(t *testing.T) {
	store := NewMemoryStore()
	st := ForContext(store, ScopeChatAndUser, 1, 1)

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func(k string) {
			defer wg.Done()
			st.Set(k, k)
			if !st.Has(k) {
				t.Errorf("field %q not found after Set", k)
			}
		}(string(rune('a' + i)))
	}
	wg.Wait()
}

// --------------------------------------------------------------------------- //
// Middleware                                                                   //
// --------------------------------------------------------------------------- //

func TestMiddlewareAttachesState(t *testing.T) {
	p := New(WithStore(NewMemoryStore()))

	var captured *State
	inner := &fakeHandler{
		fn: func(ctx *tg.Context) {
			captured = FromContext(ctx)
		},
	}

	mw := &middleware{inner: inner, plugin: p}

	ctx := &tg.Context{
		Message: &types.Message{
			ChatID: 999,
			FromID: 888,
		},
	}
	mw.Handle(ctx)

	if captured == nil {
		t.Fatal("FromContext returned nil — middleware did not attach state")
	}

	// Verify the state is scoped correctly.
	captured.Set("test", "value")
	if got := captured.Get("test"); got != "value" {
		t.Fatalf("Get via middleware state = %v, want value", got)
	}
}

func TestMiddlewareReturnsNilWithoutPlugin(t *testing.T) {
	ctx := &tg.Context{}
	if st := FromContext(ctx); st != nil {
		t.Fatal("FromContext should return nil when no state is attached")
	}
}

func TestPluginStateForDirectAccess(t *testing.T) {
	p := New(WithStore(NewMemoryStore()))
	st := p.StateFor(100, 200)
	st.Set("key", "val")

	if got := st.Get("key"); got != "val" {
		t.Fatalf("StateFor.Get = %v, want val", got)
	}
}

// --------------------------------------------------------------------------- //
// Options                                                                      //
// --------------------------------------------------------------------------- //

func TestWithScope(t *testing.T) {
	p := New(WithScope(ScopeChat))
	if p.scope != ScopeChat {
		t.Fatalf("scope = %d, want %d", p.scope, ScopeChat)
	}
}

func TestWithGCInterval(t *testing.T) {
	p := New(WithGCInterval(0))
	if p.gcInterval != 0 {
		t.Fatalf("gcInterval = %v, want 0", p.gcInterval)
	}
}

func TestPluginName(t *testing.T) {
	p := New()
	if p.Name() != "fsm" {
		t.Fatalf("Name() = %q, want fsm", p.Name())
	}
}

// --- test helpers ---

type fakeHandler struct {
	fn func(*tg.Context)
}

func (h *fakeHandler) Check(_ *tg.Update) bool { return true }
func (h *fakeHandler) Handle(ctx *tg.Context)  { h.fn(ctx) }
