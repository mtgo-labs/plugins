package conversations

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	tg "github.com/mtgo-labs/mtgo/telegram"
	"github.com/mtgo-labs/mtgo/telegram/types"
)

// --- Mocks ------------------------------------------------------------------

type mockHandler struct {
	called atomic.Bool
}

func (h *mockHandler) Check(_ *tg.Update) bool { return true }
func (h *mockHandler) Handle(_ *tg.Context)    { h.called.Store(true) }

func makeCtx(chatID, userID int64, text string) *tg.Context {
	return &tg.Context{
		Ctx:     context.Background(),
		Message: &types.Message{ChatID: chatID, FromID: userID, Text: text},
	}
}

// --- MemoryStore CRUD tests -------------------------------------------------

func TestMemoryStoreSaveLoad(t *testing.T) {
	s := NewMemoryStore()
	key := StoreKey{ChatID: 100, UserID: 200}
	state := &ConversationState{Name: "signup", Step: 2}

	if err := s.Save(key, state); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := s.Load(key)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got == nil {
		t.Fatal("Load returned nil after Save")
	}
	if got.Name != "signup" || got.Step != 2 {
		t.Errorf("Load got name=%q step=%d, want signup/2", got.Name, got.Step)
	}
}

func TestMemoryStoreLoadMissing(t *testing.T) {
	s := NewMemoryStore()
	got, err := s.Load(StoreKey{ChatID: 1, UserID: 2})
	if err != nil {
		t.Fatalf("Load missing: %v", err)
	}
	if got != nil {
		t.Errorf("Load missing key returned non-nil: %+v", got)
	}
}

func TestMemoryStoreDelete(t *testing.T) {
	s := NewMemoryStore()
	key := StoreKey{ChatID: 10, UserID: 20}
	_ = s.Save(key, &ConversationState{Name: "x"})

	if err := s.Delete(key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got, _ := s.Load(key)
	if got != nil {
		t.Error("Load after Delete returned non-nil")
	}
}

func TestMemoryStoreList(t *testing.T) {
	s := NewMemoryStore()
	_ = s.Save(StoreKey{ChatID: 1, UserID: 10}, &ConversationState{Name: "a"})
	_ = s.Save(StoreKey{ChatID: 2, UserID: 20}, &ConversationState{Name: "b"})

	keys, err := s.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("List returned %d keys, want 2", len(keys))
	}
}

func TestMemoryStoreSaveWithData(t *testing.T) {
	s := NewMemoryStore()
	key := StoreKey{ChatID: 1, UserID: 1}
	_ = s.Save(key, &ConversationState{
		Name: "form",
		Step: 1,
		Data: json.RawMessage(`{"name":"alice"}`),
	})

	got, _ := s.Load(key)
	if got == nil {
		t.Fatal("Load returned nil")
	}
	if string(got.Data) != `{"name":"alice"}` {
		t.Errorf("Data = %s, want {\"name\":\"alice\"}", string(got.Data))
	}
}

func TestMemoryStoreOverwrite(t *testing.T) {
	s := NewMemoryStore()
	key := StoreKey{ChatID: 1, UserID: 1}
	_ = s.Save(key, &ConversationState{Name: "first", Step: 0})
	_ = s.Save(key, &ConversationState{Name: "second", Step: 5})

	got, _ := s.Load(key)
	if got.Name != "second" || got.Step != 5 {
		t.Errorf("after overwrite: name=%q step=%d, want second/5", got.Name, got.Step)
	}
}

func TestMemoryStoreListEmpty(t *testing.T) {
	s := NewMemoryStore()
	keys, err := s.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("List on empty store returned %d keys, want 0", len(keys))
	}
}

// --- StoreKey derivation tests ----------------------------------------------

func TestStoreKeyEquality(t *testing.T) {
	k1 := StoreKey{ChatID: 100, UserID: 200}
	k2 := StoreKey{ChatID: 100, UserID: 200}

	if k1 != k2 {
		t.Error("identical keys should be equal")
	}
}

func TestStoreKeyDifferentUser(t *testing.T) {
	k1 := StoreKey{ChatID: 100, UserID: 200}
	k2 := StoreKey{ChatID: 100, UserID: 201}

	if k1 == k2 {
		t.Error("different UserID should produce different keys")
	}
}

func TestStoreKeyDifferentChat(t *testing.T) {
	k1 := StoreKey{ChatID: 100, UserID: 200}
	k2 := StoreKey{ChatID: 101, UserID: 200}

	if k1 == k2 {
		t.Error("different ChatID should produce different keys")
	}
}

// --- Plugin Register / Active tests -----------------------------------------

func TestPluginRegisterAndActive(t *testing.T) {
	p := New()
	p.Register("signup", func(_ *ConversationContext) error { return nil })

	if _, ok := p.conversations["signup"]; !ok {
		t.Error("conversation not registered")
	}
}

func TestPluginActiveNoConversation(t *testing.T) {
	p := New()
	if p.Active(100, 200) {
		t.Error("Active should be false with no conversations entered")
	}
}

// --- Plugin Enter / Exit flow tests -----------------------------------------

func TestPluginEnterExitFlow(t *testing.T) {
	p := New()
	started := make(chan struct{})
	p.Register("test", func(cc *ConversationContext) error {
		close(started)
		<-cc.done
		return nil
	})

	ctx := makeCtx(100, 200, "")
	if err := p.Enter("test", ctx); err != nil {
		t.Fatalf("Enter: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	if !p.Active(100, 200) {
		t.Error("Active should be true after Enter")
	}

	exitCtx := makeCtx(100, 200, "")
	if !p.Exit(exitCtx) {
		t.Error("Exit should return true when conversation was active")
	}

	if p.Active(100, 200) {
		t.Error("Active should be false after Exit")
	}

	_ = p.Stop(context.Background())
}

func TestPluginExitNotActive(t *testing.T) {
	p := New()
	if p.Exit(makeCtx(100, 200, "")) {
		t.Error("Exit should return false when no conversation is active")
	}
}

func TestPluginEnterUnknownConversation(t *testing.T) {
	p := New()
	err := p.Enter("nonexistent", makeCtx(100, 200, ""))
	if err == nil {
		t.Fatal("expected error for unknown conversation")
	}
}

// --- ConversationContext Wait / dispatch integration ------------------------

func TestConversationWaitReceivesDispatchedUpdate(t *testing.T) {
	p := New()
	received := make(chan string, 1)

	p.Register("echo", func(cc *ConversationContext) error {
		ctx, err := cc.Wait()
		if err != nil {
			return err
		}
		received <- ctx.Message.Text
		return nil
	})

	if err := p.Enter("echo", makeCtx(100, 200, "")); err != nil {
		t.Fatalf("Enter: %v", err)
	}

	// Give handler time to reach Wait().
	time.Sleep(50 * time.Millisecond)

	p.dispatch(makeCtx(100, 200, "hello"))

	select {
	case text := <-received:
		if text != "hello" {
			t.Errorf("received %q, want hello", text)
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not receive dispatched update")
	}

	_ = p.Stop(context.Background())
}

// --- dispatch routing tests -------------------------------------------------

func TestDispatchRoutesToActive(t *testing.T) {
	p := New()
	cc := &ConversationContext{
		plugin: p,
		notify: make(chan *tg.Context, 8),
		done:   make(chan struct{}),
		name:   "test",
	}
	p.mu.Lock()
	p.active[entryKey{chatID: 100, userID: 200}] = cc
	p.mu.Unlock()

	ctx := makeCtx(100, 200, "hello")
	if !p.dispatch(ctx) {
		t.Error("dispatch should return true for active conversation")
	}

	select {
	case got := <-cc.notify:
		if got != ctx {
			t.Error("dispatched context mismatch")
		}
	default:
		t.Error("update was not delivered to notify channel")
	}
}

func TestDispatchNoActive(t *testing.T) {
	p := New()
	if p.dispatch(makeCtx(100, 200, "hello")) {
		t.Error("dispatch should return false when no active conversation")
	}
}

func TestDispatchZeroChatID(t *testing.T) {
	p := New()
	ctx := &tg.Context{Ctx: context.Background()}
	if p.dispatch(ctx) {
		t.Error("dispatch should return false for zero chat ID")
	}
}

// --- Middleware integration tests -------------------------------------------

func TestMiddlewareCancelCommand(t *testing.T) {
	p := New()
	cc := &ConversationContext{
		plugin: p,
		notify: make(chan *tg.Context, 8),
		done:   make(chan struct{}),
		name:   "test",
	}
	p.mu.Lock()
	p.active[entryKey{chatID: 100, userID: 200}] = cc
	p.mu.Unlock()

	h := &mockHandler{}
	mw := &conversationsMiddleware{inner: h, plugin: p}

	mw.Handle(makeCtx(100, 200, "/cancel"))

	if !h.called.Load() {
		t.Error("inner handler should be called for /cancel")
	}
	select {
	case <-cc.done:
	default:
		t.Error("cc.done should be closed after /cancel")
	}
	if p.Active(100, 200) {
		t.Error("conversation should be removed after /cancel")
	}
}

func TestMiddlewareActiveConversationConsumed(t *testing.T) {
	p := New()
	cc := &ConversationContext{
		plugin: p,
		notify: make(chan *tg.Context, 8),
		done:   make(chan struct{}),
		name:   "test",
	}
	p.mu.Lock()
	p.active[entryKey{chatID: 100, userID: 200}] = cc
	p.mu.Unlock()

	h := &mockHandler{}
	mw := &conversationsMiddleware{inner: h, plugin: p}

	mw.Handle(makeCtx(100, 200, "some message"))

	if h.called.Load() {
		t.Error("inner handler should NOT be called when conversation is active")
	}
}

func TestMiddlewarePassThrough(t *testing.T) {
	p := New()

	h := &mockHandler{}
	mw := &conversationsMiddleware{inner: h, plugin: p}

	mw.Handle(makeCtx(100, 200, "hello"))

	if !h.called.Load() {
		t.Error("inner handler should be called when no active conversation")
	}
}

// --- extractChatID / extractUserID tests ------------------------------------

func TestExtractChatIDFromMessage(t *testing.T) {
	ctx := makeCtx(123, 456, "")
	if got := extractChatID(ctx); got != 123 {
		t.Errorf("extractChatID = %d, want 123", got)
	}
}

func TestExtractChatIDFromCallbackQuery(t *testing.T) {
	ctx := &tg.Context{
		Ctx:           context.Background(),
		CallbackQuery: &types.CallbackQuery{ChatID: 789, UserID: 456},
	}
	if got := extractChatID(ctx); got != 789 {
		t.Errorf("extractChatID = %d, want 789", got)
	}
}

func TestExtractChatIDEmpty(t *testing.T) {
	ctx := &tg.Context{Ctx: context.Background()}
	if got := extractChatID(ctx); got != 0 {
		t.Errorf("extractChatID = %d, want 0", got)
	}
}

func TestExtractUserIDFromMessage(t *testing.T) {
	ctx := makeCtx(123, 456, "")
	if got := extractUserID(ctx); got != 456 {
		t.Errorf("extractUserID = %d, want 456", got)
	}
}

func TestExtractUserIDFromCallbackQuery(t *testing.T) {
	ctx := &tg.Context{
		Ctx:           context.Background(),
		CallbackQuery: &types.CallbackQuery{ChatID: 789, UserID: 999},
	}
	if got := extractUserID(ctx); got != 999 {
		t.Errorf("extractUserID = %d, want 999", got)
	}
}
