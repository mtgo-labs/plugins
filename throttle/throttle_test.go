package throttle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mtgo-labs/mtgo/tg"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func sendMsg(peerID int64) tg.TLObject {
	return &tg.MessagesSendMessageRequest{
		Peer: &tg.InputPeerUser{UserID: peerID},
	}
}

func sendMsgChat(chatID int64) tg.TLObject {
	return &tg.MessagesSendMessageRequest{
		Peer: &tg.InputPeerChat{ChatID: chatID},
	}
}

func sendMsgChannel(chID int64) tg.TLObject {
	return &tg.MessagesSendMessageRequest{
		Peer: &tg.InputPeerChannel{ChannelID: chID},
	}
}

func sendMedia(peerID int64) tg.TLObject {
	return &tg.MessagesSendMediaRequest{
		Peer: &tg.InputPeerUser{UserID: peerID},
	}
}

func editMsg(peerID int64) tg.TLObject {
	return &tg.MessagesEditMessageRequest{
		Peer: &tg.InputPeerUser{UserID: peerID},
	}
}

// ---------------------------------------------------------------------------
// Method-name resolution + peer extraction
// ---------------------------------------------------------------------------

func TestMethodName(t *testing.T) {
	tests := []struct {
		input tg.TLObject
		want  string
	}{
		{&tg.MessagesSendMessageRequest{}, "messages.sendMessage"},
		{&tg.MessagesSendMediaRequest{}, "messages.sendMedia"},
		{&tg.MessagesEditMessageRequest{}, "messages.editMessage"},
		{&tg.HelpGetConfigRequest{}, "help.getConfig"},
		{nil, ""},
	}
	for _, tt := range tests {
		got := methodName(tt.input)
		if got != tt.want {
			t.Errorf("methodName(%T) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestPeerExtraction(t *testing.T) {
	// Known Peer field.
	req := &tg.MessagesSendMessageRequest{Peer: &tg.InputPeerUser{UserID: 42}}
	peer := peerOf(req)
	u, ok := peer.(*tg.InputPeerUser)
	if !ok || u.UserID != 42 {
		t.Fatalf("peerOf(sendMessage) = %T(%v), want *InputPeerUser{42}", peer, peer)
	}

	// No Peer field → nil.
	cfg := &tg.HelpGetConfigRequest{}
	if peerOf(cfg) != nil {
		t.Errorf("peerOf(getConfig) = %T, want nil", peerOf(cfg))
	}

	// Cached: second call returns same result.
	peer2 := peerOf(req)
	u2, ok := peer2.(*tg.InputPeerUser)
	if !ok || u2.UserID != 42 {
		t.Fatalf("cached peerOf = %T(%v), want *InputPeerUser{42}", peer2, peer2)
	}
}

func TestChatAndUserKeys(t *testing.T) {
	tests := []struct {
		name string
		peer tg.InputPeerClass
		chat string
		user string
	}{
		{"user", &tg.InputPeerUser{UserID: 7}, "7", "7"},
		{"self", &tg.InputPeerSelf{}, "self", "self"},
		{"chat", &tg.InputPeerChat{ChatID: 3}, "3", ""},
		{"channel", &tg.InputPeerChannel{ChannelID: 9}, "9", ""},
		{"empty", nil, "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &tg.MessagesSendMessageRequest{Peer: tt.peer}
			if got := chatPeerID(peerOf(req)); got != tt.chat {
				t.Errorf("chatPeerID = %q, want %q", got, tt.chat)
			}
			if got := userPeerID(peerOf(req)); got != tt.user {
				t.Errorf("userPeerID = %q, want %q", got, tt.user)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Matchers
// ---------------------------------------------------------------------------

func TestMatchers(t *testing.T) {
	ctx := context.Background()
	sm := sendMsg(1)

	if !MatchMethod("messages.sendMessage")(ctx, sm) {
		t.Error("MatchMethod should match sendMessage")
	}
	if MatchMethod("messages.sendMedia")(ctx, sm) {
		t.Error("MatchMethod should not match sendMedia for sendMessage input")
	}
	if !MatchMethod("messages.sendMessage", "messages.sendMedia")(ctx, sm) {
		t.Error("MatchMethod with multiple should match")
	}
	if !MatchAll()(ctx, sm) {
		t.Error("MatchAll should always match")
	}
	if !MatchPrefix("messages.send")(ctx, sendMedia(1)) {
		t.Error("MatchPrefix should match sendMedia")
	}
	if MatchPrefix("messages.edit")(ctx, sendMedia(1)) {
		t.Error("MatchPrefix should not match sendMedia with edit prefix")
	}
	any := MatchAny(MatchMethod("messages.editMessage"), MatchMethod("messages.sendMessage"))
	if !any(ctx, sm) {
		t.Error("MatchAny should match when second matcher matches")
	}
	if any(ctx, &tg.HelpGetConfigRequest{}) {
		t.Error("MatchAny should not match when no matcher matches")
	}
}

// ---------------------------------------------------------------------------
// Bucket unit tests (deterministic)
// ---------------------------------------------------------------------------

func TestBucketAllowsThenDenies(t *testing.T) {
	b := &bucket{}
	const limit, burst = 3, 0
	maxAllowed := limit + burst
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	for i := 0; i < limit; i++ {
		allowed, _ := b.tryAcquire(now, maxAllowed, time.Second)
		if !allowed {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	allowed, retry := b.tryAcquire(now, maxAllowed, time.Second)
	if allowed {
		t.Fatal("4th request should be denied")
	}
	if retry <= 0 || retry > time.Second {
		t.Fatalf("retry = %s, want (0, 1s]", retry)
	}
}

func TestBucketRetryAfterPrecision(t *testing.T) {
	b := &bucket{}
	const window = 2 * time.Second
	base := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	// Fill at t=0.
	for i := 0; i < 3; i++ {
		b.tryAcquire(base, 3, window)
	}

	// Denied at t=0: oldest is at t=0, so retry = window.
	_, retry := b.tryAcquire(base, 3, window)
	if d := retry - window; d > 10*time.Millisecond || d < -10*time.Millisecond {
		t.Errorf("retry at t=0 = %s, want ~%s", retry, window)
	}

	// Denied at t=500ms: oldest at t=0, retry = window - 500ms = 1.5s.
	half := base.Add(500 * time.Millisecond)
	_, retry = b.tryAcquire(half, 3, window)
	want := 1500 * time.Millisecond
	if d := retry - want; d > 10*time.Millisecond || d < -10*time.Millisecond {
		t.Errorf("retry at t=500ms = %s, want ~%s", retry, want)
	}

	// After window: oldest falls out, request succeeds.
	after := base.Add(window + 1)
	allowed, _ := b.tryAcquire(after, 3, window)
	if !allowed {
		t.Error("should be allowed after window elapsed")
	}
}

func TestBucketBurst(t *testing.T) {
	b := &bucket{}
	const limit, burst = 2, 2
	maxAllowed := limit + burst
	now := time.Now()

	for i := 0; i < maxAllowed; i++ {
		allowed, _ := b.tryAcquire(now, maxAllowed, time.Second)
		if !allowed {
			t.Fatalf("burst request %d should be allowed", i+1)
		}
	}
	allowed, _ := b.tryAcquire(now, maxAllowed, time.Second)
	if allowed {
		t.Fatal("request beyond limit+burst should be denied")
	}
}

func TestBucketEvictsStale(t *testing.T) {
	b := &bucket{}
	const window = 100 * time.Millisecond

	// Fill at t=0.
	b.tryAcquire(time.Unix(0, 0), 1, window)
	// At t=0+window+1 the old entry is stale.
	allowed, _ := b.tryAcquire(time.Unix(0, int64(window+1)), 1, window)
	if !allowed {
		t.Fatal("stale entry should be evicted, allowing new request")
	}
}

// ---------------------------------------------------------------------------
// Global throttle
// ---------------------------------------------------------------------------

func TestGlobalThrottle(t *testing.T) {
	th := New(Config{
		Rules: []Rule{{
			Name:     "global",
			Scope:    ScopeGlobal,
			Limit:    3,
			Window:   10 * time.Second,
			Exceeded: FailFast,
		}},
		SweepInterval: time.Hour, // disable sweeping during test
	})
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := th.Allow(ctx, sendMsg(int64(i))); err != nil {
			t.Fatalf("request %d: unexpected error: %v", i+1, err)
		}
	}
	err := th.Allow(ctx, sendMsg(99))
	var te *ErrThrottled
	if !errors.As(err, &te) {
		t.Fatalf("4th request: want *ErrThrottled, got %T: %v", err, err)
	}
	if te.Rule != "global" {
		t.Errorf("rule = %q, want %q", te.Rule, "global")
	}
	if te.Scope != "global" {
		t.Errorf("scope = %q, want %q", te.Scope, "global")
	}
}

// ---------------------------------------------------------------------------
// Per-user throttle
// ---------------------------------------------------------------------------

func TestPerUserThrottle(t *testing.T) {
	th := New(Config{
		Rules: []Rule{{
			Name:     "per-user",
			Match:    MatchMethod("messages.sendMessage"),
			Scope:    ScopeUser,
			Limit:    2,
			Window:   10 * time.Second,
			Exceeded: FailFast,
		}},
		SweepInterval: time.Hour,
	})
	ctx := context.Background()

	// User A: 2 allowed, 3rd denied.
	for i := 0; i < 2; i++ {
		if err := th.Allow(ctx, sendMsg(100)); err != nil {
			t.Fatalf("user A request %d: %v", i+1, err)
		}
	}
	if err := th.Allow(ctx, sendMsg(100)); err == nil {
		t.Fatal("user A 3rd request should be denied")
	}

	// User B: independent budget.
	if err := th.Allow(ctx, sendMsg(200)); err != nil {
		t.Fatalf("user B request 1: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Per-chat throttle
// ---------------------------------------------------------------------------

func TestPerChatThrottle(t *testing.T) {
	th := New(Config{
		Rules: []Rule{{
			Name:     "per-chat",
			Scope:    ScopeChat,
			Limit:    2,
			Window:   10 * time.Second,
			Exceeded: FailFast,
		}},
		SweepInterval: time.Hour,
	})
	ctx := context.Background()

	// Chat A (group): 2 allowed, 3rd denied.
	for i := 0; i < 2; i++ {
		if err := th.Allow(ctx, sendMsgChat(10)); err != nil {
			t.Fatalf("chat A request %d: %v", i+1, err)
		}
	}
	if err := th.Allow(ctx, sendMsgChat(10)); err == nil {
		t.Fatal("chat A 3rd request should be denied")
	}

	// Chat B (channel): independent.
	if err := th.Allow(ctx, sendMsgChannel(20)); err != nil {
		t.Fatalf("chat B request: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Per-method throttle
// ---------------------------------------------------------------------------

func TestPerMethodThrottle(t *testing.T) {
	th := New(Config{
		Rules: []Rule{{
			Name:     "per-method",
			Scope:    ScopeMethod,
			Limit:    2,
			Window:   10 * time.Second,
			Exceeded: FailFast,
		}},
		SweepInterval: time.Hour,
	})
	ctx := context.Background()

	// sendMessage: 2 allowed, 3rd denied.
	for i := 0; i < 2; i++ {
		if err := th.Allow(ctx, sendMsg(1)); err != nil {
			t.Fatalf("sendMessage %d: %v", i+1, err)
		}
	}
	if err := th.Allow(ctx, sendMsg(1)); err == nil {
		t.Fatal("sendMessage 3rd should be denied")
	}

	// sendMedia: independent method budget.
	if err := th.Allow(ctx, sendMedia(1)); err != nil {
		t.Fatalf("sendMedia: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Custom key throttle
// ---------------------------------------------------------------------------

func TestCustomKeyThrottle(t *testing.T) {
	var callCount atomic.Int64
	th := New(Config{
		Rules: []Rule{{
			Name:  "custom",
			Scope: ScopeCustom,
			Key: func(_ context.Context, input tg.TLObject) string {
				callCount.Add(1)
				// Throttle by message length bucket.
				if sm, ok := input.(*tg.MessagesSendMessageRequest); ok && len(sm.Message) > 5 {
					return "long"
				}
				return "short"
			},
			Limit:    1,
			Window:   10 * time.Second,
			Exceeded: FailFast,
		}},
		SweepInterval: time.Hour,
	})
	ctx := context.Background()

	long1 := &tg.MessagesSendMessageRequest{Peer: &tg.InputPeerUser{UserID: 1}, Message: "hello world"}
	long2 := &tg.MessagesSendMessageRequest{Peer: &tg.InputPeerUser{UserID: 2}, Message: "another long"}

	if err := th.Allow(ctx, long1); err != nil {
		t.Fatalf("long1: %v", err)
	}
	if err := th.Allow(ctx, long2); err == nil {
		t.Fatal("long2 should be denied (same custom key 'long')")
	}

	short := &tg.MessagesSendMessageRequest{Peer: &tg.InputPeerUser{UserID: 3}, Message: "hi"}
	if err := th.Allow(ctx, short); err != nil {
		t.Fatalf("short: %v", err)
	}

	if callCount.Load() != 3 {
		t.Errorf("key func called %d times, want 3", callCount.Load())
	}
}

// ---------------------------------------------------------------------------
// Concurrent requests
// ---------------------------------------------------------------------------

func TestConcurrentRequests(t *testing.T) {
	const limit = 10
	th := New(Config{
		Rules: []Rule{{
			Name:     "global",
			Scope:    ScopeGlobal,
			Limit:    limit,
			Window:   time.Minute,
			Exceeded: FailFast,
		}},
		SweepInterval: time.Hour,
	})
	ctx := context.Background()

	const total = 50
	var allowed, denied atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // fire all at once
			err := th.Allow(ctx, sendMsg(1))
			if err == nil {
				allowed.Add(1)
			} else {
				denied.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if allowed.Load() != int32(limit) {
		t.Errorf("allowed = %d, want exactly %d", allowed.Load(), limit)
	}
	if denied.Load() != int32(total-limit) {
		t.Errorf("denied = %d, want exactly %d", denied.Load(), total-limit)
	}
	if a, d := allowed.Load(), denied.Load(); int(a)+int(d) != total {
		t.Errorf("allowed(%d)+denied(%d) = %d, want %d", a, d, a+d, total)
	}
}

func TestConcurrentPerKeyIsolation(t *testing.T) {
	// Each chat has its own budget; concurrent access to different chats
	// should never interfere.
	th := New(Config{
		Rules: []Rule{{
			Name:     "per-chat",
			Scope:    ScopeChat,
			Limit:    5,
			Window:   time.Minute,
			Exceeded: FailFast,
		}},
		SweepInterval: time.Hour,
	})
	ctx := context.Background()

	const chats = 20
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, chats)

	for i := 0; i < chats; i++ {
		wg.Add(1)
		chatID := int64(i + 1)
		go func(idx int) {
			defer wg.Done()
			<-start
			for j := 0; j < 5; j++ {
				if err := th.Allow(ctx, sendMsgChat(chatID)); err != nil {
					errs[idx] = err
					return
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("chat %d: unexpected throttle: %v", i+1, err)
		}
	}
}

// ---------------------------------------------------------------------------
// ErrThrottled retry-after correctness
// ---------------------------------------------------------------------------

func TestErrThrottledRetryAfter(t *testing.T) {
	const window = 200 * time.Millisecond
	th := New(Config{
		Rules: []Rule{{
			Name:     "test",
			Scope:    ScopeGlobal,
			Limit:    1,
			Window:   window,
			Exceeded: FailFast,
		}},
		SweepInterval: time.Hour,
	})
	ctx := context.Background()

	if err := th.Allow(ctx, sendMsg(1)); err != nil {
		t.Fatalf("first request: %v", err)
	}

	err := th.Allow(ctx, sendMsg(1))
	var te *ErrThrottled
	if !errors.As(err, &te) {
		t.Fatalf("want *ErrThrottled, got %T", err)
	}

	// RetryAfter should be close to the full window.
	if te.RetryAfter > window || te.RetryAfter <= 0 {
		t.Errorf("RetryAfter = %s, want (0, %s]", te.RetryAfter, window)
	}
	if te.Rule != "test" {
		t.Errorf("Rule = %q, want %q", te.Rule, "test")
	}
	if te.Scope != "global" {
		t.Errorf("Scope = %q, want %q", te.Scope, "global")
	}
}

func TestErrThrottledErrorString(t *testing.T) {
	e := &ErrThrottled{Rule: "x", RetryAfter: 5 * time.Second, Scope: "chat:1"}
	s := e.Error()
	if s == "" {
		t.Error("Error() should not be empty")
	}
	if !strings.Contains(s, "x") || !strings.Contains(s, "chat:1") {
		t.Errorf("Error() = %q, should contain rule and scope", s)
	}
}

// ---------------------------------------------------------------------------
// Wait mode behavior
// ---------------------------------------------------------------------------

func TestWaitModeBlocksThenSucceeds(t *testing.T) {
	const window = 80 * time.Millisecond
	th := New(Config{
		Rules: []Rule{{
			Name:     "wait",
			Scope:    ScopeGlobal,
			Limit:    1,
			Window:   window,
			Exceeded: Wait,
		}},
		SweepInterval: time.Hour,
	})
	ctx := context.Background()

	// First request succeeds immediately.
	start := time.Now()
	if err := th.Allow(ctx, sendMsg(1)); err != nil {
		t.Fatalf("first request: %v", err)
	}
	if d := time.Since(start); d > 20*time.Millisecond {
		t.Errorf("first request took %s, should be immediate", d)
	}

	// Second request must wait for the window to elapse.
	start = time.Now()
	if err := th.Allow(ctx, sendMsg(2)); err != nil {
		t.Fatalf("second request: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < window-30*time.Millisecond {
		t.Errorf("second request took %s, should have waited ~%s", elapsed, window)
	}
}

func TestWaitModeContextCancellation(t *testing.T) {
	th := New(Config{
		Rules: []Rule{{
			Name:     "wait",
			Scope:    ScopeGlobal,
			Limit:    1,
			Window:   10 * time.Second,
			Exceeded: Wait,
		}},
		SweepInterval: time.Hour,
	})
	ctx := context.Background()
	_ = th.Allow(ctx, sendMsg(1)) // fill the budget

	// Second request should block; cancel context to abort.
	cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	err := th.Allow(cctx, sendMsg(2))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context.DeadlineExceeded, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Custom handler behavior
// ---------------------------------------------------------------------------

func TestCustomHandler(t *testing.T) {
	var seen atomic.Bool
	th := New(Config{
		Rules: []Rule{{
			Name:     "custom-behavior",
			Scope:    ScopeGlobal,
			Limit:    1,
			Window:   10 * time.Second,
			Exceeded: Custom,
			Handler: func(_ context.Context, te *ErrThrottled) error {
				seen.Store(true)
				return fmt.Errorf("wrapped: %w", te)
			},
		}},
		SweepInterval: time.Hour,
	})
	ctx := context.Background()
	_ = th.Allow(ctx, sendMsg(1)) // fill

	err := th.Allow(ctx, sendMsg(2))
	if !seen.Load() {
		t.Error("custom handler should have been called")
	}
	if err == nil {
		t.Fatal("should return handler's error")
	}
	var te *ErrThrottled
	if !errors.As(err, &te) {
		t.Errorf("error should wrap *ErrThrottled, got %T: %v", err, err)
	}
}

// ---------------------------------------------------------------------------
// Multiple rules
// ---------------------------------------------------------------------------

func TestMultipleRules(t *testing.T) {
	th := New(Config{
		Rules: []Rule{
			{
				Name:     "per-chat",
				Match:    MatchMethod("messages.sendMessage"),
				Scope:    ScopeChat,
				Limit:    5,
				Window:   time.Minute,
				Exceeded: FailFast,
			},
			{
				Name:     "global",
				Scope:    ScopeGlobal,
				Limit:    3,
				Window:   time.Minute,
				Exceeded: FailFast,
			},
		},
		SweepInterval: time.Hour,
	})
	ctx := context.Background()

	// Global limit (3) is hit first even though per-chat allows 5.
	for i := 0; i < 3; i++ {
		if err := th.Allow(ctx, sendMsgChat(1)); err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
	}
	err := th.Allow(ctx, sendMsgChat(1))
	var te *ErrThrottled
	if !errors.As(err, &te) {
		t.Fatalf("want *ErrThrottled, got %T", err)
	}
	if te.Rule != "global" {
		t.Errorf("denying rule = %q, want 'global'", te.Rule)
	}
}

// ---------------------------------------------------------------------------
// Skipped rules (Limit < 1)
// ---------------------------------------------------------------------------

func TestSkipsInvalidRules(t *testing.T) {
	th := New(Config{
		Rules: []Rule{
			{Name: "zero-limit", Limit: 0, Window: time.Second},
			{Name: "negative", Limit: -1, Window: time.Second},
			{Name: "valid", Limit: 100, Window: time.Minute},
		},
		SweepInterval: time.Hour,
	})
	ctx := context.Background()
	// Only the "valid" rule is active; should allow.
	if err := th.Allow(ctx, sendMsg(1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(th.rules) != 1 {
		t.Errorf("active rules = %d, want 1", len(th.rules))
	}
}

// ---------------------------------------------------------------------------
// MatchPrefix scope
// ---------------------------------------------------------------------------

func TestMatchPrefixThrottle(t *testing.T) {
	th := New(Config{
		Rules: []Rule{{
			Name:     "all-sends",
			Match:    MatchPrefix("messages.send"),
			Scope:    ScopeMethod,
			Limit:    1,
			Window:   time.Minute,
			Exceeded: FailFast,
		}},
		SweepInterval: time.Hour,
	})
	ctx := context.Background()

	// sendMessage allowed (method:messages.sendMessage budget).
	if err := th.Allow(ctx, sendMsg(1)); err != nil {
		t.Fatalf("sendMessage: %v", err)
	}
	// sendMedia is a different method → different budget.
	if err := th.Allow(ctx, sendMedia(1)); err != nil {
		t.Fatalf("sendMedia: %v", err)
	}
	// Second sendMessage → denied.
	if err := th.Allow(ctx, sendMsg(2)); err == nil {
		t.Fatal("second sendMessage should be denied")
	}
	// editMessage doesn't match prefix "messages.send" → allowed (different matcher scope).
	if err := th.Allow(ctx, editMsg(1)); err != nil {
		t.Fatalf("editMessage should not be throttled: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Invoker middleware
// ---------------------------------------------------------------------------

type fakeInvoker struct {
	calls atomic.Int32
}

func (f *fakeInvoker) RPCInvoke(_ context.Context, _ tg.TLObject, _ func(*tg.Reader) (tg.TLObject, error)) (tg.TLObject, error) {
	f.calls.Add(1)
	return &tg.BoolTrue{}, nil
}

func (f *fakeInvoker) RPCInvokeRaw(_ context.Context, _ tg.TLObject) ([]byte, error) {
	f.calls.Add(1)
	return []byte{0x01}, nil
}

func TestInvokerMiddleware(t *testing.T) {
	th := New(Config{
		Rules: []Rule{{
			Name:     "mw",
			Scope:    ScopeGlobal,
			Limit:    2,
			Window:   time.Minute,
			Exceeded: FailFast,
		}},
		SweepInterval: time.Hour,
	})
	fi := &fakeInvoker{}
	wrapped := th.intercept(fi)
	ctx := context.Background()

	// Two calls pass through to the fake invoker.
	for i := 0; i < 2; i++ {
		if _, err := wrapped.RPCInvoke(ctx, sendMsg(1), nil); err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
	}
	if fi.calls.Load() != 2 {
		t.Errorf("downstream calls = %d, want 2", fi.calls.Load())
	}

	// Third call is throttled before reaching the invoker.
	_, err := wrapped.RPCInvoke(ctx, sendMsg(1), nil)
	if err == nil {
		t.Fatal("3rd call should be throttled")
	}
	if fi.calls.Load() != 2 {
		t.Errorf("downstream should not have been called, got %d", fi.calls.Load())
	}
}

func TestInvokerMiddlewareRaw(t *testing.T) {
	th := New(Config{
		Rules: []Rule{{
			Name: "mw-raw", Scope: ScopeGlobal, Limit: 1, Window: time.Minute, Exceeded: FailFast,
		}},
		SweepInterval: time.Hour,
	})
	fi := &fakeInvoker{}
	wrapped := th.intercept(fi)
	ctx := context.Background()

	if _, err := wrapped.RPCInvokeRaw(ctx, sendMsg(1)); err != nil {
		t.Fatalf("first raw call: %v", err)
	}
	if _, err := wrapped.RPCInvokeRaw(ctx, sendMsg(1)); err == nil {
		t.Fatal("second raw call should be throttled")
	}
}

// ---------------------------------------------------------------------------
// Window expiry (integration-ish)
// ---------------------------------------------------------------------------

func TestWindowExpiryAllowsAgain(t *testing.T) {
	th := New(Config{
		Rules: []Rule{{
			Name:     "expiry",
			Scope:    ScopeGlobal,
			Limit:    2,
			Window:   60 * time.Millisecond,
			Exceeded: FailFast,
		}},
		SweepInterval: time.Hour,
	})
	ctx := context.Background()

	_ = th.Allow(ctx, sendMsg(1))
	_ = th.Allow(ctx, sendMsg(1))
	if err := th.Allow(ctx, sendMsg(1)); err == nil {
		t.Fatal("3rd should be denied")
	}

	time.Sleep(80 * time.Millisecond)

	if err := th.Allow(ctx, sendMsg(1)); err != nil {
		t.Fatalf("after window expiry should be allowed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Sweep cleans stale buckets
// ---------------------------------------------------------------------------

func TestSweepRemovesStaleBuckets(t *testing.T) {
	th := New(Config{
		Rules: []Rule{{
			Name:     "sweep-test",
			Scope:    ScopeChat,
			Limit:    1,
			Window:   30 * time.Millisecond,
			Exceeded: FailFast,
		}},
		SweepInterval: 50 * time.Millisecond,
	})
	ctx := context.Background()
	_ = th.Allow(ctx, sendMsgChat(42))
	_ = th.Allow(ctx, sendMsgChat(99))

	count := 0
	th.buckets.Range(func(_, _ any) bool {
		count++
		return true
	})
	if count < 2 {
		t.Fatalf("expected ≥2 buckets before sweep, got %d", count)
	}

	// Start the sweeper, wait for at least one sweep cycle.
	th.stop = make(chan struct{})
	th.wg.Add(1)
	go th.sweepLoop()
	time.Sleep(200 * time.Millisecond)
	close(th.stop)
	th.wg.Wait()

	count = 0
	th.buckets.Range(func(_, _ any) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("expected 0 buckets after sweep, got %d", count)
	}
}
