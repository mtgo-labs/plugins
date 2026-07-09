// Package throttle provides a local rate-limiting (anti-spam) plugin for
// [mtgo](https://github.com/mtgo-labs/mtgo) Telegram bots and userbots.
//
// It prevents a client from sending too many RPC requests too quickly by
// enforcing configurable rules *before* the request reaches Telegram's servers.
// This is local preventive throttling — distinct from [tgerr.AsFloodWait],
// which handles the server-side FLOOD_WAIT error after Telegram rejects a call.
//
// # Basic usage
//
//	client.Use(throttle.New(throttle.Config{
//	    Rules: []throttle.Rule{{
//	        Name:     "send-message-per-chat",
//	        Match:    throttle.MatchMethod("messages.sendMessage"),
//	        Scope:    throttle.ScopeChat,
//	        Limit:    20,
//	        Window:   time.Minute,
//	        Exceeded: throttle.FailFast,
//	    }},
//	}))
//
// The plugin installs an invoker-level middleware on Start, so every typed RPC
// call (SendMessage, SendMedia, EditMessage, …) flows through the throttle
// rules. Requests that exceed a rule's limit are rejected with [ErrThrottled]
// (FailFast), delayed until permitted (Wait), or handed to a custom callback.
//
// # Scopes
//
// A rule's [Rule.Scope] determines the key space over which the limit applies:
//
//   - [ScopeGlobal]  — a single shared budget across all calls.
//   - [ScopeMethod]  — one budget per RPC method name.
//   - [ScopeChat]    — one budget per target chat (DM user, group, or channel).
//   - [ScopeUser]    — one budget per target user (DM peers only).
//   - [ScopeCustom]  — a caller-supplied [Rule.Key] function decides the key.
//
// Independent scopes never block each other: a per-chat limit on chat A does
// not delay messages to chat B, even under contention.
package throttle

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	telegram "github.com/mtgo-labs/mtgo/telegram"
	"github.com/mtgo-labs/mtgo/tg"
)

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

// Scope identifies the dimension along which a [Rule] counts requests.
type Scope int

const (
	// ScopeGlobal shares a single budget across every RPC call.
	ScopeGlobal Scope = iota
	// ScopeMethod maintains a separate budget per RPC method name
	// (e.g. "messages.sendMessage").
	ScopeMethod
	// ScopeChat maintains a separate budget per target conversation — a DM
	// user, a legacy group, or a channel.
	ScopeChat
	// ScopeUser maintains a separate budget per target user. Only meaningful
	// for direct-message peers; group/channel peers produce an empty key and
	// are not counted under this scope.
	ScopeUser
	// ScopeCustom defers key computation to the [Rule.Key] function. Combine
	// with [Rule.Key].
	ScopeCustom
)

// Behavior controls what happens when a rule's limit is exceeded.
type Behavior int

const (
	// FailFast returns [*ErrThrottled] immediately without retrying.
	FailFast Behavior = iota
	// Wait blocks the call until the rule permits it, honouring context
	// cancellation. The request is retried automatically when budget frees up.
	Wait
	// Custom invokes the [Rule.Handler] with the [*ErrThrottled]. The handler's
	// return value becomes the middleware's return value, letting the caller
	// transform the error, log it, or allow the request through.
	Custom
)

// Matcher decides whether a [Rule] applies to a given RPC call.
//
// Helper constructors: [MatchMethod], [MatchAll], [MatchAny], [MatchPrefix].
type Matcher func(ctx context.Context, input tg.TLObject) bool

// KeyFunc computes the throttle key for a request. Requests that resolve to the
// same key share a budget.
type KeyFunc func(ctx context.Context, input tg.TLObject) string

// Rule defines a single throttling policy.
//
// A request must pass ALL matching rules to proceed. When two rules both match,
// each consumes a token from its own independent budget; a denial from any rule
// aborts the call.
type Rule struct {
	// Name is a human-readable identifier included in [ErrThrottled].
	Name string

	// Match decides whether this rule applies to the call. A nil matcher is
	// equivalent to [MatchAll] (the rule matches every request).
	Match Matcher

	// Scope determines how the throttle key is derived. Ignored when Key is
	// set.
	Scope Scope

	// Key overrides Scope with a custom key function. Use with [ScopeCustom],
	// though any non-nil Key takes precedence regardless of Scope.
	Key KeyFunc

	// Limit is the maximum number of requests allowed within Window. Must be
	// ≥ 1; rules with Limit < 1 are silently skipped.
	Limit int

	// Window is the rolling time window over which Limit is enforced.
	// Defaults to one second when ≤ 0.
	Window time.Duration

	// Burst allows up to Burst additional requests beyond Limit within the same
	// window. The effective ceiling is Limit + Burst (≥ 0, clamped).
	Burst int

	// Exceeded selects the behavior when the limit is hit. See [Behavior].
	Exceeded Behavior

	// Handler is invoked when Exceeded is [Custom] and the limit is exceeded.
	// Its return value replaces the throttle error. If nil, [Custom] falls back
	// to [FailFast] semantics.
	Handler func(ctx context.Context, err *ErrThrottled) error
}

// Config holds the plugin configuration passed to [New].
type Config struct {
	// Rules is the ordered list of throttling policies. Rules are evaluated in
	// order; the first to deny wins.
	Rules []Rule

	// SweepInterval controls how often stale buckets (per-key rate counters
	// with no recent activity) are purged from memory. When zero (the default)
	// the interval is set to the largest rule Window (minimum one minute).
	SweepInterval time.Duration
}

// ErrThrottled is returned when a request exceeds a [Rule]'s limit under the
// [FailFast] behavior (or when a [Custom] handler propagates it). Use
// [errors.As] to extract it from a wrapped error.
//
//	type throttleErr *throttle.ErrThrottled
//	if errors.As(err, &throttleErr) {
//	    log.Printf("rate-limited by %s, retry in %s", throttleErr.Rule, throttleErr.RetryAfter)
//	}
type ErrThrottled struct {
	// Rule is the name of the [Rule] that denied the request.
	Rule string
	// RetryAfter is the soonest the caller may retry and expect to succeed.
	RetryAfter time.Duration
	// Scope is the throttle key that was over budget (e.g. "chat:123",
	// "method:messages.sendMessage", "global").
	Scope string
}

func (e *ErrThrottled) Error() string {
	return fmt.Sprintf("throttle: rule %q exceeded (scope %s), retry after %s",
		e.Rule, e.Scope, e.RetryAfter)
}

// ---------------------------------------------------------------------------
// Matcher constructors
// ---------------------------------------------------------------------------

// MatchMethod returns a [Matcher] that matches requests whose resolved TL
// method name equals any of the given names (e.g. "messages.sendMessage").
func MatchMethod(methods ...string) Matcher {
	set := make(map[string]struct{}, len(methods))
	for _, m := range methods {
		set[m] = struct{}{}
	}
	return func(_ context.Context, input tg.TLObject) bool {
		_, ok := set[methodName(input)]
		return ok
	}
}

// MatchAll matches every request.
func MatchAll() Matcher {
	return func(context.Context, tg.TLObject) bool { return true }
}

// MatchAny returns a [Matcher] that matches when any of the given matchers
// matches (logical OR). With no arguments it matches nothing.
func MatchAny(matchers ...Matcher) Matcher {
	return func(ctx context.Context, input tg.TLObject) bool {
		for _, m := range matchers {
			if m(ctx, input) {
				return true
			}
		}
		return false
	}
}

// MatchPrefix returns a [Matcher] that matches requests whose method name
// starts with prefix (e.g. "messages.send" matches messages.sendMessage,
// messages.sendMedia, messages.sendMultiMedia).
func MatchPrefix(prefix string) Matcher {
	return func(_ context.Context, input tg.TLObject) bool {
		name := methodName(input)
		if len(name) < len(prefix) {
			return false
		}
		return name[:len(prefix)] == prefix
	}
}

// ---------------------------------------------------------------------------
// Key-function helpers for ScopeCustom
// ---------------------------------------------------------------------------

// MethodKey derives a key from the request's TL method name.
func MethodKey(_ context.Context, input tg.TLObject) string {
	return "method:" + methodName(input)
}

// ChatKey derives a key from the request's target chat.
func ChatKey(_ context.Context, input tg.TLObject) string {
	if c := chatPeerID(peerOf(input)); c != "" {
		return "chat:" + c
	}
	return ""
}

// UserKey derives a key from the request's target user (DM peers only).
func UserKey(_ context.Context, input tg.TLObject) string {
	if u := userPeerID(peerOf(input)); u != "" {
		return "user:" + u
	}
	return ""
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

// Throttle is the rate-limiting plugin. It implements [telegram.Plugin] so it
// can be registered with [telegram.Client.Use]. On Start it installs an
// invoker middleware that intercepts every outgoing RPC call.
//
// A Throttle is safe for concurrent use by multiple goroutines.
type Throttle struct {
	rules         []compiledRule
	buckets       sync.Map // string → *bucket
	sweepInterval time.Duration
	stop          chan struct{}
	stopOnce      sync.Once
	wg            sync.WaitGroup
	started       bool
}

// New creates a Throttle from the given config. Rules with Limit < 1 are
// silently skipped. A Window ≤ 0 defaults to one second; a negative Burst is
// clamped to zero. The returned Throttle can be used standalone via
// [Throttle.Allow] or registered as a plugin via [telegram.Client.Use].
func New(cfg Config) *Throttle {
	rules := make([]compiledRule, 0, len(cfg.Rules))
	var maxWindow time.Duration
	for _, r := range cfg.Rules {
		if r.Limit < 1 {
			continue
		}
		window := r.Window
		if window <= 0 {
			window = time.Second
		}
		burst := r.Burst
		if burst < 0 {
			burst = 0
		}
		match := r.Match
		if match == nil {
			match = MatchAll()
		}
		keyFn := r.Key
		if keyFn == nil {
			keyFn = scopeKeyFunc(r.Scope)
		}
		rules = append(rules, compiledRule{
			name:       r.Name,
			match:      match,
			keyFn:      keyFn,
			maxAllowed: r.Limit + burst,
			window:     window,
			exceeded:   r.Exceeded,
			handler:    r.Handler,
		})
		if window > maxWindow {
			maxWindow = window
		}
	}

	sweep := cfg.SweepInterval
	if sweep <= 0 {
		sweep = maxWindow
		if sweep < time.Minute {
			sweep = time.Minute
		}
	}

	return &Throttle{
		rules:         rules,
		sweepInterval: sweep,
	}
}

func scopeKeyFunc(s Scope) KeyFunc {
	switch s {
	case ScopeMethod:
		return MethodKey
	case ScopeChat:
		return ChatKey
	case ScopeUser:
		return UserKey
	default: // ScopeGlobal, ScopeCustom (with nil Key → global)
		return func(context.Context, tg.TLObject) string { return "global" }
	}
}

// --- telegram.Plugin interface ---

// Name returns "throttle".
func (t *Throttle) Name() string { return "throttle" }

// Start installs the invoker middleware and launches the background sweeper.
// The middleware intercepts all outgoing RPC calls via [telegram.Client.Raw].
func (t *Throttle) Start(_ context.Context, client *telegram.Client) error {
	if client != nil {
		client.UseInvokerMiddleware(t.Middleware())
	}
	t.stop = make(chan struct{})
	t.started = true
	t.wg.Add(1)
	go t.sweepLoop()
	return nil
}

// Stop terminates the background sweeper and waits for it to exit.
func (t *Throttle) Stop(_ context.Context) error {
	t.stopOnce.Do(func() {
		if t.stop != nil {
			close(t.stop)
		}
	})
	t.wg.Wait()
	t.started = false
	return nil
}

// Middleware returns the invoker middleware that this plugin installs on Start.
// Exposed so callers can register it manually without the plugin lifecycle
// (useful in tests or when middleware composition is managed externally):
//
//	client.UseInvokerMiddleware(throttle.New(cfg).Middleware())
func (t *Throttle) Middleware() telegram.InvokerMiddleware {
	return t.intercept
}

func (t *Throttle) intercept(next tg.Invoker) tg.Invoker {
	return &throttledInvoker{next: next, throttle: t}
}

// Allow checks whether input satisfies all throttle rules without a client.
// It is the standalone entry point: the same logic the invoker middleware uses.
// Returns nil if the request is permitted, or an [*ErrThrottled] (or
// [context.Cancelled]) when denied.
func (t *Throttle) Allow(ctx context.Context, input tg.TLObject) error {
	for i := range t.rules {
		r := &t.rules[i]
		if !r.match(ctx, input) {
			continue
		}
		key := r.keyFn(ctx, input)
		if key == "" {
			continue // scope produced no key (e.g. ScopeUser on a group) — skip
		}
		if err := r.acquire(ctx, key, &t.buckets); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Compiled rule + rate limiter
// ---------------------------------------------------------------------------

type compiledRule struct {
	name       string
	match      Matcher
	keyFn      KeyFunc
	maxAllowed int
	window     time.Duration
	exceeded   Behavior
	handler    func(context.Context, *ErrThrottled) error
}

// acquire records a request against the rule. Returns nil on success.
func (r *compiledRule) acquire(ctx context.Context, key string, store *sync.Map) error {
	for {
		b := getBucket(store, key)
		allowed, retryAfter := b.tryAcquire(time.Now(), r.maxAllowed, r.window)
		if allowed {
			return nil
		}
		switch r.exceeded {
		case Wait:
			timer := time.NewTimer(retryAfter)
			select {
			case <-timer.C:
				continue
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		case Custom:
			te := &ErrThrottled{Rule: r.name, RetryAfter: retryAfter, Scope: key}
			if r.handler != nil {
				return r.handler(ctx, te)
			}
			return te
		default: // FailFast
			return &ErrThrottled{Rule: r.name, RetryAfter: retryAfter, Scope: key}
		}
	}
}

// bucket is a sliding-window log rate counter for a single key.
type bucket struct {
	mu    sync.Mutex
	times []time.Time
}

// tryAcquire records a request at now if the window has capacity.
// Returns (true, 0) on success or (false, retryAfter) when over budget.
func (b *bucket) tryAcquire(now time.Time, maxAllowed int, window time.Duration) (bool, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Evict entries outside the window (compact in place).
	cutoff := now.Add(-window)
	n := 0
	for _, ts := range b.times {
		if ts.After(cutoff) {
			b.times[n] = ts
			n++
		}
	}
	b.times = b.times[:n]

	if len(b.times) < maxAllowed {
		b.times = append(b.times, now)
		return true, 0
	}

	// Over budget: the caller may retry once the oldest entry falls out.
	oldest := b.times[0]
	retryAfter := oldest.Add(window).Sub(now)
	if retryAfter < 0 {
		retryAfter = 0
	}
	return false, retryAfter
}

func getBucket(store *sync.Map, key string) *bucket {
	if v, ok := store.Load(key); ok {
		return v.(*bucket)
	}
	b := &bucket{}
	actual, _ := store.LoadOrStore(key, b)
	return actual.(*bucket)
}

// ---------------------------------------------------------------------------
// Background sweeper
// ---------------------------------------------------------------------------

func (t *Throttle) sweepLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(t.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-t.stop:
			return
		case <-ticker.C:
			t.sweep()
		}
	}
}

func (t *Throttle) sweep() {
	cutoff := time.Now().Add(-t.sweepInterval)
	t.buckets.Range(func(key, val any) bool {
		b := val.(*bucket)
		b.mu.Lock()
		n := 0
		for _, ts := range b.times {
			if ts.After(cutoff) {
				b.times[n] = ts
				n++
			}
		}
		b.times = b.times[:n]
		empty := len(b.times) == 0
		b.mu.Unlock()
		if empty {
			t.buckets.Delete(key)
		}
		return true
	})
}

// ---------------------------------------------------------------------------
// Invoker adapter
// ---------------------------------------------------------------------------

// throttledInvoker wraps a downstream [tg.Invoker] with throttle enforcement.
type throttledInvoker struct {
	next     tg.Invoker
	throttle *Throttle
}

func (ti *throttledInvoker) RPCInvoke(ctx context.Context, input tg.TLObject, decode func(*tg.Reader) (tg.TLObject, error)) (tg.TLObject, error) {
	if err := ti.throttle.Allow(ctx, input); err != nil {
		return nil, err
	}
	return ti.next.RPCInvoke(ctx, input, decode)
}

func (ti *throttledInvoker) RPCInvokeRaw(ctx context.Context, input tg.TLObject) ([]byte, error) {
	if err := ti.throttle.Allow(ctx, input); err != nil {
		return nil, err
	}
	return ti.next.RPCInvokeRaw(ctx, input)
}

// ---------------------------------------------------------------------------
// Method-name resolution (reverse tg.NamesMap lookup)
// ---------------------------------------------------------------------------

var (
	nameOnce sync.Once
	nameRev  map[uint32]string
)

func methodName(input tg.TLObject) string {
	if input == nil {
		return ""
	}
	nameOnce.Do(func() {
		nameRev = make(map[uint32]string, len(tg.NamesMap))
		for name, id := range tg.NamesMap {
			nameRev[id] = name
		}
	})
	return nameRev[input.ConstructorID()]
}

// ---------------------------------------------------------------------------
// Peer extraction (cached reflection)
// ---------------------------------------------------------------------------

// peerFieldCache maps a constructor ID to the struct-field index of a field
// named "Peer" of type tg.InputPeerClass, or -1 when no such field exists.
var peerFieldCache sync.Map

const peerIdxNone = -1

// peerOf extracts the InputPeerClass from a request's "Peer" field, using a
// one-time reflective scan cached per constructor ID. Returns nil when the
// request type has no Peer field.
func peerOf(input tg.TLObject) tg.InputPeerClass {
	if input == nil {
		return nil
	}
	cid := input.ConstructorID()
	v, ok := peerFieldCache.Load(cid)
	if !ok {
		v = findPeerFieldIndex(input)
		peerFieldCache.Store(cid, v)
	}
	idx := v.(int)
	if idx == peerIdxNone {
		return nil
	}
	rv := reflect.ValueOf(input)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct || idx >= rv.NumField() {
		return nil
	}
	peer, _ := rv.Field(idx).Interface().(tg.InputPeerClass)
	return peer
}

var inputPeerType = reflect.TypeOf((*tg.InputPeerClass)(nil)).Elem()

func findPeerFieldIndex(input tg.TLObject) int {
	t := reflect.TypeOf(input)
	if t == nil {
		return peerIdxNone
	}
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return peerIdxNone
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Name == "Peer" && f.Type == inputPeerType {
			return i
		}
	}
	return peerIdxNone
}

// chatPeerID returns the string identifier of the conversation targeted by a
// peer: user ID for DMs, chat ID for legacy groups, channel ID for channels.
func chatPeerID(peer tg.InputPeerClass) string {
	switch p := peer.(type) {
	case *tg.InputPeerUser:
		return strconv.FormatInt(p.UserID, 10)
	case *tg.InputPeerChat:
		return strconv.FormatInt(p.ChatID, 10)
	case *tg.InputPeerChannel:
		return strconv.FormatInt(p.ChannelID, 10)
	case *tg.InputPeerSelf:
		return "self"
	}
	return ""
}

// userPeerID returns the string identifier of the target user, or "" when the
// peer is not a direct user (groups/channels have no single target user).
func userPeerID(peer tg.InputPeerClass) string {
	switch p := peer.(type) {
	case *tg.InputPeerUser:
		return strconv.FormatInt(p.UserID, 10)
	case *tg.InputPeerSelf:
		return "self"
	}
	return ""
}
