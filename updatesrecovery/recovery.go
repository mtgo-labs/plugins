package updatesrecovery

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	telegram "github.com/mtgo-labs/mtgo/telegram"
	"github.com/mtgo-labs/mtgo/tg"
)

// Plugin persists and recovers Telegram update state (pts, qts, date, seq).
// It implements the [tg.Plugin] interface and is registered via
// [client.Use].
//
// The plugin is concurrency-safe and does not block the main update dispatch
// loop: state tracking is O(1) under a read-lock, persistence is debounced
// to a background goroutine, and gap recovery runs in its own goroutine.
type Plugin struct {
	store Store
	opts  options

	client *telegram.Client
	log    *slog.Logger

	// In-memory state protected by mu.
	mu       sync.RWMutex
	state    State
	hasState bool // true after first state is established (loaded or fetched)

	// Debounced persistence.
	saveCh chan struct{} // signal: state changed, persist soon
	stopCh chan struct{}
	wg     sync.WaitGroup

	// Gap recovery single-flight.
	recovering atomic.Bool
	// Idle watchdog: last update timestamp (unix seconds).
	lastUpdate atomic.Int64

	// Gap buffer timer (debounces getDifference calls).
	gapTimer   *time.Timer
	gapTimerMu sync.Mutex

	// rpc is the RPC client used for getDifference. Defaults to client.Raw()
	// in Start; tests can set it directly via setRPC.
	rpc      differenceRPC
	channels *channelManager
}

// differenceRPC is the minimal interface for update gap recovery.
type differenceRPC interface {
	UpdatesGetDifference(ctx context.Context, req *tg.UpdatesGetDifferenceRequest) (tg.DifferenceClass, error)
	UpdatesGetChannelDifference(ctx context.Context, req *tg.UpdatesGetChannelDifferenceRequest) (tg.ChannelDifferenceClass, error)
}

type options struct {
	saveInterval  time.Duration
	gapBuffer     time.Duration
	maxIterations int
	skipReconnect bool
	idleTimeout   time.Duration
	log           *slog.Logger
}

// Option configures the plugin.
type Option func(*options)

// WithSaveInterval sets the debounce interval for persisting state to storage.
// Default is 2s. Set to 0 to persist on every state change (higher I/O).
func WithSaveInterval(d time.Duration) Option {
	return func(o *options) { o.saveInterval = d }
}

// WithGapBuffer sets the delay before triggering getDifference after a gap is
// detected. If the gap is filled by the next arriving update before the timer
// fires, the expensive RPC is skipped. Default 500ms. Set to 0 for immediate
// recovery.
func WithGapBuffer(d time.Duration) Option {
	return func(o *options) { o.gapBuffer = d }
}

// WithMaxIterations caps the number of getDifference loop iterations during
// gap recovery. Each iteration fetches up to 100 missed updates. A value of 0
// means unlimited (fetch the entire backlog). Default is 100.
func WithMaxIterations(n int) Option {
	return func(o *options) { o.maxIterations = n }
}

// WithSkipReconnectRecovery disables gap recovery on reconnect. When true, the
// plugin still tracks and persists state, but does not call getDifference to
// fetch updates missed during the disconnect. This reduces bandwidth for
// use cases where stale updates are not needed.
func WithSkipReconnectRecovery(skip bool) Option {
	return func(o *options) { o.skipReconnect = skip }
}

// WithIdleTimeout sets the idle watchdog interval. If no updates arrive within
// this duration, the plugin assumes a server-side update stall and triggers
// gap recovery via getDifference. Default is 15 minutes. Set to 0 to disable.
func WithIdleTimeout(d time.Duration) Option {
	return func(o *options) { o.idleTimeout = d }
}

// WithLogger sets a structured logger for diagnostic messages.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.log = l }
}

// New creates an updates-recovery plugin backed by the given [Store].
//
//	client.Use(updatesrecovery.New(updatesrecovery.Storage(store, "my_bot")))
//
// Pass nil to create a disabled plugin (all hooks are no-ops). This is useful
// for feature-flagging recovery without changing call sites.
func New(store Store, opts ...Option) *Plugin {
	o := options{
		saveInterval:  2 * time.Second,
		gapBuffer:     500 * time.Millisecond,
		maxIterations: 100,
		idleTimeout:   15 * time.Minute,
	}
	for _, fn := range opts {
		fn(&o)
	}
	if o.log == nil {
		o.log = slog.Default()
	}
	return &Plugin{
		store: store,
		opts:  o,
		log:   o.log,
	}
}

// Name returns the plugin identifier.
func (p *Plugin) Name() string { return "updates-recovery" }

// Start loads persisted state, registers lifecycle hooks, and performs initial
// recovery if saved state exists.
func (p *Plugin) Start(ctx context.Context, client *telegram.Client) error {
	if p.store == nil {
		p.log.Debug("updates-recovery: store is nil, plugin disabled")
		return nil
	}
	p.client = client

	// Load persisted state.
	saved, err := p.store.LoadState()
	if err != nil {
		p.log.Warn("updates-recovery: load state failed, starting fresh", "error", err)
	} else if saved != nil {
		p.mu.Lock()
		p.state = *saved
		p.hasState = true
		p.mu.Unlock()
		p.log.Debug("updates-recovery: restored state",
			"pts", saved.Pts, "qts", saved.Qts, "date", saved.Date, "seq", saved.Seq)
	}

	// Start debounced save goroutine.
	p.saveCh = make(chan struct{}, 1)
	p.stopCh = make(chan struct{})
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.saveLoop()
	}()

	// Initialize channel manager for per-channel gap recovery.
	var channelStore ChannelStore
	if cs, ok := p.store.(ChannelStore); ok {
		channelStore = cs
	}
	p.channels = newChannelManager(channelStore, nil, p.log, p.dispatchRecovered)

	// Load persisted channel states.
	if err := p.channels.loadPersisted(); err != nil {
		p.log.Warn("updates-recovery: load channel states failed", "error", err)
	}

	// Register lifecycle hooks.
	client.OnUpdateReceived(p.onUpdateReceived)
	client.OnReconnect(p.onReconnect)

	// Start idle update watchdog (if enabled).
	if p.opts.idleTimeout > 0 {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.idleWatchdog(ctx)
		}()
	}

	// Register invoker middleware to capture pts feedback from RPC responses
	// (messages.affectedMessages, messages.affectedHistory).
	client.UseInvokerMiddleware(func(next tg.Invoker) tg.Invoker {
		return &affectedInvoker{next: next, plugin: p}
	})

	// If we have saved state, recover any updates missed while offline.
	if p.hasState {
		go p.recoverAccount(ctx, "restart")
	}

	return nil
}

// Stop flushes pending state to storage and stops background goroutines.
func (p *Plugin) Stop(ctx context.Context) error {
	if p.store == nil {
		return nil
	}
	close(p.stopCh)
	waitDone := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-ctx.Done():
	}

	// Final flush.
	p.flushState()

	p.gapTimerMu.Lock()
	if p.gapTimer != nil {
		p.gapTimer.Stop()
		p.gapTimer = nil
	}
	p.gapTimerMu.Unlock()

	return nil
}

// onUpdateReceived is the OnUpdateReceived hook. It runs synchronously in the
// session receive goroutine and MUST be fast. All expensive work (persistence,
// recovery) is deferred to background goroutines.
func (p *Plugin) onUpdateReceived(_ *telegram.Client, updates tg.UpdatesClass) {
	if p.store == nil {
		return
	}
	// Update last-received timestamp for the idle watchdog.
	p.lastUpdate.Store(time.Now().UnixNano())

	// Process channel-scoped updates for per-channel gap detection.
	if p.channels != nil {
		p.channels.processIncoming(updates)
	}

	info, tooLong, items := extractBatch(updates)

	// UpdatesTooLong: server signals the client should call getDifference.
	if tooLong {
		p.triggerGapRecovery(context.Background(), "updatesTooLong")
		return
	}

	// For Updates/UpdatesCombined, aggregate pts from individual updates.
	if len(items) > 0 {
		var aggInfo updateInfo
		aggInfo.date = info.date
		aggInfo.seq = info.seq
		aggInfo.seqStart = info.seqStart
		for _, upd := range items {
			m := extractUpdateMeta(upd)
			mergeMeta(&aggInfo, &m)
		}
		info = aggInfo
	}

	if info.pts == 0 && info.qts == 0 && info.seq == 0 && info.date == 0 {
		return // nothing to track
	}
	// Cold start: no baseline state yet. Accept the first update as the
	// baseline rather than treating it as a gap. This happens when there
	// is no persisted state and the initial updates.getState has not run.
	p.mu.RLock()
	hasState := p.hasState
	cur := p.state
	p.mu.RUnlock()

	if !hasState {
		p.advanceState(info)
		return
	}

	// Seq-based gap detection for Updates/UpdatesCombined batches.
	if info.seq > 0 {
		switch classifySeq(cur, info) {
		case gapSeq:
			p.triggerGapRecovery(context.Background(), "seq-gap")
			return
		case gapDuplicate:
			return // entire batch is stale
		}
	}

	// Classify against current state.
	kind := classifyAccount(cur, info)

	switch kind {
	case gapDuplicate:
		return
	case gapAccount:
		// Do NOT advance state — the gap will be recovered via
		// getDifference, which fetches all updates from the current
		// state to the gap point and updates state accordingly.
		p.triggerGapRecovery(context.Background(), "pts-gap")
		return
	case gapNone:
		p.advanceState(info)
	}
}

// onReconnect is the OnReconnect hook. It triggers gap recovery to fetch
// updates missed during the disconnect, unless skipReconnect is set.
func (p *Plugin) onReconnect(client *telegram.Client) {
	if p.store == nil || p.opts.skipReconnect {
		return
	}
	go p.recoverAccount(context.Background(), "reconnect")
	if p.channels != nil {
		p.channels.recoverAll(context.Background())
	}
}

// advanceState updates the in-memory state and signals the save goroutine.
// Called from the update hook — must be fast.
func (p *Plugin) advanceState(info updateInfo) {
	p.mu.Lock()
	if info.pts > 0 {
		p.state.Pts = info.pts
	}
	if info.qts > 0 {
		p.state.Qts = info.qts
	}
	if info.seq > 0 {
		p.state.Seq = info.seq
	}
	if info.date > 0 {
		p.state.Date = info.date
	}
	p.hasState = true
	p.mu.Unlock()

	// Non-blocking signal to save goroutine.
	select {
	case p.saveCh <- struct{}{}:
	default:
	}
}

// triggerGapRecovery debounces getDifference calls. If gapBuffer > 0, the
// recovery is deferred by gapBuffer; if the gap self-resolves via the next
// update, the timer is cancelled.
func (p *Plugin) triggerGapRecovery(ctx context.Context, reason string) {
	if p.opts.gapBuffer <= 0 {
		go p.recoverAccount(ctx, reason)
		return
	}

	p.gapTimerMu.Lock()
	defer p.gapTimerMu.Unlock()

	if p.gapTimer != nil {
		return // a recovery is already pending
	}

	p.gapTimer = time.AfterFunc(p.opts.gapBuffer, func() {
		p.gapTimerMu.Lock()
		p.gapTimer = nil
		p.gapTimerMu.Unlock()
		p.recoverAccount(ctx, reason)
	})
}

// recoverAccount calls updates.getDifference in a loop until all missed
// updates are fetched, then dispatches recovered updates to handlers.
func (p *Plugin) recoverAccount(ctx context.Context, reason string) {
	if !p.recovering.CompareAndSwap(false, true) {
		return
	}
	defer p.recovering.Store(false)

	if p.rpc == nil && p.client != nil {
		p.rpc = p.client.Raw()
	}
	if p.rpc == nil {
		return
	}
	if p.channels != nil && p.channels.rpc == nil {
		p.channels.rpc = p.rpc
	}

	p.log.Debug("updates-recovery: starting gap recovery", "reason", reason)

	maxIter := p.opts.maxIterations
	for iter := 0; maxIter == 0 || iter < maxIter; iter++ {
		p.mu.RLock()
		pts, date, qts := p.state.Pts, p.state.Date, p.state.Qts
		p.mu.RUnlock()

		req := &tg.UpdatesGetDifferenceRequest{
			PTS:  pts,
			Date: date,
			Qts:  qts,
		}

		diff, err := p.rpc.UpdatesGetDifference(ctx, req)
		if err != nil {
			p.log.Warn("updates-recovery: getDifference failed", "error", err)
			return
		}

		done, err := p.applyDifference(ctx, diff)
		if err != nil {
			p.log.Warn("updates-recovery: applyDifference failed", "error", err)
			return
		}
		if done {
			p.log.Debug("updates-recovery: gap recovery complete", "reason", reason)
			p.signalSave()
			return
		}
	}
	p.log.Warn("updates-recovery: hit iteration cap", "iterations", maxIter)
	p.signalSave()
}

// applyDifference processes one getDifference response, dispatching recovered
// updates to handlers and advancing state. Returns done=true when no more
// pages remain.
func (p *Plugin) applyDifference(ctx context.Context, diff tg.DifferenceClass) (bool, error) {
	switch d := diff.(type) {
	case *tg.UpdatesDifferenceEmpty:
		p.advanceState(updateInfo{date: d.Date, seq: d.Seq})
		return true, nil

	case *tg.UpdatesDifference:
		p.dispatchRecovered(d.NewMessages, d.OtherUpdates, d.Users, d.Chats)
		if d.State != nil {
			p.advanceState(updateInfo{
				pts:  d.State.PTS,
				qts:  d.State.Qts,
				date: d.State.Date,
				seq:  d.State.Seq,
			})
		}
		return true, nil

	case *tg.UpdatesDifferenceSlice:
		p.dispatchRecovered(d.NewMessages, d.OtherUpdates, d.Users, d.Chats)
		if d.IntermediateState != nil {
			p.advanceState(updateInfo{
				pts:  d.IntermediateState.PTS,
				qts:  d.IntermediateState.Qts,
				date: d.IntermediateState.Date,
				seq:  d.IntermediateState.Seq,
			})
		}
		return false, nil // more pages

	case *tg.UpdatesDifferenceTooLong:
		p.advanceState(updateInfo{pts: d.PTS})
		return false, nil // retry with new pts

	default:
		return true, fmt.Errorf("unknown difference type %T", diff)
	}
}

// dispatchRecovered wraps recovered messages and updates into a tg.Updates
// batch and dispatches them through the client's handler pipeline.
func (p *Plugin) dispatchRecovered(messages []tg.MessageClass, updates []tg.UpdateClass, users []tg.UserClass, chats []tg.ChatClass) {
	if p.client == nil {
		return
	}
	all := make([]tg.UpdateClass, 0, len(messages)+len(updates))
	for _, msg := range messages {
		all = append(all, &tg.UpdateNewMessage{Message: msg})
	}
	all = append(all, updates...)
	if len(all) == 0 {
		return
	}
	p.client.HandleUpdates(&tg.Updates{
		Updates: all,
		Users:   users,
		Chats:   chats,
	})
}

// --- debounced persistence ---

func (p *Plugin) saveLoop() {
	interval := p.opts.saveInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-p.saveCh:
			// Debounce: wait for the tick rather than saving immediately.
		case <-ticker.C:
			p.flushState()
		}
	}
}

func (p *Plugin) signalSave() {
	select {
	case p.saveCh <- struct{}{}:
	default:
	}
}

func (p *Plugin) flushState() {
	if p.store == nil {
		return
	}
	p.mu.RLock()
	if !p.hasState {
		p.mu.RUnlock()
		return
	}
	s := p.state
	p.mu.RUnlock()

	if err := p.store.SaveState(&s); err != nil {
		p.log.Warn("updates-recovery: save state failed", "error", err)
	}
}

// State returns the current tracked state. Primarily for diagnostics.
func (p *Plugin) State() State {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state
}

// idleWatchdog periodically checks whether updates have stopped arriving.
// If no update is received within idleTimeout, it triggers gap recovery
// to catch server-side update stalls.
func (p *Plugin) idleWatchdog(ctx context.Context) {
	checkInterval := time.Minute
	if p.opts.idleTimeout > 0 && p.opts.idleTimeout < 3*checkInterval {
		checkInterval = p.opts.idleTimeout / 3
	}
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			last := p.lastUpdate.Load()
			if last == 0 {
				continue
			}
			elapsed := time.Since(time.Unix(0, last))
			if elapsed > p.opts.idleTimeout {
				p.log.Debug("updates-recovery: idle timeout, triggering recovery",
					"idle", elapsed.String(), "timeout", p.opts.idleTimeout.String())
				p.triggerGapRecovery(context.Background(), "idle-timeout")
			}
		}
	}
}

// HandleAffected feeds pts feedback from RPC responses (e.g. from
// messages.readHistory, messages.deleteMessages) into the update state.
// Without this, local pts desyncs after mutations that the server confirms
// with a pts bump but no corresponding update.
//
// When the plugin is registered via [Client.Use], this is called automatically
// via an invoker middleware. It is also safe to call manually after RPC calls
// that return messages.affectedMessages or messages.affectedHistory.
func (p *Plugin) HandleAffected(result tg.TLObject) {
	if p.store == nil {
		return
	}
	switch r := result.(type) {
	case *tg.MessagesAffectedMessages:
		if r.PTS > 0 {
			p.advanceState(updateInfo{pts: r.PTS, ptsCount: r.PTSCount})
		}
	case *tg.MessagesAffectedHistory:
		if r.PTS > 0 {
			p.advanceState(updateInfo{pts: r.PTS, ptsCount: r.PTSCount})
		}
	}
}

// affectedInvoker wraps a tg.Invoker to capture pts feedback from RPC
// responses containing messages.affectedMessages or messages.affectedHistory.
type affectedInvoker struct {
	next   tg.Invoker
	plugin *Plugin
}

func (a *affectedInvoker) RPCInvoke(ctx context.Context, input tg.TLObject, decode func(*tg.Reader) (tg.TLObject, error)) (tg.TLObject, error) {
	result, err := a.next.RPCInvoke(ctx, input, decode)
	if err == nil {
		a.plugin.HandleAffected(result)
	}
	return result, err
}

func (a *affectedInvoker) RPCInvokeRaw(ctx context.Context, input tg.TLObject) ([]byte, error) {
	return a.next.RPCInvokeRaw(ctx, input)
}
