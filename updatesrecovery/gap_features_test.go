package updatesrecovery

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mtgo-labs/mtgo/tg"
)

// --- G9: seq-based gap detection ---

func TestClassifySeq(t *testing.T) {
	tests := []struct {
		name  string
		state State
		info  updateInfo
		want  gapKind
	}{
		{
			name:  "combined in sequence",
			state: State{Seq: 5},
			info:  updateInfo{seq: 10, seqStart: 6},
			want:  gapNone,
		},
		{
			name:  "combined gap",
			state: State{Seq: 5},
			info:  updateInfo{seq: 15, seqStart: 10},
			want:  gapSeq,
		},
		{
			name:  "combined duplicate",
			state: State{Seq: 10},
			info:  updateInfo{seq: 8, seqStart: 5},
			want:  gapDuplicate,
		},
		{
			name:  "plain updates in sequence",
			state: State{Seq: 5},
			info:  updateInfo{seq: 6},
			want:  gapNone,
		},
		{
			name:  "plain updates gap",
			state: State{Seq: 5},
			info:  updateInfo{seq: 10},
			want:  gapSeq,
		},
		{
			name:  "plain updates duplicate",
			state: State{Seq: 10},
			info:  updateInfo{seq: 10},
			want:  gapDuplicate,
		},
		{
			name:  "no seq info",
			state: State{},
			info:  updateInfo{date: 100},
			want:  gapNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifySeq(tt.state, tt.info)
			if got != tt.want {
				t.Fatalf("classifySeq = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeqGapDetection(t *testing.T) {
	store := NewMemoryStore()
	rpc := &fakeRPC{
		diffs: []tg.DifferenceClass{
			&tg.UpdatesDifferenceEmpty{Date: 200, Seq: 10},
		},
	}
	p := newTestPlugin(t, store)
	p.rpc = rpc

	// Establish initial state with seq=5.
	p.mu.Lock()
	p.state = State{Pts: 10, Seq: 5, Date: 100}
	p.hasState = true
	p.mu.Unlock()

	// Send UpdatesCombined with seqStart=10 (gap: expected 6).
	p.onUpdateReceived(nil, &tg.UpdatesCombined{
		SeqStart: 10,
		Seq:      15,
		Date:     200,
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{PTS: 11, PTSCount: 1},
		},
	})

	// Seq gap should trigger recovery.
	deadline := time.After(2 * time.Second)
	for atomic.LoadInt32(&rpc.calls) == 0 {
		select {
		case <-deadline:
			t.Fatalf("seq gap did not trigger recovery (calls=%d)", atomic.LoadInt32(&rpc.calls))
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestSeqDuplicateIgnored(t *testing.T) {
	store := NewMemoryStore()
	rpc := &fakeRPC{}
	p := newTestPlugin(t, store)
	p.rpc = rpc

	p.mu.Lock()
	p.state = State{Pts: 10, Seq: 10, Date: 100}
	p.hasState = true
	p.mu.Unlock()

	// Send UpdatesCombined with seqStart=5 (duplicate: <= state.Seq=10).
	p.onUpdateReceived(nil, &tg.UpdatesCombined{
		SeqStart: 5,
		Seq:      8,
		Date:     200,
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{PTS: 11, PTSCount: 1},
		},
	})

	time.Sleep(100 * time.Millisecond)
	if calls := atomic.LoadInt32(&rpc.calls); calls != 0 {
		t.Fatalf("getDifference called %d times for seq duplicate, want 0", calls)
	}

	// State should not change.
	if s := p.State(); s.Seq != 10 || s.Pts != 10 {
		t.Fatalf("state changed for duplicate seq: %+v", s)
	}
}

func TestSeqInSequenceAdvances(t *testing.T) {
	store := NewMemoryStore()
	rpc := &fakeRPC{}
	p := newTestPlugin(t, store)
	p.rpc = rpc

	p.mu.Lock()
	p.state = State{Pts: 10, Seq: 5, Date: 100}
	p.hasState = true
	p.mu.Unlock()

	// Send UpdatesCombined with seqStart=6 (in sequence: == state.Seq+1).
	p.onUpdateReceived(nil, &tg.UpdatesCombined{
		SeqStart: 6,
		Seq:      10,
		Date:     200,
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{PTS: 11, PTSCount: 1},
		},
	})

	time.Sleep(100 * time.Millisecond)
	if calls := atomic.LoadInt32(&rpc.calls); calls != 0 {
		t.Fatalf("getDifference called %d times for in-sequence update, want 0", calls)
	}

	s := p.State()
	if s.Seq != 10 {
		t.Fatalf("state Seq = %d, want 10", s.Seq)
	}
	if s.Pts != 11 {
		t.Fatalf("state Pts = %d, want 11", s.Pts)
	}
}

// --- G6: HandleAffected pts feedback ---

func TestHandleAffectedMessages(t *testing.T) {
	store := NewMemoryStore()
	p := newTestPlugin(t, store)

	// Establish initial state.
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      10,
		PTSCount: 1,
		Date:     100,
	})

	// Simulate RPC response with affected pts.
	p.HandleAffected(&tg.MessagesAffectedMessages{
		PTS:      15,
		PTSCount: 5,
	})

	if s := p.State(); s.Pts != 15 {
		t.Fatalf("state Pts = %d, want 15 after HandleAffected", s.Pts)
	}
}

func TestHandleAffectedHistory(t *testing.T) {
	store := NewMemoryStore()
	p := newTestPlugin(t, store)

	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      10,
		PTSCount: 1,
		Date:     100,
	})

	p.HandleAffected(&tg.MessagesAffectedHistory{
		PTS:      20,
		PTSCount: 10,
	})

	if s := p.State(); s.Pts != 20 {
		t.Fatalf("state Pts = %d, want 20 after HandleAffected", s.Pts)
	}
}

func TestHandleAffectedNilStore(t *testing.T) {
	p := New(nil)
	// Should be a no-op, not panic.
	p.HandleAffected(&tg.MessagesAffectedMessages{PTS: 10, PTSCount: 1})
}

func TestHandleAffectedNonAffectedType(t *testing.T) {
	store := NewMemoryStore()
	p := newTestPlugin(t, store)

	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      10,
		PTSCount: 1,
		Date:     100,
	})

	// Non-affected types should be ignored.
	p.HandleAffected(&tg.UpdatesDifferenceEmpty{})

	if s := p.State(); s.Pts != 10 {
		t.Fatalf("state Pts = %d, want 10 (unaffected by non-affected type)", s.Pts)
	}
}

// --- G4: idle update watchdog ---

func TestIdleWatchdogTriggersRecovery(t *testing.T) {
	store := NewMemoryStore()
	rpc := &fakeRPC{
		diffs: []tg.DifferenceClass{
			&tg.UpdatesDifferenceEmpty{Date: 200, Seq: 5},
		},
	}
	p := newTestPlugin(t, store, WithIdleTimeout(50*time.Millisecond))
	p.rpc = rpc
	p.hasState = true
	p.state = State{Pts: 5, Date: 100}

	// Simulate an update received long ago.
	p.lastUpdate.Store(time.Now().Add(-1 * time.Hour).UnixNano())

	// Start the watchdog manually (normally done by Start).
	p.stopCh = make(chan struct{})
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.idleWatchdog(context.Background())
	}()
	defer func() {
		close(p.stopCh)
		p.wg.Wait()
	}()

	// Wait for recovery to be triggered.
	deadline := time.After(2 * time.Second)
	for atomic.LoadInt32(&rpc.calls) == 0 {
		select {
		case <-deadline:
			t.Fatalf("idle watchdog did not trigger recovery (calls=%d)", atomic.LoadInt32(&rpc.calls))
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
func TestIdleWatchdogNoTriggerWhenRecent(t *testing.T) {
	store := NewMemoryStore()
	rpc := &fakeRPC{}
	p := newTestPlugin(t, store, WithIdleTimeout(500*time.Millisecond))
	p.rpc = rpc
	p.hasState = true
	p.state = State{Pts: 5, Date: 100}

	// Simulate a recent update.
	p.lastUpdate.Store(time.Now().UnixNano())

	p.stopCh = make(chan struct{})
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.idleWatchdog(context.Background())
	}()

	// Wait within the idle timeout — should not trigger.
	time.Sleep(300 * time.Millisecond)
	close(p.stopCh)
	p.wg.Wait()

	if calls := atomic.LoadInt32(&rpc.calls); calls != 0 {
		t.Fatalf("idle watchdog triggered recovery %d times for recent update", calls)
	}
}

func TestIdleWatchdogDisabled(t *testing.T) {
	store := NewMemoryStore()
	p := newTestPlugin(t, store, WithIdleTimeout(0))
	if p.opts.idleTimeout != 0 {
		t.Fatalf("idleTimeout = %v, want 0", p.opts.idleTimeout)
	}
}

func TestIdleWatchdogDefaultTimeout(t *testing.T) {
	store := NewMemoryStore()
	p := newTestPlugin(t, store)
	if p.opts.idleTimeout != 15*time.Minute {
		t.Fatalf("default idleTimeout = %v, want 15m", p.opts.idleTimeout)
	}
}

func TestIdleWatchdogNoTriggerBeforeFirstUpdate(t *testing.T) {
	store := NewMemoryStore()
	rpc := &fakeRPC{}
	p := newTestPlugin(t, store, WithIdleTimeout(50*time.Millisecond))
	p.rpc = rpc
	p.hasState = true
	p.state = State{Pts: 5, Date: 100}

	// lastUpdate is zero — no updates received yet.
	p.stopCh = make(chan struct{})
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.idleWatchdog(context.Background())
	}()

	time.Sleep(200 * time.Millisecond)
	close(p.stopCh)
	p.wg.Wait()

	if calls := atomic.LoadInt32(&rpc.calls); calls != 0 {
		t.Fatalf("idle watchdog triggered recovery before any update received (calls=%d)", calls)
	}
}

// TestAffectedInvokerMiddleware verifies the invoker wrapper captures pts.
func TestAffectedInvokerMiddleware(t *testing.T) {
	store := NewMemoryStore()
	p := newTestPlugin(t, store)

	// Establish initial state.
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      10,
		PTSCount: 1,
		Date:     100,
	})

	// Create an affectedInvoker wrapping a fake invoker that returns
	// MessagesAffectedMessages.
	fake := &fakeInvoker{
		result: &tg.MessagesAffectedMessages{PTS: 25, PTSCount: 15},
	}
	wrapped := &affectedInvoker{next: fake, plugin: p}

	result, err := wrapped.RPCInvoke(
		context.Background(),
		&tg.UpdatesGetDifferenceRequest{},
		func(r *tg.Reader) (tg.TLObject, error) {
			return fake.result, nil
		},
	)
	if err != nil {
		t.Fatalf("RPCInvoke error: %v", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	// The plugin state should have been advanced by HandleAffected.
	if s := p.State(); s.Pts != 25 {
		t.Fatalf("state Pts = %d, want 25 after invoker middleware", s.Pts)
	}
}

func TestAffectedInvokerMiddlewarePassesErrors(t *testing.T) {
	store := NewMemoryStore()
	p := newTestPlugin(t, store)

	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      10,
		PTSCount: 1,
		Date:     100,
	})

	fake := &fakeInvoker{err: errFake}
	wrapped := &affectedInvoker{next: fake, plugin: p}

	_, err := wrapped.RPCInvoke(
		context.Background(),
		&tg.UpdatesGetDifferenceRequest{},
		func(r *tg.Reader) (tg.TLObject, error) { return nil, nil },
	)
	if err != errFake {
		t.Fatalf("expected error to pass through, got %v", err)
	}

	// State should not have changed.
	if s := p.State(); s.Pts != 10 {
		t.Fatalf("state Pts = %d, want 10 (should not advance on error)", s.Pts)
	}
}

// --- helpers ---

var errFake = errString("fake rpc error")

type errString string

func (e errString) Error() string { return string(e) }

// fakeInvoker implements tg.Invoker for middleware tests.
type fakeInvoker struct {
	result tg.TLObject
	err    error
}

func (f *fakeInvoker) RPCInvoke(_ context.Context, _ tg.TLObject, _ func(*tg.Reader) (tg.TLObject, error)) (tg.TLObject, error) {
	return f.result, f.err
}

func (f *fakeInvoker) RPCInvokeRaw(_ context.Context, _ tg.TLObject) ([]byte, error) {
	return nil, f.err
}
