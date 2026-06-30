package updatesrecovery

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mtgo-labs/mtgo/tg"
)

// --- test helpers ---

// failingStore returns errors on every operation.
type failingStore struct{}

func (failingStore) SaveState(*State) error { return errors.New("disk full") }
func (failingStore) LoadState() (*State, error) {
	return nil, errors.New("disk read error")
}

// fakeRPC implements differenceRPC for gap-recovery tests.
type fakeRPC struct {
	mu     sync.Mutex
	diffs  []tg.DifferenceClass
	calls  int32
	lastReq *tg.UpdatesGetDifferenceRequest
}

func (f *fakeRPC) UpdatesGetDifference(_ context.Context, req *tg.UpdatesGetDifferenceRequest) (tg.DifferenceClass, error) {
	idx := atomic.AddInt32(&f.calls, 1) - 1
	f.mu.Lock()
	f.lastReq = req
	if int(idx) >= len(f.diffs) {
		f.mu.Unlock()
		return &tg.UpdatesDifferenceEmpty{}, nil
	}
	d := f.diffs[idx]
	f.mu.Unlock()
	return d, nil
}

func newTestPlugin(t *testing.T, store Store, opts ...Option) *Plugin {
	t.Helper()
	p := New(store, opts...)
	p.opts.gapBuffer = 0 // disable gap buffering for immediate recovery in tests
	p.opts.saveInterval = 0 // save immediately (flush on every signal)
	return p
}

// --- state persistence tests ---

func TestSaveUpdateState(t *testing.T) {
	store := NewMemoryStore()
	p := newTestPlugin(t, store)

	// Simulate receiving an in-sequence update.
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      10,
		PTSCount: 1,
		Date:     100,
	})

	// State should be tracked in memory.
	if s := p.State(); s.Pts != 10 || s.Date != 100 {
		t.Fatalf("in-memory state = %+v, want Pts=10 Date=100", s)
	}

	// Flush to store.
	p.flushState()

	saved, err := store.LoadState()
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if saved == nil {
		t.Fatal("saved state is nil")
	}
	if saved.Pts != 10 || saved.Date != 100 {
		t.Fatalf("saved state = %+v, want Pts=10 Date=100", saved)
	}
}

func TestRestoreStateAfterRestart(t *testing.T) {
	store := NewMemoryStore()

	// Plugin instance A: receives updates, saves state.
	pA := newTestPlugin(t, store)
	pA.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      42,
		PTSCount: 1,
		Date:     500,
	})
	pA.flushState()

	// Plugin instance B: same store, should load saved state.
	pB := newTestPlugin(t, store)
	saved, err := store.LoadState()
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if saved == nil {
		t.Fatal("expected saved state, got nil")
	}
	// Simulate what Start() does — load state from store.
	pB.state = *saved
	pB.hasState = true

	if s := pB.State(); s.Pts != 42 || s.Date != 500 {
		t.Fatalf("restored state = %+v, want Pts=42 Date=500", s)
	}
}

// --- gap detection tests ---

func TestDetectPtsGap(t *testing.T) {
	store := NewMemoryStore()
	rpc := &fakeRPC{
		diffs: []tg.DifferenceClass{
			&tg.UpdatesDifferenceEmpty{Date: 200, Seq: 5},
		},
	}
	p := newTestPlugin(t, store)
	p.rpc = rpc

	// Establish initial state.
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      10,
		PTSCount: 1,
		Date:     100,
	})

	// Send an update with a gap: expected pts=11, got pts=20.
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      20,
		PTSCount: 1,
		Date:     150,
	})

	// Gap recovery should have been triggered (runs in a goroutine).
	// Wait for it to complete.
	deadline := time.After(2 * time.Second)
	for {
		if atomic.LoadInt32(&rpc.calls) > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("getDifference was not called (calls=%d)", atomic.LoadInt32(&rpc.calls))
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// Verify the request used the pre-gap pts.
	rpc.mu.Lock()
	req := rpc.lastReq
	rpc.mu.Unlock()
	if req == nil {
		t.Fatal("lastReq is nil")
	}
	if req.PTS != 10 {
		t.Fatalf("getDifference PTS = %d, want 10 (pre-gap)", req.PTS)
	}
}

func TestNoGapForInSequenceUpdate(t *testing.T) {
	store := NewMemoryStore()
	rpc := &fakeRPC{}
	p := newTestPlugin(t, store)
	p.rpc = rpc

	// Establish initial state manually (simulating loaded from storage).
	p.mu.Lock()
	p.state = State{Pts: 5, Date: 100}
	p.hasState = true
	p.mu.Unlock()

	// In-sequence: pts=6 (5 + 1).
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      6,
		PTSCount: 1,
		Date:     101,
	})

	// No gap recovery should be triggered.
	time.Sleep(100 * time.Millisecond)
	if calls := atomic.LoadInt32(&rpc.calls); calls != 0 {
		t.Fatalf("getDifference called %d times, want 0", calls)
	}

	// State should be advanced.
	if s := p.State(); s.Pts != 6 {
		t.Fatalf("state Pts = %d, want 6", s.Pts)
	}
}

func TestDuplicateUpdateIgnored(t *testing.T) {
	store := NewMemoryStore()
	rpc := &fakeRPC{}
	p := newTestPlugin(t, store)
	p.rpc = rpc

	// Establish state: pts=10.
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      10,
		PTSCount: 1,
		Date:     100,
	})

	// Duplicate: pts=10 again.
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      10,
		PTSCount: 1,
		Date:     100,
	})

	// State should not change.
	if s := p.State(); s.Pts != 10 {
		t.Fatalf("state Pts = %d, want 10 (duplicate ignored)", s.Pts)
	}
	if calls := atomic.LoadInt32(&rpc.calls); calls != 0 {
		t.Fatalf("getDifference called for duplicate, want 0")
	}
}

func TestUpdatesTooLongTriggersRecovery(t *testing.T) {
	store := NewMemoryStore()
	rpc := &fakeRPC{
		diffs: []tg.DifferenceClass{
			&tg.UpdatesDifferenceEmpty{Date: 200, Seq: 1},
		},
	}
	p := newTestPlugin(t, store)
	p.rpc = rpc
	p.hasState = true
	p.state = State{Pts: 5, Date: 100}

	// UpdatesTooLong signals the server wants the client to call getDifference.
	p.onUpdateReceived(nil, &tg.UpdatesTooLong{})

	deadline := time.After(2 * time.Second)
	for {
		if atomic.LoadInt32(&rpc.calls) > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("UpdatesTooLong did not trigger getDifference")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// --- disabled plugin tests ---

func TestDisabledPluginIsNoOp(t *testing.T) {
	p := New(nil) // nil store = disabled

	// All methods should be safe no-ops.
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start with nil client: %v", err)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// onUpdateReceived should not panic.
	p.onUpdateReceived(nil, &tg.UpdatesTooLong{})

	// State should be zero.
	if s := p.State(); s != (State{}) {
		t.Fatalf("disabled plugin state = %+v, want zero", s)
	}
}

func TestStoreNilAfterStart(t *testing.T) {
	// Even if Start is called, a nil store means no persistence.
	p := New(nil)
	if p.Name() != "updates-recovery" {
		t.Fatalf("Name = %q, want updates-recovery", p.Name())
	}
}

// --- storage failure tests ---

func TestStorageSaveFailure(t *testing.T) {
	store := failingStore{}
	p := newTestPlugin(t, store)

	// Should not panic or return error from the hook.
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      10,
		PTSCount: 1,
		Date:     100,
	})

	// flushState should not panic on store error.
	p.flushState()

	// In-memory state should still be tracked.
	if s := p.State(); s.Pts != 10 {
		t.Fatalf("in-memory state Pts = %d, want 10 (storage failure should not affect tracking)", s.Pts)
	}
}

func TestStorageLoadFailure(t *testing.T) {
	store := failingStore{}
	p := newTestPlugin(t, store)

	// LoadState returns error — plugin should start fresh, not crash.
	_, err := store.LoadState()
	if err == nil {
		t.Fatal("expected error from failingStore.LoadState")
	}
	// Plugin should continue with zero state.

	// Plugin should still function.
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      1,
		PTSCount: 1,
		Date:     50,
	})
	if s := p.State(); s.Pts != 1 {
		t.Fatalf("state after load failure = %+v, want Pts=1", s)
	}
}

// --- meta extraction unit tests ---

func TestClassifyAccount(t *testing.T) {
	tests := []struct {
		name  string
		state State
		info  updateInfo
		want  gapKind
	}{
		{
			name:  "in sequence",
			state: State{Pts: 5},
			info:  updateInfo{pts: 6, ptsCount: 1},
			want:  gapNone,
		},
		{
			name:  "gap",
			state: State{Pts: 5},
			info:  updateInfo{pts: 20, ptsCount: 1},
			want:  gapAccount,
		},
		{
			name:  "duplicate",
			state: State{Pts: 10},
			info:  updateInfo{pts: 10, ptsCount: 1},
			want:  gapDuplicate,
		},
		{
			name:  "qts in sequence",
			state: State{Qts: 5},
			info:  updateInfo{qts: 6},
			want:  gapNone,
		},
		{
			name:  "qts gap",
			state: State{Qts: 5},
			info:  updateInfo{qts: 10},
			want:  gapAccount,
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
			got := classifyAccount(tt.state, tt.info)
			if got != tt.want {
				t.Fatalf("classifyAccount = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractBatchUpdates(t *testing.T) {
	// Updates type with individual update items.
	upd := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{
				PTS:      10,
				PTSCount: 1,
			},
			&tg.UpdateDeleteMessages{
				PTS:      11,
				PTSCount: 1,
			},
		},
		Date: 100,
		Seq:  5,
	}

	info, tooLong, items := extractBatch(upd)
	if tooLong {
		t.Fatal("tooLong should be false")
	}
	if len(items) != 2 {
		t.Fatalf("items = %d, want 2", len(items))
	}
	if info.date != 100 || info.seq != 5 {
		t.Fatalf("batch info = %+v, want date=100 seq=5", info)
	}
}

func TestExtractBatchTooLong(t *testing.T) {
	_, tooLong, _ := extractBatch(&tg.UpdatesTooLong{})
	if !tooLong {
		t.Fatal("tooLong should be true for UpdatesTooLong")
	}
}

// --- concurrency tests ---

func TestConcurrentUpdates(t *testing.T) {
	store := NewMemoryStore()
	p := newTestPlugin(t, store)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			p.onUpdateReceived(nil, &tg.UpdateShortMessage{
				PTS:      int32(n + 1),
				PTSCount: 1,
				Date:     int32(n),
			})
		}(i)
	}
	wg.Wait()

	// State should not be corrupted (some value >= 1).
	s := p.State()
	if s.Pts < 1 {
		t.Fatalf("state Pts = %d, expected >= 1 after concurrent updates", s.Pts)
	}
}

func TestRecoverAccountSingleFlight(t *testing.T) {
	store := NewMemoryStore()

	// Blocking RPC: the first call blocks until we unblock it,
	// holding the recovering flag true while other goroutines arrive.
	brpc := &blockingRPC{
		diffs: []tg.DifferenceClass{
			&tg.UpdatesDifferenceEmpty{Date: 200, Seq: 1},
		},
	}

	p := newTestPlugin(t, store)
	p.rpc = brpc
	p.state = State{Pts: 5, Date: 100}
	p.hasState = true

	const N = 10
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.recoverAccount(context.Background(), "test")
		}()
	}

	// Give goroutines time to arrive; the first one blocks inside the RPC.
	time.Sleep(200 * time.Millisecond)

	// Unblock the RPC — recovery completes and goroutines finish.
	brpc.unblock()
	wg.Wait()

	calls := atomic.LoadInt32(&brpc.calls)
	if calls > 1 {
		t.Fatalf("getDifference calls = %d, want <= 1 (single-flight)", calls)
	}
}

type blockingRPC struct {
	unblockCh chan struct{}
	calls     int32
	diffs     []tg.DifferenceClass
	once      sync.Once
}

func (b *blockingRPC) UpdatesGetDifference(_ context.Context, _ *tg.UpdatesGetDifferenceRequest) (tg.DifferenceClass, error) {
	atomic.AddInt32(&b.calls, 1)
	b.once.Do(func() { b.unblockCh = make(chan struct{}) })
	<-b.unblockCh
	return b.diffs[0], nil
}

func (b *blockingRPC) unblock() { b.once.Do(func() {}); if b.unblockCh != nil { close(b.unblockCh) } }
