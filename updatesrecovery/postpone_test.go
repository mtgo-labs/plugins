package updatesrecovery

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/mtgo-labs/mtgo/tg"
)

// newPostponePlugin creates a plugin configured for postpone-buffer testing.
// gapBuffer controls the postpone deadline; postponeThreshold controls the
// small/large gap boundary (0 disables).
func newPostponePlugin(t *testing.T, rpc differenceRPC, gapBuffer time.Duration, opts ...Option) *Plugin {
	t.Helper()
	allOpts := append([]Option{WithGapBuffer(gapBuffer)}, opts...)
	p := New(NewMemoryStore(), allOpts...)
	p.opts.saveInterval = 0
	p.rpc = rpc
	p.hasState = true
	return p
}

// --- small gap postponed, then filled ---

func TestPostponeSmallGapFilled(t *testing.T) {
	rpc := &fakeRPC{
		diffs: []tg.DifferenceClass{
			&tg.UpdatesDifferenceEmpty{Date: 200, Seq: 5},
		},
	}
	p := newPostponePlugin(t, rpc, 200*time.Millisecond)
	p.state = State{Pts: 10, Date: 100}

	// Small gap: expected pts=11, got pts=12 (gap=2 < threshold 3).
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      12,
		PTSCount: 1,
		Date:     101,
	})

	// State should NOT have advanced (update is buffered).
	if s := p.State(); s.Pts != 10 {
		t.Fatalf("state Pts = %d, want 10 (update should be postponed)", s.Pts)
	}

	// The missing update arrives and fills the gap.
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      11,
		PTSCount: 1,
		Date:     102,
	})

	// Both updates should now be applied.
	if s := p.State(); s.Pts != 12 {
		t.Fatalf("state Pts = %d, want 12 (gap filled, postponed applied)", s.Pts)
	}

	// No getDifference should have been called.
	time.Sleep(350 * time.Millisecond) // wait past the deadline
	if calls := atomic.LoadInt32(&rpc.calls); calls != 0 {
		t.Fatalf("getDifference called %d times, want 0 (gap was filled)", calls)
	}
}

// --- small gap postponed, deadline expires, recovery triggered ---

func TestPostponeSmallGapTimeout(t *testing.T) {
	rpc := &fakeRPC{
		diffs: []tg.DifferenceClass{
			&tg.UpdatesDifferenceEmpty{Date: 200, Seq: 5},
		},
	}
	p := newPostponePlugin(t, rpc, 100*time.Millisecond)
	p.state = State{Pts: 10, Date: 100}

	// Small gap: expected pts=11, got pts=12 (gap=2 < 3).
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      12,
		PTSCount: 1,
		Date:     101,
	})

	// Wait for the deadline to fire and recovery to run.
	deadline := time.After(2 * time.Second)
	for atomic.LoadInt32(&rpc.calls) == 0 {
		select {
		case <-deadline:
			t.Fatalf("getDifference not called after postpone deadline (calls=%d)", atomic.LoadInt32(&rpc.calls))
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

// --- large gap triggers immediate recovery ---

func TestPostponeLargeGapImmediate(t *testing.T) {
	rpc := &fakeRPC{
		diffs: []tg.DifferenceClass{
			&tg.UpdatesDifferenceEmpty{Date: 200, Seq: 5},
		},
	}
	// gapBuffer=0 so triggerGapRecovery runs immediately; threshold=3 default.
	p := newPostponePlugin(t, rpc, 0)
	p.state = State{Pts: 10, Date: 100}

	// Large gap: expected pts=11, got pts=20 (gap=10 >= 3).
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      20,
		PTSCount: 1,
		Date:     101,
	})

	// Recovery should fire immediately (no postpone buffer).
	deadline := time.After(2 * time.Second)
	for atomic.LoadInt32(&rpc.calls) == 0 {
		select {
		case <-deadline:
			t.Fatalf("getDifference not called for large gap (calls=%d)", atomic.LoadInt32(&rpc.calls))
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// --- multiple small gaps buffered, then filled in order ---

func TestPostponeMultipleSmallGapsFilled(t *testing.T) {
	rpc := &fakeRPC{}
	// threshold=5 so gaps 2 and 3 are both postponed.
	p := newPostponePlugin(t, rpc, 500*time.Millisecond, WithPostponeThreshold(5))
	p.state = State{Pts: 10, Date: 100}

	// First small gap: expected 11, got 12 (gap=2 < 5).
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      12,
		PTSCount: 1,
		Date:     101,
	})

	// Second small gap: expected 11 (state still 10), got 13 (gap=3 < 5).
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      13,
		PTSCount: 1,
		Date:     102,
	})

	// State should not have advanced.
	if s := p.State(); s.Pts != 10 {
		t.Fatalf("state Pts = %d, want 10 (updates should be postponed)", s.Pts)
	}

	// The missing update arrives and fills the entire chain.
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      11,
		PTSCount: 1,
		Date:     103,
	})

	// All three updates should be applied.
	if s := p.State(); s.Pts != 13 {
		t.Fatalf("state Pts = %d, want 13 (all gaps filled)", s.Pts)
	}

	time.Sleep(550 * time.Millisecond)
	if calls := atomic.LoadInt32(&rpc.calls); calls != 0 {
		t.Fatalf("getDifference called %d times, want 0", calls)
	}
}

// --- postponeThreshold=0 disables postponement ---

func TestPostponeDisabledByThreshold(t *testing.T) {
	rpc := &fakeRPC{
		diffs: []tg.DifferenceClass{
			&tg.UpdatesDifferenceEmpty{Date: 200, Seq: 5},
		},
	}
	// gapBuffer > 0 but postponeThreshold=0: all gaps recover via the gapBuffer
	// debounce, no postponement.
	p := newPostponePlugin(t, rpc, 100*time.Millisecond, WithPostponeThreshold(0))
	p.state = State{Pts: 10, Date: 100}

	// Gap of 2 (would normally be postponed), but threshold=0 disables it.
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      12,
		PTSCount: 1,
		Date:     101,
	})

	// Recovery should fire after the gapBuffer debounce.
	deadline := time.After(2 * time.Second)
	for atomic.LoadInt32(&rpc.calls) == 0 {
		select {
		case <-deadline:
			t.Fatalf("getDifference not called with postponement disabled (calls=%d)", atomic.LoadInt32(&rpc.calls))
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

// --- large gap while postponed clears buffer and recovers ---

func TestPostponeLargeGapClearsBuffer(t *testing.T) {
	rpc := &fakeRPC{
		diffs: []tg.DifferenceClass{
			&tg.UpdatesDifferenceEmpty{Date: 200, Seq: 5},
		},
	}
	p := newPostponePlugin(t, rpc, 200*time.Millisecond)
	p.state = State{Pts: 10, Date: 100}

	// Small gap: postponed.
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      12,
		PTSCount: 1,
		Date:     101,
	})
	if s := p.State(); s.Pts != 10 {
		t.Fatalf("state Pts = %d, want 10 (postponed)", s.Pts)
	}

	// Large gap: clears postpone buffer, triggers recovery via gapTimer.
	p.onUpdateReceived(nil, &tg.UpdateShortMessage{
		PTS:      20,
		PTSCount: 1,
		Date:     102,
	})

	// Wait for recovery.
	deadline := time.After(2 * time.Second)
	for atomic.LoadInt32(&rpc.calls) == 0 {
		select {
		case <-deadline:
			t.Fatalf("getDifference not called after large gap (calls=%d)", atomic.LoadInt32(&rpc.calls))
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// Should have exactly one recovery call (postpone timer was cleared).
	if calls := atomic.LoadInt32(&rpc.calls); calls > 1 {
		t.Fatalf("getDifference called %d times, want <= 1 (postpone timer should be cleared)", calls)
	}
}
