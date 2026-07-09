package updatesrecovery

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mtgo-labs/mtgo/tg"
)

// fakeChannelRPC tracks getChannelDifference calls and returns canned responses.
type fakeChannelRPC struct {
	responses []tg.ChannelDifferenceClass
	calls     atomic.Int32
}

func (f *fakeChannelRPC) UpdatesGetChannelDifference(_ context.Context, req *tg.UpdatesGetChannelDifferenceRequest) (tg.ChannelDifferenceClass, error) {
	idx := f.calls.Add(1) - 1
	if int(idx) >= len(f.responses) {
		return &tg.UpdatesChannelDifferenceEmpty{Final: true, PTS: req.PTS}, nil
	}
	return f.responses[idx], nil
}

func (f *fakeChannelRPC) UpdatesGetDifference(_ context.Context, _ *tg.UpdatesGetDifferenceRequest) (tg.DifferenceClass, error) {
	return &tg.UpdatesDifferenceEmpty{}, nil
}

type nilLogger struct{}

func (nilLogger) Debug(string, ...any) {}
func (nilLogger) Warn(string, ...any)  {}
func (nilLogger) Error(string, ...any) {}

func testLogger() Logger { return nilLogger{} }

func newTestChannelStore() *memoryStore { return &memoryStore{} }

func TestChannelManagerGapDetection(t *testing.T) {
	cm := newChannelManager(newTestChannelStore(), &fakeChannelRPC{}, testLogger(), nil)
	cm.mu.Lock()
	cm.channels[123] = &channelInfo{pts: 8}
	cm.mu.Unlock()

	cm.checkChannelUpdate(&tg.UpdateDeleteChannelMessages{
		ChannelID: 123, Messages: []int32{1}, PTS: 10, PTSCount: 2,
	})

	cm.mu.Lock()
	got := cm.channels[123].pts
	cm.mu.Unlock()
	if got != 10 {
		t.Fatalf("after gap: pts=%d, want 10", got)
	}
}

func TestChannelManagerInSequence(t *testing.T) {
	cm := newChannelManager(newTestChannelStore(), &fakeChannelRPC{}, testLogger(), nil)
	cm.mu.Lock()
	cm.channels[100] = &channelInfo{pts: 50}
	cm.mu.Unlock()

	cm.checkChannelUpdate(&tg.UpdateDeleteChannelMessages{
		ChannelID: 100, PTS: 51, PTSCount: 1,
	})

	cm.mu.Lock()
	got := cm.channels[100].pts
	cm.mu.Unlock()
	if got != 51 {
		t.Fatalf("after in-sequence: pts=%d, want 51", got)
	}
}

func TestChannelManagerDuplicate(t *testing.T) {
	cm := newChannelManager(newTestChannelStore(), &fakeChannelRPC{}, testLogger(), nil)
	cm.mu.Lock()
	cm.channels[100] = &channelInfo{pts: 50}
	cm.mu.Unlock()

	cm.checkChannelUpdate(&tg.UpdateDeleteChannelMessages{
		ChannelID: 100, PTS: 50, PTSCount: 1,
	})

	cm.mu.Lock()
	got := cm.channels[100].pts
	cm.mu.Unlock()
	if got != 50 {
		t.Fatalf("after duplicate: pts=%d, want 50", got)
	}
}

func TestChannelManagerRecoveryFinal(t *testing.T) {
	rpc := &fakeChannelRPC{
		responses: []tg.ChannelDifferenceClass{
			&tg.UpdatesChannelDifference{Final: true, PTS: 100},
		},
	}
	store := newTestChannelStore()
	cm := newChannelManager(store, rpc, testLogger(), nil)
	cm.mu.Lock()
	cm.channels[200] = &channelInfo{pts: 90, accessHash: 12345}
	cm.mu.Unlock()

	cm.recoverChannel(context.Background(), 200, 12345, 90)

	if rpc.calls.Load() != 1 {
		t.Fatalf("expected 1 RPC call, got %d", rpc.calls.Load())
	}
	cm.mu.Lock()
	got := cm.channels[200].pts
	cm.mu.Unlock()
	if got != 100 {
		t.Fatalf("after recovery: pts=%d, want 100", got)
	}
}

func TestChannelManagerRecoveryChannelPrivate(t *testing.T) {
	cm := newChannelManager(newTestChannelStore(), &errRPC{"CHANNEL_PRIVATE"}, testLogger(), nil)
	cm.mu.Lock()
	cm.channels[300] = &channelInfo{pts: 50, accessHash: 999}
	cm.mu.Unlock()

	cm.recoverChannel(context.Background(), 300, 999, 50)

	cm.mu.Lock()
	_, exists := cm.channels[300]
	cm.mu.Unlock()
	if exists {
		t.Fatal("channel should be removed after CHANNEL_PRIVATE")
	}
}

func TestChannelManagerCacheAccessHash(t *testing.T) {
	cm := newChannelManager(newTestChannelStore(), &fakeChannelRPC{}, testLogger(), nil)
	cm.cacheAccessHashes(&tg.Updates{
		Chats: []tg.ChatClass{&tg.Channel{ID: 500, AccessHash: 0xabc}},
	})

	cm.mu.Lock()
	info, exists := cm.channels[500]
	cm.mu.Unlock()
	if !exists || info.accessHash != 0xabc {
		t.Fatalf("channel not cached correctly: exists=%v hash=%x", exists, info.accessHash)
	}
}

func TestChannelManagerRecoverAll(t *testing.T) {
	rpc := &fakeChannelRPC{
		responses: []tg.ChannelDifferenceClass{
			&tg.UpdatesChannelDifferenceEmpty{Final: true, PTS: 42},
			&tg.UpdatesChannelDifferenceEmpty{Final: true, PTS: 42},
		},
	}
	cm := newChannelManager(newTestChannelStore(), rpc, testLogger(), nil)
	cm.mu.Lock()
	cm.channels[1] = &channelInfo{pts: 10, accessHash: 100}
	cm.channels[2] = &channelInfo{pts: 20, accessHash: 200}
	cm.mu.Unlock()

	cm.recoverAll(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cm.mu.Lock()
		p1, ok1 := cm.channels[1]
		p2, ok2 := cm.channels[2]
		cm.mu.Unlock()
		if ok1 && ok2 && p1.pts == 42 && p2.pts == 42 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for channel recovery")
}

func TestChannelStorePersistence(t *testing.T) {
	store := newTestChannelStore()
	cm := newChannelManager(store, &fakeChannelRPC{}, testLogger(), nil)
	cm.setChannelPts(700, 300)

	states, _ := store.LoadAllChannelStates()
	found := false
	for _, s := range states {
		if s.ChannelID == 700 && s.PTS == 300 {
			found = true
		}
	}
	if !found {
		t.Fatal("channel state not persisted")
	}

	cm.removeChannel(700)
	states, _ = store.LoadAllChannelStates()
	for _, s := range states {
		if s.ChannelID == 700 {
			t.Fatal("channel should be deleted from store")
		}
	}
}


func TestChannelManagerLoadPersisted(t *testing.T) {
	store := newTestChannelStore()
	_ = store.SaveChannelState(100, 0xabc, 55)

	cm := newChannelManager(store, &fakeChannelRPC{}, testLogger(), nil)
	if err := cm.loadPersisted(); err != nil {
		t.Fatalf("loadPersisted: %v", err)
	}

	cm.mu.Lock()
	info, exists := cm.channels[100]
	cm.mu.Unlock()
	if !exists || info.pts != 55 || info.accessHash != 0xabc {
		t.Fatalf("loaded wrong: exists=%v pts=%d hash=%x", exists, info.pts, info.accessHash)
	}
}

type errRPC struct{ err string }

func (e *errRPC) UpdatesGetChannelDifference(context.Context, *tg.UpdatesGetChannelDifferenceRequest) (tg.ChannelDifferenceClass, error) {
	return nil, &testErr{e.err}
}
func (e *errRPC) UpdatesGetDifference(context.Context, *tg.UpdatesGetDifferenceRequest) (tg.DifferenceClass, error) {
	return nil, &testErr{e.err}
}

type testErr struct{ msg string }

func (e *testErr) Error() string { return e.msg }
