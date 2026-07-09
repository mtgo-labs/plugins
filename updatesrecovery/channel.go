package updatesrecovery

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/mtgo-labs/mtgo/tg"
)

const (
	// channelDiffLimitUser is the max page size for getChannelDifference.
	channelDiffLimitUser = 100
)

// channelInfo tracks the per-channel state needed for gap detection and
// recovery.
type channelInfo struct {
	pts        int32
	accessHash int64
	recovering bool // prevents concurrent getChannelDifference for the same channel
}

// dispatchFunc dispatches recovered messages/updates through the handler pipeline.
type dispatchFunc func(messages []tg.MessageClass, updates []tg.UpdateClass, users []tg.UserClass, chats []tg.ChatClass)

// channelManager tracks per-channel pts and access hashes, detects gaps, and
// runs getChannelDifference recovery.
type channelManager struct {
	mu       sync.Mutex
	channels map[int64]*channelInfo

	channelStore ChannelStore
	rpc          channelRPC
	log          Logger
	dispatch     dispatchFunc
}

// channelRPC is the minimal interface for channel difference recovery.
type channelRPC interface {
	UpdatesGetChannelDifference(ctx context.Context, req *tg.UpdatesGetChannelDifferenceRequest) (tg.ChannelDifferenceClass, error)
}

// Logger is the minimal logging interface used by the channel manager.
type Logger interface {
	Debug(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

func newChannelManager(store ChannelStore, rpc channelRPC, log Logger, dispatch dispatchFunc) *channelManager {
	return &channelManager{
		channels:     make(map[int64]*channelInfo),
		channelStore: store,
		rpc:          rpc,
		log:          log,
		dispatch:     dispatch,
	}
}

// loadPersisted loads all channel states from the store into memory.
func (cm *channelManager) loadPersisted() error {
	if cm.channelStore == nil {
		return nil
	}
	states, err := cm.channelStore.LoadAllChannelStates()
	if err != nil {
		return err
	}
	cm.mu.Lock()
	for _, cs := range states {
		cm.channels[cs.ChannelID] = &channelInfo{
			pts:        cs.PTS,
			accessHash: cs.AccessHash,
		}
	}
	count := len(cm.channels)
	cm.mu.Unlock()
	if count > 0 && cm.log != nil {
		cm.log.Debug("updates-recovery: loaded channel states", "count", count)
	}
	return nil
}

// processIncoming scans an UpdatesClass for channel-scoped updates, caches
// access hashes from the Chats field, and checks each channel update for gaps.
func (cm *channelManager) processIncoming(updates tg.UpdatesClass) {
	cm.cacheAccessHashes(updates)

	var channelUpds []tg.UpdateClass
	switch u := updates.(type) {
	case *tg.Updates:
		channelUpds = filterChannelUpdates(u.Updates)
	case *tg.UpdatesCombined:
		channelUpds = filterChannelUpdates(u.Updates)
	}

	for _, upd := range channelUpds {
		cm.checkChannelUpdate(upd)
	}
}

// cacheAccessHashes extracts channel access hashes from the Chats field.
func (cm *channelManager) cacheAccessHashes(updates tg.UpdatesClass) {
	var chats []tg.ChatClass
	switch u := updates.(type) {
	case *tg.Updates:
		chats = u.Chats
	case *tg.UpdatesCombined:
		chats = u.Chats
	}
	for _, chat := range chats {
		if ch, ok := chat.(*tg.Channel); ok {
			cm.mu.Lock()
			info, exists := cm.channels[ch.ID]
			if !exists {
				cm.channels[ch.ID] = &channelInfo{accessHash: ch.AccessHash}
			} else if info.accessHash == 0 {
				info.accessHash = ch.AccessHash
			}
			cm.mu.Unlock()
		}
	}
}

// checkChannelUpdate examines a single channel-scoped update for gaps.
func (cm *channelManager) checkChannelUpdate(upd tg.UpdateClass) {
	chID, pts, ptsCount := extractChannelPts(upd)
	if chID == 0 || pts == 0 {
		return
	}

	cm.mu.Lock()
	info, exists := cm.channels[chID]

	// First time seeing this channel: initialize pts so the update fits
	// as the next expected one (localPts = pts - ptsCount).
	if !exists {
		info = &channelInfo{pts: pts - ptsCount}
		cm.channels[chID] = info
	}

	if pts <= info.pts {
		cm.mu.Unlock()
		return // duplicate or stale
	}

	expected := info.pts + ptsCount
	if pts == expected {
		info.pts = pts
		accessHash := info.accessHash
		cm.mu.Unlock()
		cm.persistChannel(chID, accessHash, pts)
		return
	}

	// Gap detected.
	accessHash := info.accessHash
	info.pts = pts
	recovering := info.recovering
	cm.mu.Unlock()

	if recovering {
		return
	}

	if cm.log != nil {
		cm.log.Debug("updates-recovery: channel pts gap", "channel", chID, "expected", expected, "got", pts)
	}

	go cm.recoverChannel(context.Background(), chID, accessHash, expected)
}

// recoverChannel calls getChannelDifference in a loop until all missed updates
// are fetched or a terminal condition is reached.
func (cm *channelManager) recoverChannel(ctx context.Context, channelID, accessHash int64, pts int32) {
	cm.mu.Lock()
	info, exists := cm.channels[channelID]
	if !exists {
		cm.mu.Unlock()
		return
	}
	if info.recovering {
		cm.mu.Unlock()
		return
	}
	info.recovering = true
	startPts := info.pts
	if pts > 0 && pts < startPts {
		startPts = pts
	}
	cm.mu.Unlock()

	defer func() {
		cm.mu.Lock()
		if info, ok := cm.channels[channelID]; ok {
			info.recovering = false
		}
		cm.mu.Unlock()
	}()

	if accessHash == 0 {
		if cm.log != nil {
			cm.log.Warn("updates-recovery: skipping channel recovery — no access hash", "channel", channelID)
		}
		return
	}

	inputChannel := &tg.InputChannel{
		ChannelID:  channelID,
		AccessHash: accessHash,
	}

	for range 100 {
		diff, err := cm.rpc.UpdatesGetChannelDifference(ctx, &tg.UpdatesGetChannelDifferenceRequest{
			Channel: inputChannel,
			Filter:  &tg.ChannelMessagesFilterEmpty{},
			PTS:     startPts,
			Limit:   channelDiffLimitUser,
		})
		if err != nil {
			if isChannelPrivateErr(err) {
				cm.removeChannel(channelID)
				if cm.log != nil {
					cm.log.Debug("updates-recovery: channel private, removing", "channel", channelID)
				}
				return
			}
			if cm.log != nil {
				cm.log.Warn("updates-recovery: getChannelDifference failed", "channel", channelID, "error", err)
			}
			return
		}

		done := cm.applyChannelDifference(channelID, diff)
		if done {
			return
		}

		if timeout := channelDiffTimeout(diff); timeout > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(timeout) * time.Second):
			}
		}
	}
}

// applyChannelDifference processes one getChannelDifference response, advancing
// channel pts and dispatching recovered updates. Returns true if recovery is
// complete (Final flag or empty difference).
func (cm *channelManager) applyChannelDifference(channelID int64, diff tg.ChannelDifferenceClass) bool {
	switch d := diff.(type) {
	case *tg.UpdatesChannelDifferenceEmpty:
		cm.setChannelPts(channelID, d.PTS)
		return true

	case *tg.UpdatesChannelDifference:
		cm.setChannelPts(channelID, d.PTS)
		if cm.dispatch != nil {
			cm.dispatch(d.NewMessages, d.OtherUpdates, d.Users, d.Chats)
		}
		return d.Final

	case *tg.UpdatesChannelDifferenceTooLong:
		if d.Dialog != nil {
			if dialog, ok := d.Dialog.(*tg.Dialog); ok {
				cm.setChannelPts(channelID, dialog.PTS)
			}
		}
		if cm.dispatch != nil {
			cm.dispatch(d.Messages, nil, d.Users, d.Chats)
		}
		return true
	}
	return true
}

// removeChannel deletes a channel from tracking and persistent storage.
func (cm *channelManager) removeChannel(channelID int64) {
	cm.mu.Lock()
	delete(cm.channels, channelID)
	cm.mu.Unlock()
	if cm.channelStore != nil {
		_ = cm.channelStore.DeleteChannelState(channelID)
	}
}

// setChannelPts updates the in-memory and persistent channel pts.
func (cm *channelManager) setChannelPts(channelID int64, pts int32) {
	if pts == 0 {
		return
	}
	cm.mu.Lock()
	info, exists := cm.channels[channelID]
	if !exists {
		info = &channelInfo{pts: pts}
		cm.channels[channelID] = info
	} else {
		info.pts = pts
	}
	accessHash := info.accessHash
	cm.mu.Unlock()
	cm.persistChannel(channelID, accessHash, pts)
}

// persistChannel saves channel state to the store if available.
func (cm *channelManager) persistChannel(channelID, accessHash int64, pts int32) {
	if cm.channelStore == nil {
		return
	}
	_ = cm.channelStore.SaveChannelState(channelID, accessHash, pts)
}

// recoverAll iterates all tracked channels and recovers each. Called on
// reconnect.
func (cm *channelManager) recoverAll(ctx context.Context) {
	cm.mu.Lock()
	type chSnapshot struct {
		id         int64
		accessHash int64
		pts        int32
	}
	var channels []chSnapshot
	for id, info := range cm.channels {
		if info.accessHash != 0 {
			channels = append(channels, chSnapshot{id, info.accessHash, info.pts})
		}
	}
	cm.mu.Unlock()

	for _, ch := range channels {
		go cm.recoverChannel(ctx, ch.id, ch.accessHash, ch.pts)
	}
}

// --- helpers ---

// extractChannelPts reads channel_id, pts, and pts_count from a channel-scoped
// update using reflection (via extractUpdateMeta).
func extractChannelPts(upd tg.UpdateClass) (channelID int64, pts, ptsCount int32) {
	m := extractUpdateMeta(upd)
	return m.channelID, m.pts, m.ptsCount
}

// filterChannelUpdates returns only updates that are channel-scoped.
func filterChannelUpdates(updates []tg.UpdateClass) []tg.UpdateClass {
	var result []tg.UpdateClass
	for _, upd := range updates {
		switch upd.(type) {
		case *tg.UpdateNewChannelMessage,
			*tg.UpdateEditChannelMessage,
			*tg.UpdateDeleteChannelMessages,
			*tg.UpdateChannelMessageForwards,
			*tg.UpdateChannelReadMessagesContents,
			*tg.UpdateChannelTooLong,
			*tg.UpdateChannelAvailableMessages:
			result = append(result, upd)
		}
	}
	return result
}

// channelDiffTimeout extracts the Timeout field from a ChannelDifferenceClass.
func channelDiffTimeout(diff tg.ChannelDifferenceClass) int32 {
	switch d := diff.(type) {
	case *tg.UpdatesChannelDifference:
		return d.Timeout
	case *tg.UpdatesChannelDifferenceTooLong:
		return d.Timeout
	case *tg.UpdatesChannelDifferenceEmpty:
		return d.Timeout
	}
	return 0
}

func isChannelPrivateErr(err error) bool {
	return strings.Contains(err.Error(), "CHANNEL_PRIVATE") ||
		strings.Contains(err.Error(), "CHANNEL_INVALID")
}
