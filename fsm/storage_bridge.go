package fsm

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/mtgo-labs/storage"
)

// storageBridge adapts a [storage.StateStore] to the plugin's [Store]
// interface. It handles JSON marshaling of values and conversion between
// time.Time expiry and unix timestamps.
type storageBridge struct {
	store storage.StateStore
}

// Storage wraps a [storage.StateStore] to satisfy the plugin's [Store]
// interface. Use this to bridge persistent backends into the plugin.
func Storage(s storage.StateStore) Store {
	return &storageBridge{store: s}
}

func (b *storageBridge) Set(key Key, field string, entry Entry) error {
	data, err := json.Marshal(entry.Value)
	if err != nil {
		return fmt.Errorf("fsm: marshal state value: %w", err)
	}
	var expiresAt int64
	if !entry.ExpiresAt.IsZero() {
		expiresAt = entry.ExpiresAt.Unix()
	}
	now := time.Now().Unix()
	return b.store.SaveState(&storage.StateEntry{
		Scope:     int(key.Scope),
		ChatID:    key.ChatID,
		UserID:    key.UserID,
		Key:       field,
		Value:     data,
		ExpiresAt: expiresAt,
		CreatedAt: now,
		UpdatedAt: now,
	})
}

func (b *storageBridge) Get(key Key, field string) (Entry, bool, error) {
	row, err := b.store.LoadState(int(key.Scope), key.ChatID, key.UserID, field)
	if err != nil {
		return Entry{}, false, fmt.Errorf("fsm: load state: %w", err)
	}
	if row == nil {
		return Entry{}, false, nil
	}
	var value any
	if len(row.Value) > 0 {
		if err := json.Unmarshal(row.Value, &value); err != nil {
			return Entry{}, false, fmt.Errorf("fsm: unmarshal state value: %w", err)
		}
	}
	var expiresAt time.Time
	if row.ExpiresAt > 0 {
		expiresAt = time.Unix(row.ExpiresAt, 0)
	}
	return Entry{Value: value, ExpiresAt: expiresAt}, true, nil
}

func (b *storageBridge) Delete(key Key, field string) error {
	return b.store.DeleteState(int(key.Scope), key.ChatID, key.UserID, field)
}

func (b *storageBridge) Clear(key Key) error {
	return b.store.ClearState(int(key.Scope), key.ChatID, key.UserID)
}

var _ Store = (*storageBridge)(nil)
