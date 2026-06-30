package fsm

import (
	"sync"
)

// MemoryStore is a concurrency-safe in-memory Store. Use it for testing or
// single-process bots that do not need state to survive restarts.
//
// It is NOT the default — the plugin auto-detects the client's storage adapter.
type MemoryStore struct {
	mu   sync.RWMutex
	data map[Key]map[string]Entry
}

// NewMemoryStore returns a ready-to-use in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[Key]map[string]Entry)}
}

func (m *MemoryStore) Set(key Key, field string, entry Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data[key] == nil {
		m.data[key] = make(map[string]Entry)
	}
	m.data[key][field] = entry
	return nil
}

func (m *MemoryStore) Get(key Key, field string) (Entry, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fields, ok := m.data[key]
	if !ok {
		return Entry{}, false, nil
	}
	entry, ok := fields[field]
	if !ok {
		return Entry{}, false, nil
	}
	return entry, true, nil
}

func (m *MemoryStore) Delete(key Key, field string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if fields, ok := m.data[key]; ok {
		delete(fields, field)
		if len(fields) == 0 {
			delete(m.data, key)
		}
	}
	return nil
}

func (m *MemoryStore) Clear(key Key) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

// Cleanup removes all expired entries and returns the count removed.
func (m *MemoryStore) Cleanup() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	removed := 0
	for key, fields := range m.data {
		for field, entry := range fields {
			if entry.Expired() {
				delete(fields, field)
				removed++
			}
		}
		if len(fields) == 0 {
			delete(m.data, key)
		}
	}
	return removed
}

// Compile-time interface check.
var (
	_ Store   = (*MemoryStore)(nil)
	_ Cleaner = (*MemoryStore)(nil)
)
