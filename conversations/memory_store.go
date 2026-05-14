package conversations

import (
	"encoding/json"
	"sync"
)

// MemoryStore is an in-memory ConversationStore suitable for single-process
// bots that do not need state to survive restarts.
type MemoryStore struct {
	mu    sync.RWMutex
	state map[StoreKey]*ConversationState
}

// NewMemoryStore returns a ready-to-use in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		state: make(map[StoreKey]*ConversationState),
	}
}

func (m *MemoryStore) Save(key StoreKey, state *ConversationState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *state
	if state.Data != nil {
		cp.Data = make(json.RawMessage, len(state.Data))
		copy(cp.Data, state.Data)
	}
	m.state[key] = &cp
	return nil
}

func (m *MemoryStore) Load(key StoreKey) (*ConversationState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.state[key]
	if !ok {
		return nil, nil
	}
	cp := *s
	if s.Data != nil {
		cp.Data = make(json.RawMessage, len(s.Data))
		copy(cp.Data, s.Data)
	}
	return &cp, nil
}

func (m *MemoryStore) Delete(key StoreKey) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.state, key)
	return nil
}

func (m *MemoryStore) List() ([]StoreKey, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]StoreKey, 0, len(m.state))
	for k := range m.state {
		keys = append(keys, k)
	}
	return keys, nil
}
