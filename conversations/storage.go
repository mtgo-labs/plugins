package conversations

import (
	"encoding/json"
	"fmt"

	"github.com/mtgo-labs/storage"
)

type typedStorageAdapter struct {
	store storage.ConversationStore
}

// Storage wraps a storage.ConversationStore to satisfy the ConversationStore
// interface. Use this to bridge persistent backends into the plugin.
func Storage(cs storage.ConversationStore) ConversationStore {
	return &typedStorageAdapter{store: cs}
}

// WithStorage is an alias for Storage.
var WithStorage = Storage

func (s *typedStorageAdapter) Save(key StoreKey, state *ConversationState) error {
	data, err := json.Marshal(state.Data)
	if err != nil {
		return fmt.Errorf("conversations: marshal state: %w", err)
	}
	c := &storage.Conversation{
		ChatID: key.ChatID,
		UserID: key.UserID,
		Name:   state.Name,
		Step:   state.Step,
		Data:   data,
	}
	return s.store.SaveConversation(c)
}

func (s *typedStorageAdapter) Load(key StoreKey) (*ConversationState, error) {
	c, err := s.store.LoadConversation(key.ChatID, key.UserID)
	if err != nil {
		return nil, err
	}
	if c == nil {
		return nil, nil
	}
	var data json.RawMessage
	if len(c.Data) > 0 {
		data = json.RawMessage(c.Data)
	}
	return &ConversationState{
		Name: c.Name,
		Step: c.Step,
		Data: data,
	}, nil
}

func (s *typedStorageAdapter) Delete(key StoreKey) error {
	return s.store.DeleteConversation(key.ChatID, key.UserID)
}

func (s *typedStorageAdapter) List() ([]StoreKey, error) {
	// kvstorage.ConversationStore has no list method; conversation
	// restoration from persistent backends is not supported.
	return nil, nil
}
