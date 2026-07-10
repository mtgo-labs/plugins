// Package conversations provides a plugin for managing multi-step conversational
// flows in Telegram bots built on the mtgo framework.
//
// It allows registering named conversation handlers that track per-user state
// across multiple messages. Incoming updates are automatically routed to the
// active conversation for a given chat+user pair, pausing normal handler
// dispatch until the conversation completes or is cancelled.
//
// Conversation state is persisted through a pluggable ConversationStore. An
// in-memory store is provided by default; adapters for persistent backends are
// available via the Storage function.
//
// Basic usage:
//
//	p := conversations.New()
//	p.Register("signup", func(cc *conversations.ConversationContext) error {
//	    cc.SendMessage("What is your name?")
//	    ctx, err := cc.WaitMessage()
//	    // ... collect more steps
//	    return nil
//	})
//	client.Use(p)
//
// Entering a conversation from a handler:
//
//	p.Enter("signup", ctx)
//
// Sending /cancel at any time exits the active conversation.
package conversations

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	tg "github.com/mtgo-labs/mtgo/telegram"
	"github.com/mtgo-labs/storage"
)

// ConversationFunc is the handler for a named conversation. It receives a
// ConversationContext and should call Wait/WaitMessage/WaitCallback to receive
// subsequent user input. Return nil to end the conversation normally.
type ConversationFunc func(c *ConversationContext) error

// StoreKey uniquely identifies an active conversation by chat and user ID.
type StoreKey struct {
	ChatID int64
	UserID int64
}

// ConversationState holds the persistent state of an active conversation,
// including its registered name, the current step number, and arbitrary
// user data serialized as JSON.
type ConversationState struct {
	Name string          `json:"name"`
	Step int             `json:"step"`
	Data json.RawMessage `json:"data,omitempty"`
}

// ConversationStore persists conversation state across restarts.
// Implementations must be safe for concurrent use.
type ConversationStore interface {
	Save(key StoreKey, state *ConversationState) error
	Load(key StoreKey) (*ConversationState, error)
	Delete(key StoreKey) error
	List() ([]StoreKey, error)
}

// ConversationContext is the per-user runtime handle for an active conversation.
// It provides methods to wait for the next update, persist step data, and send
// messages without leaving the conversation flow.
type ConversationContext struct {
	plugin *Plugin
	client *tg.Client
	chatID int64
	userID int64
	ctx    context.Context
	notify chan *tg.Context
	done   chan struct{}
	name   string
	step   int
	data   json.RawMessage
}

type entryKey struct {
	chatID int64
	userID int64
}

// Plugin implements tg.Plugin to provide multi-step conversation management.
// Register named conversations with Register, then trigger them with Enter.
type Plugin struct {
	mu            sync.Mutex
	wg            sync.WaitGroup
	conversations map[string]ConversationFunc
	active        map[entryKey]*ConversationContext
	client        *tg.Client
	store         ConversationStore
}

// New creates a conversations plugin. If no store is provided, an in-memory
// store is used. Pass a ConversationStore implementation for persistent state.
func New(store ...ConversationStore) *Plugin {
	p := &Plugin{
		conversations: make(map[string]ConversationFunc),
		active:        make(map[entryKey]*ConversationContext),
		store:         NewMemoryStore(),
	}
	if len(store) > 0 && store[0] != nil {
		p.store = store[0]
	}
	return p
}

func (p *Plugin) Name() string {
	return "conversations"
}

func (p *Plugin) Start(ctx context.Context, client *tg.Client) error {
	p.client = client

	if adapter := client.Adapter(); adapter != nil {
		if cs, ok := adapter.(storage.ConversationStore); ok {
			if _, ok := p.store.(*typedStorageAdapter); !ok {
				p.store = Storage(cs)
			}
		}
	}

	client.UseMiddleware(func(next tg.Handler) tg.Handler {
		return &conversationsMiddleware{inner: next, plugin: p}
	})

	keys, err := p.store.List()
	if err != nil {
		return fmt.Errorf("conversations: restore: %w", err)
	}
	for _, key := range keys {
		state, err := p.store.Load(key)
		if err != nil || state == nil {
			_ = p.store.Delete(key)
			continue
		}
		fn, ok := p.conversations[state.Name]
		if !ok {
			_ = p.store.Delete(key)
			continue
		}
		ek := entryKey{chatID: key.ChatID, userID: key.UserID}
		cc := &ConversationContext{
			plugin: p,
			client: client,
			chatID: key.ChatID,
			userID: key.UserID,
			ctx:    ctx,
			notify: make(chan *tg.Context, 8),
			done:   make(chan struct{}),
			name:   state.Name,
			step:   state.Step,
			data:   state.Data,
		}
		p.active[ek] = cc
		p.wg.Add(1)
		go func(ek entryKey, fn ConversationFunc, cc *ConversationContext) {
			defer p.wg.Done()
			defer func() {
				p.mu.Lock()
				delete(p.active, ek)
				p.mu.Unlock()
			}()
			defer func() {
				if r := recover(); r != nil {
					cc.client.Log.Errorf("conversation %q panic: %v", cc.name, r)
				}
			}()
			_ = fn(cc)
		}(ek, fn, cc)
	}

	return nil
}

func (p *Plugin) Stop(ctx context.Context) error {
	p.mu.Lock()
	for _, cc := range p.active {
		close(cc.done)
	}
	p.active = make(map[entryKey]*ConversationContext)
	p.mu.Unlock()
	p.wg.Wait()
	return nil
}

// Register adds a named conversation handler. The name is used with Enter to
// start the conversation.
func (p *Plugin) Register(name string, fn ConversationFunc) {
	p.mu.Lock()
	p.conversations[name] = fn
	p.mu.Unlock()
}

// Enter starts the named conversation for the chat and user derived from ctx.
// If a conversation is already active for that pair it is cancelled first.
// Returns an error if the name is not registered.
func (p *Plugin) Enter(name string, ctx *tg.Context) error {
	chatID := extractChatID(ctx)
	if chatID == 0 {
		return fmt.Errorf("conversations: cannot determine chat")
	}
	userID := extractUserID(ctx)
	key := entryKey{chatID: chatID, userID: userID}

	p.mu.Lock()
	fn, ok := p.conversations[name]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("conversations: unknown conversation %q", name)
	}

	if existing, ok := p.active[key]; ok {
		close(existing.done)
		delete(p.active, key)
	}

	cc := &ConversationContext{
		plugin: p,
		client: ctx.Client,
		chatID: chatID,
		userID: userID,
		ctx:    ctx.Ctx,
		notify: make(chan *tg.Context, 8),
		done:   make(chan struct{}),
		name:   name,
	}
	p.active[key] = cc
	p.mu.Unlock()

	_ = p.store.Save(StoreKey{ChatID: chatID, UserID: userID}, &ConversationState{
		Name: name,
		Step: 0,
	})

	p.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("conversations: handler panic name=%s chat_id=%d user_id=%d: %v", name, chatID, userID, r)
			}
			p.mu.Lock()
			delete(p.active, key)
			p.mu.Unlock()
			_ = p.store.Delete(StoreKey{ChatID: chatID, UserID: userID})
		}()
		_ = fn(cc)
	})

	return nil
}

// Exit cancels the active conversation for the chat and user derived from ctx.
// Returns true if a conversation was active and has been stopped.
func (p *Plugin) Exit(ctx *tg.Context) bool {
	chatID := extractChatID(ctx)
	userID := extractUserID(ctx)
	key := entryKey{chatID: chatID, userID: userID}

	p.mu.Lock()
	cc, ok := p.active[key]
	if ok {
		close(cc.done)
		delete(p.active, key)
	}
	p.mu.Unlock()

	_ = p.store.Delete(StoreKey{ChatID: chatID, UserID: userID})
	return ok
}

// Active reports whether a conversation is currently running for the given
// chat and user pair.
func (p *Plugin) Active(chatID, userID int64) bool {
	p.mu.Lock()
	_, ok := p.active[entryKey{chatID: chatID, userID: userID}]
	p.mu.Unlock()
	return ok
}

func (cc *ConversationContext) saveState() {
	state := &ConversationState{
		Name: cc.name,
		Step: cc.step,
		Data: cc.data,
	}
	_ = cc.plugin.store.Save(StoreKey{ChatID: cc.chatID, UserID: cc.userID}, state)
}

func (p *Plugin) dispatch(ctx *tg.Context) bool {
	chatID := extractChatID(ctx)
	if chatID == 0 {
		return false
	}
	userID := extractUserID(ctx)
	if userID == 0 {
		return false
	}

	key := entryKey{chatID: chatID, userID: userID}

	p.mu.Lock()
	cc, ok := p.active[key]
	p.mu.Unlock()

	if !ok {
		return false
	}

	select {
	case cc.notify <- ctx:
	default:
	}

	return true
}

func extractChatID(ctx *tg.Context) int64 {
	if ctx.Message != nil {
		return ctx.Message.ChatID
	}
	if ctx.EditedMessage != nil {
		return ctx.EditedMessage.ChatID
	}
	if ctx.BusinessMessage != nil {
		return ctx.BusinessMessage.ChatID
	}
	if ctx.EditedBusinessMessage != nil {
		return ctx.EditedBusinessMessage.ChatID
	}
	if ctx.CallbackQuery != nil {
		return ctx.CallbackQuery.ChatID
	}
	return 0
}

func extractUserID(ctx *tg.Context) int64 {
	if ctx.Message != nil {
		return ctx.Message.FromID
	}
	if ctx.EditedMessage != nil {
		return ctx.EditedMessage.FromID
	}
	if ctx.CallbackQuery != nil {
		return ctx.CallbackQuery.UserID
	}
	if ctx.BusinessMessage != nil {
		return ctx.BusinessMessage.FromID
	}
	if ctx.EditedBusinessMessage != nil {
		return ctx.EditedBusinessMessage.FromID
	}
	return 0
}
