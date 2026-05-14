package conversations

import (
	"encoding/json"
	"errors"
	"time"

	tg "github.com/mtgo-labs/mtgo/telegram"
	"github.com/mtgo-labs/mtgo/telegram/params"
)

var (
	// ErrConversationDone is returned by Wait methods when the conversation
	// is cancelled (by user sending /cancel or calling Exit).
	ErrConversationDone = errors.New("conversations: conversation exited")
	// ErrConversationSkip is returned to indicate the current update should
	// be skipped without advancing the conversation.
	ErrConversationSkip = errors.New("conversations: skip")
	// ErrConversationHalt is returned to immediately terminate the
	// conversation and remove its state.
	ErrConversationHalt = errors.New("conversations: halted")
)

// WaitFilter is a predicate applied to incoming updates during WaitUntil and
// WaitFor. Return true to accept the update and advance the conversation.
type WaitFilter func(*tg.Context) bool

// Wait blocks until the next update arrives for this conversation, the
// conversation is cancelled, or the context is done. It increments the step
// counter and persists state before blocking.
func (cc *ConversationContext) Wait() (*tg.Context, error) {
	cc.step++
	cc.saveState()

	select {
	case ctx := <-cc.notify:
		return ctx, nil
	case <-cc.done:
		return nil, ErrConversationDone
	case <-cc.ctx.Done():
		return nil, cc.ctx.Err()
	}
}

// WaitUntil repeatedly waits for updates and returns the first one that
// satisfies filter.
func (cc *ConversationContext) WaitUntil(filter WaitFilter) (*tg.Context, error) {
	for {
		ctx, err := cc.Wait()
		if err != nil {
			return nil, err
		}
		if filter(ctx) {
			return ctx, nil
		}
	}
}

// WaitFor is like WaitUntil but with a timeout. Returns an error if the
// deadline is exceeded.
func (cc *ConversationContext) WaitFor(filter WaitFilter, timeout time.Duration) (*tg.Context, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case ctx := <-cc.notify:
			if filter(ctx) {
				cc.step++
				cc.saveState()
				return ctx, nil
			}
		case <-deadline.C:
			return nil, errors.New("conversations: wait timed out")
		case <-cc.done:
			return nil, ErrConversationDone
		case <-cc.ctx.Done():
			return nil, cc.ctx.Err()
		}
	}
}

// WaitMessage waits for the next text message from the user.
func (cc *ConversationContext) WaitMessage() (*tg.Context, error) {
	return cc.WaitUntil(func(ctx *tg.Context) bool {
		return ctx.Message != nil && ctx.Message.Text != ""
	})
}

// WaitCallback waits for the next callback query from the user.
func (cc *ConversationContext) WaitCallback() (*tg.Context, error) {
	return cc.WaitUntil(func(ctx *tg.Context) bool {
		return ctx.CallbackQuery != nil
	})
}

// Step returns the current conversation step number (0-based, incremented by Wait).
func (cc *ConversationContext) Step() int {
	return cc.step
}

// SetData serializes v as JSON and persists it as the conversation's data blob.
func (cc *ConversationContext) SetData(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	cc.data = b
	cc.saveState()
	return nil
}

// GetData deserializes the persisted conversation data into v.
func (cc *ConversationContext) GetData(v any) error {
	if cc.data == nil {
		return nil
	}
	return json.Unmarshal(cc.data, v)
}

// SendMessage sends a text message to the conversation's chat.
func (cc *ConversationContext) SendMessage(text string, opts ...params.SendMessage) error {
	var sendOpts []*params.SendMessage
	for i := range opts {
		sendOpts = append(sendOpts, &opts[i])
	}
	_, err := cc.client.SendMessage(cc.ctx, cc.chatID, text, sendOpts...)
	return err
}

// Skip returns an error that tells the dispatcher to ignore the current update
// without advancing the conversation.
func Skip() error {
	return ErrConversationSkip
}

// Halt returns an error that immediately terminates the conversation and
// removes its persisted state.
func Halt() error {
	return ErrConversationHalt
}
