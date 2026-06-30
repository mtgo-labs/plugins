package fsm

import (
	tg "github.com/mtgo-labs/mtgo/telegram"
)

// pluginDataKey is the key under which the *State handle is stored in
// ctx.PluginData.
const pluginDataKey = "fsm_state"

// middleware wraps the handler chain, attaching a per-scope [State] handle to
// every context before delegating to the next handler.
type middleware struct {
	inner  tg.Handler
	plugin *Plugin
}

func (m *middleware) Check(update *tg.Update) bool {
	return m.inner.Check(update)
}

func (m *middleware) Handle(ctx *tg.Context) {
	key := m.plugin.resolveKey(ctx)
	st := &State{store: m.plugin.store, key: key}

	if ctx.PluginData == nil {
		ctx.PluginData = make(map[string]any)
	}
	ctx.PluginData[pluginDataKey] = st

	m.inner.Handle(ctx)
}

// FromContext retrieves the [State] handle attached by the plugin middleware.
// Returns nil if the plugin was not registered.
func FromContext(ctx *tg.Context) *State {
	if s, ok := ctx.PluginData[pluginDataKey].(*State); ok {
		return s
	}
	return nil
}

// extractIDs returns the chat ID and user ID from the context, inspecting
// message, callback, and other update types. Returns (0, 0) if neither can be
// determined.
func extractIDs(ctx *tg.Context) (chatID, userID int64) {
	if ctx.Message != nil {
		return ctx.Message.ChatID, ctx.Message.FromID
	}
	if ctx.EditedMessage != nil {
		return ctx.EditedMessage.ChatID, ctx.EditedMessage.FromID
	}
	if ctx.BusinessMessage != nil {
		return ctx.BusinessMessage.ChatID, ctx.BusinessMessage.FromID
	}
	if ctx.EditedBusinessMessage != nil {
		return ctx.EditedBusinessMessage.ChatID, ctx.EditedBusinessMessage.FromID
	}
	if ctx.CallbackQuery != nil {
		return ctx.CallbackQuery.ChatID, ctx.CallbackQuery.UserID
	}
	return 0, 0
}
