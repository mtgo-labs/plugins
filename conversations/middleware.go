package conversations

import (
	"strings"

	tg "github.com/mtgo-labs/mtgo/telegram"
)

type conversationsMiddleware struct {
	inner  tg.Handler
	plugin *Plugin
}

func (m *conversationsMiddleware) Check(update *tg.Update) bool {
	return true
}

func isCancelCommand(ctx *tg.Context) bool {
	if ctx.Message == nil {
		return false
	}
	text := strings.TrimSpace(ctx.Message.Text)
	return text == "/cancel" || strings.HasPrefix(text, "/cancel@")
}

func (m *conversationsMiddleware) Handle(ctx *tg.Context) {
	if isCancelCommand(ctx) {
		chatID := extractChatID(ctx)
		userID := extractUserID(ctx)
		key := entryKey{chatID: chatID, userID: userID}
		m.plugin.mu.Lock()
		if cc, ok := m.plugin.active[key]; ok {
			close(cc.done)
			delete(m.plugin.active, key)
		}
		m.plugin.mu.Unlock()
		_ = m.plugin.store.Delete(StoreKey{ChatID: chatID, UserID: userID})
		m.inner.Handle(ctx)
		return
	}

	if m.plugin.dispatch(ctx) {
		return
	}
	m.inner.Handle(ctx)
}
