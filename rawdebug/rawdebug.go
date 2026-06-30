// Package rawdebug provides an opt-in plugin for inspecting raw MTProto
// traffic during local development and debugging of [mtgo] clients.
//
// It hooks into the existing mtgo observability surfaces — invoker
// middleware (RPC requests, responses, and errors), the update-received
// hook, and the connect/reconnect lifecycle hooks — to emit structured
// debug records without adding any logging to the mtgo core itself.
//
// # Safety
//
// The plugin is safe by default: request/response/update bodies are NOT
// logged unless [Config.LogBodies] is set, and known-secret patterns
// (auth keys, phone numbers, session strings, bot/api tokens) are
// scrubbed from all output. Redaction can be disabled with
// [WithoutRedaction], but only do this for trusted local debugging.
//
// Do NOT enable this plugin in production unless logs are properly
// redacted and the output destination is access-controlled.
//
// # Usage
//
//	client.Use(rawdebug.New(rawdebug.Config{
//	    LogRequests:  true,
//	    LogResponses: true,
//	    LogUpdates:   true,
//	    LogErrors:    true,
//	    Format:       rawdebug.FormatText,
//	}))
//
// [mtgo]: https://github.com/mtgo-labs/mtgo
package rawdebug

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mtgo-labs/mtgo/telegram"
	"github.com/mtgo-labs/mtgo/tg"
	"github.com/mtgo-labs/mtgo/tgerr"
)

// Format selects the output representation for debug records.
type Format int

const (
	// FormatText emits one human-readable line per record (the default).
	FormatText Format = iota
	// FormatJSON emits one JSON object per record (JSON Lines / NDJSON).
	FormatJSON
)

// Config controls what the plugin logs and how. All fields are optional;
// safe defaults are applied by [New].
type Config struct {
	// LogRequests emits a record before each RPC call is dispatched,
	// showing the method, request type, DC, and a trace ID.
	LogRequests bool

	// LogResponses emits a record after each successful RPC call,
	// showing the response type, duration, DC, and trace ID.
	LogResponses bool

	// LogErrors emits a record after each failed RPC call, showing the
	// error code, type, message, duration, DC, and trace ID.
	LogErrors bool

	// LogUpdates emits a record for each incoming update batch, showing
	// the update type and DC.
	LogUpdates bool

	// LogTransport emits records for connect/reconnect lifecycle events.
	LogTransport bool

	// Format selects text or JSON output. Defaults to [FormatText].
	Format Format

	// Writer receives the debug output. Defaults to [os.Stderr].
	Writer io.Writer

	// Methods, when non-empty, restricts RPC logging to calls whose
	// resolved method name matches one of the entries (e.g.
	// "messages.sendMessage"). Matches are case-insensitive on the
	// qualified name. An empty slice logs all methods.
	Methods []string

	// UpdateTypes, when non-empty, restricts update logging to batches
	// whose resolved type matches one of the entries (e.g.
	// "updateNewMessage"). An empty slice logs all updates.
	UpdateTypes []string

	// ErrorsOnly suppresses success response and pre-call request
	// records, emitting only failed-RPC error records. This overrides
	// LogRequests and LogResponses for the RPC path.
	ErrorsOnly bool

	// SlowThreshold, when non-zero, suppresses RPC records whose
	// round-trip duration is below the threshold. Useful to surface
	// only slow calls. Zero logs all durations.
	SlowThreshold time.Duration

	// LogBodies enables logging of request, response, and update bodies,
	// serialized as JSON. This is DANGEROUS: bodies may contain secrets.
	// Defaults to false. When enabled, output is still scrubbed by the
	// redactor unless [WithoutRedaction] is also used.
	LogBodies bool
}

// Option configures a [Plugin] at construction time.
type Option func(*Plugin)

// WithoutRedaction disables scrubbing of auth keys, phone numbers, session
// strings, and tokens from logged output.
//
// DANGEROUS: only use for trusted local debugging where the output
// destination is fully under your control. Redaction is ON by default.
func WithoutRedaction() Option {
	return func(p *Plugin) { p.redact = false }
}

// Plugin is a [telegram.Plugin] that emits raw MTProto debug records. It
// implements [tg.Plugin] so it can be registered with client.Use.
//
// A Plugin is safe for concurrent use: all output is serialized through an
// internal mutex and a monotonic trace-ID counter.
type Plugin struct {
	cfg    Config
	redact bool

	mu     sync.Mutex // guards Writer
	seq    atomic.Uint64
	client *telegram.Client

	nameOnce sync.Once
	idToName map[uint32]string
}

// New creates a raw debug plugin. Redaction is enabled by default, body
// logging is disabled by default, and the writer defaults to [os.Stderr].
//
// Pass [Option] values to adjust non-default behavior:
//
//	client.Use(rawdebug.New(cfg, rawdebug.WithoutRedaction()))
func New(cfg Config, opts ...Option) *Plugin {
	if cfg.Writer == nil {
		cfg.Writer = os.Stderr
	}
	cfg.Methods = normalizeList(cfg.Methods)
	cfg.UpdateTypes = normalizeList(cfg.UpdateTypes)

	p := &Plugin{
		cfg:    cfg,
		redact: true, // safe by default
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Name implements [telegram.Plugin].
func (p *Plugin) Name() string { return "rawdebug" }

// Start implements [telegram.Plugin]. It registers the RPC invoker
// middleware and the update/transport hooks on the client. It is safe to
// call once per client; calling Start more than once is a no-op.
func (p *Plugin) Start(_ context.Context, client *telegram.Client) error {
	p.client = client
	p.initNames()

	// RPC path: requests, responses, and errors.
	client.UseInvokerMiddleware(func(next tg.Invoker) tg.Invoker {
		return &rpcInvoker{p: p, next: next}
	})

	// Update path.
	if p.cfg.LogUpdates {
		client.OnUpdateReceived(func(_ *telegram.Client, updates tg.UpdatesClass) {
			p.logUpdate(updates)
		})
	}

	// Transport path.
	if p.cfg.LogTransport {
		client.OnConnected(func(*telegram.Client) { p.logTransport("connected") })
		client.OnReconnect(func(*telegram.Client) { p.logTransport("reconnected") })
	}
	return nil
}

// Stop implements [telegram.Plugin]. The hooks are tied to the client
// lifecycle, so there is nothing to clean up.
func (p *Plugin) Stop(context.Context) error { return nil }

// --- RPC invoker -----------------------------------------------------------

type rpcInvoker struct {
	p    *Plugin
	next tg.Invoker
}

func (i *rpcInvoker) RPCInvoke(ctx context.Context, input tg.TLObject, decode func(*tg.Reader) (tg.TLObject, error)) (tg.TLObject, error) {
	method := i.p.methodName(input)
	trace := i.p.nextSeq()
	start := time.Now()
	dc := i.p.dc()

	logRequest := i.p.cfg.LogRequests && !i.p.cfg.ErrorsOnly && i.p.cfg.SlowThreshold == 0 && i.p.methodAllowed(method)
	if logRequest {
		i.p.emit(i.p.formatRequest(trace, method, typeName(input), dc, input))
	}

	result, err := i.next.RPCInvoke(ctx, input, decode)
	duration := time.Since(start)

	allowed := i.p.methodAllowed(method)
	switch {
	case err != nil:
		if i.p.cfg.LogErrors && allowed && i.p.slowEnough(duration) {
			i.p.emit(i.p.formatError(trace, method, typeName(result), dc, duration, err, result))
		}
	default:
		if i.p.cfg.LogResponses && !i.p.cfg.ErrorsOnly && allowed && i.p.slowEnough(duration) {
			i.p.emit(i.p.formatResponse(trace, method, typeName(result), dc, duration, result))
		}
	}
	return result, err
}

func (i *rpcInvoker) RPCInvokeRaw(ctx context.Context, input tg.TLObject) ([]byte, error) {
	method := i.p.methodName(input)
	trace := i.p.nextSeq()
	start := time.Now()
	dc := i.p.dc()

	logRequest := i.p.cfg.LogRequests && !i.p.cfg.ErrorsOnly && i.p.cfg.SlowThreshold == 0 && i.p.methodAllowed(method)
	if logRequest {
		i.p.emit(i.p.formatRequest(trace, method, typeName(input), dc, input))
	}

	raw, err := i.next.RPCInvokeRaw(ctx, input)
	duration := time.Since(start)

	allowed := i.p.methodAllowed(method)
	if err != nil {
		if i.p.cfg.LogErrors && allowed && i.p.slowEnough(duration) {
			i.p.emit(i.p.formatError(trace, method, "bytes", dc, duration, err, nil))
		}
	} else if i.p.cfg.LogResponses && !i.p.cfg.ErrorsOnly && allowed && i.p.slowEnough(duration) {
		i.p.emit(i.p.formatRawResponse(trace, method, dc, duration, raw))
	}
	return raw, err
}

// --- Update + transport ----------------------------------------------------

func (p *Plugin) logUpdate(updates tg.UpdatesClass) {
	if !p.cfg.LogUpdates {
		return
	}
	utype := p.methodName(updates) // UpdatesClass is a TLObject; same reverse lookup
	if !p.updateAllowed(utype) {
		return
	}
	p.emit(p.formatUpdate(utype, p.dc(), updates))
}

func (p *Plugin) logTransport(event string) {
	if !p.cfg.LogTransport {
		return
	}
	p.emit(p.formatTransport(event, p.dc()))
}

// --- filtering helpers -----------------------------------------------------

func (p *Plugin) methodAllowed(method string) bool {
	if len(p.cfg.Methods) == 0 {
		return true
	}
	return slices.Contains(p.cfg.Methods, method)
}

func (p *Plugin) updateAllowed(utype string) bool {
	if len(p.cfg.UpdateTypes) == 0 {
		return true
	}
	return slices.Contains(p.cfg.UpdateTypes, utype)
}

func (p *Plugin) slowEnough(d time.Duration) bool {
	if p.cfg.SlowThreshold <= 0 {
		return true
	}
	return d >= p.cfg.SlowThreshold
}

// --- name resolution -------------------------------------------------------

func (p *Plugin) initNames() {
	p.nameOnce.Do(func() {
		p.idToName = make(map[uint32]string, len(tg.NamesMap))
		for name, id := range tg.NamesMap {
			// Prefer the first (canonical) mapping for an ID.
			if _, exists := p.idToName[id]; !exists {
				p.idToName[id] = name
			}
		}
	})
}

// methodName resolves the Telegram qualified name for a TLObject (e.g.
// "help.getConfig", "updateNewMessage") from its constructor ID. Returns
// "unknown" for nil or unrecognized objects.
func (p *Plugin) methodName(obj tg.TLObject) string {
	if obj == nil {
		return "unknown"
	}
	p.initNames()
	if name, ok := p.idToName[obj.ConstructorID()]; ok {
		return name
	}
	return "unknown"
}

// typeName returns the Go type name of a TLObject for logging.
func typeName(obj tg.TLObject) string {
	if obj == nil {
		return ""
	}
	return fmt.Sprintf("%T", obj)
}

// --- output ----------------------------------------------------------------

func (p *Plugin) nextSeq() uint64 { return p.seq.Add(1) }

func (p *Plugin) dc() int {
	if p.client == nil {
		return 0
	}
	return p.client.Config().DC
}

// write writes one formatted, newline-terminated line to the configured
// writer under the output mutex. The line is scrubbed by the redactor
// unless redaction is disabled.
func (p *Plugin) write(line string) {
	if p.redact {
		line = scrub(line)
	}
	p.mu.Lock()
	fmt.Fprintln(p.cfg.Writer, line)
	p.mu.Unlock()
}

// bodyJSON serializes a TLObject to compact JSON for body logging. Returns
// "" if marshaling fails or the object is nil. The result is intended to be
// passed through the redactor by the formatter.
func (p *Plugin) bodyJSON(obj any) string {
	if obj == nil {
		return ""
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return ""
	}
	return string(b)
}

func normalizeList(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if t := strings.TrimSpace(strings.ToLower(s)); t != "" {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// errorDetail extracts structured RPC error info, if present.
type errorDetail struct {
	code    int
	etype   string
	message string
	wait    time.Duration
	isFlood bool
}

func errorInfo(err error) errorDetail {
	var d errorDetail
	if err == nil {
		return d
	}
	if rpcErr, ok := tgerr.As(err); ok {
		d.code = rpcErr.Code
		d.etype = rpcErr.Type
		d.message = rpcErr.Message
	}
	if wait, ok := tgerr.AsFloodWait(err); ok {
		d.isFlood = true
		d.wait = wait
	}
	if d.message == "" {
		d.message = err.Error()
	}
	return d
}
