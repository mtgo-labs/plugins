package rawdebug

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// record is the common envelope for every debug event. Each formatter
// (text/JSON) projects it to its own representation.
type record struct {
	Kind         string  `json:"kind"`             // request, response, error, update, transport
	Trace        uint64  `json:"trace,omitempty"`  // monotonic RPC trace ID
	Method       string  `json:"method,omitempty"` // resolved TL method name
	RequestType  string  `json:"req_type,omitempty"`
	ResponseType string  `json:"resp_type,omitempty"`
	UpdateType   string  `json:"update_type,omitempty"`
	DC           int     `json:"dc,omitempty"`
	DurationMS   float64 `json:"duration_ms,omitempty"`
	ErrCode      int     `json:"err_code,omitempty"`
	ErrType      string  `json:"err_type,omitempty"`
	ErrMessage   string  `json:"err_message,omitempty"`
	IsFlood      bool    `json:"is_flood,omitempty"`
	FloodWaitMS  float64 `json:"flood_wait_ms,omitempty"`
	Event        string  `json:"event,omitempty"` // transport event name
	Body         string  `json:"body,omitempty"`  // redacted JSON body (when LogBodies)
}

// --- formatters called from the invoker ------------------------------------

func (p *Plugin) formatRequest(trace uint64, method, reqType string, dc int, input bodySource) record {
	r := record{
		Kind: "request", Trace: trace, Method: method,
		RequestType: reqType, DC: dc,
	}
	if p.cfg.LogBodies {
		r.Body = p.bodyJSON(input)
	}
	return r
}

func (p *Plugin) formatResponse(trace uint64, method, respType string, dc int, d time.Duration, result bodySource) record {
	r := record{
		Kind: "response", Trace: trace, Method: method,
		ResponseType: respType, DC: dc, DurationMS: ms(d),
	}
	if p.cfg.LogBodies {
		r.Body = p.bodyJSON(result)
	}
	return r
}

func (p *Plugin) formatRawResponse(trace uint64, method string, dc int, d time.Duration, raw []byte) record {
	r := record{
		Kind: "response", Trace: trace, Method: method,
		ResponseType: "bytes", DC: dc, DurationMS: ms(d),
	}
	if p.cfg.LogBodies {
		r.Body = fmt.Sprintf("%x", raw)
	}
	return r
}

func (p *Plugin) formatError(trace uint64, method, respType string, dc int, d time.Duration, err error, result bodySource) record {
	ei := errorInfo(err)
	r := record{
		Kind: "error", Trace: trace, Method: method,
		ResponseType: respType, DC: dc, DurationMS: ms(d),
		ErrCode: ei.code, ErrType: ei.etype, ErrMessage: ei.message,
		IsFlood: ei.isFlood,
	}
	if ei.isFlood {
		r.FloodWaitMS = ms(ei.wait)
	}
	if p.cfg.LogBodies {
		r.Body = p.bodyJSON(result)
	}
	return r
}

func (p *Plugin) formatUpdate(utype string, dc int, updates bodySource) record {
	r := record{Kind: "update", UpdateType: utype, DC: dc}
	if p.cfg.LogBodies {
		r.Body = p.bodyJSON(updates)
	}
	return r
}

func (p *Plugin) formatTransport(event string, dc int) record {
	return record{Kind: "transport", Event: event, DC: dc}
}

// bodySource is satisfied by tg.TLObject (and []byte is handled inline).
// Declared as a type alias to avoid importing tg in this file.
type bodySource interface{}

// --- projection ------------------------------------------------------------

// format converts a record into its wire line according to the configured Format.
func (p *Plugin) format(r record) string {
	if p.cfg.Format == FormatJSON {
		return formatJSON(r)
	}
	return formatText(r)
}

// emit helpers above return records; bridge them through format + emit.
func (p *Plugin) emit(r record) { p.write(p.format(r)) }

func formatText(r record) string {
	var b strings.Builder
	switch r.Kind {
	case "request":
		fmt.Fprintf(&b, "[rawdebug] → #%d %s req=%s dc=%d",
			r.Trace, r.Method, shortType(r.RequestType), r.DC)
	case "response":
		fmt.Fprintf(&b, "[rawdebug] ← #%d %s resp=%s %s dc=%d",
			r.Trace, r.Method, shortType(r.ResponseType), durStr(r.DurationMS), r.DC)
	case "error":
		fmt.Fprintf(&b, "[rawdebug] ✗ #%d %s resp=%s %s dc=%d err=%d %s",
			r.Trace, r.Method, shortType(r.ResponseType), durStr(r.DurationMS), r.DC, r.ErrCode, r.ErrType)
		if r.ErrMessage != "" && r.ErrMessage != r.ErrType {
			fmt.Fprintf(&b, " msg=%q", r.ErrMessage)
		}
		if r.IsFlood {
			fmt.Fprintf(&b, " flood_wait=%s", durStr(r.FloodWaitMS))
		}
	case "update":
		fmt.Fprintf(&b, "[rawdebug] ⟳ update=%s dc=%d", r.UpdateType, r.DC)
	case "transport":
		fmt.Fprintf(&b, "[rawdebug] ⚡ %s dc=%d", r.Event, r.DC)
	default:
		fmt.Fprintf(&b, "[rawdebug] %v", r)
	}
	if r.Body != "" {
		fmt.Fprintf(&b, " body=%s", r.Body)
	}
	return b.String()
}

func formatJSON(r record) string {
	// JSON Lines: one compact object per line.
	b, err := json.Marshal(r)
	if err != nil {
		return fmt.Sprintf(`{"kind":"%s","error":"marshal failed"}`, r.Kind)
	}
	return string(b)
}

// --- small helpers ---------------------------------------------------------

func ms(d time.Duration) float64 { return float64(d.Nanoseconds()) / float64(time.Millisecond) }

func durStr(msVal float64) string {
	if msVal >= 1000 {
		return fmt.Sprintf("%.2fs", msVal/1000)
	}
	return fmt.Sprintf("%.2fms", msVal)
}

// shortType trims a Go type name to its short form (package.Type → Type).
func shortType(s string) string {
	if s == "" {
		return "?"
	}
	if i := strings.LastIndex(s, "."); i >= 0 {
		return s[i+1:]
	}
	return s
}
