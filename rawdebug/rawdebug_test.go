package rawdebug

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mtgo-labs/mtgo/tg"
	"github.com/mtgo-labs/mtgo/tgerr"
)

// fakeInvoker implements tg.Invoker for testing the RPC middleware path.
type fakeInvoker struct {
	result tg.TLObject
	err    error
	delay  time.Duration
	calls  int
	lastIn tg.TLObject
}

func (f *fakeInvoker) RPCInvoke(ctx context.Context, input tg.TLObject, decode func(*tg.Reader) (tg.TLObject, error)) (tg.TLObject, error) {
	f.calls++
	f.lastIn = input
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.result, nil
}

func (f *fakeInvoker) RPCInvokeRaw(ctx context.Context, input tg.TLObject) ([]byte, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return []byte{0xDE, 0xAD, 0xBE, 0xEF}, nil
}

func newTestPlugin(cfg Config) (*Plugin, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	cfg.Writer = buf
	return New(cfg), buf
}

// invoker wraps a plugin's middleware around a fake next invoker.
func testInvoker(p *Plugin, next tg.Invoker) *rpcInvoker {
	p.initNames()
	return &rpcInvoker{p: p, next: next}
}

// --- RPC logging -----------------------------------------------------------

func TestRPCRequestResponseLogging(t *testing.T) {
	p, buf := newTestPlugin(Config{LogRequests: true, LogResponses: true})
	ri := testInvoker(p, &fakeInvoker{result: &tg.Config{}})

	_, err := ri.RPCInvoke(context.Background(), &tg.HelpGetConfigRequest{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out := buf.String()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines (request+response), got %d: %q", len(lines), out)
	}
	if !strings.Contains(lines[0], "→") || !strings.Contains(lines[0], "help.getConfig") {
		t.Errorf("request line mismatch: %q", lines[0])
	}
	if !strings.Contains(lines[1], "←") || !strings.Contains(lines[1], "help.getConfig") {
		t.Errorf("response line mismatch: %q", lines[1])
	}
}

func TestRPCErrorLogging(t *testing.T) {
	p, buf := newTestPlugin(Config{LogErrors: true})
	rpcErr := tgerr.New(400, "MESSAGE_TOO_LONG")
	ri := testInvoker(p, &fakeInvoker{err: rpcErr})

	_, _ = ri.RPCInvoke(context.Background(), &tg.HelpGetConfigRequest{}, nil)

	out := buf.String()
	if !strings.Contains(out, "✗") {
		t.Errorf("error line missing marker: %q", out)
	}
	if !strings.Contains(out, "help.getConfig") {
		t.Errorf("error line missing method: %q", out)
	}
	if !strings.Contains(out, "400") {
		t.Errorf("error line missing code: %q", out)
	}
	if !strings.Contains(out, "MESSAGE_TOO_LONG") {
		t.Errorf("error line missing error type: %q", out)
	}
}

func TestRPCFloodWaitLogging(t *testing.T) {
	p, buf := newTestPlugin(Config{LogErrors: true})
	rpcErr := tgerr.New(420, "FLOOD_WAIT_60")
	ri := testInvoker(p, &fakeInvoker{err: rpcErr})

	_, _ = ri.RPCInvoke(context.Background(), &tg.HelpGetConfigRequest{}, nil)

	out := buf.String()
	if !strings.Contains(out, "flood_wait") {
		t.Errorf("flood wait not logged: %q", out)
	}
}

func TestRPCInvokeRawLogging(t *testing.T) {
	p, buf := newTestPlugin(Config{LogResponses: true})
	ri := testInvoker(p, &fakeInvoker{})

	_, err := ri.RPCInvokeRaw(context.Background(), &tg.HelpGetConfigRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(buf.String(), "←") || !strings.Contains(buf.String(), "bytes") {
		t.Errorf("raw response not logged: %q", buf.String())
	}
}

// --- Update logging --------------------------------------------------------

func TestUpdateLogging(t *testing.T) {
	p, buf := newTestPlugin(Config{LogUpdates: true})
	p.initNames()

	p.logUpdate(&tg.UpdatesCombined{})

	out := buf.String()
	if !strings.Contains(out, "⟳") || !strings.Contains(out, "updatesCombined") {
		t.Errorf("update not logged correctly: %q", out)
	}
}

func TestTransportLogging(t *testing.T) {
	p, buf := newTestPlugin(Config{LogTransport: true})

	p.logTransport("connected")
	p.logTransport("reconnected")

	out := buf.String()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 transport lines, got %d: %q", len(lines), out)
	}
	if !strings.Contains(lines[0], "connected") || !strings.Contains(lines[1], "reconnected") {
		t.Errorf("transport events mismatch: %q", out)
	}
}

// --- Filtering -------------------------------------------------------------

func TestMethodFiltering(t *testing.T) {
	p, buf := newTestPlugin(Config{
		LogResponses: true,
		Methods:      []string{"messages.sendMessage"},
	})
	ri := testInvoker(p, &fakeInvoker{result: &tg.Config{}})

	// Non-matching method: should produce no output.
	_, _ = ri.RPCInvoke(context.Background(), &tg.HelpGetConfigRequest{}, nil)
	if buf.Len() != 0 {
		t.Errorf("non-matching method should be filtered, got: %q", buf.String())
	}
}

func TestUpdateTypeFiltering(t *testing.T) {
	p, buf := newTestPlugin(Config{
		LogUpdates:  true,
		UpdateTypes: []string{"updateNewMessage"},
	})
	p.initNames()

	// Non-matching update type: no output.
	p.logUpdate(&tg.UpdatesCombined{})
	if buf.Len() != 0 {
		t.Errorf("non-matching update should be filtered, got: %q", buf.String())
	}
}

func TestErrorsOnly(t *testing.T) {
	p, buf := newTestPlugin(Config{
		LogRequests:  true,
		LogResponses: true,
		LogErrors:    true,
		ErrorsOnly:   true,
	})
	ri := testInvoker(p, &fakeInvoker{result: &tg.Config{}})

	// Success call with ErrorsOnly: no output.
	_, _ = ri.RPCInvoke(context.Background(), &tg.HelpGetConfigRequest{}, nil)
	if buf.Len() != 0 {
		t.Errorf("success should be suppressed with ErrorsOnly, got: %q", buf.String())
	}
}

func TestSlowThreshold(t *testing.T) {
	p, buf := newTestPlugin(Config{
		LogResponses:  true,
		SlowThreshold: 50 * time.Millisecond,
	})

	// Fast call: suppressed.
	riFast := testInvoker(p, &fakeInvoker{result: &tg.Config{}})
	_, _ = riFast.RPCInvoke(context.Background(), &tg.HelpGetConfigRequest{}, nil)
	if buf.Len() != 0 {
		t.Errorf("fast call should be suppressed, got: %q", buf.String())
	}

	// Slow call: logged.
	buf.Reset()
	riSlow := testInvoker(p, &fakeInvoker{result: &tg.Config{}, delay: 60 * time.Millisecond})
	_, _ = riSlow.RPCInvoke(context.Background(), &tg.HelpGetConfigRequest{}, nil)
	if buf.Len() == 0 {
		t.Errorf("slow call should be logged")
	}
}

// --- JSON output -----------------------------------------------------------

func TestJSONOutput(t *testing.T) {
	p, buf := newTestPlugin(Config{
		LogResponses: true,
		Format:       FormatJSON,
	})
	ri := testInvoker(p, &fakeInvoker{result: &tg.Config{}})

	_, _ = ri.RPCInvoke(context.Background(), &tg.HelpGetConfigRequest{}, nil)

	out := strings.TrimSpace(buf.String())
	var rec record
	if err := json.Unmarshal([]byte(out), &rec); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	if rec.Kind != "response" {
		t.Errorf("kind = %q, want response", rec.Kind)
	}
	if rec.Method != "help.getConfig" {
		t.Errorf("method = %q, want help.getConfig", rec.Method)
	}
	if rec.DurationMS <= 0 {
		t.Errorf("duration_ms = %v, want > 0", rec.DurationMS)
	}
}

func TestJSONErrorOutput(t *testing.T) {
	p, buf := newTestPlugin(Config{
		LogErrors: true,
		Format:    FormatJSON,
	})
	ri := testInvoker(p, &fakeInvoker{err: tgerr.New(401, "AUTH_KEY_UNREGISTERED")})

	_, _ = ri.RPCInvoke(context.Background(), &tg.HelpGetConfigRequest{}, nil)

	var rec record
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &rec); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if rec.Kind != "error" || rec.ErrCode != 401 {
		t.Errorf("error record mismatch: %+v", rec)
	}
}

// --- Redaction -------------------------------------------------------------

func TestScrubSecrets(t *testing.T) {
	cases := []struct{ name, in string }{
		{"bot token", `bot_token="` + strings.Repeat("0", 10) + ":" + strings.Repeat("A", 36) + `"`},
		{"api hash", `api_hash="` + strings.Repeat("a", 32) + `"`},
		{"phone", `phone="+` + strings.Repeat("1", 11) + `"`},
		{"auth key hex", strings.Repeat("ab", 256)},
		{"long base64", strings.Repeat("A", 100)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scrub(tc.in)
			if !strings.Contains(got, "REDACTED") {
				t.Errorf("secret not redacted in %q -> %q", tc.name, got)
			}
		})
	}
}

func TestRedactionJSONFields(t *testing.T) {
	in := `{"auth_key":"sensitive_data_here","phone_number":"+12345678901","session":"abc"}`
	got := scrub(in)
	if strings.Contains(got, "sensitive_data_here") || strings.Contains(got, "+12345678901") || strings.Contains(got, `"session":"abc"`) {
		t.Errorf("JSON secret not redacted: %q", got)
	}
}

func TestRedactionAppliedToOutput(t *testing.T) {
	// With LogBodies, a request body containing a phone must be redacted.
	p, buf := newTestPlugin(Config{
		LogRequests: true,
		LogBodies:   true,
	})
	ri := testInvoker(p, &fakeInvoker{})

	// Craft a request whose JSON includes a phone-like value via a custom type.
	_, _ = ri.RPCInvoke(context.Background(), &secretRequest{}, nil)

	out := buf.String()
	if strings.Contains(out, "+15551234567") {
		t.Errorf("phone leaked in body: %q", out)
	}
	if !strings.Contains(out, "REDACTED") {
		t.Errorf("expected redaction marker, got: %q", out)
	}
}

func TestWithoutRedaction(t *testing.T) {
	p, buf := newTestPlugin(Config{LogRequests: true, LogBodies: true})
	WithoutRedaction()(p) // disable redaction
	ri := testInvoker(p, &fakeInvoker{})

	_, _ = ri.RPCInvoke(context.Background(), &secretRequest{}, nil)

	if !strings.Contains(buf.String(), "+15551234567") {
		t.Errorf("phone should be visible without redaction: %q", buf.String())
	}
}

// secretRequest is a minimal TLObject carrying a phone for body-redaction tests.
type secretRequest struct{}

func (secretRequest) ConstructorID() uint32 { return 0xc4f9186b } // help.getConfig ID
func (s secretRequest) Encode(b *bytes.Buffer) error {
	return nil
}
func (s secretRequest) MarshalJSON() ([]byte, error) {
	return []byte(`{"phone":"+15551234567","_":"help.getConfig"}`), nil
}

// --- Disabled plugin -------------------------------------------------------

func TestDisabledPlugin(t *testing.T) {
	p, buf := newTestPlugin(Config{}) // everything off
	ri := testInvoker(p, &fakeInvoker{result: &tg.Config{}})

	_, _ = ri.RPCInvoke(context.Background(), &tg.HelpGetConfigRequest{}, nil)
	p.logUpdate(&tg.UpdatesCombined{})
	p.logTransport("connected")

	if buf.Len() != 0 {
		t.Errorf("disabled plugin should produce no output, got: %q", buf.String())
	}
}

// --- Name resolution -------------------------------------------------------

func TestMethodNameResolution(t *testing.T) {
	p, _ := newTestPlugin(Config{})
	p.initNames()

	if got := p.methodName(&tg.HelpGetConfigRequest{}); got != "help.getConfig" {
		t.Errorf("methodName = %q, want help.getConfig", got)
	}
	if got := p.methodName(nil); got != "unknown" {
		t.Errorf("nil methodName = %q, want unknown", got)
	}
}

func TestNormalizeList(t *testing.T) {
	got := normalizeList([]string{"  Help.GetConfig ", "", "Messages.SendMessage"})
	want := []string{"help.getconfig", "messages.sendmessage"}
	if len(got) != len(want) {
		t.Fatalf("normalizeList = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("normalizeList[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestPlainErrorFallback(t *testing.T) {
	// A non-tgerr error should still log with its message.
	p, buf := newTestPlugin(Config{LogErrors: true})
	ri := testInvoker(p, &fakeInvoker{err: errors.New("connection reset")})

	_, _ = ri.RPCInvoke(context.Background(), &tg.HelpGetConfigRequest{}, nil)

	out := buf.String()
	if !strings.Contains(out, "connection reset") {
		t.Errorf("plain error message not logged: %q", out)
	}
}
