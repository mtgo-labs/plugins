package rawdebug

import "regexp"

// Redaction patterns for known-sensitive Telegram data. Each pattern is
// applied to the final output line; matches are replaced with [REDACTED].
//
// These cover the categories requested:
//   - auth key: 256-byte keys rendered as hex (512 chars) or base64
//   - session strings: long base64url/session payloads
//   - phone numbers: E.164 format (+digits)
//   - tokens: bot tokens (id:AA...), api_hash (32 hex), api_id:hash pairs
var redactors = []*redaction{
	// Bot tokens: <numeric_id>:<35-char token> (e.g. 5998453459:AAH...).
	newRedact(`\b\d{7,}:[A-Za-z0-9_-]{30,}\b`, "[REDACTED:token]"),

	// API hash: 32 lowercase hex chars, common in config/session dumps.
	newRedact(`\b[0-9a-f]{32}\b`, "[REDACTED:api_hash]"),

	// Auth key as hex (256 bytes = 512 hex chars).
	newRedact(`\b[0-9a-fA-F]{256,}\b`, "[REDACTED:auth_key]"),

	// Long base64 blobs (≥88 chars ≈ 64+ bytes): covers session strings,
	// serialized auth keys, and other opaque secrets.
	newRedact(`[A-Za-z0-9+/_-]{88,}`, "[REDACTED:secret]"),

	// Phone numbers in E.164 format.
	newRedact(`\+\d{7,15}`, "[REDACTED:phone]"),

	// JSON fields that commonly carry secrets, matched case-insensitively.
	// These catch structured bodies logged via LogBodies.
	newRedact(`(?i)"(auth_?key|auth_key_data)"\s*:\s*"[^"]*"`, `"$1":"[REDACTED]"`),
	newRedact(`(?i)"(phone_?number|phone)"\s*:\s*"[^"]*"`, `"$1":"[REDACTED]"`),
	newRedact(`(?i)"(session|session_?string|session_?data)"\s*:\s*"[^"]*"`, `"$1":"[REDACTED]"`),
	newRedact(`(?i)"(bot_?token|api_?hash|token)"\s*:\s*"[^"]*"`, `"$1":"[REDACTED]"`),
}

type redaction struct {
	re      *regexp.Regexp
	repl    string
	hasSlot bool // replacement contains $1 (capture group)
}

func newRedact(pattern, repl string) *redaction {
	return &redaction{
		re:      regexp.MustCompile(pattern),
		repl:    repl,
		hasSlot: regexp.MustCompile(`\$1`).MatchString(repl),
	}
}

// scrub applies all redaction patterns to s and returns the result. It is
// called on every output line when redaction is enabled.
func scrub(s string) string {
	for _, r := range redactors {
		s = r.re.ReplaceAllString(s, r.repl)
	}
	return s
}
