// Copyright 2026 The Kstack Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package safe renders an error for a log line or a stored message.
//
// Two callers, for the two places a diagnostic ends up. Logs go through logging's handler,
// which renders every record whoever wrote it, so an ordinary slog call needs nothing here.
// A message that is persisted instead — a condition served to the UI — has no such sink, so
// it is rendered where it is recorded.
package safe

import (
	"regexp"
	"strings"
)

// MaxLen bounds a rendered error. A log line is read once, by a human, so the cap only has
// to stop a kilobyte-scale response body (a verbose /readyz) from landing whole — which is
// why it is not clustersvc's much tighter cap on a message re-read on every watch frame.
const MaxLen = 2048

const redacted = "[redacted]"

// What a credential looks like inside an error, rather than a keyword list — which is how
// the next format slips past. Applied in this order, so a header's value goes whole before
// anything reads the URL that may be inside it.
var (
	// A header echoed back by a server, value to the end of its line.
	headerRE = regexp.MustCompile(`(?i)\b(authorization|proxy-authorization|cookie|set-cookie)\s*[:=][^\n]*`)
	// A URL, to the first character that cannot be in one. Its query and userinfo are the
	// two parts that carry credentials; the host and path are the diagnostic.
	urlRE = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.\-]*://[^\s"'<>\\]+`)
	// A bearer token in free text, and a JWT wherever it appears. A JWT is identified by
	// its header segment, the base64 of `{"` — a length guess over dot-separated words
	// would also match a hostname.
	bearerRE = regexp.MustCompile(`(?i)\bbearer\s+[^\s"']+`)
	jwtRE    = regexp.MustCompile(`eyJ[A-Za-z0-9_\-]*\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]*`)
)

// Safe renders err bounded and with the credential-carrying shapes removed.
func Safe(err error) string {
	if err == nil {
		return ""
	}
	return String(err.Error())
}

// String renders text the same way, for a value that reaches a log without ever having
// been an error — a log message, an attribute a dependency set.
func String(s string) string {
	// The captured name, never a re-split of the match: the delimiter may be either, and a
	// value carries delimiters of its own.
	s = headerRE.ReplaceAllString(s, "$1: "+redacted)
	s = urlRE.ReplaceAllStringFunc(s, redactURL)
	s = bearerRE.ReplaceAllString(s, "Bearer "+redacted)
	s = jwtRE.ReplaceAllString(s, redacted)
	// Last, so no rule ever reads a string a cut left half a token in.
	if len(s) > MaxLen {
		return s[:MaxLen] + "…"
	}
	return s
}

// redactURL keeps the scheme, host and path — what says which server refused — and drops
// the query and the userinfo.
func redactURL(u string) string {
	head, _, hasQuery := strings.Cut(u, "?")
	if i := strings.Index(head, "//"); i >= 0 {
		authority, rest, _ := strings.Cut(head[i+2:], "/")
		if at := strings.LastIndex(authority, "@"); at >= 0 {
			head = head[:i+2] + redacted + "@" + authority[at+1:]
			if rest != "" {
				head += "/" + rest
			}
		}
	}
	if hasQuery {
		return head + "?" + redacted
	}
	return head
}
