// Copyright 2026 The Kubetail Authors
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

//! Re-emits the sidecar child's stdout/stderr as host `tracing` events.
//!
//! The sidecar logs through Go's `slog` JSON handler, so each line is a JSON
//! object carrying at least `level` and `msg`. We parse the level and surface
//! it at the matching host severity; any other JSON fields ride along as
//! structured fields on the tracing event. Lines that aren't slog JSON (Go
//! panics, runtime messages) are forwarded verbatim at the fallback level
//! for the pipe they arrived on.

use serde::Deserialize;

/// One structured log line emitted by the sidecar.
#[derive(Deserialize)]
struct SidecarLog {
    /// Sidecar-side severity: `DEBUG`, `INFO`, `WARN`, or `ERROR`.
    level: String,
    /// The human-readable message.
    msg: String,
    /// The sidecar's own timestamp, consumed but discarded (the host's `tracing`
    /// subscriber stamps each event). Named so it stays out of `extra` below.
    #[serde(default, rename = "time")]
    _time: serde::de::IgnoredAny,
    /// Every remaining JSON field, re-attached to the tracing event verbatim.
    #[serde(flatten)]
    extra: serde_json::Map<String, serde_json::Value>,
}

/// Host-side severity a sidecar line is re-emitted at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Severity {
    Error,
    Warn,
    Info,
    Debug,
}

/// What a sidecar output line should become: the severity to emit it at, the
/// message text, and a JSON string of the sidecar's own structured fields
/// (empty when there are none).
type Classified = (Severity, String, String);

/// Decides how a line of sidecar output should be re-emitted, without touching
/// `tracing` — split from [`forward_sidecar_line`] so the parsing/level-mapping
/// rules unit-test in isolation.
///
/// `None` for an empty/whitespace-only line. A line that parses as the sidecar's
/// `slog` JSON is classified by its own `level`; anything else (Go panics,
/// runtime messages) is forwarded verbatim at `fallback`.
fn classify(raw: &[u8], fallback: Severity) -> Option<Classified> {
    let text = String::from_utf8_lossy(raw);
    let text = text.trim_end();
    if text.is_empty() {
        return None;
    }

    match serde_json::from_str::<SidecarLog>(text) {
        Ok(log) => {
            let severity = match log.level.as_str() {
                "ERROR" => Severity::Error,
                "WARN" => Severity::Warn,
                "DEBUG" => Severity::Debug,
                // Unknown level labels default to INFO rather than drop.
                _ => Severity::Info,
            };
            let extra = if log.extra.is_empty() {
                String::new()
            } else {
                serde_json::to_string(&log.extra).unwrap_or_default()
            };
            Some((severity, log.msg, extra))
        }
        // Not the sidecar's JSON format: forward the raw line at `fallback`.
        Err(_) => Some((fallback, text.to_string(), String::new())),
    }
}

/// Re-emits one line of sidecar output as a host `tracing` event.
///
/// The sidecar's JSON lines carry their own severity, so a sidecar `WARN`
/// surfaces as a host `WARN` regardless of the pipe it arrived on (Go's `slog`
/// writes to stderr by default, so without this everything would land at the
/// stderr fallback). Non-JSON lines (panics, runtime messages) forward verbatim
/// at `fallback`.
pub(super) fn forward_sidecar_line(raw: &[u8], fallback: Severity) {
    let Some((severity, msg, extra)) = classify(raw, fallback) else {
        return;
    };

    // `tracing` needs the level fixed at compile time, so dispatch per arm.
    // `fields` carries the sidecar's own structured keys when there are any.
    macro_rules! emit {
        ($level:ident) => {
            if extra.is_empty() {
                tracing::$level!(target: "sidecar", "{}", msg);
            } else {
                tracing::$level!(target: "sidecar", fields = %extra, "{}", msg);
            }
        };
    }
    match severity {
        Severity::Error => emit!(error),
        Severity::Warn => emit!(warn),
        Severity::Info => emit!(info),
        Severity::Debug => emit!(debug),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A representative sidecar `slog` line, mirroring the JSON the sidecar
    /// emits at startup (level, msg, timestamp, plus call-site fields).
    const STARTUP_LINE: &str = r#"{"time":"2026-05-22T09:07:14.929385+03:00","level":"INFO","msg":"sidecar starting","socket":"/tmp/kstack.sock","pid":21311}"#;

    fn classify_str(line: &str, fallback: Severity) -> Classified {
        classify(line.as_bytes(), fallback).expect("line should produce an event")
    }

    #[test]
    fn classify_maps_each_sidecar_level() {
        for (label, expected) in [
            ("ERROR", Severity::Error),
            ("WARN", Severity::Warn),
            ("INFO", Severity::Info),
            ("DEBUG", Severity::Debug),
        ] {
            let line = format!(r#"{{"level":"{label}","msg":"x"}}"#);
            let (severity, _, _) = classify_str(&line, Severity::Warn);
            assert_eq!(
                severity, expected,
                "level {label} should map to {expected:?}"
            );
        }
    }

    #[test]
    fn classify_defaults_an_unknown_level_to_info() {
        // slog could be reconfigured to emit lowercase or custom labels; an
        // unrecognized one must still surface, not be dropped or escalated.
        let (severity, _, _) = classify_str(r#"{"level":"trace","msg":"x"}"#, Severity::Warn);
        assert_eq!(severity, Severity::Info);
    }

    #[test]
    fn classify_extracts_msg_and_structured_fields() {
        let (severity, msg, extra) = classify_str(STARTUP_LINE, Severity::Warn);
        assert_eq!(severity, Severity::Info);
        assert_eq!(msg, "sidecar starting");

        // Call-site fields are preserved as JSON...
        let parsed: serde_json::Value = serde_json::from_str(&extra).expect("extra is JSON");
        assert_eq!(parsed["socket"], "/tmp/kstack.sock");
        assert_eq!(parsed["pid"], 21311);
        // ...but the sidecar's own timestamp is dropped (the host stamps it).
        assert!(
            parsed.get("time").is_none(),
            "time must not leak into extra"
        );
    }

    #[test]
    fn classify_omits_extra_when_there_are_no_other_fields() {
        let (_, _, extra) = classify_str(r#"{"level":"INFO","msg":"hello"}"#, Severity::Warn);
        assert!(extra.is_empty(), "no call-site fields means no extra");
    }

    #[test]
    fn classify_forwards_non_json_at_the_fallback_level() {
        // Go panics and runtime messages are not slog JSON; they must still
        // be forwarded, verbatim, at the level of the pipe they arrived on.
        let raw = "panic: runtime error: invalid memory address";
        let (severity, msg, extra) = classify_str(raw, Severity::Warn);
        assert_eq!(severity, Severity::Warn);
        assert_eq!(msg, raw);
        assert!(extra.is_empty());
    }

    #[test]
    fn classify_skips_empty_and_blank_lines() {
        assert!(classify(b"", Severity::Info).is_none());
        assert!(classify(b"   \n", Severity::Info).is_none());
    }

    #[test]
    fn classify_trims_a_trailing_newline() {
        // The drain stream may hand us the line with its terminator attached;
        // it must not end up tacked onto the message.
        let (_, msg, _) = classify_str("{\"level\":\"INFO\",\"msg\":\"x\"}\n", Severity::Warn);
        assert_eq!(msg, "x");
    }

    #[test]
    fn classify_handles_invalid_utf8_without_panicking() {
        // 0xFF is never valid UTF-8; lossy decoding must not panic, and a
        // non-JSON result falls back as usual.
        let (severity, _, _) = classify(&[0xff, b'o', b'k'], Severity::Warn)
            .expect("invalid utf-8 still produces an event");
        assert_eq!(severity, Severity::Warn);
    }

    #[test]
    fn classify_treats_json_missing_required_fields_as_raw() {
        // A JSON object without `level`/`msg` is not a sidecar log line; it
        // is forwarded as raw text rather than silently losing the level.
        let raw = r#"{"foo":"bar"}"#;
        let (severity, msg, _) = classify_str(raw, Severity::Warn);
        assert_eq!(severity, Severity::Warn);
        assert_eq!(msg, raw);
    }
}
