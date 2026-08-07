//! Bounded parser for provider-sent Server-Sent Event streams. This is the
//! *client* side of SSE (Anthropic/OpenAI/Google streaming responses); the
//! frozen server-side framing ProjectOS emits lives in `pos-api` and is not
//! this module's concern.
//!
//! Provider bytes are untrusted input (L6-adjacent): every buffer here has a
//! stated cap, and overflow is a typed error, never an allocation race.

/// One SSE event's `data:` payload can carry a whole model turn; 1 MiB is
/// far above any observed provider delta and low enough to stop a runaway
/// stream from ballooning memory (L8).
const EVENT_DATA_BYTES_MAX: usize = 1024 * 1024;

/// A parsed provider event: the optional `event:` name and the joined
/// `data:` payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SseEvent {
    pub event: Option<String>,
    pub data: String,
}

/// Typed parse failure. Carries sizes and shapes, never payload bytes, so it
/// can travel into weather messages without echoing provider content.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SseParseError {
    EventTooLarge { bytes: usize },
}

impl std::fmt::Display for SseParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EventTooLarge { bytes } => write!(
                formatter,
                "SSE event exceeds {EVENT_DATA_BYTES_MAX} bytes ({bytes} buffered)"
            ),
        }
    }
}

/// Incremental SSE decoder: feed transport chunks in, take complete events
/// out. Carries partial lines across chunk boundaries because providers cut
/// frames wherever their flush landed.
#[derive(Default)]
pub struct SseDecoder {
    line_buffer: Vec<u8>,
    event_name: Option<String>,
    data_lines: Vec<String>,
}

impl SseDecoder {
    /// Consumes one transport chunk and appends every event it completed.
    ///
    /// # Errors
    ///
    /// [`SseParseError::EventTooLarge`] once the buffered event crosses the
    /// cap; the stream is unusable after that (the caller drops it).
    pub fn feed(&mut self, chunk: &[u8], events: &mut Vec<SseEvent>) -> Result<(), SseParseError> {
        for byte in chunk {
            if *byte == b'\n' {
                self.line_complete(events)?;
                continue;
            }
            self.line_buffer.push(*byte);
            if self.buffered_len() > EVENT_DATA_BYTES_MAX {
                return Err(SseParseError::EventTooLarge {
                    bytes: self.buffered_len(),
                });
            }
        }
        Ok(())
    }

    fn buffered_len(&self) -> usize {
        self.line_buffer.len() + self.data_lines.iter().map(String::len).sum::<usize>()
    }

    fn line_complete(&mut self, events: &mut Vec<SseEvent>) -> Result<(), SseParseError> {
        if self.line_buffer.last() == Some(&b'\r') {
            self.line_buffer.pop();
        }
        let line = String::from_utf8_lossy(&self.line_buffer).into_owned();
        self.line_buffer.clear();

        if line.is_empty() {
            if !self.data_lines.is_empty() || self.event_name.is_some() {
                events.push(SseEvent {
                    event: self.event_name.take(),
                    data: self.data_lines.join("\n"),
                });
                self.data_lines.clear();
            }
            return Ok(());
        }
        if let Some(value) = line.strip_prefix("data:") {
            self.data_lines
                .push(value.strip_prefix(' ').unwrap_or(value).to_owned());
            if self.buffered_len() > EVENT_DATA_BYTES_MAX {
                return Err(SseParseError::EventTooLarge {
                    bytes: self.buffered_len(),
                });
            }
        } else if let Some(value) = line.strip_prefix("event:") {
            self.event_name = Some(value.strip_prefix(' ').unwrap_or(value).to_owned());
        }
        // Comment lines (":") and fields we do not use (id:, retry:) are
        // ignored per the SSE contract.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{SseDecoder, SseEvent, SseParseError};

    fn collect(chunks: &[&str]) -> Vec<SseEvent> {
        let mut decoder = SseDecoder::default();
        let mut events = Vec::new();
        for chunk in chunks {
            decoder
                .feed(chunk.as_bytes(), &mut events)
                .expect("bounded fixture input");
        }
        events
    }

    #[test]
    fn events_survive_arbitrary_chunk_boundaries() {
        let whole = collect(&["event: delta\ndata: {\"a\":1}\n\ndata: [DONE]\n\n"]);
        let split = collect(&[
            "event: de",
            "lta\nda",
            "ta: {\"a\":1}\n",
            "\ndata: [DONE]\n\n",
        ]);
        assert_eq!(whole, split);
        assert_eq!(whole.len(), 2);
        assert_eq!(whole[0].event.as_deref(), Some("delta"));
        assert_eq!(whole[0].data, "{\"a\":1}");
        assert_eq!(whole[1].event, None);
        assert_eq!(whole[1].data, "[DONE]");
    }

    #[test]
    fn multi_line_data_joins_and_comments_are_ignored() {
        let events = collect(&[": keep-alive\ndata: first\ndata: second\n\n"]);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].data, "first\nsecond");
    }

    #[test]
    fn a_runaway_event_is_a_typed_error_not_an_allocation() {
        let mut decoder = SseDecoder::default();
        let mut events = Vec::new();
        let oversized = vec![b'x'; 2 * 1024 * 1024];
        let error = decoder
            .feed(&oversized, &mut events)
            .expect_err("an unbounded line must be refused");
        assert!(matches!(error, SseParseError::EventTooLarge { .. }));
    }
}
