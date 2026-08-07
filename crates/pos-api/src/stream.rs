//! SSE framing for the stream surface (m0-s06, frozen by §3.2).
//!
//! A stream item travels as one SSE frame: `id:` carries the per-stream
//! sequence number, `event:` the item kind, `data:` one line of canonical
//! JSON. Resume is cursor-based: a client that reconnects presents the last
//! `id` it saw (the `Last-Event-ID` header both browsers and our transports
//! send) and receives every frame after it, or a typed gap error when the
//! window no longer reaches back that far — never a silent hole (L8).
//!
//! This module is pure framing: no transport, no runtime, no I/O. The axum
//! handler and the Tauri channel both consume it, which is what keeps the
//! two transports byte-identical on the stream surface.

use crate::ApiError;
use std::collections::VecDeque;

/// Frames a resuming client can still be served from memory. At the M0 frame
/// budget (~2 KiB of JSON per step frame) this bounds the window near 512 KiB
/// per stream; a client further behind than this refetches through a query
/// instead of an unbounded replay buffer (L8).
pub const STREAM_RESUME_WINDOW_LEN: usize = 256;

/// Reconnection hint emitted once at the head of every SSE response. Two
/// seconds keeps a dropped desktop/web client honest about retry pressure
/// without hammering the server.
pub const SSE_RETRY_MS: u16 = 2_000;

/// One item of a stream, already serialized. `stream_seq` starts at 1 and is
/// contiguous per stream, mirroring the event-log discipline so resume logic
/// needs no special cases.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamFrame {
    pub stream_seq: u64,
    /// SSE `event:` name — a fixed vocabulary token (`run.step`), never
    /// ingested content.
    pub event_kind: &'static str,
    /// One line of canonical JSON. The framing asserts the invariant that
    /// serde_json compact output cannot contain a raw newline.
    pub data_json: String,
}

impl StreamFrame {
    /// Renders the frame in SSE wire form:
    /// `id: <seq>\nevent: <kind>\ndata: <json>\n\n`.
    #[must_use]
    pub fn to_sse(&self) -> String {
        debug_assert!(
            !self.data_json.contains('\n') && !self.data_json.contains('\r'),
            "frame data must be single-line JSON; a raw newline would split the SSE data field"
        );
        debug_assert!(
            !self.event_kind.contains('\n') && !self.event_kind.contains(':'),
            "event kinds are fixed vocabulary tokens"
        );
        format!(
            "id: {}\nevent: {}\ndata: {}\n\n",
            self.stream_seq, self.event_kind, self.data_json
        )
    }
}

/// Renders a full SSE response body: the retry hint followed by every frame.
/// Both transports call this one encoder, so stream bytes cannot drift.
#[must_use]
pub fn sse_body(frames: &[StreamFrame]) -> String {
    let mut body = format!("retry: {SSE_RETRY_MS}\n\n");
    for frame in frames {
        body.push_str(&frame.to_sse());
    }
    body
}

/// Parses a client-presented resume cursor. `None` means a fresh subscribe.
///
/// # Errors
///
/// Returns the typed envelope when the presented value is not a base-10
/// sequence number — a malformed cursor is a client bug surfaced, not a
/// silent restart from zero.
pub fn parse_resume_cursor(last_event_id: Option<&str>) -> Result<Option<u64>, ApiError> {
    let Some(text) = last_event_id else {
        return Ok(None);
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    trimmed.parse::<u64>().map(Some).map_err(|_| ApiError {
        code: "invalid_input",
        message: format!("Last-Event-ID {trimmed:?} is not a stream sequence number"),
        retriable: false,
    })
}

/// Bounded replay window a live stream maintains so dropped clients can
/// resume. Eviction is silent for the producer and *loud* for a consumer that
/// fell out of the window — [`ResumeWindow::frames_after`] reports the gap.
#[derive(Debug, Default)]
pub struct ResumeWindow {
    frames: VecDeque<StreamFrame>,
}

impl ResumeWindow {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a frame, evicting the oldest once the window is full.
    ///
    /// The contiguity assertion is the pre-append half of the pair; the
    /// post-resume half lives in [`Self::frames_after`].
    pub fn push(&mut self, frame: StreamFrame) {
        if let Some(last) = self.frames.back() {
            debug_assert_eq!(
                frame.stream_seq,
                last.stream_seq + 1,
                "stream seqs are contiguous; a gap here breaks every resuming client"
            );
        }
        if self.frames.len() == STREAM_RESUME_WINDOW_LEN {
            self.frames.pop_front();
        }
        self.frames.push_back(frame);
    }

    /// Every frame after `cursor` (exclusive), oldest first. `cursor = None`
    /// replays the whole window (fresh subscribe).
    ///
    /// # Errors
    ///
    /// Returns the typed envelope when the cursor predates the window — the
    /// client must refetch state through a query instead of trusting a replay
    /// with a hole in it.
    pub fn frames_after(&self, cursor: Option<u64>) -> Result<Vec<StreamFrame>, ApiError> {
        let Some(cursor) = cursor else {
            return Ok(self.frames.iter().cloned().collect());
        };
        let Some(oldest) = self.frames.front() else {
            // An empty window can serve any cursor: there is nothing after it.
            return Ok(Vec::new());
        };
        if cursor + 1 < oldest.stream_seq {
            return Err(ApiError {
                code: "resume_window_exceeded",
                message: format!(
                    "cursor {cursor} predates the {STREAM_RESUME_WINDOW_LEN}-frame resume \
                     window (oldest retained seq {}); refetch state through a query",
                    oldest.stream_seq
                ),
                retriable: false,
            });
        }
        let resumed: Vec<StreamFrame> = self
            .frames
            .iter()
            .filter(|frame| frame.stream_seq > cursor)
            .cloned()
            .collect();
        debug_assert!(
            resumed
                .first()
                .is_none_or(|first| first.stream_seq == cursor + 1),
            "resume must hand back exactly the frames after the cursor"
        );
        Ok(resumed)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ResumeWindow, SSE_RETRY_MS, STREAM_RESUME_WINDOW_LEN, StreamFrame, parse_resume_cursor,
        sse_body,
    };

    fn frame(seq: u64) -> StreamFrame {
        StreamFrame {
            stream_seq: seq,
            event_kind: "run.step",
            data_json: format!("{{\"step\":{seq}}}"),
        }
    }

    #[test]
    fn the_sse_wire_form_is_exact() {
        assert_eq!(
            frame(7).to_sse(),
            "id: 7\nevent: run.step\ndata: {\"step\":7}\n\n"
        );
        assert_eq!(
            sse_body(&[frame(1), frame(2)]),
            format!(
                "retry: {SSE_RETRY_MS}\n\nid: 1\nevent: run.step\ndata: {{\"step\":1}}\n\n\
                 id: 2\nevent: run.step\ndata: {{\"step\":2}}\n\n"
            )
        );
    }

    #[test]
    fn a_dropped_connection_resumes_from_its_sequence_number() {
        let mut window = ResumeWindow::new();
        for seq in 1..=10 {
            window.push(frame(seq));
        }
        // The client saw frames 1..=6, then the connection dropped.
        let resumed = window
            .frames_after(Some(6))
            .expect("cursor 6 is inside the window");
        let seqs: Vec<u64> = resumed.iter().map(|frame| frame.stream_seq).collect();
        assert_eq!(seqs, vec![7, 8, 9, 10]);
        // The resumed bytes are the same bytes a never-dropped client got.
        assert_eq!(resumed[0].to_sse(), frame(7).to_sse());
    }

    #[test]
    fn a_fresh_subscribe_replays_the_window_and_the_current_cursor_replays_nothing() {
        let mut window = ResumeWindow::new();
        for seq in 1..=3 {
            window.push(frame(seq));
        }
        assert_eq!(window.frames_after(None).expect("fresh subscribe").len(), 3);
        assert!(
            window
                .frames_after(Some(3))
                .expect("cursor at head is valid")
                .is_empty()
        );
        assert!(ResumeWindow::new().frames_after(Some(99)).is_ok());
    }

    #[test]
    fn a_cursor_older_than_the_window_is_a_typed_gap_not_a_silent_hole() {
        let mut window = ResumeWindow::new();
        // Push enough to evict frame 1..=4.
        let last_seq = STREAM_RESUME_WINDOW_LEN as u64 + 4;
        for seq in 1..=last_seq {
            window.push(frame(seq));
        }
        assert_eq!(window.len(), STREAM_RESUME_WINDOW_LEN);
        let error = window
            .frames_after(Some(2))
            .expect_err("an evicted cursor must be a typed gap");
        assert_eq!(error.code, "resume_window_exceeded");
        assert!(!error.retriable);
        // The boundary cursor (exactly the frame before the oldest) still works.
        let oldest_seq = last_seq - STREAM_RESUME_WINDOW_LEN as u64 + 1;
        assert!(window.frames_after(Some(oldest_seq - 1)).is_ok());
    }

    #[test]
    fn cursor_parsing_accepts_numbers_and_rejects_garbage() {
        assert_eq!(parse_resume_cursor(None).expect("no header"), None);
        assert_eq!(parse_resume_cursor(Some("")).expect("empty header"), None);
        assert_eq!(
            parse_resume_cursor(Some(" 42 ")).expect("padded number"),
            Some(42)
        );
        let error = parse_resume_cursor(Some("seven")).expect_err("words are not cursors");
        assert_eq!(error.code, "invalid_input");
    }
}
