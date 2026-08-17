// Typed readers over the generated transcript wire types (m1-s03). Same rule
// as `evidence.ts`: validation narrows runtime bytes into the ts-rs shapes,
// and an unrecognised payload becomes an error state rather than rendered
// content.
//
// One field here is load-bearing beyond validation: every segment carries
// `asrText` beside `text`. The viewer renders `text` and can show `asrText`
// next to it, which is how "the model's original output stays recoverable"
// reaches a user instead of staying an assertion in a test.

import type { TranscriptReport, TranscriptSegmentRow, TranscriptSpeakerRow } from "./gen/api";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function asTranscriptReport(value: unknown): TranscriptReport | null {
  if (!isRecord(value) || !Array.isArray(value.segments) || !Array.isArray(value.speakers)) {
    return null;
  }
  if (
    typeof value.evidenceId !== "string" ||
    typeof value.pass !== "number" ||
    typeof value.rowCountMax !== "number"
  ) {
    return null;
  }
  const segments: TranscriptSegmentRow[] = [];
  for (const candidate of value.segments) {
    const segment = asSegment(candidate);
    if (segment === null) {
      return null;
    }
    segments.push(segment);
  }
  const speakers: TranscriptSpeakerRow[] = [];
  for (const candidate of value.speakers) {
    const speaker = asSpeaker(candidate);
    if (speaker === null) {
      return null;
    }
    speakers.push(speaker);
  }
  return {
    evidenceId: value.evidenceId,
    pass: value.pass,
    segments,
    speakers,
    rowCountMax: value.rowCountMax,
  };
}

function asSegment(value: unknown): TranscriptSegmentRow | null {
  if (!isRecord(value)) {
    return null;
  }
  const { segmentIndex, startMs, endMs, startsTurn, speakerIndex, text, asrText, edited } = value;
  if (
    typeof segmentIndex !== "number" ||
    typeof startMs !== "number" ||
    typeof endMs !== "number" ||
    typeof startsTurn !== "boolean" ||
    typeof speakerIndex !== "number" ||
    typeof text !== "string" ||
    typeof asrText !== "string" ||
    typeof edited !== "boolean"
  ) {
    return null;
  }
  return { segmentIndex, startMs, endMs, startsTurn, speakerIndex, text, asrText, edited };
}

function asSpeaker(value: unknown): TranscriptSpeakerRow | null {
  if (!isRecord(value)) {
    return null;
  }
  const { speakerIndex, name } = value;
  if (typeof speakerIndex !== "number" || typeof name !== "string") {
    return null;
  }
  return { speakerIndex, name };
}

/// The name a user gave a speaker, or the honest placeholder.
///
/// v1 has no diarization: a segment nobody has attributed belongs to
/// "Unattributed", not to "Speaker A". Inventing an identity on a recording a
/// citation points at is exactly the fabrication L3 forbids.
export function speakerLabel(
  speakerIndex: number,
  speakers: readonly TranscriptSpeakerRow[],
): string {
  const named = speakers.find((speaker) => speaker.speakerIndex === speakerIndex);
  if (named !== undefined) {
    return named.name;
  }
  return speakerIndex === 0 ? "Unattributed" : `Speaker ${speakerIndex}`;
}

/// `m:ss` from the start of the recording — what a transcript gutter shows and
/// what a citation deep-link will carry (m1-s12).
export function timecode(startMs: number): string {
  const totalSeconds = Math.floor(startMs / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${seconds.toString().padStart(2, "0")}`;
}
