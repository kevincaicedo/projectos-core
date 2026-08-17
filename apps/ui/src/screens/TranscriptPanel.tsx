// The transcript viewer (m1-s03): turn-by-turn speech with a timestamp
// gutter, an editable line, and a speaker a human assigns.
//
// Two rules this panel exists to make visible:
//
// 1. **The model's words are never overwritten.** An edited line shows the
//    correction *and* what the model said, side by side. "Original ASR
//    recoverable" is a thing the user can see, not a claim in a changelog.
// 2. **Nobody is attributed by a guess.** v1 detects turn *boundaries* from
//    pauses; who spoke is the user's to say, so an unassigned turn reads
//    "Unattributed" rather than inventing "Speaker A" (L3).
//
// Audio scrubbing, deep links, and paging past the first page are m1-s12's.

import { useState } from "react";
import type { TranscriptReport, TranscriptSegmentRow } from "../api/gen/api";
import { speakerLabel, timecode } from "../api/transcript";
import type { TranscriptController } from "./useTranscript";

interface TranscriptPanelProps {
  readonly projectSelected: boolean;
  readonly controller: TranscriptController;
}

export function TranscriptPanel({ projectSelected, controller }: TranscriptPanelProps) {
  if (!projectSelected) {
    return (
      <section className="card" data-transcript="no-project">
        <Header />
        <div className="teaching">
          <p>Select a project to read and correct its transcripts.</p>
        </div>
      </section>
    );
  }
  return (
    <section className="card" data-transcript={controller.view.state}>
      <Header />
      <RecordingPicker controller={controller} />
      {controller.selectedEvidenceId === null ? (
        <div className="teaching" data-transcript-state="no-evidence">
          <p>Choose a recording to read and correct its transcript.</p>
        </div>
      ) : (
        <Body controller={controller} />
      )}
    </section>
  );
}

function RecordingPicker({ controller }: { readonly controller: TranscriptController }) {
  const { recordings } = controller;
  if (recordings.state === "loading") {
    return <p data-transcript-recordings="loading">Looking for recordings…</p>;
  }
  if (recordings.state === "empty") {
    return (
      <p className="micro-label" data-transcript-recordings="empty">
        No recordings in this project yet.
      </p>
    );
  }
  if (recordings.state === "error") {
    return (
      <div data-transcript-recordings="error">
        <p>{recordings.error.message}</p>
        <button type="button" onClick={controller.refresh}>
          Try again
        </button>
      </div>
    );
  }
  return (
    <ul className="transcript-recordings" data-transcript-recordings="success">
      {recordings.data.map((row) => (
        <li key={row.evidenceId}>
          <button
            type="button"
            data-recording={row.evidenceId}
            aria-pressed={controller.selectedEvidenceId === row.evidenceId}
            onClick={() => controller.select(row.evidenceId)}
          >
            {row.title ?? row.externalId}
          </button>
        </li>
      ))}
    </ul>
  );
}

function Header() {
  return (
    <div className="run-feed-header">
      <h2 id="transcript-title">Transcript</h2>
      <span className="micro-label">turns · timestamps · your corrections</span>
    </div>
  );
}

function Body({ controller }: { readonly controller: TranscriptController }) {
  const { view } = controller;
  if (view.state === "loading") {
    return <p data-transcript-state="loading">Reading the transcript…</p>;
  }
  if (view.state === "empty") {
    return (
      <div className="teaching" data-transcript-state="empty">
        <p>This item has no transcript yet.</p>
        <p className="micro-label">
          Audio is transcribed by the TRANSCRIBE stage; a recording still in the pipeline has
          nothing to show here.
        </p>
      </div>
    );
  }
  if (view.state === "error") {
    return (
      <div data-transcript-state="error">
        <p>{view.error.message}</p>
        <button type="button" onClick={controller.refresh}>
          Try again
        </button>
      </div>
    );
  }
  return <Turns report={view.data} controller={controller} />;
}

function Turns({
  report,
  controller,
}: {
  readonly report: TranscriptReport;
  readonly controller: TranscriptController;
}) {
  return (
    <div data-transcript-state="success">
      {controller.lastError !== null ? (
        <p data-transcript-edit-error={controller.lastError.code}>{controller.lastError.message}</p>
      ) : null}
      <ol className="transcript-turns">
        {report.segments.map((segment) => (
          <Segment
            key={segment.segmentIndex}
            segment={segment}
            report={report}
            controller={controller}
          />
        ))}
      </ol>
      <p className="micro-label" data-transcript-bound={report.rowCountMax}>
        Showing {report.segments.length} of at most {report.rowCountMax} segments · pass{" "}
        {report.pass}
      </p>
    </div>
  );
}

function Segment({
  segment,
  report,
  controller,
}: {
  readonly segment: TranscriptSegmentRow;
  readonly report: TranscriptReport;
  readonly controller: TranscriptController;
}) {
  const [draft, setDraft] = useState<string | null>(null);
  const editing = draft !== null;
  return (
    <li
      className="transcript-turn"
      data-segment-index={segment.segmentIndex}
      data-starts-turn={segment.startsTurn}
      data-edited={segment.edited}
    >
      <span className="micro-label" data-segment-time={segment.startMs}>
        {timecode(segment.startMs)}
      </span>
      <SpeakerControl segment={segment} report={report} controller={controller} />
      {editing ? (
        <span className="transcript-edit">
          <input
            aria-label={`Correct the words at ${timecode(segment.startMs)}`}
            data-segment-input={segment.segmentIndex}
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
          />
          <button
            type="button"
            data-segment-save={segment.segmentIndex}
            disabled={controller.pending}
            onClick={() => {
              controller.correct(segment.segmentIndex, draft);
              setDraft(null);
            }}
          >
            Save
          </button>
          <button type="button" onClick={() => setDraft(null)}>
            Cancel
          </button>
        </span>
      ) : (
        <button
          type="button"
          className="transcript-text"
          data-segment-text={segment.segmentIndex}
          onClick={() => setDraft(segment.text)}
        >
          {segment.text}
        </button>
      )}
      {segment.edited ? (
        <span className="micro-label" data-segment-asr={segment.segmentIndex}>
          model heard: {segment.asrText}
        </span>
      ) : null}
    </li>
  );
}

function SpeakerControl({
  segment,
  report,
  controller,
}: {
  readonly segment: TranscriptSegmentRow;
  readonly report: TranscriptReport;
  readonly controller: TranscriptController;
}) {
  const [draft, setDraft] = useState<string | null>(null);
  if (draft !== null) {
    return (
      <span className="transcript-edit transcript-speaker-edit">
        <input
          aria-label="Name this speaker"
          data-speaker-input={segment.speakerIndex}
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
        />
        <button
          type="button"
          data-speaker-save={segment.speakerIndex}
          disabled={controller.pending}
          onClick={() => {
            // Index 0 means "nobody has said who this is". Naming *that* would
            // label every unattributed turn at once, so the first naming
            // creates speaker 1 and moves this turn onto it.
            const speakerIndex = segment.speakerIndex === 0 ? 1 : segment.speakerIndex;
            controller.nameSpeaker(speakerIndex, draft);
            if (segment.speakerIndex === 0) {
              controller.assignSpeaker(segment.segmentIndex, speakerIndex);
            }
            setDraft(null);
          }}
        >
          Save
        </button>
      </span>
    );
  }
  const label = speakerLabel(segment.speakerIndex, report.speakers);
  return (
    <button
      type="button"
      className="transcript-speaker"
      data-segment-speaker={segment.segmentIndex}
      onClick={() => setDraft(label === "Unattributed" ? "" : label)}
    >
      {label}
    </button>
  );
}
