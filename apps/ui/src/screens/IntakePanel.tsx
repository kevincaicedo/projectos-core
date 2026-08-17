// The intake panel (m1-s07): drop a file, watch it become Evidence.
//
// Three things this panel exists to make visible, in order of how often they
// matter:
//
// 1. **Re-dropping a file you already have is a no-op, and it says so.** It
//    is the most common thing that happens here — a partner re-imports a
//    folder — and it must read as "you already have this", never as a second
//    copy and never as a failure.
// 2. **A refusal names itself.** A file over a stated bound, or one the
//    runtime could not read, gets its own row with the typed code. A batch of
//    two hundred does not fail because one of them did.
// 3. **What the file *is* came from its bytes.** The row shows the media kind
//    the sniffer decided, not the extension the file happened to carry.

import { useState } from "react";
import type { IngestSubmitRow } from "../api/gen/api";
import { byteSize, intakeSummary } from "../api/intake";
import type { IntakeController } from "./useIntake";

interface IntakePanelProps {
  readonly projectSelected: boolean;
  readonly controller: IntakeController;
}

export function IntakePanel({ projectSelected, controller }: IntakePanelProps) {
  if (!projectSelected) {
    return (
      <section className="card" data-intake="no-project">
        <Header />
        <div className="teaching">
          <p>Select a project to add Evidence to it.</p>
        </div>
      </section>
    );
  }
  return (
    <section className="card" data-intake={controller.busy ? "busy" : "ready"}>
      <Header />
      <DropZone controller={controller} />
      {controller.lastError !== null ? (
        <p data-intake-error={controller.lastError.code}>{controller.lastError.message}</p>
      ) : null}
      {controller.report !== null ? <Summary controller={controller} /> : null}
    </section>
  );
}

function Header() {
  return (
    <div className="run-feed-header">
      <h2 id="intake-title">Add Evidence</h2>
      <span className="micro-label">recordings · notes · transcripts · exports</span>
    </div>
  );
}

function DropZone({ controller }: { readonly controller: IntakeController }) {
  const [over, setOver] = useState(false);
  return (
    <div
      className={over ? "intake-drop intake-drop-over" : "intake-drop"}
      data-intake-drop={over ? "over" : "idle"}
      onDragOver={(event) => {
        event.preventDefault();
        setOver(true);
      }}
      onDragLeave={() => setOver(false)}
      onDrop={(event) => {
        // The desktop shell gets its paths from the native window event
        // instead; letting the webview also handle the drop would ingest the
        // same file twice on one gesture.
        event.preventDefault();
        setOver(false);
        if (controller.desktop) {
          return;
        }
        controller.submitFiles(Array.from(event.dataTransfer.files));
      }}
    >
      <p>
        {controller.desktop
          ? "Drop files here, or choose them."
          : "Drop files here, or choose them. Watch folders are a desktop capability."}
      </p>
      {controller.desktop ? (
        <button
          type="button"
          data-intake-choose="desktop"
          disabled={controller.busy}
          onClick={controller.choose}
        >
          Choose files…
        </button>
      ) : (
        <label className="intake-file-label">
          <span>Choose files…</span>
          <input
            type="file"
            multiple
            data-intake-choose="web"
            disabled={controller.busy}
            onChange={(event) => {
              controller.submitFiles(Array.from(event.target.files ?? []));
              event.target.value = "";
            }}
          />
        </label>
      )}
      {controller.busy ? <p data-intake-state="busy">Streaming into the project…</p> : null}
    </div>
  );
}

function Summary({ controller }: { readonly controller: IntakeController }) {
  const report = controller.report;
  if (report === null) {
    return null;
  }
  return (
    <div data-intake-summary={report.addedCount === 0 ? "no-new-evidence" : "added"}>
      <p
        data-intake-counts={`${report.addedCount}/${report.duplicateCount}/${report.refusedCount}`}
      >
        {intakeSummary(report)}
      </p>
      {!report.backgroundWorkersRunning ? (
        <p className="micro-label" data-intake-workers="stopped">
          Queued, but nothing in this process is running the pipeline yet.
        </p>
      ) : null}
      <ul className="intake-items">
        {report.items.map((item) => (
          <IntakeItem
            key={`${item.fileName}:${item.evidenceId ?? item.refusedCode ?? ""}`}
            item={item}
          />
        ))}
      </ul>
      <button type="button" data-intake-dismiss onClick={controller.dismiss}>
        Dismiss
      </button>
    </div>
  );
}

function IntakeItem({ item }: { readonly item: IngestSubmitRow }) {
  return (
    <li data-intake-item={item.outcome} data-intake-file={item.fileName}>
      <span>{item.fileName}</span>{" "}
      <span className="micro-label" data-intake-media={item.mediaKind ?? "unknown"}>
        {item.outcome === "duplicate"
          ? "already ingested"
          : item.outcome === "refused"
            ? `refused · ${item.refusedCode ?? "unknown"}`
            : `${item.mediaKind ?? "unknown"} · ${byteSize(item.byteSize)}`}
      </span>
      {item.outcome === "refused" && item.refusedDetail !== null ? (
        <span className="micro-label" data-intake-refused-detail={item.refusedCode ?? "unknown"}>
          {item.refusedDetail}
        </span>
      ) : null}
    </li>
  );
}
