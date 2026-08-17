// The home stage (m0-s09): workspace/project home with teaching empty
// states, the seam-outcome notice for palette-dispatched commands, and the
// capability registry (m0-s17 walking-skeleton evidence). Copy uses the
// fixed domain nouns — Evidence, Decision, Run, Task — never synonyms.

import type { OpenProjectRow } from "../api/gen/api";
import type { QueryView } from "../api/query";
import { CapabilityRegistryView } from "../CapabilityRegistryView";
import { ProjectActions } from "./ProjectActions";
import type { SeamNotice } from "./seam";
import { SourceHealthPanel } from "./SourceHealthPanel";
import type { SourceHealthController } from "./useSourceHealth";
import { TranscriptPanel } from "./TranscriptPanel";
import type { TranscriptController } from "./useTranscript";
import { RunFeedPanel } from "../runs/RunFeedPanel";
import type { EchoRunController } from "../runs/useEchoRun";

interface HomeScreenProps {
  readonly projects: QueryView<readonly OpenProjectRow[]>;
  readonly selected: OpenProjectRow | null;
  readonly notice: SeamNotice | null;
  readonly focusToken: number;
  readonly onChanged: () => void;
  readonly echoRun: EchoRunController;
  readonly sourceHealth: SourceHealthController;
  readonly transcript: TranscriptController;
}

export function HomeScreen({
  projects,
  selected,
  notice,
  focusToken,
  onChanged,
  echoRun,
  sourceHealth,
  transcript,
}: HomeScreenProps) {
  return (
    <main className="stage">
      <header className="stage-header">
        <div>
          <h1 className="stage-title">ProjectOS</h1>
          <p className="stage-meta" data-stage-project={selected?.projectId ?? "none"}>
            {selected === null
              ? "Every project is an append-only log you own · the walking skeleton grows one epic at a time"
              : `${selected.name ?? "Untitled Project"} · every artifact links back to why it exists`}
          </p>
        </div>
      </header>

      {notice !== null && <SeamNoticeCard notice={notice} />}

      {selected === null ? (
        <WorkspaceHome projects={projects} />
      ) : (
        <ProjectHome project={selected} />
      )}

      <SourceHealthPanel projectSelected={selected !== null} controller={sourceHealth} />
      <TranscriptPanel projectSelected={selected !== null} controller={transcript} />
      <RunFeedPanel project={selected} controller={echoRun} />
      <ProjectActions onChanged={onChanged} focusToken={focusToken} />
      <section className="card">
        <CapabilityRegistryView />
      </section>
    </main>
  );
}

function WorkspaceHome({ projects }: { projects: QueryView<readonly OpenProjectRow[]> }) {
  if (projects.state !== "empty") {
    return null;
  }
  // The teaching empty state: what this place is and how to fill it.
  return (
    <section className="teaching" data-teaching="workspace">
      <h2>This workspace has no open project</h2>
      <p>
        Create a project below (or press ⌘K). From M1 the project ingests Evidence — meetings,
        documents, messages — and every Insight, Decision, and Task will link back to it.
      </p>
    </section>
  );
}

function ProjectHome({ project }: { project: OpenProjectRow }) {
  return (
    <section className="card" data-project-home={project.projectId}>
      <h2>{project.name ?? "Untitled Project"}</h2>
      <p className="mono">
        {project.projectId.slice(0, 12)} · {project.template} · format v{project.formatVersion} ·
        head seq {project.headSeq}
      </p>
      <div className="teaching" data-teaching="project">
        <p>
          This project is an empty log with a heartbeat. M1 brings Evidence ingestion and search; M2
          brings the Navigator that answers with citations; agents will run here with every step
          ledgered before it executes.
        </p>
      </div>
    </section>
  );
}

function SeamNoticeCard({ notice }: { notice: SeamNotice }) {
  return (
    <section
      className={notice.kind === "refused" ? "notice-clay" : "notice-amber"}
      data-seam-notice={notice.kind}
    >
      <strong>{notice.title}.</strong> {notice.detail}
    </section>
  );
}
