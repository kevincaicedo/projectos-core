// The icon rail (doc 06 §3): three groups — think, work, configure. M0 has
// one live screen (Home); every other destination is present, disabled, and
// honest about which milestone brings it, so the shell teaches the roadmap
// instead of hiding it.

interface RailProps {
  readonly onHome: () => void;
}

interface RailDestination {
  readonly id: string;
  readonly glyph: string;
  readonly title: string;
  readonly arrives: string;
}

const THINK: readonly RailDestination[] = [
  { id: "navigator", glyph: "◎", title: "Navigator", arrives: "M2" },
  { id: "evidence", glyph: "▤", title: "Evidence & graph", arrives: "M1" },
  { id: "decisions", glyph: "◆", title: "Decision records", arrives: "M3" },
];

const WORK: readonly RailDestination[] = [
  { id: "board", glyph: "▦", title: "Tickets & board", arrives: "M3" },
  { id: "runs", glyph: "▶", title: "Agent runs", arrives: "M4" },
  { id: "brain", glyph: "❋", title: "Project brain", arrives: "M2" },
];

const CONFIGURE: readonly RailDestination[] = [
  { id: "workshop", glyph: "⚒", title: "Workshop", arrives: "M7" },
  { id: "integrations", glyph: "⇄", title: "Integrations", arrives: "M1" },
  { id: "permissions", glyph: "⛨", title: "Permissions", arrives: "M4" },
];

export function Rail({ onHome }: RailProps) {
  return (
    <nav className="rail" aria-label="Screens">
      <button type="button" className="rail-item" data-active="true" title="Home" onClick={onHome}>
        ⌂
      </button>
      <RailGroup label="think" items={THINK} />
      <RailGroup label="work" items={WORK} />
      <RailGroup label="configure" items={CONFIGURE} />
    </nav>
  );
}

function RailGroup({ label, items }: { label: string; items: readonly RailDestination[] }) {
  return (
    <div className="rail-group">
      <span className="rail-label">{label}</span>
      {items.map((item) => (
        <button
          key={item.id}
          type="button"
          className="rail-item"
          disabled
          title={`${item.title} — arrives with ${item.arrives}`}
        >
          {item.glyph}
        </button>
      ))}
    </div>
  );
}
