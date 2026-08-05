import { capabilityCards, type CapabilityId, type CapabilityState } from "./api/gen/capabilities";

const stateById: Record<CapabilityId, CapabilityState> = Object.fromEntries(
  capabilityCards.map(({ id }) => [
    id,
    id === "connector.host"
      ? { mode: "local" }
      : {
          mode: "unavailable",
          reason:
            "Runtime state is not connected to this M0 shell yet; pos-api transport wiring lands in m0-s06.",
        },
  ]),
) as Record<CapabilityId, CapabilityState>;

export function CapabilityRegistryPreview() {
  return (
    <section aria-labelledby="capability-registry-title">
      <h2 id="capability-registry-title">Capability registry contract</h2>
      <p>
        This bootstrap view proves every public capability has a card. It labels transport-dependent
        states unavailable instead of hiding them.
      </p>
      <ul>
        {capabilityCards.map((card) => (
          <li key={card.id} data-capability-card={card.uiCard}>
            <h3>{card.title}</h3>
            <CapabilityStateView id={card.id} state={stateById[card.id]} />
          </li>
        ))}
      </ul>
    </section>
  );
}

function CapabilityStateView({ id, state }: { id: CapabilityId; state: CapabilityState }) {
  if (state.mode === "unavailable") {
    return (
      <p>
        Unavailable: {state.reason} Default: {defaultName(id)}.
      </p>
    );
  }
  if (id === "connector.host") {
    return (
      <p>
        Local host available. Its bounded mock tick is contract-tested; this is a bootstrap preview,
        not a live transport reading.
      </p>
    );
  }
  return <p>{state.mode === "local" ? "Local" : "Hosted"}</p>;
}

function defaultName(id: CapabilityId) {
  const card = capabilityCards.find((candidate) => candidate.id === id);
  return card?.localDefault ?? "unknown";
}
