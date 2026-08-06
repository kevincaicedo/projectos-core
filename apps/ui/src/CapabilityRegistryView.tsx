import { useCallback, useEffect, useRef, useState } from "react";
import {
  capabilityCards,
  type CapabilityId,
  type CapabilitySnapshot,
} from "./api/gen/capabilities";
import { fetchCapabilitySnapshot, type SnapshotOutcome } from "./api/transport";

// Every async surface renders all four states (PROJECTOS_STYLE): loading,
// empty, error-with-retry, and success. There is no fifth state in which the
// UI guesses what the runtime would have said.
type LoadState =
  { readonly phase: "loading" } | { readonly phase: "loaded"; readonly outcome: SnapshotOutcome };

export function CapabilityRegistryView() {
  const [state, setState] = useState<LoadState>({ phase: "loading" });
  // Monotonic request id, so a slow first response cannot overwrite a newer
  // retry's result. Ordering by arrival would make the visible state depend on
  // network timing rather than on what the user last asked for.
  const latestRequest = useRef(0);

  const load = useCallback(() => {
    latestRequest.current += 1;
    const request = latestRequest.current;
    setState({ phase: "loading" });
    void fetchCapabilitySnapshot().then((outcome) => {
      if (latestRequest.current === request) {
        setState({ phase: "loaded", outcome });
      }
    });
  }, []);

  useEffect(load, [load]);

  return (
    <section aria-labelledby="capability-registry-title">
      <h2 id="capability-registry-title">Capability registry</h2>
      <RegistryBody state={state} onRetry={load} />
    </section>
  );
}

function RegistryBody({ state, onRetry }: { state: LoadState; onRetry: () => void }) {
  if (state.phase === "loading") {
    return <p data-registry-state="loading">Reading the runtime capability registry…</p>;
  }

  if (state.outcome.kind === "failed") {
    return (
      <div data-registry-state="error">
        <p>
          The {state.outcome.transport} transport did not return runtime state:{" "}
          {state.outcome.error.message}
        </p>
        <p>
          No capability state is shown, because this build has none to show. A card here would be a
          guess.
        </p>
        <button type="button" onClick={onRetry} disabled={!state.outcome.error.retriable}>
          Try again
        </button>
      </div>
    );
  }

  const { snapshot, transport } = state.outcome;
  if (snapshot.capabilities.length === 0) {
    return (
      <p data-registry-state="empty">
        The runtime resolved no capability sockets. That is a startup fault, not an empty list.
      </p>
    );
  }

  return (
    <div data-registry-state="ready">
      <p>
        Live over {transport} · surface v{snapshot.surfaceVersion} · traits v
        {snapshot.capabilityTraitVersion} · <ConnectorHostSummary snapshot={snapshot} />
      </p>
      <ul>
        {snapshot.capabilities.map((row) => (
          <li key={row.id} data-capability-card={row.id}>
            <h3>{titleOf(row.id)}</h3>
            <p>
              {row.state.mode === "unavailable"
                ? `Unavailable: ${row.state.reason}`
                : `${row.state.mode === "local" ? "Local" : "Hosted"} · ${row.provider}`}
            </p>
          </li>
        ))}
      </ul>
    </div>
  );
}

function ConnectorHostSummary({ snapshot }: { snapshot: CapabilitySnapshot }) {
  const tick = snapshot.connectorHost;
  if (tick === null) {
    return <span data-connector-host="unknown">connector host tick did not complete</span>;
  }
  return (
    <span data-connector-host={tick.hostAvailable ? "available" : "unavailable"}>
      connector host {tick.hostAvailable ? "available" : "unavailable"} after a bounded tick of{" "}
      {tick.polledCount} item(s), cursor {tick.nextCursor}
    </span>
  );
}

function titleOf(id: CapabilityId) {
  return capabilityCards.find((card) => card.id === id)?.title ?? id;
}
