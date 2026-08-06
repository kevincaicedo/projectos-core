// Transport selection is the one platform branch this UI is allowed (L12,
// vite.config.ts). Everything below it is shell-agnostic: both transports carry
// the same pos-api bytes, and this module never invents runtime state when a
// transport is missing — it reports what actually happened.
import {
  CAPABILITY_SNAPSHOT_QUERY,
  capabilityCards,
  type ApiErrorEnvelope,
  type CapabilityId,
  type CapabilityRow,
  type CapabilitySnapshot,
  type CapabilityState,
  type ConnectorHostTick,
} from "./gen/capabilities";

declare global {
  interface Window {
    readonly __TAURI__?: {
      readonly core?: {
        readonly invoke: (command: string, args?: Record<string, unknown>) => Promise<unknown>;
      };
    };
  }
}

export type TransportName = "tauri-ipc" | "http";

export type SnapshotOutcome =
  | {
      readonly kind: "ok";
      readonly transport: TransportName;
      readonly snapshot: CapabilitySnapshot;
    }
  | {
      readonly kind: "failed";
      readonly transport: TransportName;
      readonly error: ApiErrorEnvelope;
    };

// The HTTP transport lands with pos-server in m0-s08. Until then a browser
// build reports the honest failure instead of rendering invented state.
const QUERY_PATH = `/api/query/${CAPABILITY_SNAPSHOT_QUERY}`;

export function activeTransport(): TransportName {
  return typeof window.__TAURI__?.core?.invoke === "function" ? "tauri-ipc" : "http";
}

export async function fetchCapabilitySnapshot(): Promise<SnapshotOutcome> {
  return activeTransport() === "tauri-ipc" ? fetchOverIpc() : fetchOverHttp();
}

async function fetchOverIpc(): Promise<SnapshotOutcome> {
  const invoke = window.__TAURI__?.core?.invoke;
  if (invoke === undefined) {
    return transportFailure("tauri-ipc", "transport_unavailable", "The IPC bridge disappeared.");
  }
  try {
    const raw = await invoke("api_query", { name: CAPABILITY_SNAPSHOT_QUERY });
    return decodeSuccess("tauri-ipc", raw);
  } catch (thrown: unknown) {
    // A rejected Tauri command carries the pos-api error envelope as its value.
    return decodeFailure("tauri-ipc", thrown);
  }
}

async function fetchOverHttp(): Promise<SnapshotOutcome> {
  let response: Response;
  try {
    response = await fetch(QUERY_PATH, { headers: { accept: "application/json" } });
  } catch {
    return transportFailure(
      "http",
      "transport_unavailable",
      "No ProjectOS server answered this page; the web transport arrives with pos-server in m0-s08.",
    );
  }
  const body = await response.text();
  return response.ok ? decodeSuccess("http", body) : decodeFailure("http", body);
}

function decodeSuccess(transport: TransportName, raw: unknown): SnapshotOutcome {
  if (typeof raw !== "string") {
    return transportFailure(transport, "malformed_result", "The transport returned a non-string.");
  }
  const snapshot = asSnapshot(parseJson(raw));
  if (snapshot === null) {
    return transportFailure(
      transport,
      "malformed_result",
      "The runtime returned a shape this build does not recognise.",
    );
  }
  return { kind: "ok", transport, snapshot };
}

function decodeFailure(transport: TransportName, raw: unknown): SnapshotOutcome {
  const envelope = asErrorEnvelope(parseJson(typeof raw === "string" ? raw : ""));
  if (envelope === null) {
    return transportFailure(
      transport,
      "transport_failed",
      "The runtime failed without a typed error envelope.",
    );
  }
  return { kind: "failed", transport, error: envelope };
}

function transportFailure(
  transport: TransportName,
  code: string,
  message: string,
): SnapshotOutcome {
  return { kind: "failed", transport, error: { code, message, retriable: true } };
}

function parseJson(raw: string): unknown {
  try {
    return JSON.parse(raw) as unknown;
  } catch {
    return null;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asSnapshot(value: unknown): CapabilitySnapshot | null {
  if (!isRecord(value)) {
    return null;
  }
  const { surfaceVersion, capabilityTraitVersion, capabilities, connectorHost } = value;
  if (typeof surfaceVersion !== "number" || typeof capabilityTraitVersion !== "number") {
    return null;
  }
  if (!Array.isArray(capabilities)) {
    return null;
  }
  const rows: CapabilityRow[] = [];
  for (const candidate of capabilities) {
    const row = asRow(candidate);
    if (row === null) {
      return null;
    }
    rows.push(row);
  }
  const tick = asTick(connectorHost);
  if (tick === undefined) {
    return null;
  }
  return { surfaceVersion, capabilityTraitVersion, capabilities: rows, connectorHost: tick };
}

function asRow(value: unknown): CapabilityRow | null {
  if (!isRecord(value)) {
    return null;
  }
  const id = asCapabilityId(value.id);
  const provider = value.provider;
  const state = asState(value.state);
  if (id === null || typeof provider !== "string" || state === null) {
    return null;
  }
  return { id, provider, state };
}

// An id the generated catalog does not know about is rejected rather than
// rendered: a card the UI cannot explain is worse than a visible gap.
function asCapabilityId(value: unknown): CapabilityId | null {
  const known = capabilityCards.some((card) => card.id === value);
  return known ? (value as CapabilityId) : null;
}

function asState(value: unknown): CapabilityState | null {
  if (!isRecord(value)) {
    return null;
  }
  const { mode, reason } = value;
  if (mode === "local") {
    return { mode: "local" };
  }
  if (mode === "hosted") {
    return { mode: "hosted" };
  }
  if (mode === "unavailable" && typeof reason === "string" && reason.length > 0) {
    return { mode: "unavailable", reason };
  }
  return null;
}

// `undefined` means an unrecognised shape; `null` means the runtime honestly
// reported no tick. Collapsing those two would let a broken payload render as a
// healthy host.
function asTick(value: unknown): ConnectorHostTick | null | undefined {
  if (value === null) {
    return null;
  }
  if (!isRecord(value)) {
    return undefined;
  }
  const { hostAvailable, polledCount, nextCursor } = value;
  if (
    typeof hostAvailable !== "boolean" ||
    typeof polledCount !== "number" ||
    typeof nextCursor !== "number"
  ) {
    return undefined;
  }
  return { hostAvailable, polledCount, nextCursor };
}

function asErrorEnvelope(value: unknown): ApiErrorEnvelope | null {
  if (!isRecord(value)) {
    return null;
  }
  const { code, message, retriable } = value;
  if (typeof code !== "string" || typeof message !== "string" || typeof retriable !== "boolean") {
    return null;
  }
  return { code, message, retriable };
}
