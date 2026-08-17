// Transport selection is the one platform branch this UI is allowed (L12,
// vite.config.ts). Everything below it is shell-agnostic: both transports carry
// the same pos-api bytes, and this module never invents runtime state when a
// transport is missing — it reports what actually happened.
import { Channel } from "@tauri-apps/api/core";
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
import { SseDecoder, type SseMessage } from "./sse";

declare global {
  interface Window {
    readonly __TAURI__?: {
      readonly core?: {
        readonly invoke: (command: string, args?: Record<string, unknown>) => Promise<unknown>;
      };
      // Native dialogs and menu events (m0-s07). Present only in the Tauri
      // webview; `src/api/shell.ts` degrades to no-ops without them.
      readonly dialog?: {
        readonly open: (options: {
          directory?: boolean;
          multiple?: boolean;
          title?: string;
        }) => Promise<unknown>;
      };
      readonly event?: {
        readonly listen: <T>(
          name: string,
          handler: (event: { payload: T }) => void,
        ) => Promise<() => void>;
      };
    };
  }
}

export type TransportName = "tauri-ipc" | "http";

/// The generic dispatch result: raw registry bytes (already parsed) or the
/// typed envelope. Screens narrow `value` through their own validators —
/// an unrecognised shape renders as an error, never as content.
export type DispatchOutcome =
  | { readonly kind: "ok"; readonly transport: TransportName; readonly value: unknown }
  | {
      readonly kind: "failed";
      readonly transport: TransportName;
      readonly error: ApiErrorEnvelope;
    };

/// Dispatches a registry query over whichever transport this shell has.
export async function apiQuery(name: string, inputJson?: string): Promise<DispatchOutcome> {
  if (activeTransport() === "tauri-ipc") {
    return invokeIpc("api_query", {
      name,
      ...(inputJson === undefined ? {} : { input: inputJson }),
    });
  }
  const query = inputJson === undefined ? "" : `?input=${encodeURIComponent(inputJson)}`;
  return fetchHttp(`/api/query/${name}${query}`, { headers: { accept: "application/json" } });
}

/// Dispatches a registry command. After a command resolves, callers refetch
/// affected queries — reconciliation is re-reading the runtime's truth,
/// never keeping a forked local copy of it (L1 in the browser).
export async function apiCommand(name: string, inputJson: string): Promise<DispatchOutcome> {
  if (activeTransport() === "tauri-ipc") {
    return invokeIpc("api_command", { name, input: inputJson });
  }
  return fetchHttp(`/api/cmd/${name}`, {
    method: "POST",
    headers: { accept: "application/json", "content-type": "application/json" },
    body: inputJson,
  });
}

/// Dispatches a command whose bytes are a file the browser holds (m1-s07).
///
/// This is the one place the two transports genuinely differ, and the reason
/// is physical rather than architectural: the desktop shell has the file's
/// *path* and the core opens it, while a browser has only the bytes and has
/// to put them on the wire. Both end at the same registry command, so the
/// report a user sees is the same bytes either way (L12).
export async function apiUpload(
  name: string,
  inputJson: string,
  file: File,
): Promise<DispatchOutcome> {
  if (activeTransport() === "tauri-ipc") {
    // Unreachable through the desktop drop zone, which sends paths. Reported
    // rather than silently mis-dispatched, because a shell that quietly did
    // something else here would be the parity bug this comment exists about.
    return {
      kind: "failed",
      transport: "tauri-ipc",
      error: {
        code: "invalid_input",
        message: "The desktop shell ingests files by path, not by upload.",
        retriable: false,
      },
    };
  }
  return fetchHttp(`/api/upload/${name}?input=${encodeURIComponent(inputJson)}`, {
    method: "POST",
    headers: { accept: "application/json", "content-type": "application/octet-stream" },
    body: file,
  });
}

export interface StreamHandlers {
  readonly onMessage: (message: SseMessage) => void;
  readonly onError: (error: ApiErrorEnvelope) => void;
  readonly onEnd: () => void;
}

export interface StreamSubscription {
  readonly transport: TransportName;
  readonly close: () => void;
}

/// Follows a registry stream through a real incremental HTTP body or the
/// desktop Channel. Both paths feed the same decoder and therefore expose the
/// same complete SSE messages to the shared React bundle.
export function apiStream(
  name: string,
  inputJson: string,
  resumeAfter: number | null,
  handlers: StreamHandlers,
): StreamSubscription {
  const transport = activeTransport();
  const decoder = new SseDecoder();
  let open = true;
  const receive = (chunk: string) => {
    if (!open) {
      return;
    }
    try {
      for (const message of decoder.push(chunk)) {
        dispatchStreamMessage(message, handlers);
      }
    } catch (thrown: unknown) {
      open = false;
      handlers.onError(malformedStream(thrown));
    }
  };
  const finish = () => {
    if (!open) {
      return;
    }
    try {
      decoder.finish();
      handlers.onEnd();
    } catch (thrown: unknown) {
      open = false;
      handlers.onError(malformedStream(thrown));
    }
  };

  if (transport === "tauri-ipc") {
    let channel: Channel<string>;
    try {
      channel = new Channel<string>(receive);
    } catch (thrown: unknown) {
      handlers.onError(malformedStream(thrown));
      return { transport, close: () => undefined };
    }
    const invoke = window.__TAURI__?.core?.invoke;
    if (invoke === undefined) {
      handlers.onError(transportUnavailable("The IPC bridge disappeared."));
    } else {
      void invoke("api_stream", {
        name,
        input: inputJson,
        resumeAfter,
        channel,
      }).catch((thrown: unknown) => {
        if (open) {
          open = false;
          handlers.onError(decodeError(thrown));
        }
      });
    }
    return {
      transport,
      close: () => {
        open = false;
        channel.onmessage = () => undefined;
      },
    };
  }

  const controller = new AbortController();
  void followHttpStream(
    name,
    inputJson,
    resumeAfter,
    controller,
    receive,
    finish,
    () => open,
  ).catch((thrown: unknown) => {
    if (open && !controller.signal.aborted) {
      open = false;
      handlers.onError(decodeError(thrown));
    }
  });
  return {
    transport,
    close: () => {
      open = false;
      controller.abort();
    },
  };
}

async function followHttpStream(
  name: string,
  inputJson: string,
  resumeAfter: number | null,
  controller: AbortController,
  receive: (chunk: string) => void,
  finish: () => void,
  isOpen: () => boolean,
): Promise<void> {
  const query = new URLSearchParams({ input: inputJson });
  if (resumeAfter !== null) {
    query.set("from", String(resumeAfter));
  }
  const response = await fetch(`/api/stream/${name}?${query.toString()}`, {
    headers: { accept: "text/event-stream" },
    credentials: "same-origin",
    signal: controller.signal,
  });
  if (!response.ok) {
    throw decodeError(await response.text());
  }
  if (response.body === null) {
    throw transportUnavailable("The ProjectOS server returned no Run stream body.");
  }
  const reader = response.body.getReader();
  const textDecoder = new TextDecoder();
  while (isOpen()) {
    const result = await reader.read();
    if (result.done) {
      receive(textDecoder.decode());
      finish();
      return;
    }
    receive(textDecoder.decode(result.value, { stream: true }));
  }
}

function dispatchStreamMessage(message: SseMessage, handlers: StreamHandlers): void {
  if (message.event !== "stream.error") {
    handlers.onMessage(message);
    return;
  }
  const error = asErrorEnvelope(parseJson(message.data));
  handlers.onError(
    error ?? {
      code: "malformed_result",
      message: "The runtime returned an invalid stream error envelope.",
      retriable: false,
    },
  );
}

function malformedStream(thrown: unknown): ApiErrorEnvelope {
  return {
    code: "malformed_result",
    message: thrown instanceof Error ? thrown.message : "The Run stream framing was invalid.",
    retriable: false,
  };
}

function transportUnavailable(message: string): ApiErrorEnvelope {
  return { code: "transport_unavailable", message, retriable: true };
}

function decodeError(thrown: unknown): ApiErrorEnvelope {
  if (isRecord(thrown) && typeof thrown.code === "string") {
    const envelope = asErrorEnvelope(thrown);
    if (envelope !== null) {
      return envelope;
    }
  }
  const envelope = asErrorEnvelope(parseJson(typeof thrown === "string" ? thrown : ""));
  return (
    envelope ?? {
      code: "transport_failed",
      message: thrown instanceof Error ? thrown.message : "The Run stream failed.",
      retriable: true,
    }
  );
}

async function invokeIpc(command: string, args: Record<string, unknown>): Promise<DispatchOutcome> {
  const invoke = window.__TAURI__?.core?.invoke;
  if (invoke === undefined) {
    return {
      kind: "failed",
      transport: "tauri-ipc",
      error: {
        code: "transport_unavailable",
        message: "The IPC bridge disappeared.",
        retriable: true,
      },
    };
  }
  try {
    const raw = await invoke(command, args);
    return decodeDispatch("tauri-ipc", typeof raw === "string" ? raw : null);
  } catch (thrown: unknown) {
    return decodeDispatchFailure("tauri-ipc", thrown);
  }
}

async function fetchHttp(target: string, init: RequestInit): Promise<DispatchOutcome> {
  let response: Response;
  try {
    response = await fetch(target, init);
  } catch {
    return {
      kind: "failed",
      transport: "http",
      error: {
        code: "transport_unavailable",
        message: "No ProjectOS server answered this page.",
        retriable: true,
      },
    };
  }
  const body = await response.text();
  return response.ok ? decodeDispatch("http", body) : decodeDispatchFailure("http", body);
}

function decodeDispatch(transport: TransportName, raw: string | null): DispatchOutcome {
  const value = raw === null ? null : parseJson(raw);
  if (value === null) {
    return {
      kind: "failed",
      transport,
      error: {
        code: "malformed_result",
        message: "The runtime returned bytes this build does not recognise.",
        retriable: false,
      },
    };
  }
  return { kind: "ok", transport, value };
}

function decodeDispatchFailure(transport: TransportName, raw: unknown): DispatchOutcome {
  const envelope = asErrorEnvelope(parseJson(typeof raw === "string" ? raw : ""));
  return {
    kind: "failed",
    transport,
    error: envelope ?? {
      code: "transport_failed",
      message: "The runtime failed without a typed error envelope.",
      retriable: true,
    },
  };
}

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

export function activeTransport(): TransportName {
  return typeof window.__TAURI__?.core?.invoke === "function" ? "tauri-ipc" : "http";
}

/// The capability snapshot rides the same dispatcher as every other query;
/// only its validator is specific. A shape the generated catalog does not
/// know about becomes an error, never a card.
export async function fetchCapabilitySnapshot(): Promise<SnapshotOutcome> {
  const outcome = await apiQuery(CAPABILITY_SNAPSHOT_QUERY);
  if (outcome.kind === "failed") {
    return outcome;
  }
  const snapshot = asSnapshot(outcome.value);
  return snapshot === null
    ? {
        kind: "failed",
        transport: outcome.transport,
        error: {
          code: "malformed_result",
          message: "The runtime returned a shape this build does not recognise.",
          retriable: false,
        },
      }
    : { kind: "ok", transport: outcome.transport, snapshot };
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
