// Seam-outcome notices (m0-s09): palette commands that dispatch into the
// registry surface their typed answers here — including the honest
// not_yet_supported envelopes from engines that land in later stories.
// A refused command renders as refusal; nothing is faked as success.

import { apiCommand, apiQuery } from "../api/transport";

export interface SeamNotice {
  readonly kind: "refused" | "info";
  readonly title: string;
  readonly detail: string;
}

export async function dispatchCommandNotice(name: string): Promise<SeamNotice> {
  const outcome = await apiCommand(name, "{}");
  if (outcome.kind === "failed") {
    return {
      kind: "refused",
      title: `${name} answered with ${outcome.error.code}`,
      detail: outcome.error.message,
    };
  }
  return { kind: "info", title: `${name} completed`, detail: "" };
}

export async function dispatchQueryNotice(name: string): Promise<SeamNotice> {
  const outcome = await apiQuery(name);
  if (outcome.kind === "failed") {
    return {
      kind: "refused",
      title: `${name} answered with ${outcome.error.code}`,
      detail: outcome.error.message,
    };
  }
  return { kind: "info", title: `${name} completed`, detail: "" };
}

/// The run feed's teaching notice: the stream surface is registered and its
/// SSE framing is frozen (m0-s06); the first producer is the echo agent.
export function runFeedNotice(): SeamNotice {
  return {
    kind: "info",
    title: "Run feed",
    detail:
      "The run-step stream (run.steps) is registered with resumable SSE framing; live steps " +
      "arrive with the echo agent in m0-s13.",
  };
}
