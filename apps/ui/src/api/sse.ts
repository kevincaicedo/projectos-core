// Incremental SSE decoding shared by the HTTP body reader and the Tauri
// channel. It accepts arbitrary chunk boundaries, including a CR/LF split,
// and emits only complete events. The runtime's retry directive is transport
// metadata, so it is consumed without becoming invented Run state.

const SSE_BUFFER_CHARS_MAX = 128 * 1024;

export interface SseMessage {
  readonly id: number | null;
  readonly event: string;
  readonly data: string;
}

export class SseDecoder {
  private buffer = "";

  push(chunk: string): readonly SseMessage[] {
    this.buffer = `${this.buffer}${chunk}`.replaceAll("\r\n", "\n");
    if (this.buffer.length > SSE_BUFFER_CHARS_MAX) {
      throw new Error("the Run stream exceeded its bounded frame buffer");
    }

    const messages: SseMessage[] = [];
    let boundary = this.buffer.indexOf("\n\n");
    while (boundary >= 0) {
      const block = this.buffer.slice(0, boundary);
      this.buffer = this.buffer.slice(boundary + 2);
      const message = parseBlock(block);
      if (message !== null) {
        messages.push(message);
      }
      boundary = this.buffer.indexOf("\n\n");
    }
    return messages;
  }

  finish(): void {
    if (this.buffer.trim().length > 0) {
      throw new Error("the Run stream ended inside an SSE frame");
    }
  }
}

function parseBlock(block: string): SseMessage | null {
  let id: number | null = null;
  let event = "message";
  const data: string[] = [];
  for (const line of block.split("\n")) {
    if (line.length === 0 || line.startsWith(":")) {
      continue;
    }
    const separator = line.indexOf(":");
    const field = separator < 0 ? line : line.slice(0, separator);
    const rawValue = separator < 0 ? "" : line.slice(separator + 1);
    const value = rawValue.startsWith(" ") ? rawValue.slice(1) : rawValue;
    if (field === "id") {
      id = parseId(value);
    } else if (field === "event") {
      event = value;
    } else if (field === "data") {
      data.push(value);
    }
  }
  if (data.length === 0) {
    return null;
  }
  return { id, event, data: data.join("\n") };
}

function parseId(value: string): number {
  if (!/^[1-9][0-9]*$/.test(value)) {
    throw new Error("the Run stream returned an invalid event id");
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error("the Run stream event id exceeded the browser integer range");
  }
  return parsed;
}
