// The browser half of the gateway protocol. docs/protocol.md is the reference.

/** The room's two streams. */
export type StreamName = "ops" | "presence";

/** Messages the gateway sends. */
export type ServerMessage =
  | { type: "hello"; namespace: string; room: string }
  | {
      type: "subscribed";
      stream: StreamName;
      start_offset: number | null;
      live_offset: number | null;
    }
  | {
      type: "event";
      stream: StreamName;
      offset: number | null;
      skipped_before?: number;
      payload: string;
    }
  | { type: "ack"; id: number; offset: number | null }
  | { type: "counter"; id: number; value: number }
  | { type: "snapshot"; id: number; payload: string | null }
  | {
      type: "error";
      id?: number;
      stream?: StreamName;
      code:
        | "bad_request"
        | "publish_failed"
        | "subscribe_failed"
        | "subscription_ended"
        | "counter_failed"
        | "snapshot_failed"
        | "trimmed";
      oldest?: number;
      message: string;
    };

/** One record delivered on a stream. */
export interface GatewayEvent {
  stream: StreamName;
  /** Log offset on `ops`; `null` on `presence`, which has no log. */
  offset: number | null;
  /** Offsets just before this one that hold no event. */
  skippedBefore: number;
  payload: Uint8Array;
}

/** Raised when the gateway reports that a request failed. */
export class GatewayError extends Error {
  override name = "GatewayError";
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

interface Pending {
  resolve: (value: number | string | null) => void;
  reject: (error: GatewayError) => void;
}

/**
 * A connection to the gateway. Events and subscription changes are reported
 * through the callbacks; publishes resolve with the record's log offset.
 */
export class GatewayClient {
  /** Called once, first, with the room this gateway serves. */
  onHello: (namespace: string, room: string) => void = () => {};
  /** Called for every event, in the order the broker delivered them. */
  onEvent: (event: GatewayEvent) => void = () => {};
  /** Called once a subscription is registered with the broker. */
  onSubscribed: (
    stream: StreamName,
    startOffset: number | null,
    liveOffset: number | null,
  ) => void = () => {};
  /** Called for errors not tied to a publish, such as a subscription ending. */
  onError: (error: GatewayError, stream?: StreamName) => void = () => {};
  /** Called when the connection closes. */
  onClose: () => void = () => {};

  readonly #socket: WebSocket;
  readonly #pending = new Map<number, Pending>();
  #nextId = 0;

  private constructor(socket: WebSocket) {
    this.#socket = socket;
    socket.addEventListener("message", (message) => this.#receive(String(message.data)));
    socket.addEventListener("close", () => {
      for (const pending of this.#pending.values()) {
        pending.reject(new GatewayError("closed", "the gateway connection closed"));
      }
      this.#pending.clear();
      this.onClose();
    });
  }

  /** Open a connection to the gateway at `url`, a `ws:` or `wss:` URL. */
  static connect(url: string): Promise<GatewayClient> {
    return new Promise((resolve, reject) => {
      const socket = new WebSocket(url);
      socket.addEventListener("open", () => resolve(new GatewayClient(socket)), { once: true });
      socket.addEventListener("error", () => reject(new Error(`cannot reach ${url}`)), {
        once: true,
      });
    });
  }

  /**
   * Relay `stream` from `from`: a log offset (the last one handled, plus one)
   * or `"live"`. Replaces an earlier subscription to the same stream.
   */
  subscribe(stream: StreamName, from: number | "live"): void {
    this.#send({ type: "subscribe", stream, from });
  }

  /**
   * Publish one record. With `ack`, resolves once the broker has it, with its
   * log offset when the broker acknowledged after writing. Without, resolves
   * as soon as the request is sent.
   */
  publish(stream: StreamName, payload: Uint8Array, ack = true): Promise<number | null> {
    const message = { type: "publish", stream, payload: toBase64(payload), ack };
    if (!ack) {
      this.#send({ ...message, id: this.#nextId++ });
      return Promise.resolve(null);
    }
    return this.#request(message) as Promise<number | null>;
  }

  /**
   * Add `delta` to this session's sequence counter, `canvas.seq/<room>:<key>`,
   * and resolve with the new sum.
   */
  counterAdd(key: string, delta: number): Promise<number> {
    return this.#request({ type: "counter_add", counter: "seq", key, delta }) as Promise<number>;
  }

  /** Read the room's snapshot: the bytes the snapshotter wrote, or `null` for none yet. */
  async snapshot(): Promise<Uint8Array | null> {
    const payload = (await this.#request({ type: "snapshot_get" })) as string | null;
    return payload === null ? null : fromBase64(payload);
  }

  close(): void {
    this.#socket.close();
  }

  #request(message: object): Promise<number | string | null> {
    const id = this.#nextId++;
    this.#send({ ...message, id });
    return new Promise((resolve, reject) => this.#pending.set(id, { resolve, reject }));
  }

  #send(message: object): void {
    this.#socket.send(JSON.stringify(message));
  }

  #receive(text: string): void {
    const message = JSON.parse(text) as ServerMessage;
    switch (message.type) {
      case "event":
        this.onEvent({
          stream: message.stream,
          offset: message.offset,
          skippedBefore: message.skipped_before ?? 0,
          payload: fromBase64(message.payload),
        });
        break;
      case "hello":
        this.onHello(message.namespace, message.room);
        break;
      case "subscribed":
        this.onSubscribed(message.stream, message.start_offset, message.live_offset);
        break;
      case "counter":
        this.#take(message.id)?.resolve(message.value);
        break;
      case "ack":
        this.#take(message.id)?.resolve(message.offset);
        break;
      case "snapshot":
        this.#take(message.id)?.resolve(message.payload);
        break;
      case "error": {
        const error = new GatewayError(message.code, message.message);
        const pending = this.#take(message.id);
        if (pending) {
          pending.reject(error);
        } else {
          this.onError(error, message.stream);
        }
        break;
      }
    }
  }

  #take(id: number | undefined): Pending | undefined {
    if (id === undefined) return undefined;
    const pending = this.#pending.get(id);
    this.#pending.delete(id);
    return pending;
  }
}

function toBase64(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function fromBase64(text: string): Uint8Array {
  return Uint8Array.from(atob(text), (char) => char.charCodeAt(0));
}
