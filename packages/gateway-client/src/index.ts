// The browser half of the gateway protocol. docs/protocol.md is the reference.
// Streams, caches and counters are named by their alias in the gateway's
// scope file.

/** The protocol version this client speaks. */
export const PROTOCOL = 1;

/** Messages the gateway sends. */
export type ServerMessage =
  | {
      type: "hello";
      protocol: number;
      features: string[];
      namespace: string;
      cache_ttl_ms: Record<string, number>;
      missing?: string[];
    }
  | {
      type: "subscribed";
      stream: string;
      start_offset: number | null;
      live_offset: number | null;
    }
  | {
      type: "event";
      stream: string;
      offset: number | null;
      skipped_before?: number;
      payload: string;
    }
  | { type: "ack"; id: number; offset: number | null }
  | { type: "counter"; id: number; value: number }
  | { type: "cache_value"; id: number; payload: string | null }
  | { type: "cache_entries"; cache: string; entries: WireEntry[] }
  | ({ type: "cache_change"; cache: string } & WireEntry)
  | {
      type: "error";
      id?: number;
      stream?: string;
      cache?: string;
      code:
        | "bad_request"
        | "unsupported"
        | "publish_failed"
        | "subscribe_failed"
        | "subscription_ended"
        | "counter_failed"
        | "cache_failed"
        | "trimmed"
        | "watch_failed"
        | "signed_out"
        | "forbidden"
        | "unavailable";
      oldest?: number;
      message: string;
    };

/**
 * The sign-in, and the scope to open under the key the gateway's scope file
 * names, such as `{ room: "lobby", token }`.
 */
export interface Join {
  token: string;
  [scopeField: string]: string;
}

/** What the gateway answered a join with. */
export interface Hello {
  namespace: string;
  /** The features asked for that the gateway accepted. */
  features: string[];
  /** How long an entry lasts without a write, for each cache with a TTL. */
  cacheTtlMs: Record<string, number>;
  /** Optional resources the sign-in does not reach. */
  missing: string[];
}

interface WireEntry {
  key: string;
  payload: string | null;
  expires_in_ms: number | null;
}

/** One cache entry as the gateway reports it. */
export interface CacheEntry {
  key: string;
  /** The entry's value, or `null` when it was deleted. */
  payload: Uint8Array | null;
  /** Milliseconds until the entry expires, or `null` if it never does. */
  expiresInMs: number | null;
}

/** One record delivered on a stream. */
export interface GatewayEvent {
  stream: string;
  /** Log offset; `null` on a stream with no log. */
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

/**
 * Everything a {@link GatewayClient} offers, without its private state, so
 * code can accept a stand-in such as `FakeGateway` from `felix-gateway-client/fake`.
 */
export type Gateway = Pick<GatewayClient, keyof GatewayClient>;

interface Pending {
  resolve: (value: number | string | null) => void;
  reject: (error: GatewayError) => void;
}

/**
 * A connection to the gateway. Events and subscription changes are reported
 * through the callbacks; publishes resolve with the record's log offset.
 */
export class GatewayClient {
  /** Called once, first, when the gateway has opened the session. */
  onHello: (hello: Hello) => void = () => {};
  /** Called with every entry of a cache when a watch of it starts. */
  onCacheEntries: (cache: string, entries: CacheEntry[]) => void = () => {};
  /** Called for each entry written or deleted after that. */
  onCacheChange: (cache: string, entry: CacheEntry) => void = () => {};
  /** Called for every event, in the order the broker delivered them. */
  onEvent: (event: GatewayEvent) => void = () => {};
  /** Called once a subscription is registered with the broker. */
  onSubscribed: (stream: string, start: number | null, live: number | null) => void = () => {};
  /**
   * Called for errors not tied to a request, such as a subscription ending,
   * with the stream or cache they are about.
   */
  onError: (error: GatewayError, stream?: string, cache?: string) => void = () => {};
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

  /**
   * Open a connection to the gateway at `url`, a `ws:` or `wss:` URL, and
   * join the scope in `join`, asking for `features`. The gateway answers with
   * `hello`, or refuses with an error and closes.
   */
  static connect(url: string, join: Join, features: string[] = []): Promise<GatewayClient> {
    return new Promise((resolve, reject) => {
      const socket = new WebSocket(url);
      socket.addEventListener(
        "open",
        () => {
          const client = new GatewayClient(socket);
          client.#send({ ...join, type: "join", protocol: PROTOCOL, features });
          resolve(client);
        },
        { once: true },
      );
      socket.addEventListener("error", () => reject(new Error(`cannot reach ${url}`)), {
        once: true,
      });
    });
  }

  /**
   * Relay `stream` from `from`: a log offset (the last one handled, plus one)
   * or `"live"`. Replaces an earlier subscription to the same stream.
   */
  subscribe(stream: string, from: number | "live"): void {
    this.#send({ type: "subscribe", stream, from });
  }

  /**
   * Publish one record. With `ack`, resolves once the broker has it, with its
   * log offset when the broker acknowledged after writing. Without, resolves
   * as soon as the request is sent.
   */
  publish(stream: string, payload: Uint8Array, ack = true): Promise<number | null> {
    const message = { type: "publish", stream, payload: toBase64(payload), ack };
    if (!ack) {
      this.#send({ ...message, id: this.#nextId++ });
      return Promise.resolve(null);
    }
    return this.#request(message) as Promise<number | null>;
  }

  /** Add `delta` to `key` of `counter` and resolve with the new sum. */
  counterAdd(counter: string, key: string, delta: number): Promise<number> {
    return this.#request({ type: "counter_add", counter, key, delta }) as Promise<number>;
  }

  /** Read `key` of `cache`: its value, or `null` when there is none. */
  async cacheGet(cache: string, key: string): Promise<Uint8Array | null> {
    const payload = (await this.#request({ type: "cache_get", cache, key })) as string | null;
    return payload === null ? null : fromBase64(payload);
  }

  /** Write `key` of `cache`. It expires after the cache's TTL unless written again. */
  cachePut(cache: string, key: string, payload: Uint8Array): void {
    this.#send({ type: "cache_put", cache, key, payload: toBase64(payload) });
  }

  /** Delete `key` of `cache`. */
  cacheDelete(cache: string, key: string): void {
    this.#send({ type: "cache_delete", cache, key });
  }

  /** Watch the entries of `cache`. Replaces an earlier watch of it. */
  cacheWatch(cache: string): void {
    this.#send({ type: "cache_watch", cache });
  }

  /**
   * Have the gateway read this connection's subscriptions no faster than a
   * link of `bitsPerSecond` would carry them, or at full speed with `null`.
   */
  throttle(bitsPerSecond: number | null): void {
    this.#send({ type: "throttle", bits_per_second: bitsPerSecond });
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
        this.onHello({
          namespace: message.namespace,
          features: message.features,
          cacheTtlMs: message.cache_ttl_ms,
          missing: message.missing ?? [],
        });
        break;
      case "subscribed":
        this.onSubscribed(message.stream, message.start_offset, message.live_offset);
        break;
      case "cache_entries":
        this.onCacheEntries(message.cache, message.entries.map(cacheEntry));
        break;
      case "cache_change":
        this.onCacheChange(message.cache, cacheEntry(message));
        break;
      case "counter":
        this.#take(message.id)?.resolve(message.value);
        break;
      case "ack":
        this.#take(message.id)?.resolve(message.offset);
        break;
      case "cache_value":
        this.#take(message.id)?.resolve(message.payload);
        break;
      case "error": {
        const error = new GatewayError(message.code, message.message);
        const pending = this.#take(message.id);
        if (pending) {
          pending.reject(error);
        } else {
          this.onError(error, message.stream, message.cache);
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

function cacheEntry(entry: WireEntry): CacheEntry {
  return {
    key: entry.key,
    payload: entry.payload === null ? null : fromBase64(entry.payload),
    expiresInMs: entry.expires_in_ms,
  };
}

function toBase64(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function fromBase64(text: string): Uint8Array {
  return Uint8Array.from(atob(text), (char) => char.charCodeAt(0));
}
