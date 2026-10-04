// An in-memory stand-in for the gateway, for testing code written against
// GatewayClient without a gateway or a broker. Everything is answered on a
// later task, as a socket would.

import { GatewayError, type CacheEntry, type Gateway } from "./index.js";

/**
 * The streams, caches and counters of one scope, shared by every
 * {@link FakeGateway} connected to it.
 */
export class FakeScope {
  /** Each stream's records, indexed by log offset. */
  readonly streams = new Map<string, Uint8Array[]>();
  /** Each cache's entries by key. */
  readonly caches = new Map<string, Map<string, Uint8Array>>();
  /** Each counter's sums by key. */
  readonly counters = new Map<string, Map<string, number>>();
  /** The first offset each stream still holds; a subscribe from earlier is `trimmed`. */
  readonly retainedFrom = new Map<string, number>();
  readonly connections = new Set<FakeGateway>();

  /** The records of `stream`, which a test may read or fill directly. */
  log(stream: string): Uint8Array[] {
    let log = this.streams.get(stream);
    if (!log) this.streams.set(stream, (log = []));
    return log;
  }

  /** Append a record to `stream`, deliver it to subscribers and return its offset. */
  append(stream: string, payload: Uint8Array): number {
    const offset = this.log(stream).push(payload) - 1;
    for (const connection of this.connections) connection.deliver(stream, offset);
    return offset;
  }

  /** Answer a `cacheGet`. Override it to compute a value when it is read. */
  readCache(cache: string, key: string): Uint8Array | null {
    return this.caches.get(cache)?.get(key) ?? null;
  }

  /** Write `key` of `cache`, or delete it with `null`, and tell its watchers. */
  writeCache(cache: string, key: string, payload: Uint8Array | null): void {
    let entries = this.caches.get(cache);
    if (!entries) this.caches.set(cache, (entries = new Map()));
    if (payload === null) entries.delete(key);
    else entries.set(key, payload);
    for (const connection of this.connections) {
      connection.changed(cache, { key, payload, expiresInMs: null });
    }
  }

  /** Add `delta` to `key` of `counter` and return the new sum. */
  addCounter(counter: string, key: string, delta: number): number {
    let sums = this.counters.get(counter);
    if (!sums) this.counters.set(counter, (sums = new Map()));
    const value = (sums.get(key) ?? 0) + delta;
    sums.set(key, value);
    return value;
  }
}

/** One connection to a {@link FakeScope}, shaped like a `GatewayClient`. */
export class FakeGateway implements Gateway {
  onHello: Gateway["onHello"] = () => {};
  onEvent: Gateway["onEvent"] = () => {};
  onSubscribed: Gateway["onSubscribed"] = () => {};
  onError: Gateway["onError"] = () => {};
  onClose: Gateway["onClose"] = () => {};
  onCacheEntries: Gateway["onCacheEntries"] = () => {};
  onCacheChange: Gateway["onCacheChange"] = () => {};
  /** While set, live records are lost on the way, as Felix drops them for a slow reader. */
  dropping = false;
  /** While set, a publish lands in the log but its answer is lost, as when the owner fails. */
  losingAcks = false;
  /** The last rate passed to {@link throttle}. */
  bitsPerSecond: number | null = null;
  readonly #from = new Map<string, number>();
  readonly #watching = new Set<string>();

  constructor(readonly scope: FakeScope) {
    scope.connections.add(this);
  }

  subscribe(stream: string, from: number | "live"): void {
    setTimeout(() => {
      const oldest = this.scope.retainedFrom.get(stream) ?? 0;
      if (typeof from === "number" && from < oldest) {
        this.onError(new GatewayError("trimmed", `oldest is ${oldest}`), stream);
        return;
      }
      const tail = this.scope.log(stream).length;
      const start = from === "live" ? tail : from;
      this.#from.set(stream, start);
      this.onSubscribed(stream, start, tail);
      for (let offset = start; offset < tail; offset++) this.deliver(stream, offset, false);
    });
  }

  /** Deliver the record at `offset` of `stream` if this connection subscribes to it. */
  deliver(stream: string, offset: number, live = true): void {
    const from = this.#from.get(stream);
    if (from === undefined || offset < from || (live && this.dropping)) return;
    const payload = this.scope.log(stream)[offset]!;
    this.onEvent({ stream, offset, skippedBefore: 0, payload });
  }

  async publish(stream: string, payload: Uint8Array, ack = true): Promise<number | null> {
    const offset = this.scope.append(stream, payload);
    if (this.losingAcks) throw new GatewayError("publish_failed", "connection lost");
    return ack ? offset : null;
  }

  async counterAdd(counter: string, key: string, delta: number): Promise<number> {
    return this.scope.addCounter(counter, key, delta);
  }

  async cacheGet(cache: string, key: string): Promise<Uint8Array | null> {
    await new Promise((resolve) => setTimeout(resolve));
    return this.scope.readCache(cache, key);
  }

  cachePut(cache: string, key: string, payload: Uint8Array): void {
    this.scope.writeCache(cache, key, payload);
  }

  cacheDelete(cache: string, key: string): void {
    this.scope.writeCache(cache, key, null);
  }

  cacheWatch(cache: string): void {
    setTimeout(() => {
      this.#watching.add(cache);
      const entries = [...(this.scope.caches.get(cache) ?? [])].map(
        ([key, payload]): CacheEntry => ({ key, payload, expiresInMs: null }),
      );
      this.onCacheEntries(cache, entries);
    });
  }

  /** Report a change to `cache` if this connection watches it. */
  changed(cache: string, entry: CacheEntry): void {
    if (this.#watching.has(cache)) this.onCacheChange(cache, entry);
  }

  throttle(bitsPerSecond: number | null): void {
    this.bitsPerSecond = bitsPerSecond;
  }

  close(): void {
    this.#from.clear();
    this.#watching.clear();
    this.scope.connections.delete(this);
    this.onClose();
  }
}
