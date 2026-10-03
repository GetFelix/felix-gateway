import {
  EMPTY_DOC,
  apply,
  decodeOp,
  encodeOp,
  encodeSnapshot,
  stateHash,
  type Op,
} from "@felix-canvas/model";

import { GatewayError, type GatewayEvent } from "../src/gateway.js";
import type { Gateway } from "../src/session.js";

export const other = 0x0dd0n;
const op = (seq: number): Op =>
  seq === 0
    ? {
        sid: other,
        seq,
        shape: 1n,
        kind: "create",
        fields: { type: "rect", x: 0, y: 0, w: 10, h: 10, z: "V" },
      }
    : { sid: other, seq, shape: 1n, kind: "patch", fields: { x: seq } };

/** One room's log, snapshot and retention point, shared by every fake connection. */
export class Room {
  readonly log: Uint8Array[] = [];
  readonly connections = new Set<FakeGateway>();
  snapshotAt: number | null = null;
  oldest = 0;
  /** Runs while a snapshot read is in flight, before it is answered. */
  duringSnapshotRead: () => void = () => {};

  append(payload: Uint8Array): number {
    const offset = this.log.push(payload) - 1;
    for (const connection of this.connections) connection.deliver(offset);
    return offset;
  }

  write(count: number): void {
    for (let i = 0; i < count; i++) this.append(encodeOp(op(this.log.length)));
  }

  snapshot(): Uint8Array | null {
    if (this.snapshotAt === null) return null;
    const doc = this.log
      .slice(0, this.snapshotAt + 1)
      .reduce((state, bytes, offset) => apply(state, decodeOp(bytes), offset), EMPTY_DOC);
    return encodeSnapshot({ doc, offset: this.snapshotAt });
  }

  hash(): string {
    return stateHash(
      this.log.reduce((state, bytes, offset) => apply(state, decodeOp(bytes), offset), EMPTY_DOC),
    );
  }
}

/** The gateway as a session sees it, relaying one {@link Room}. */
export class FakeGateway implements Gateway {
  onHello: Gateway["onHello"] = () => {};
  onEvent: Gateway["onEvent"] = () => {};
  onSubscribed: Gateway["onSubscribed"] = () => {};
  onError: Gateway["onError"] = () => {};
  onClose: Gateway["onClose"] = () => {};
  onCacheEntries: Gateway["onCacheEntries"] = () => {};
  onCacheChange: Gateway["onCacheChange"] = () => {};
  readonly requests: string[];
  /** While set, live records are lost on the way, as Felix drops them for a slow reader. */
  dropping = false;
  /** While set, an op publish lands in the log but its answer is lost, as when the owner fails. */
  losingAcks = false;
  presenceSubscribes = 0;
  #from: number | null = null;
  #counter = 0;

  constructor(
    readonly room: Room,
    requests: string[],
  ) {
    this.requests = requests;
    room.connections.add(this);
  }

  subscribe(stream: string, from: number | "live"): void {
    if (stream === "presence") this.presenceSubscribes++;
    if (stream !== "ops") return;
    this.requests.push(`subscribe ${from}`);
    setTimeout(() => {
      if (typeof from === "number" && from < this.room.oldest) {
        this.onError(new GatewayError("trimmed", `oldest is ${this.room.oldest}`), "ops");
        return;
      }
      const tail = this.room.log.length;
      this.#from = from === "live" ? tail : from;
      this.onSubscribed("ops", this.#from, tail);
      for (let offset = this.#from; offset < tail; offset++) this.deliver(offset, false);
    });
  }

  deliver(offset: number, live = true): void {
    if (this.#from === null || offset < this.#from || (live && this.dropping)) return;
    const event: GatewayEvent = {
      stream: "ops",
      offset,
      skippedBefore: 0,
      payload: this.room.log[offset]!,
    };
    this.onEvent(event);
  }

  async publish(stream: string, payload: Uint8Array): Promise<number | null> {
    if (stream !== "ops") return null;
    const offset = this.room.append(payload);
    if (this.losingAcks) throw new GatewayError("publish_failed", "connection lost");
    return offset;
  }

  async counterAdd(_counter: string, _key: string, delta: number): Promise<number> {
    return (this.#counter += delta);
  }

  /** Only the snapshot is read: cache "snap", key "latest". */
  async cacheGet(cache: string, key: string): Promise<Uint8Array | null> {
    if (cache !== "snap" || key !== "latest") throw new GatewayError("bad_request", cache);
    this.requests.push("snapshot");
    await new Promise((resolve) => setTimeout(resolve));
    this.room.duringSnapshotRead();
    return this.room.snapshot();
  }

  cachePut(): void {}
  cacheDelete(): void {}
  cacheWatch(): void {}

  throttle(bitsPerSecond: number | null): void {
    this.requests.push(`throttle ${bitsPerSecond}`);
  }

  close(): void {
    this.#from = null;
    this.room.connections.delete(this);
    this.onClose();
  }
}

export async function until(condition: () => boolean): Promise<void> {
  const deadline = Date.now() + 3000;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}
