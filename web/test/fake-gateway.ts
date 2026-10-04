import {
  EMPTY_DOC,
  apply,
  decodeOp,
  encodeOp,
  encodeSnapshot,
  stateHash,
  type Op,
} from "@felix-canvas/model";

import { GatewayError } from "felix-gateway-client";
import { FakeGateway as ScopeConnection, FakeScope } from "felix-gateway-client/fake";

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

/** One room's ops log, snapshot and retention point, shared by every fake connection. */
export class Room {
  readonly scope: FakeScope;
  snapshotAt: number | null = null;
  /** Runs while a snapshot read is in flight, before it is answered. */
  duringSnapshotRead: () => void = () => {};

  constructor() {
    const room = this;
    this.scope = new (class extends FakeScope {
      override readCache(cache: string, key: string): Uint8Array | null {
        if (cache !== "snap" || key !== "latest") return super.readCache(cache, key);
        room.duringSnapshotRead();
        return room.snapshot();
      }
    })();
  }

  get log(): Uint8Array[] {
    return this.scope.log("ops");
  }

  get oldest(): number {
    return this.scope.retainedFrom.get("ops") ?? 0;
  }

  set oldest(offset: number) {
    this.scope.retainedFrom.set("ops", offset);
  }

  append(payload: Uint8Array): number {
    return this.scope.append("ops", payload);
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

/**
 * The gateway as a session sees it, relaying one {@link Room}. Only ops are
 * relayed, and each request is logged in `requests` for tests to check the order.
 */
export class FakeGateway extends ScopeConnection {
  readonly requests: string[];
  presenceSubscribes = 0;

  constructor(
    readonly room: Room,
    requests: string[],
  ) {
    super(room.scope);
    this.requests = requests;
  }

  override subscribe(stream: string, from: number | "live"): void {
    if (stream === "presence") this.presenceSubscribes++;
    if (stream !== "ops") return;
    this.requests.push(`subscribe ${from}`);
    super.subscribe(stream, from);
  }

  override async publish(stream: string, payload: Uint8Array): Promise<number | null> {
    if (stream !== "ops") return null;
    return super.publish(stream, payload);
  }

  /** Only the snapshot is read: cache "snap", key "latest". */
  override async cacheGet(cache: string, key: string): Promise<Uint8Array | null> {
    if (cache !== "snap" || key !== "latest") throw new GatewayError("bad_request", cache);
    this.requests.push("snapshot");
    return super.cacheGet(cache, key);
  }

  override cachePut(): void {}
  override cacheDelete(): void {}
  override cacheWatch(): void {}

  override throttle(bitsPerSecond: number | null): void {
    this.requests.push(`throttle ${bitsPerSecond}`);
    super.throttle(bitsPerSecond);
  }
}

export async function until(condition: () => boolean): Promise<void> {
  const deadline = Date.now() + 3000;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}
