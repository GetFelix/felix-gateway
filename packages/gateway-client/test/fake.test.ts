import { describe, expect, it } from "vitest";

import { FakeGateway, FakeScope } from "../src/fake.js";
import type { CacheEntry, Gateway, GatewayEvent } from "../src/index.js";

const bytes = (...values: number[]) => new Uint8Array(values);

async function until(condition: () => boolean): Promise<void> {
  const deadline = Date.now() + 3000;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function watch(gateway: FakeGateway): GatewayEvent[] {
  const events: GatewayEvent[] = [];
  gateway.onEvent = (event) => events.push(event);
  return events;
}

// The same function as in docs/protocol.md, "Subscribe before reading".
function catchUp(
  client: Gateway,
  stream: string,
  from: number,
  apply: (event: GatewayEvent) => void,
): Promise<void> {
  return new Promise((resolve) => {
    let target: number | null = null; // the offset where the history ends
    client.onSubscribed = (_stream, _start, live) => {
      target = live ?? 0;
      if (from >= target) resolve(); // nothing to wait for
    };
    client.onEvent = (event) => {
      if (target === null) return; // from the subscription this one replaced
      apply(event);
      if (event.offset !== null && event.offset + 1 >= target) resolve();
    };
    client.subscribe(stream, from);
  });
}

describe("catching up from a snapshot", () => {
  const filled = () => {
    const scope = new FakeScope();
    for (let i = 0; i < 10; i++) scope.append("ops", bytes(i));
    return scope;
  };

  it("resolves at the last event of the history and then keeps delivering", async () => {
    const scope = filled();
    const offsets: (number | null)[] = [];
    await catchUp(new FakeGateway(scope), "ops", 7, (event) => offsets.push(event.offset));
    expect(offsets).toEqual([7, 8, 9]);
    scope.append("ops", bytes(10));
    expect(offsets).toEqual([7, 8, 9, 10]);
  });

  it("has nothing to wait for when the snapshot covers the log", async () => {
    const offsets: (number | null)[] = [];
    await catchUp(new FakeGateway(filled()), "ops", 10, (event) => offsets.push(event.offset));
    expect(offsets).toEqual([]);
  });

  it("applies an event once when it also reached the subscription it replaces", async () => {
    const scope = filled();
    const gateway = new FakeGateway(scope);
    gateway.subscribe("ops", "live");
    await new Promise((resolve) => setTimeout(resolve));
    const offsets: (number | null)[] = [];
    const caught = catchUp(gateway, "ops", 7, (event) => offsets.push(event.offset));
    scope.append("ops", bytes(10)); // reaches the live subscription before the new one registers
    await caught;
    expect(offsets).toEqual([7, 8, 9, 10]);
  });
});

describe("FakeGateway", () => {
  it("replays from an offset, then delivers live records", async () => {
    const scope = new FakeScope();
    scope.append("ops", bytes(0));
    scope.append("ops", bytes(1));
    const gateway = new FakeGateway(scope);
    const events = watch(gateway);
    const subscribed: unknown[] = [];
    gateway.onSubscribed = (...args) => subscribed.push(args);
    gateway.subscribe("ops", 1);
    await until(() => subscribed.length === 1);
    expect(subscribed).toEqual([["ops", 1, 2]]);
    await new FakeGateway(scope).publish("ops", bytes(2));
    expect(events.map((event) => event.offset)).toEqual([1, 2]);
  });

  it("refuses a subscribe from before the retained range", async () => {
    const scope = new FakeScope();
    scope.retainedFrom.set("ops", 3);
    const gateway = new FakeGateway(scope);
    const codes: string[] = [];
    gateway.onError = (error, stream) => codes.push(`${error.code} ${stream}`);
    gateway.subscribe("ops", 0);
    await until(() => codes.length === 1);
    expect(codes).toEqual(["trimmed ops"]);
  });

  it("drops live records while dropping and loses acks while losing them", async () => {
    const scope = new FakeScope();
    const gateway = new FakeGateway(scope);
    const events = watch(gateway);
    gateway.subscribe("ops", "live");
    await new Promise((resolve) => setTimeout(resolve));
    gateway.dropping = true;
    scope.append("ops", bytes(0));
    gateway.dropping = false;
    gateway.losingAcks = true;
    await expect(gateway.publish("ops", bytes(1))).rejects.toMatchObject({
      code: "publish_failed",
    });
    expect(scope.log("ops")).toHaveLength(2);
    expect(events.map((event) => event.offset)).toEqual([1]);
  });

  it("keeps caches and counters shared between connections", async () => {
    const scope = new FakeScope();
    const a = new FakeGateway(scope);
    const b = new FakeGateway(scope);
    const seen: [string, CacheEntry[]][] = [];
    b.onCacheEntries = (cache, entries) => seen.push([cache, entries]);
    b.onCacheChange = (cache, entry) => seen.push([cache, [entry]]);
    a.cachePut("members", "a", bytes(1));
    b.cacheWatch("members");
    await until(() => seen.length === 1);
    a.cacheDelete("members", "a");
    expect(seen).toEqual([
      ["members", [{ key: "a", payload: bytes(1), expiresInMs: null }]],
      ["members", [{ key: "a", payload: null, expiresInMs: null }]],
    ]);
    await expect(b.cacheGet("members", "a")).resolves.toBeNull();
    await a.counterAdd("seq", "next", 2);
    await expect(b.counterAdd("seq", "next", 1)).resolves.toBe(3);
  });

  it("stops delivering once closed", async () => {
    const scope = new FakeScope();
    const gateway = new FakeGateway(scope);
    const events = watch(gateway);
    let closed = false;
    gateway.onClose = () => (closed = true);
    gateway.subscribe("ops", "live");
    await new Promise((resolve) => setTimeout(resolve));
    gateway.close();
    scope.append("ops", bytes(0));
    expect(closed).toBe(true);
    expect(events).toEqual([]);
  });
});
