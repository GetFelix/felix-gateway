import { describe, expect, it } from "vitest";

import { FakeGateway, FakeScope } from "../src/fake.js";
import type { CacheEntry, GatewayEvent } from "../src/index.js";

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
