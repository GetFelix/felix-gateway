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

// The same function as in docs/protocol.md, "cache_put_if".
async function update(
  client: Gateway,
  cache: string,
  key: string,
  edit: (current: Uint8Array | null) => Uint8Array,
): Promise<void> {
  for (;;) {
    const entry = await client.cacheGetVersioned(cache, key);
    const result = await client.cachePutIf(
      cache,
      key,
      edit(entry?.payload ?? null),
      entry ? { version: entry.version } : "absent",
    );
    if (result.applied) return;
  }
}

describe("the read, edit, write loop", () => {
  it("keeps every one of several concurrent edits", async () => {
    const scope = new FakeScope();
    const count = (current: Uint8Array | null) => bytes((current?.[0] ?? 0) + 1);
    await Promise.all([1, 2, 3].map(() => update(new FakeGateway(scope), "world", "chunk", count)));
    expect(scope.readCache("world", "chunk")).toEqual(bytes(3));
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

  it("lets exactly one of two conditional writes at one version apply", async () => {
    const scope = new FakeScope();
    const a = new FakeGateway(scope);
    const b = new FakeGateway(scope);
    const first = await Promise.all([
      a.cachePutIf("world", "c", bytes(1), "absent"),
      b.cachePutIf("world", "c", bytes(2), "absent"),
    ]);
    const winner = first.find((result) => result.applied)!;
    const loser = first.find((result) => !result.applied)!;
    expect(first.filter((result) => result.applied)).toHaveLength(1);
    expect(loser).toEqual({ applied: false, version: winner.version });

    const read = await b.cacheGetVersioned("world", "c");
    expect(read?.version).toBe(winner.version);
    const second = await Promise.all([
      a.cachePutIf("world", "c", bytes(3), { version: read!.version }),
      b.cachePutIf("world", "c", bytes(4), { version: read!.version }),
    ]);
    expect(second.filter((result) => result.applied)).toHaveLength(1);
    const next = second.find((result) => result.applied)!.version;
    expect(next).toBeGreaterThan(read!.version);

    await expect(a.cacheDeleteIf("world", "c", read!.version)).resolves.toEqual({
      applied: false,
      version: next,
    });
    await expect(a.cacheDeleteIf("world", "c", next!)).resolves.toEqual({
      applied: true,
      version: null,
    });
    await expect(a.cacheGetVersioned("world", "c")).resolves.toBeNull();
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
