import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { GatewayClient, GatewayError, PROTOCOL, type GatewayEvent } from "../src/index.js";

/** A WebSocket that records what is sent and lets a test answer. */
class FakeSocket extends EventTarget {
  static last: FakeSocket | null = null;
  readonly sent: Record<string, unknown>[] = [];

  constructor(readonly url: string) {
    super();
    FakeSocket.last = this;
    setTimeout(() => this.dispatchEvent(new Event("open")));
  }

  send(text: string): void {
    this.sent.push(JSON.parse(text) as Record<string, unknown>);
  }

  receive(message: object): void {
    this.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(message) }));
  }

  close(): void {
    this.dispatchEvent(new Event("close"));
  }
}

const bytes = (...values: number[]) => new Uint8Array(values);

async function connect(): Promise<{ client: GatewayClient; socket: FakeSocket }> {
  const client = await GatewayClient.connect("ws://gateway/ws", { room: "lobby", token: "t" }, [
    "binary",
  ]);
  return { client, socket: FakeSocket.last! };
}

beforeEach(() => vi.stubGlobal("WebSocket", FakeSocket));
afterEach(() => vi.unstubAllGlobals());

describe("GatewayClient", () => {
  it("joins with the protocol version, the scope and the features", async () => {
    const { socket } = await connect();
    expect(socket.sent[0]).toEqual({
      type: "join",
      room: "lobby",
      token: "t",
      protocol: PROTOCOL,
      features: ["binary"],
    });
  });

  it("reports hello with defaults for fields an older gateway leaves out", async () => {
    const { client, socket } = await connect();
    const hello = vi.fn();
    client.onHello = hello;
    socket.receive({
      type: "hello",
      protocol: 1,
      features: [],
      namespace: "default",
      cache_ttl_ms: { members: 30000 },
    });
    expect(hello).toHaveBeenCalledWith({
      namespace: "default",
      features: [],
      cacheTtlMs: { members: 30000 },
      missing: [],
    });
  });

  it("encodes publishes and resolves them with the acknowledged offset", async () => {
    const { client, socket } = await connect();
    const offset = client.publish("ops", bytes(1, 2, 255));
    expect(socket.sent[1]).toEqual({
      type: "publish",
      stream: "ops",
      payload: "AQL/",
      ack: true,
      id: 0,
    });
    socket.receive({ type: "ack", id: 0, offset: 41 });
    await expect(offset).resolves.toBe(41);
  });

  it("resolves an unacknowledged publish at once", async () => {
    const { client, socket } = await connect();
    await expect(client.publish("ops", bytes(1), false)).resolves.toBeNull();
    expect(socket.sent[1]).toMatchObject({ type: "publish", ack: false });
  });

  it("decodes events", async () => {
    const { client, socket } = await connect();
    const events: GatewayEvent[] = [];
    client.onEvent = (event) => events.push(event);
    socket.receive({ type: "event", stream: "ops", offset: 7, payload: "AQL/" });
    socket.receive({ type: "event", stream: "ops", offset: 9, skipped_before: 1, payload: "" });
    expect(events).toEqual([
      { stream: "ops", offset: 7, skippedBefore: 0, payload: bytes(1, 2, 255) },
      { stream: "ops", offset: 9, skippedBefore: 1, payload: bytes() },
    ]);
  });

  it("answers counter and cache requests by id", async () => {
    const { client, socket } = await connect();
    const sum = client.counterAdd("seq", "next", 1);
    const value = client.cacheGet("snap", "latest");
    const missing = client.cacheGet("snap", "other");
    socket.receive({ type: "cache_value", id: 2, payload: null });
    socket.receive({ type: "cache_value", id: 1, payload: "AQ==" });
    socket.receive({ type: "counter", id: 0, value: 12 });
    await expect(sum).resolves.toBe(12);
    await expect(value).resolves.toEqual(bytes(1));
    await expect(missing).resolves.toBeNull();
  });

  it("rejects the request an error names and reports the rest", async () => {
    const { client, socket } = await connect();
    const errors: [GatewayError, string | undefined][] = [];
    client.onError = (error, stream) => errors.push([error, stream]);
    const publish = client.publish("ops", bytes(1));
    socket.receive({ type: "error", id: 0, code: "forbidden", message: "no" });
    socket.receive({ type: "error", stream: "ops", code: "trimmed", oldest: 5, message: "gone" });
    await expect(publish).rejects.toMatchObject({ code: "forbidden" });
    expect(errors.map(([error, stream]) => [error.code, stream])).toEqual([["trimmed", "ops"]]);
  });

  it("says when a rate-limited request may be retried", async () => {
    const { client, socket } = await connect();
    const publish = client.publish("ops", bytes(1));
    socket.receive({
      type: "error",
      id: 0,
      stream: "ops",
      code: "rate_limited",
      retry_after_ms: 120,
      message: "slow down",
    });
    await expect(publish).rejects.toMatchObject({ code: "rate_limited", retryAfterMs: 120 });
  });

  it("decodes cache entries and changes", async () => {
    const { client, socket } = await connect();
    const seen: unknown[] = [];
    client.onCacheEntries = (cache, entries) => seen.push([cache, entries]);
    client.onCacheChange = (cache, entry) => seen.push([cache, entry]);
    socket.receive({
      type: "cache_entries",
      cache: "members",
      entries: [{ key: "a", payload: "AQ==", expires_in_ms: 100 }],
    });
    socket.receive({
      type: "cache_change",
      cache: "members",
      key: "a",
      payload: null,
      expires_in_ms: null,
    });
    expect(seen).toEqual([
      ["members", [{ key: "a", payload: bytes(1), expiresInMs: 100 }]],
      ["members", { key: "a", payload: null, expiresInMs: null }],
    ]);
  });

  it("rejects pending requests when the connection closes", async () => {
    const { client } = await connect();
    const closed = vi.fn();
    client.onClose = closed;
    const sum = client.counterAdd("seq", "next", 1);
    client.close();
    await expect(sum).rejects.toMatchObject({ code: "closed" });
    expect(closed).toHaveBeenCalledOnce();
  });
});
