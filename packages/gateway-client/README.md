# felix-gateway-client

The browser client for [felix-gateway](https://github.com/GetFelix/felix-gateway). It opens a WebSocket, joins one
scope with an ID token, and then publishes, subscribes,
reads and watches caches and adds to counters, each named by its alias in the
gateway's scope file.
[docs/protocol.md](https://github.com/GetFelix/felix-gateway/blob/main/docs/protocol.md)
describes the protocol.

```ts
import { GatewayClient } from "felix-gateway-client";

const client = await GatewayClient.connect("wss://example.com/ws", { room: "lobby", token });
client.onEvent = (event) => console.log(event.stream, event.offset, event.payload);
client.subscribe("ops", "live");
await client.publish("ops", new TextEncoder().encode("hello"));
```

Two sessions that write one cache key with `cachePut` overwrite each other.
Join with the `cache_versions` feature to avoid that: `cacheGetVersioned` reads
a key with its version, and `cachePutIf` writes it only if the key is still at
that version. A refused write returns the current version instead of throwing,
so the usual loop is read, edit, write, and read again when refused. `edit` runs
again on every refusal, so it must be safe to repeat. `url` and `token` are the
gateway's WebSocket URL and the ID token from signing in:

```ts
import { GatewayClient, type Gateway } from "felix-gateway-client";

const client = await GatewayClient.connect(url, { room: "lobby", token }, ["cache_versions"]);
// Count how many times the chunk was edited, without losing anyone else's edit.
await update(client, "world", "chunk", (current) => Uint8Array.of((current?.[0] ?? 0) + 1));

/** Apply `edit` to a cache entry, trying again whenever someone else wrote it first. */
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
```

`felix-gateway-client/fake` has `FakeScope` and `FakeGateway`, an in-memory
stand-in with the same shape as `GatewayClient`, for tests that should not need
a gateway or a broker. It gives every cache write a new version that is never reused, as Felix does, so
conditional writes behave as they would against a gateway, and it can drop live records and
lose acks to exercise recovery paths.

## License

MIT
