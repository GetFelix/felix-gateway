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

Two sessions editing one cache key can avoid overwriting each other by joining
with the `cache_versions` feature, reading a key with `cacheGetVersioned` and
writing it with `cachePutIf`, which applies only if the key is still at the
version that was read:

```ts
const client = await GatewayClient.connect(url, { room: "lobby", token }, ["cache_versions"]);
const entry = await client.cacheGetVersioned("world", "chunk");
const result = await client.cachePutIf(
  "world",
  "chunk",
  next,
  entry ? { version: entry.version } : "absent",
);
if (!result.applied) {
  // Someone else wrote first: read the key again and retry from its version.
}
```

`felix-gateway-client/fake` has `FakeScope` and `FakeGateway`, an in-memory
stand-in with the same shape as `GatewayClient`, for tests that should not need
a gateway or a broker. It can drop live records and lose acks to exercise
recovery paths.

## License

MIT
