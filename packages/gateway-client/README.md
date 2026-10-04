# felix-gateway-client

The browser client for the Felix gateway. It opens a WebSocket, joins one
scope (a room or a match) with an ID token, and then publishes, subscribes,
reads and watches caches and adds to counters, each named by its alias in the
gateway's scope file. The protocol is described in the gateway's
`docs/protocol.md`.

```ts
import { GatewayClient } from "felix-gateway-client";

const client = await GatewayClient.connect("wss://example.com/ws", { room: "lobby", token });
client.onEvent = (event) => console.log(event.stream, event.offset, event.payload);
client.subscribe("ops", "live");
await client.publish("ops", new TextEncoder().encode("hello"));
```

`felix-gateway-client/fake` has `FakeScope` and `FakeGateway`, an in-memory
stand-in with the same shape as `GatewayClient`, for tests that should not need
a gateway or a broker. It can drop live records and lose acks to exercise
recovery paths.

## License

MIT
