//! The gateway against a real Felix broker, control plane and the stand-in
//! identity provider. Ignored by default because they need the development
//! stack running; see the README, or run `cargo test -- --include-ignored`
//! with the `GATEWAY_*` variables set. `GATEWAY_SCOPE_FILE` must be the test
//! scope file, `dev/scope.toml`, which matches what `dev/seed.mjs` creates.
//!
//! With `GATEWAY_FELIX_CREDENTIAL_FILE` and the client certificate set, the
//! same tests run over shared connections. The tests named `shared_*` always
//! do, against the dev stack's second broker.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use felix_client::{CacheWatchFilter, TokenFuture, TokenProvider};
use felix_gateway::{Config, Gateway, Heartbeat, Refused, ScopeConfig, SharedConfig};
use felix_wire::AckMode;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const WAIT: Duration = Duration::from_secs(10);

fn config() -> Config {
    Config::from_env().expect("GATEWAY_* environment for the dev stack")
}

async fn start_gateway() -> (Gateway, SocketAddr) {
    serve(config()).await
}

/// Where `dev/up.sh` leaves certificates and credentials.
fn dev_state(file: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../dev/state")
        .join(file)
}

/// The gateway on one shared connection to the broker that binds tokens to
/// client certificates.
fn shared_config() -> Config {
    let mut config = config();
    config.brokers = vec!["127.0.0.1:5001".into()];
    config.ca_file = Some(dev_state("ca.pem"));
    config.shared = Some(SharedConfig {
        credential_file: dev_state("gateway.token"),
        client_cert: dev_state("gateway-cert.pem"),
        client_key: dev_state("gateway-key.pem"),
        connections: 1,
    });
    config
}

async fn serve(config: Config) -> (Gateway, SocketAddr) {
    let gateway = Gateway::new(&config).expect("read the broker CA");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = gateway
        .router()
        .into_make_service_with_connect_info::<SocketAddr>();
    tokio::spawn(async move { axum::serve(listener, router).await });
    (gateway, addr)
}

/// A tag unique to one test run, so records left in the durable stream by
/// earlier runs, or by tests running alongside, can be told apart.
fn run_tag(test: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{test}-{nanos}")
}

/// An ID token for `user` from the development IdP, as a browser would hold
/// after signing in.
async fn sign_in(user: &str) -> String {
    let config = config();
    let url = format!(
        "{}/token?sub={user}&aud={}",
        config.oidc_issuer, config.oidc_client_id
    );
    let answer: Value = reqwest::get(url)
        .await
        .and_then(reqwest::Response::error_for_status)
        .expect("the development IdP answers")
        .json()
        .await
        .unwrap();
    answer["id_token"].as_str().unwrap().to_string()
}

struct Browser {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    next_id: u64,
}

impl Browser {
    /// Open a connection and send `join`, without waiting for the answer.
    async fn connect(gateway: SocketAddr, room: &str, token: &str) -> Self {
        Self::connect_with(gateway, room, token, json!([])).await
    }

    /// As [`Browser::connect`], asking for `features`.
    async fn connect_with(gateway: SocketAddr, room: &str, token: &str, features: Value) -> Self {
        let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{gateway}/ws"))
            .await
            .expect("open WebSocket");
        let mut browser = Self { socket, next_id: 0 };
        browser
            .send(json!({"type": "join", "protocol": 1, "features": features, "room": room, "token": token}))
            .await;
        browser
    }

    /// Join `room` signed in as `user`, and wait until the gateway confirms.
    async fn join(gateway: SocketAddr, user: &str, room: &str) -> Self {
        let mut browser = Self::connect(gateway, room, &sign_in(user).await).await;
        let hello = browser.recv().await;
        assert_eq!(hello["type"], "hello", "{hello}");
        assert_eq!(hello["room"], room);
        assert_eq!(
            (hello["protocol"].as_u64(), &hello["features"]),
            (Some(1), &json!([]))
        );
        assert!(
            hello["cache_ttl_ms"]["members"].as_u64() > Some(0),
            "{hello}"
        );
        browser
    }

    async fn open(gateway: SocketAddr) -> Self {
        Self::join(gateway, "ana", "lobby").await
    }

    /// The error a refused join is answered with. The gateway closes after it.
    async fn refusal(&mut self) -> Value {
        let error = self.recv().await;
        assert_eq!(error["type"], "error", "{error}");
        let closed = tokio::time::timeout(WAIT, self.socket.next())
            .await
            .unwrap();
        assert!(
            !matches!(closed, Some(Ok(Message::Text(_)))),
            "nothing follows a refusal: {closed:?}"
        );
        error
    }

    async fn send(&mut self, message: Value) {
        self.socket
            .send(Message::text(message.to_string()))
            .await
            .unwrap();
    }

    async fn recv(&mut self) -> Value {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            let frame = tokio::time::timeout_at(deadline, self.socket.next())
                .await
                .expect("timed out waiting for the gateway")
                .expect("socket closed")
                .unwrap();
            if let Message::Text(text) = frame {
                return serde_json::from_str(&text).unwrap();
            }
        }
    }

    async fn recv_type(&mut self, kind: &str) -> Value {
        loop {
            let message = self.recv().await;
            if message["type"] == kind {
                return message;
            }
            assert_ne!(message["type"], "error", "unexpected error: {message}");
        }
    }

    async fn subscribe(&mut self, stream: &str, from: Value) -> Value {
        self.send(json!({"type": "subscribe", "stream": stream, "from": from}))
            .await;
        self.recv_type("subscribed").await
    }

    /// The next change to the member entry `key`.
    async fn member_change(&mut self, key: &str) -> Value {
        loop {
            let change = self.recv_type("cache_change").await;
            if change["cache"] == "members" && change["key"] == key {
                return change;
            }
        }
    }

    /// The keys in the next full member list.
    async fn member_keys(&mut self) -> Vec<String> {
        self.send(json!({"type": "cache_watch", "cache": "members"}))
            .await;
        let list = self.recv_type("cache_entries").await;
        assert_eq!(list["cache"], "members");
        list["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["key"].as_str().unwrap().to_string())
            .collect()
    }

    /// Publish without waiting for the ack; returns the request id.
    async fn publish(&mut self, stream: &str, payload: &str, ack: bool) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.send(json!({
            "type": "publish",
            "stream": stream,
            "payload": BASE64.encode(payload),
            "ack": ack,
            "id": id,
        }))
        .await;
        id
    }

    /// Events whose payload starts with `tag`, until `count` have arrived.
    /// Acks met on the way are returned too, by request id.
    async fn events(
        &mut self,
        tag: &str,
        count: usize,
    ) -> (Vec<(Option<u64>, String)>, Vec<Value>) {
        let mut events = Vec::new();
        let mut acks = Vec::new();
        while events.len() < count {
            let message = self.recv().await;
            match message["type"].as_str() {
                Some("event") => {
                    let payload = BASE64.decode(message["payload"].as_str().unwrap()).unwrap();
                    let payload = String::from_utf8(payload).unwrap();
                    if payload.starts_with(tag) {
                        events.push((message["offset"].as_u64(), payload));
                    }
                }
                Some("ack") => acks.push(message),
                _ => panic!("unexpected message: {message}"),
            }
        }
        (events, acks)
    }
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn two_browsers_see_each_others_ops_in_one_offset_order() {
    let (gateway, addr) = start_gateway().await;
    let tag = run_tag("two-browsers");
    let mut alice = Browser::open(addr).await;
    let mut bob = Browser::open(addr).await;
    alice.subscribe("ops", json!("live")).await;
    bob.subscribe("ops", json!("live")).await;

    for i in 0..5 {
        alice
            .publish("ops", &format!("{tag}/alice/{i}"), true)
            .await;
        bob.publish("ops", &format!("{tag}/bob/{i}"), true).await;
    }

    let (seen_by_alice, alice_acks) = alice.events(&tag, 10).await;
    let (seen_by_bob, _) = bob.events(&tag, 10).await;
    assert_eq!(seen_by_alice, seen_by_bob, "both browsers see one order");

    let offsets: Vec<u64> = seen_by_alice
        .iter()
        .map(|(offset, _)| offset.expect("ops events carry offsets"))
        .collect();
    assert!(
        offsets.windows(2).all(|pair| pair[0] < pair[1]),
        "offsets increase: {offsets:?}"
    );
    for author in ["alice", "bob"] {
        let own: Vec<&str> = seen_by_alice
            .iter()
            .map(|(_, payload)| payload.as_str())
            .filter(|payload| payload.contains(&format!("/{author}/")))
            .collect();
        let sent: Vec<String> = (0..5).map(|i| format!("{tag}/{author}/{i}")).collect();
        assert_eq!(own, sent, "{author}'s ops keep the order they were sent in");
    }

    // An ack names the offset the event arrived at. Acks may trail the events,
    // so collect the rest before comparing.
    let mut acks = alice_acks;
    while acks.len() < 5 {
        acks.push(alice.recv_type("ack").await);
    }
    for ack in acks {
        let id = ack["id"].as_u64().unwrap();
        let payload = format!("{tag}/alice/{id}");
        let event_offset = seen_by_alice
            .iter()
            .find(|(_, seen)| *seen == payload)
            .and_then(|(offset, _)| *offset);
        assert_eq!(ack["offset"].as_u64(), event_offset, "ack {ack}");
    }

    let latency = gateway.metrics();
    assert!(latency.felix_publish_ack["ops"].count >= 10);
    eprintln!("gateway latency: {latency:?}");
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn subscribing_from_an_offset_replays_the_log() {
    let (_gateway, addr) = start_gateway().await;
    let tag = run_tag("replay");
    let mut writer = Browser::open(addr).await;
    let mut first = None;
    for i in 0..3 {
        writer.publish("ops", &format!("{tag}/{i}"), true).await;
        let ack = writer.recv_type("ack").await;
        first.get_or_insert(ack["offset"].as_u64().expect("acks carry offsets"));
    }

    let mut reader = Browser::open(addr).await;
    let subscribed = reader.subscribe("ops", json!(first.unwrap())).await;
    assert_eq!(subscribed["start_offset"].as_u64(), first);
    let (events, _) = reader.events(&tag, 3).await;
    let payloads: Vec<&str> = events.iter().map(|(_, payload)| payload.as_str()).collect();
    assert_eq!(payloads, [0, 1, 2].map(|i| format!("{tag}/{i}")));
    assert_eq!(events[0].0, first);
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn presence_is_relayed_without_offsets() {
    let (_gateway, addr) = start_gateway().await;
    let tag = run_tag("presence");
    let mut viewer = Browser::open(addr).await;
    let mut mover = Browser::open(addr).await;
    viewer.subscribe("presence", json!("live")).await;

    mover
        .publish("presence", &format!("{tag}/cursor"), false)
        .await;
    let (events, _) = viewer.events(&tag, 1).await;
    assert_eq!(events, [(None, format!("{tag}/cursor"))]);
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_bad_message_is_answered_and_the_session_continues() {
    let (_gateway, addr) = start_gateway().await;
    let mut browser = Browser::open(addr).await;
    browser
        .send(json!({"type": "subscribe", "stream": "ops"}))
        .await;
    assert_eq!(browser.recv().await["code"], "bad_request");
    browser
        .send(json!({"type": "subscribe", "stream": "chat", "from": "live"}))
        .await;
    let error = browser.recv().await;
    assert_eq!(
        (error["code"].as_str(), error["stream"].as_str()),
        (Some("bad_request"), Some("chat"))
    );
    browser
        .send(json!({"type": "x.teleport", "to": "studio"}))
        .await;
    assert_eq!(browser.recv().await["code"], "unsupported");
    browser
        .send(json!({"type": "publish", "stream": "ops", "payload": "not base64!", "ack": true, "id": 9}))
        .await;
    let error = browser.recv().await;
    assert_eq!(
        (error["code"].as_str(), error["id"].as_u64()),
        (Some("bad_request"), Some(9))
    );

    let id = browser.publish("ops", "still here", true).await;
    assert_eq!(browser.recv_type("ack").await["id"], id);
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn metrics_report_both_legs() {
    let (_gateway, addr) = start_gateway().await;
    let mut browser = Browser::open(addr).await;
    browser.publish("ops", "metrics", true).await;
    browser.recv_type("ack").await;

    // The first ping goes out when the session starts. tungstenite only sends
    // the pong while the socket is polled, so keep polling it between checks.
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let metrics = http_get(addr, "/metrics").await;
        if metrics["browser_rtt"]["count"].as_u64() > Some(0) {
            assert!(metrics["felix_publish_ack"]["ops"]["count"].as_u64() > Some(0));
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no RTT sample: {metrics}"
        );
        let _ = tokio::time::timeout(Duration::from_millis(50), browser.socket.next()).await;
    }
    drop(browser);
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn counter_adds_answer_with_the_running_sum() {
    let (_gateway, addr) = start_gateway().await;
    let key = run_tag("seq").replace(|c: char| !c.is_ascii_alphanumeric(), "-");
    let mut browser = Browser::open(addr).await;
    let mut sums = Vec::new();
    for (id, delta) in [(1, 256), (2, 256), (3, 1)] {
        browser
            .send(json!({"type": "counter_add", "counter": "seq", "key": key, "delta": delta, "id": id}))
            .await;
        let reply = browser.recv_type("counter").await;
        assert_eq!(reply["id"], id);
        sums.push(reply["value"].as_i64().unwrap());
    }
    assert_eq!(sums, [256, 512, 513]);

    browser
        .send(json!({"type": "counter_add", "counter": "seq", "key": "no/slashes", "delta": 1, "id": 9}))
        .await;
    let error = browser.recv().await;
    assert_eq!(
        (error["code"].as_str(), error["id"].as_u64()),
        (Some("bad_request"), Some(9))
    );
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn an_op_published_while_the_snapshot_is_read_reaches_a_browser_that_subscribed_first() {
    let (_gateway, addr) = start_gateway().await;
    let tag = run_tag("join");
    let mut joiner = Browser::open(addr).await;
    let mut writer = Browser::open(addr).await;

    // The join path: subscribe at the live tail, then read the snapshot.
    let subscribed = joiner.subscribe("ops", json!("live")).await;
    let live = subscribed["live_offset"]
        .as_u64()
        .expect("a live subscription names the tail");
    assert_eq!(subscribed["start_offset"].as_u64(), Some(live));
    joiner
        .send(json!({"type": "cache_get", "cache": "snap", "key": "latest", "id": 1}))
        .await;
    writer.publish("ops", &format!("{tag}/during"), true).await;
    let written = writer.recv_type("ack").await["offset"].as_u64();

    let mut snapshot = None;
    let mut event = None;
    while snapshot.is_none() || event.is_none() {
        let message = joiner.recv().await;
        match message["type"].as_str() {
            Some("cache_value") => {
                assert_eq!(message["id"], 1);
                snapshot = Some(message);
            }
            Some("event") => {
                let payload = BASE64.decode(message["payload"].as_str().unwrap()).unwrap();
                if payload.starts_with(tag.as_bytes()) {
                    event = message["offset"].as_u64();
                }
            }
            _ => panic!("unexpected message: {message}"),
        }
    }
    assert_eq!(event, written, "the op arrives on the live subscription");
    assert!(event >= Some(live));
    let payload = &snapshot.unwrap()["payload"];
    assert!(payload.is_null() || payload.is_string(), "{payload}");
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_room_without_a_snapshot_answers_null() {
    // Nothing writes a snapshot of the studio during the tests.
    let (_gateway, addr) = start_gateway().await;
    let mut browser = Browser::join(addr, "ana", "studio").await;
    browser
        .send(json!({"type": "cache_get", "cache": "snap", "key": "latest", "id": 3}))
        .await;
    let reply = browser.recv_type("cache_value").await;
    assert_eq!(
        reply,
        json!({"type": "cache_value", "id": 3, "payload": null})
    );
    browser
        .send(json!({"type": "cache_get", "cache": "snap", "key": "a/b", "id": 4}))
        .await;
    assert_eq!(browser.recv().await["code"], "bad_request");
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn members_are_listed_watched_and_expire() {
    let mut config = config();
    let scope = std::fs::read_to_string(std::env::var("GATEWAY_SCOPE_FILE").unwrap()).unwrap();
    assert!(scope.contains("ttl_s = 30"), "the test scope file");
    config.scope = Arc::new(ScopeConfig::parse(&scope.replace("ttl_s = 30", "ttl_s = 2")).unwrap());
    let (_gateway, addr) = serve(config).await;
    let key = run_tag("member").replace(|c: char| !c.is_ascii_alphanumeric(), "-");
    let mut watcher = Browser::open(addr).await;
    assert!(!watcher.member_keys().await.contains(&key));

    let mut member = Browser::open(addr).await;
    let set = json!({"type": "cache_put", "cache": "members", "key": key, "payload": BASE64.encode("ana")});
    member.send(set.clone()).await;
    let change = watcher.member_change(&key).await;
    assert_eq!(change["payload"], BASE64.encode("ana"));
    let expires_in = change["expires_in_ms"]
        .as_u64()
        .expect("member entries expire");
    assert!((1..=2000).contains(&expires_in), "{change}");

    member
        .send(json!({"type": "cache_delete", "cache": "members", "key": key}))
        .await;
    assert_eq!(watcher.member_change(&key).await["payload"], Value::Null);

    member.send(set.clone()).await;
    watcher.member_change(&key).await;
    let token = sign_in("ana").await;
    let leave = |room: &str, key: &str| {
        json!({"room": room, "token": token, "cache": "members", "key": key}).to_string()
    };
    assert_eq!(
        http_post(addr, "/members/leave", &leave("lobby", &key)).await,
        204
    );
    assert_eq!(watcher.member_change(&key).await["payload"], Value::Null);
    assert_eq!(
        http_post(addr, "/members/leave", &leave("lobby", "a:b")).await,
        400
    );
    let snap = json!({"room": "lobby", "token": token, "cache": "snap", "key": key}).to_string();
    assert_eq!(http_post(addr, "/members/leave", &snap).await, 400);
    let ben =
        json!({"room": "studio", "token": sign_in("ben").await, "cache": "members", "key": key})
            .to_string();
    assert_eq!(http_post(addr, "/members/leave", &ben).await, 403);

    member.send(set).await;
    watcher.member_change(&key).await;
    assert!(Browser::open(addr).await.member_keys().await.contains(&key));
    // Not refreshed, so it expires, and Felix tells watchers.
    assert_eq!(watcher.member_change(&key).await["payload"], Value::Null);
    assert!(
        !Browser::open(addr).await.member_keys().await.contains(&key),
        "an entry past its TTL is not listed"
    );

    member
        .send(json!({"type": "cache_put", "cache": "members", "key": "a:b", "payload": ""}))
        .await;
    assert_eq!(member.recv().await["code"], "bad_request");
}

/// A fixed token, so a test can hold a connection to exactly what one room
/// token allows.
struct Fixed(String);

impl TokenProvider for Fixed {
    fn token(&self) -> TokenFuture<'_> {
        let token = self.0.clone();
        Box::pin(async move { Ok(token) })
    }
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_token_for_one_room_cannot_reach_another_at_the_broker() {
    let (gateway, _addr) = start_gateway().await;
    let config = config();
    let (tenant, namespace) = (config.tenant.as_str(), config.namespace.as_str());
    // Ana is a member of both rooms, so a refusal below comes from the
    // narrowing, not from membership. The connection goes straight to Felix;
    // no gateway code stands between it and the broker.
    let token = gateway
        .scope_token(&sign_in("ana").await, "lobby")
        .await
        .expect("ana may open the lobby");
    let felix = Arc::new(
        gateway
            .connect_felix(Arc::new(Fixed(token)))
            .await
            .expect("connect to Felix"),
    );
    let tag = run_tag("narrowed");

    felix
        .publish(
            tenant,
            namespace,
            "demo.ops.lobby",
            tag.clone().into_bytes(),
            AckMode::PerMessage,
        )
        .await
        .expect("the lobby token publishes to the lobby");
    felix
        .subscribe_from(tenant, namespace, "demo.ops.lobby", None)
        .await
        .expect("and subscribes to it");

    for stream in ["demo.ops.studio", "demo.presence.studio"] {
        let publish = felix
            .publish(
                tenant,
                namespace,
                stream,
                tag.clone().into_bytes(),
                AckMode::PerMessage,
            )
            .await;
        assert!(publish.is_err(), "published to {stream}: {publish:?}");
        let subscribe = felix.subscribe_from(tenant, namespace, stream, None).await;
        assert!(subscribe.is_err(), "subscribed to {stream}");
    }
    let client = felix.client().await;
    let read = client
        .cache_get(tenant, namespace, "demo.snap.studio", "latest")
        .await;
    assert!(read.is_err(), "read the studio snapshot: {read:?}");
    let add = client
        .counter_add(tenant, namespace, "demo.seq.studio", "narrowed", 1)
        .await;
    assert!(add.is_err(), "added to a studio counter: {add:?}");
    let put = client
        .cache_put(
            tenant,
            namespace,
            "demo.members.studio",
            "narrowed",
            b"ana".to_vec().into(),
            Some(1000),
        )
        .await;
    assert!(put.is_err(), "joined the studio's member list: {put:?}");
    let watch = felix
        .watch_cache_retained(
            tenant,
            namespace,
            "demo.members.studio",
            CacheWatchFilter::Prefix(String::new()),
        )
        .await;
    assert!(watch.is_err(), "watched the studio's member list");
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_member_of_one_room_is_refused_another() {
    let (gateway, addr) = start_gateway().await;
    let ben = sign_in("ben").await;
    assert!(matches!(
        gateway.scope_token(&ben, "studio").await,
        Err(Refused::Forbidden)
    ));

    let mut browser = Browser::connect(addr, "studio", &ben).await;
    assert_eq!(browser.refusal().await["code"], "forbidden");
    Browser::join(addr, "ben", "lobby").await;
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_join_without_a_valid_sign_in_or_room_is_refused() {
    let (_gateway, addr) = start_gateway().await;
    let mut forged = Browser::connect(addr, "lobby", "not.a.token").await;
    assert_eq!(forged.refusal().await["code"], "signed_out");

    let token = sign_in("ana").await;
    let mut bad_room = Browser::connect(addr, "lobby/../studio", &token).await;
    assert_eq!(bad_room.refusal().await["code"], "bad_request");

    let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let mut future = Browser { socket, next_id: 0 };
    future
        .send(json!({"type": "join", "protocol": 2, "room": "lobby", "token": token}))
        .await;
    assert_eq!(future.refusal().await["code"], "unsupported");

    let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    let mut early = Browser { socket, next_id: 0 };
    early
        .send(json!({"type": "subscribe", "stream": "ops", "from": "live"}))
        .await;
    assert_eq!(early.refusal().await["code"], "bad_request");
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_throttled_browser_falls_behind_and_still_gets_every_record() {
    const RECORDS: usize = 1000;
    let (_gateway, addr) = start_gateway().await;
    let tag = run_tag("throttled");
    let padding = "x".repeat(200);
    let mut writer = Browser::open(addr).await;
    let mut fast = Browser::open(addr).await;
    let mut slow = Browser::open(addr).await;
    fast.subscribe("ops", json!("live")).await;
    slow.send(json!({"type": "throttle", "bits_per_second": 100_000}))
        .await;
    slow.subscribe("ops", json!("live")).await;

    for i in 0..RECORDS {
        writer
            .publish("ops", &format!("{tag}/{i:04}/{padding}"), true)
            .await;
    }
    let (events, _) = fast.events(&tag, RECORDS).await;
    let payloads: Vec<&str> = events.iter().map(|(_, payload)| payload.as_str()).collect();
    let sent: Vec<String> = (0..RECORDS)
        .map(|i| format!("{tag}/{i:04}/{padding}"))
        .collect();
    assert_eq!(payloads, sent, "the browser keeping up gets every record");

    // By now Felix has dropped records for the throttled browser. Its
    // subscription replays them from the log, so it gets each record once,
    // in order.
    tokio::time::sleep(Duration::from_secs(2)).await;
    slow.send(json!({"type": "throttle", "bits_per_second": null}))
        .await;
    let (events, _) = slow.events(&tag, RECORDS).await;
    let payloads: Vec<&str> = events.iter().map(|(_, payload)| payload.as_str()).collect();
    assert_eq!(payloads, sent, "the throttled browser gets every record");
    let offsets: Vec<u64> = events.iter().map(|(offset, _)| offset.unwrap()).collect();
    assert!(
        offsets.windows(2).all(|pair| pair[0] < pair[1]),
        "{offsets:?}"
    );
}

/// A scope like `dev/scope.toml`'s `ops` and `members`, with tight limits.
const LIMITED_SCOPE: &str = r#"
    [scope]
    field = "room"

    [[scope.streams]]
    alias = "ops"
    name = "demo.ops.{scope}"
    actions = ["publish", "subscribe"]

    [[scope.caches]]
    alias = "members"
    name = "demo.members.{scope}"
    actions = ["read", "write", "watch"]
    ttl_s = 30

    [limits]
    max_payload_bytes = 1024

    [limits.session]
    writes_per_s = 2
    write_burst = 3
"#;

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_burst_over_the_write_limit_is_refused_and_a_later_publish_lands() {
    let mut config = config();
    config.scope = Arc::new(ScopeConfig::parse(LIMITED_SCOPE).unwrap());
    let (gateway, addr) = serve(config).await;
    let tag = run_tag("limited");

    let token = sign_in("ana").await;
    let mut browser = Browser::connect_with(addr, "lobby", &token, json!(["rate_limited"])).await;
    let hello = browser.recv().await;
    assert_eq!(hello["features"], json!(["rate_limited"]), "{hello}");

    // The session's burst is 3 writes; the next two are refused at once,
    // without reaching Felix, and say when one would fit.
    for i in 0..5 {
        browser.publish("ops", &format!("{tag}/{i}"), true).await;
    }
    let mut acked = Vec::new();
    let mut refused = Vec::new();
    for _ in 0..5 {
        let reply = browser.recv().await;
        match reply["type"].as_str() {
            Some("ack") => acked.push(reply["id"].as_u64().unwrap()),
            Some("error") => {
                assert_eq!(
                    (reply["code"].as_str(), reply["stream"].as_str()),
                    (Some("rate_limited"), Some("ops")),
                    "{reply}"
                );
                let wait = reply["retry_after_ms"].as_u64().expect("retry_after_ms");
                assert!((1..=1000).contains(&wait), "{reply}");
                refused.push((reply["id"].as_u64().unwrap(), wait));
            }
            _ => panic!("unexpected reply: {reply}"),
        }
    }
    acked.sort_unstable();
    assert_eq!(acked, [0, 1, 2]);
    let ids: Vec<u64> = refused.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, [3, 4]);

    let wait = refused.iter().map(|(_, wait)| *wait).max().unwrap();
    tokio::time::sleep(Duration::from_millis(wait + 50)).await;
    let id = browser.publish("ops", &format!("{tag}/retry"), true).await;
    assert_eq!(browser.recv_type("ack").await["id"], id);

    // A payload over the alias's size limit is a bad request: retrying it
    // cannot help.
    let id = browser.publish("ops", &"x".repeat(2048), true).await;
    let too_big = browser.recv().await;
    assert_eq!(
        (too_big["code"].as_str(), too_big["id"].as_u64()),
        (Some("bad_request"), Some(id)),
        "{too_big}"
    );

    // A browser that did not ask for `rate_limited` gets the code it knows.
    let mut old = Browser::join(addr, "ben", "lobby").await;
    for i in 0..4 {
        old.publish("ops", &format!("{tag}/old/{i}"), true).await;
    }
    let mut codes = Vec::new();
    for _ in 0..4 {
        let reply = old.recv().await;
        codes.push(reply["code"].as_str().unwrap_or("ack").to_string());
    }
    codes.sort();
    assert_eq!(codes, ["ack", "ack", "ack", "publish_failed"]);

    let refusals = &gateway.metrics().limits_refused;
    assert_eq!(refusals["session_rate"], 3);
    assert_eq!(refusals["message_size"], 1);
}

/// One session per person, pinged every 200 ms and closed after a second of
/// silence.
fn heartbeat_config() -> Config {
    let mut config = config();
    let scope = std::fs::read_to_string(std::env::var("GATEWAY_SCOPE_FILE").unwrap()).unwrap();
    config.scope = Arc::new(
        ScopeConfig::parse(&format!("{scope}\n[limits]\nsessions_per_principal = 1\n")).unwrap(),
    );
    config.heartbeat = Heartbeat {
        interval: Duration::from_millis(200),
        timeout: Duration::from_secs(1),
    };
    config
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_browser_that_stops_answering_pings_is_closed_and_frees_its_session() {
    let (_gateway, addr) = serve(heartbeat_config()).await;
    // A socket that is not read never answers a ping, like a browser whose
    // network dropped.
    let mut gone = Browser::join(addr, "cleo", "lobby").await;
    let token = sign_in("cleo").await;
    let mut refused = Browser::connect(addr, "lobby", &token).await;
    assert_eq!(refused.refusal().await["code"], "unavailable");

    tokio::time::sleep(Duration::from_millis(2500)).await;
    Browser::join(addr, "cleo", "lobby").await;
    let closed = tokio::time::timeout(WAIT, async {
        while let Some(Ok(frame)) = gone.socket.next().await {
            if matches!(frame, Message::Close(_)) {
                break;
            }
        }
    });
    assert!(closed.await.is_ok(), "the gateway closed the silent socket");
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn a_browser_that_answers_pings_keeps_its_session() {
    let (_gateway, addr) = serve(heartbeat_config()).await;
    let mut browser = Browser::join(addr, "cleo", "lobby").await;
    // Reading the socket answers pings, as a browser does on its own.
    let mut pings = 0;
    let until = tokio::time::Instant::now() + Duration::from_millis(2500);
    while let Ok(frame) = tokio::time::timeout_at(until, browser.socket.next()).await {
        match frame {
            Some(Ok(Message::Ping(_))) => pings += 1,
            other => panic!("unexpected frame: {other:?}"),
        }
    }
    assert!(pings >= 5, "{pings} pings");
    assert_eq!(
        browser.subscribe("ops", json!("live")).await["stream"],
        "ops"
    );
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn shared_users_on_one_connection_each_have_their_own_subscription_cap() {
    let (_gateway, addr) = serve(shared_config()).await;
    let tag = run_tag("shared-cap");
    // The broker allows each user 6 subscriptions on a connection. Ana takes
    // all of hers over three sessions.
    let mut anas = Vec::new();
    for _ in 0..3 {
        let mut ana = Browser::open(addr).await;
        ana.subscribe("ops", json!("live")).await;
        ana.subscribe("presence", json!("live")).await;
        anas.push(ana);
    }
    let mut over = Browser::open(addr).await;
    over.send(json!({"type": "subscribe", "stream": "ops", "from": "live"}))
        .await;
    let refused = over.recv().await;
    assert_eq!(
        (refused["code"].as_str(), refused["stream"].as_str()),
        (Some("subscribe_failed"), Some("ops")),
        "{refused}"
    );

    // Ben shares the connection and still has room of his own.
    let mut ben = Browser::join(addr, "ben", "lobby").await;
    ben.subscribe("ops", json!("live")).await;
    ben.subscribe("presence", json!("live")).await;
    let id = ben.publish("ops", &format!("{tag}/ben"), true).await;
    assert_eq!(ben.recv_type("ack").await["id"], id);
    let (events, _) = anas[0].events(&tag, 1).await;
    assert_eq!(events[0].1, format!("{tag}/ben"));
}

/// `subject`, a scope token, delegated to the gateway signed in as `actor`.
async fn delegate_as(actor: &str, subject: &str) -> String {
    let delegated: Value = request_delegation(actor, subject)
        .await
        .error_for_status()
        .expect("the token is delegated")
        .json()
        .await
        .unwrap();
    delegated["access_token"].as_str().unwrap().to_string()
}

/// The control plane's answer to `actor` asking for `subject` delegated to it.
async fn request_delegation(actor: &str, subject: &str) -> reqwest::Response {
    let config = config();
    let base = format!(
        "{}/v1/tenants/{}/token",
        config.control_plane, config.tenant
    );
    let http = reqwest::Client::new();
    let exchanged: Value = http
        .post(format!("{base}/exchange"))
        .bearer_auth(sign_in(actor).await)
        .json(&json!({"audience": "felix-controlplane", "requested": ["token.delegate"]}))
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .expect("the gateway's sign-in is exchanged")
        .json()
        .await
        .unwrap();
    http.post(format!("{base}/delegate"))
        .bearer_auth(exchanged["felix_token"].as_str().unwrap())
        .json(&json!({
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token": subject,
        }))
        .send()
        .await
        .expect("the delegate request is answered")
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn shared_connections_refuse_a_token_not_delegated_to_the_gateway() {
    let (gateway, _addr) = serve(shared_config()).await;
    let config = config();
    let ana = gateway
        .scope_token(&sign_in("ana").await, "lobby")
        .await
        .expect("ana may open the lobby");

    let undelegated = gateway.attach_shared(Arc::new(Fixed(ana.clone()))).await;
    assert!(
        undelegated.is_err(),
        "a token without act passed on the gateway's certificate"
    );
    // The scope token was minted for this gateway (`may_act`), so the control
    // plane will not delegate it to another one.
    let elsewhere = request_delegation("demo-other-gateway", &ana).await;
    assert_eq!(
        elsewhere.status(),
        reqwest::StatusCode::FORBIDDEN,
        "a token minted for this gateway was delegated to another"
    );

    let ours = delegate_as("demo-gateway", &ana).await;
    let identity = gateway
        .attach_shared(Arc::new(Fixed(ours)))
        .await
        .expect("a token delegated to the gateway passes");
    identity
        .client()
        .publisher()
        .await
        .unwrap()
        .publish(
            &config.tenant,
            &config.namespace,
            "demo.ops.lobby",
            run_tag("delegated").into_bytes(),
            AckMode::PerMessage,
        )
        .await
        .expect("and publishes with ana's grants");
}

async fn http_post(addr: SocketAddr, path: &str, body: &str) -> u16 {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response[9..12].parse().unwrap()
}

async fn http_get(addr: SocketAddr, path: &str) -> Value {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    serde_json::from_str(body).unwrap()
}
