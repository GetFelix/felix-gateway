//! The gateway against a real Felix broker, control plane and the stand-in
//! identity provider. Ignored by default because they need the development
//! stack running; see the README, or run `cargo test -- --include-ignored`
//! with the `CANVAS_*` variables set.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use felix_canvas_gateway::{Config, Gateway, Refused};
use felix_client::{CacheWatchFilter, TokenFuture, TokenProvider};
use felix_wire::AckMode;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const WAIT: Duration = Duration::from_secs(10);

fn config() -> Config {
    Config::from_env().expect("CANVAS_* environment for the dev stack")
}

async fn start_gateway() -> (Gateway, SocketAddr) {
    serve(config()).await
}

async fn serve(config: Config) -> (Gateway, SocketAddr) {
    let gateway = Gateway::new(&config).expect("read the broker CA");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = gateway.router();
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
        let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{gateway}/ws"))
            .await
            .expect("open WebSocket");
        let mut browser = Self { socket, next_id: 0 };
        browser
            .send(json!({"type": "join", "room": room, "token": token}))
            .await;
        browser
    }

    /// Join `room` signed in as `user`, and wait until the gateway confirms.
    async fn join(gateway: SocketAddr, user: &str, room: &str) -> Self {
        let mut browser = Self::connect(gateway, room, &sign_in(user).await).await;
        let hello = browser.recv().await;
        assert_eq!(hello["type"], "hello", "{hello}");
        assert_eq!(hello["room"], room);
        assert!(hello["member_ttl_ms"].as_u64() > Some(0), "{hello}");
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
            let change = self.recv_type("member").await;
            if change["key"] == key {
                return change;
            }
        }
    }

    /// The keys in the next full member list.
    async fn member_keys(&mut self) -> Vec<String> {
        self.send(json!({"type": "watch_members"})).await;
        let list = self.recv_type("members").await;
        list["members"]
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
    assert!(latency.felix_publish_ack_ops.count >= 10);
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
        .send(json!({"type": "subscribe", "stream": "chat"}))
        .await;
    assert_eq!(browser.recv().await["code"], "bad_request");
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
            assert!(metrics["felix_publish_ack_ops"]["count"].as_u64() > Some(0));
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
    joiner.send(json!({"type": "snapshot_get", "id": 1})).await;
    writer.publish("ops", &format!("{tag}/during"), true).await;
    let written = writer.recv_type("ack").await["offset"].as_u64();

    let mut snapshot = None;
    let mut event = None;
    while snapshot.is_none() || event.is_none() {
        let message = joiner.recv().await;
        match message["type"].as_str() {
            Some("snapshot") => {
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
    browser.send(json!({"type": "snapshot_get", "id": 3})).await;
    let reply = browser.recv_type("snapshot").await;
    assert_eq!(reply, json!({"type": "snapshot", "id": 3, "payload": null}));
}

#[tokio::test]
#[ignore = "needs a Felix broker"]
async fn members_are_listed_watched_and_expire() {
    let mut config = config();
    config.member_ttl = Duration::from_secs(2);
    let (_gateway, addr) = serve(config).await;
    let key = run_tag("member").replace(|c: char| !c.is_ascii_alphanumeric(), "-");
    let mut watcher = Browser::open(addr).await;
    assert!(!watcher.member_keys().await.contains(&key));

    let mut member = Browser::open(addr).await;
    let set = json!({"type": "set_member", "key": key, "payload": BASE64.encode("ana")});
    member.send(set.clone()).await;
    let change = watcher.member_change(&key).await;
    assert_eq!(change["payload"], BASE64.encode("ana"));
    let expires_in = change["expires_in_ms"]
        .as_u64()
        .expect("member entries expire");
    assert!((1..=2000).contains(&expires_in), "{change}");

    member
        .send(json!({"type": "remove_member", "key": key}))
        .await;
    assert_eq!(watcher.member_change(&key).await["payload"], Value::Null);

    member.send(set.clone()).await;
    watcher.member_change(&key).await;
    let token = sign_in("ana").await;
    let leave =
        |room: &str, key: &str| json!({"room": room, "token": token, "key": key}).to_string();
    assert_eq!(
        http_post(addr, "/members/leave", &leave("lobby", &key)).await,
        204
    );
    assert_eq!(watcher.member_change(&key).await["payload"], Value::Null);
    assert_eq!(
        http_post(addr, "/members/leave", &leave("lobby", "a:b")).await,
        400
    );
    let ben = json!({"room": "studio", "token": sign_in("ben").await, "key": key}).to_string();
    assert_eq!(http_post(addr, "/members/leave", &ben).await, 403);

    member.send(set).await;
    watcher.member_change(&key).await;
    assert!(Browser::open(addr).await.member_keys().await.contains(&key));
    // Not refreshed, so it expires.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(
        !Browser::open(addr).await.member_keys().await.contains(&key),
        "an entry past its TTL is not listed"
    );

    member
        .send(json!({"type": "set_member", "key": "a:b", "payload": ""}))
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
        .room_token(&sign_in("ana").await, "lobby")
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
            "canvas.ops.lobby",
            tag.clone().into_bytes(),
            AckMode::PerMessage,
        )
        .await
        .expect("the lobby token publishes to the lobby");
    felix
        .subscribe_from(tenant, namespace, "canvas.ops.lobby", None)
        .await
        .expect("and subscribes to it");

    for stream in ["canvas.ops.studio", "canvas.presence.studio"] {
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
        .cache_get(tenant, namespace, "canvas.snap.studio", "latest")
        .await;
    assert!(read.is_err(), "read the studio snapshot: {read:?}");
    let add = client
        .counter_add(tenant, namespace, "canvas.seq.studio", "narrowed", 1)
        .await;
    assert!(add.is_err(), "added to a studio counter: {add:?}");
    let put = client
        .cache_put(
            tenant,
            namespace,
            "canvas.members.studio",
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
            "canvas.members.studio",
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
        gateway.room_token(&ben, "studio").await,
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
    let mut early = Browser { socket, next_id: 0 };
    early
        .send(json!({"type": "subscribe", "stream": "ops", "from": "live"}))
        .await;
    assert_eq!(early.refusal().await["code"], "bad_request");
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
