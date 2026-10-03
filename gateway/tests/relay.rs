//! The gateway against a real Felix broker. Ignored by default because they
//! need the development stack running; see the README, or run
//! `cargo test -- --include-ignored` with the `CANVAS_*` variables set.

use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use felix_canvas_gateway::{Config, Gateway};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const WAIT: Duration = Duration::from_secs(10);

async fn start_gateway() -> (Gateway, SocketAddr) {
    let config = Config::from_env().expect("CANVAS_* environment for the dev stack");
    let gateway = Gateway::connect(&config).await.expect("connect to Felix");
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

struct Browser {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    next_id: u64,
}

impl Browser {
    async fn open(gateway: SocketAddr) -> Self {
        let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{gateway}/ws"))
            .await
            .expect("open WebSocket");
        let mut browser = Self { socket, next_id: 0 };
        let hello = browser.recv().await;
        assert_eq!(hello["type"], "hello", "the first message names the room");
        assert!(hello["room"].is_string(), "{hello}");
        browser
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

    // The first ping goes out when the session starts, and tungstenite
    // answers it while the socket is being read.
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
        tokio::time::sleep(Duration::from_millis(50)).await;
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
