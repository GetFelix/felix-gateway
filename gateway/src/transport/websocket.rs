//! [`BrowserConnection`] over an axum WebSocket.

use std::time::Instant;

use anyhow::Result;
use axum::extract::ws::{Message, WebSocket};

use super::{BrowserConnection, Incoming};

/// A browser on a WebSocket. Text frames carry protocol messages; the
/// round trip is measured with WebSocket pings, which browsers answer
/// without any page code.
pub struct WebSocketConnection {
    socket: WebSocket,
    next_ping: u64,
    outstanding: Option<(u64, Instant)>,
}

impl WebSocketConnection {
    /// Wrap an upgraded socket.
    pub fn new(socket: WebSocket) -> Self {
        Self {
            socket,
            next_ping: 0,
            outstanding: None,
        }
    }
}

impl BrowserConnection for WebSocketConnection {
    async fn recv(&mut self) -> Option<Incoming> {
        loop {
            match self.socket.recv().await? {
                Ok(Message::Text(text)) => return Some(Incoming::Message(text.to_string())),
                Ok(Message::Pong(body)) => {
                    if let Some((id, sent)) = self.outstanding
                        && body.as_ref() == id.to_be_bytes()
                    {
                        self.outstanding = None;
                        return Some(Incoming::RoundTrip(sent.elapsed()));
                    }
                }
                Ok(Message::Close(_)) | Err(_) => return None,
                Ok(Message::Binary(_) | Message::Ping(_)) => {}
            }
        }
    }

    async fn send(&mut self, message: String) -> Result<()> {
        self.socket.send(Message::Text(message.into())).await?;
        Ok(())
    }

    async fn ping(&mut self) -> Result<()> {
        // One probe at a time. A browser too slow to answer the last one
        // simply contributes fewer samples.
        if self.outstanding.is_some() {
            return Ok(());
        }
        let id = self.next_ping;
        self.next_ping += 1;
        self.socket
            .send(Message::Ping(id.to_be_bytes().to_vec().into()))
            .await?;
        self.outstanding = Some((id, Instant::now()));
        Ok(())
    }
}
