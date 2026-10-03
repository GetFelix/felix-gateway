//! The browser side of a session, behind one small trait so a WebTransport
//! listener can sit beside the WebSocket one later.

pub mod websocket;

use std::time::Duration;

use anyhow::Result;

/// What a browser connection yields.
#[derive(Debug)]
pub enum Incoming {
    /// One protocol message, as JSON text.
    Message(String),
    /// The browser answered a [`BrowserConnection::ping`] after this long.
    RoundTrip(Duration),
}

/// One browser connection carrying protocol messages.
///
/// `recv` must be cancel-safe: the relay polls it alongside its outbound
/// queues and drops it whenever one of those is ready first.
pub trait BrowserConnection: Send {
    /// The next thing the browser sent, or `None` once it has gone.
    fn recv(&mut self) -> impl Future<Output = Option<Incoming>> + Send;

    /// Send one protocol message.
    fn send(&mut self, message: String) -> impl Future<Output = Result<()>> + Send;

    /// Start a round-trip measurement, answered later as [`Incoming::RoundTrip`].
    fn ping(&mut self) -> impl Future<Output = Result<()>> + Send;
}
