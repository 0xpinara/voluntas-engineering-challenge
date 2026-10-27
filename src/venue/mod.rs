//! Exchange adapters. A venue only knows how to discover symbols over REST and how to encode
//! commands and decode frames on its websocket. Books, arbitration, monitoring and output are
//! shared, so adding an exchange means implementing [`Venue`] and [`Session`].

pub mod kalshi;
pub mod polymarket;

use std::{future::Future, sync::Arc, time::Duration};

use anyhow::{Context, ensure};
use chrono::{DateTime, Utc};
use tokio_tungstenite::tungstenite::handshake::client::Request;

use crate::collector::Tx;

pub type Sym = Arc<str>;

/// Fixed point unit shared by prices and quantities: 1_000_000 = 1 USD or 1 contract/share.
pub const ONE: i64 = 1_000_000;

pub use crate::Side;

/// One price level change, in fixed point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Level {
    pub side: Side,
    pub px: i64,
    pub qty: i64,
}

/// How a venue names book states identically on every connection. This is what lets the
/// collector merge redundant sockets exactly rather than heuristically.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Seq {
    /// Per-book exchange clock. One exchange action may emit several deltas sharing a clock,
    /// always in the same order on every connection (Kalshi).
    Clock(u64),
    /// Hash of the book state the message produces (Polymarket).
    Hash(u64),
}

/// What every book message carries: the symbol, the exchange's name for the resulting state,
/// and the exchange's timestamp.
#[derive(Debug)]
pub struct Head {
    pub sym: Sym,
    pub seq: Seq,
    pub ts: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub enum Event {
    /// A full book, levels as (price, quantity) in any order.
    Book {
        head: Head,
        bids: Vec<(i64, i64)>,
        asks: Vec<(i64, i64)>,
    },
    /// Level changes forming one atomic transition. With `rel` the quantities are deltas
    /// (Kalshi), otherwise new absolute sizes (Polymarket).
    Delta {
        head: Head,
        rel: bool,
        levels: Vec<Level>,
    },
    /// Symbols the venue announced on the socket as newly listed / no longer trading.
    Live(Vec<Sym>),
    Dead(Vec<Sym>),
}

pub trait Venue: Send + Sync + Sized + 'static {
    type Session: Session;

    /// Handshake for replica `slot`. Venues alternate hosts across slots, so the replicas of a
    /// shard take different network paths and fail independently.
    fn request(&self, slot: usize) -> anyhow::Result<Request>;

    fn session(&self, shard: usize) -> Self::Session;

    /// Streams universe changes into `tx` until it fails; the caller restarts it.
    fn discover(self: Arc<Self>, tx: Tx) -> impl Future<Output = anyhow::Result<()>> + Send;
}

/// Per-connection protocol state.
pub trait Session: Send + 'static {
    fn subscribe(&mut self, syms: &[Sym], out: &mut Vec<String>);

    fn unsubscribe(&mut self, syms: &[Sym], out: &mut Vec<String>);

    /// Called every few seconds: keepalives and retries.
    fn tick(&mut self, _out: &mut Vec<String>) {}

    /// Decodes one text frame. An error means this stream can no longer be trusted (for example
    /// a sequence gap): the connection is rebuilt and its books are re-synced.
    fn decode(
        &mut self,
        frame: &str,
        events: &mut Vec<Event>,
        out: &mut Vec<String>,
    ) -> anyhow::Result<()>;
}

/// Parses a decimal string into exact fixed point (6 decimals). Prices never touch floats.
pub fn fixed(s: &str) -> anyhow::Result<i64> {
    let (neg, digits) = s.strip_prefix('-').map_or((false, s), |rest| (true, rest));
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    let frac = frac.trim_end_matches('0');
    ensure!(
        !(int.is_empty() && frac.is_empty())
            && frac.len() <= 6
            && int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()),
        "not an exact 6dp decimal: {s:?}"
    );
    let int: i64 = if int.is_empty() { 0 } else { int.parse()? };
    let frac: i64 = if frac.is_empty() {
        0
    } else {
        frac.parse::<i64>()? * 10i64.pow(6 - frac.len() as u32)
    };
    let v = int
        .checked_mul(ONE)
        .and_then(|v| v.checked_add(frac))
        .context("decimal overflows i64")?;
    Ok(if neg { -v } else { v })
}

pub fn from_millis(ms: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ms)
}

pub(crate) fn http() -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .gzip(true)
        .timeout(Duration::from_secs(30))
        .user_agent("voluntas-collector/0.1")
        .build()?)
}

/// Spacing between REST calls, process-wide: discovery runs a full scan and an incremental poll
/// concurrently, and must stay under the venue's (unauthenticated) rate limit rather than rely
/// on 429s.
const REST_SPACING: Duration = Duration::from_millis(250);

async fn rate_limit() {
    static NEXT: std::sync::Mutex<Option<tokio::time::Instant>> = std::sync::Mutex::new(None);
    let now = tokio::time::Instant::now();
    let at = {
        let mut next = NEXT.lock().unwrap();
        let at = next.map_or(now, |n| n.max(now));
        *next = Some(at + REST_SPACING);
        at
    };
    tokio::time::sleep_until(at).await;
}

/// GET with retries: discovery must survive transient REST failures and rate limits.
pub(crate) async fn get_json<T: serde::de::DeserializeOwned>(
    http: &reqwest::Client,
    url: &str,
) -> anyhow::Result<T> {
    let mut wait = Duration::from_millis(500);
    for attempt in 1.. {
        rate_limit().await;
        let res = async { http.get(url).send().await?.error_for_status()?.json().await };
        match res.await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < 6 => {
                crate::log!("GET {url}: {e}; retrying in {wait:?}");
                tokio::time::sleep(wait).await;
                wait *= 2;
            }
            Err(e) => return Err(e).with_context(|| format!("GET {url}")),
        }
    }
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::{ONE, fixed};

    #[test]
    fn fixed_point_is_exact() {
        assert_eq!(fixed("0.4500").unwrap(), 450_000);
        assert_eq!(fixed("0.08").unwrap(), 80_000);
        assert_eq!(fixed("-54.00").unwrap(), -54_000_000);
        assert_eq!(fixed("33343.4").unwrap(), 33_343_400_000);
        assert_eq!(fixed("219.217767").unwrap(), 219_217_767);
        assert_eq!(fixed("1").unwrap(), ONE);
        assert_eq!(fixed("0.123400000").unwrap(), 123_400);
        assert!(fixed("0.1234567").is_err());
        assert!(fixed("1e-6").is_err());
        assert!(fixed("").is_err());
    }
}
