//! Polymarket: Gamma REST discovery and the CLOB `market` websocket channel. Every outcome token
//! is its own book, so the symbol is the asset id.

use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
    time::Duration,
};

use anyhow::bail;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, handshake::client::Request};

use super::{
    Event, Head, Level, Seq, Session, Side, Sym, Venue, fixed, from_millis, get_json, http,
};
use crate::collector::{Msg, Tx};

const WS: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const GAMMA: &str = "https://gamma-api.polymarket.com";
const BATCH: usize = 500;
/// New listings and resolutions also arrive live on the socket; this scan reconciles.
const RESCAN: Duration = Duration::from_secs(1800);

pub struct Polymarket {
    ws: Vec<String>,
    /// Follow listings announced on the socket (off when collecting a fixed symbol list).
    discover: bool,
}

impl Polymarket {
    pub fn new(ws: Vec<String>, discover: bool) -> Self {
        let ws = if ws.is_empty() { vec![WS.into()] } else { ws };
        Self { ws, discover }
    }
}

impl Venue for Polymarket {
    type Session = PolySession;

    fn request(&self, slot: usize) -> anyhow::Result<Request> {
        Ok(self.ws[slot % self.ws.len()]
            .as_str()
            .into_client_request()?)
    }

    /// Shard 0's sockets also carry `new_market` / `market_resolved`, giving instant discovery
    /// with redundancy, without every socket paying for the extra traffic.
    fn session(&self, shard: usize) -> PolySession {
        PolySession {
            started: false,
            lifecycle: self.discover && shard == 0,
        }
    }

    async fn discover(self: Arc<Self>, tx: Tx) -> anyhow::Result<()> {
        let http = http()?;
        loop {
            tx.send(Msg::ScanStart)?;
            let mut cursor = String::new();
            loop {
                let after = if cursor.is_empty() {
                    String::new()
                } else {
                    format!("&after_cursor={cursor}")
                };
                let page: GammaPage = get_json(
                    &http,
                    &format!("{GAMMA}/markets/keyset?closed=false&limit=100{after}"),
                )
                .await?;
                let live: Vec<Sym> = page
                    .markets
                    .iter()
                    .filter(|m| m.enable_order_book && m.accepting_orders == Some(true))
                    .flat_map(GammaMarket::assets)
                    .collect();
                tx.send(Msg::Live(live))?;
                match page.next_cursor {
                    Some(c) if !c.is_empty() && !page.markets.is_empty() => cursor = c,
                    _ => break,
                }
            }
            tx.send(Msg::ScanDone)?;
            tokio::time::sleep(RESCAN).await;
        }
    }
}

#[derive(Deserialize)]
struct GammaPage {
    #[serde(default)]
    markets: Vec<GammaMarket>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    #[serde(default)]
    enable_order_book: bool,
    accepting_orders: Option<bool>,
    clob_token_ids: Option<String>,
    #[serde(default)]
    position_ids: Vec<String>,
    version: Option<String>,
}

impl GammaMarket {
    /// Protocol V2 markets trade position ids; CTF markets trade the token ids.
    fn assets(&self) -> Vec<Sym> {
        if self.version.as_deref() == Some("v2") {
            return self
                .position_ids
                .iter()
                .map(|s| s.as_str().into())
                .collect();
        }
        let ids: Vec<String> = self
            .clob_token_ids
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        ids.into_iter().map(Into::into).collect()
    }
}

pub struct PolySession {
    started: bool,
    lifecycle: bool,
}

impl Session for PolySession {
    fn subscribe(&mut self, syms: &[Sym], out: &mut Vec<String>) {
        for chunk in syms.chunks(BATCH) {
            let msg = if self.started {
                json!({"assets_ids": chunk, "operation": "subscribe"})
            } else {
                self.started = true;
                json!({"assets_ids": chunk, "type": "market", "custom_feature_enabled": self.lifecycle})
            };
            out.push(msg.to_string());
        }
    }

    fn unsubscribe(&mut self, syms: &[Sym], out: &mut Vec<String>) {
        if self.started {
            out.extend(
                syms.chunks(BATCH)
                    .map(|c| json!({"assets_ids": c, "operation": "unsubscribe"}).to_string()),
            );
        }
    }

    /// Polymarket closes a socket whose first frame isn't a subscription ("invalid subscription
    /// payload"), and replicas subscribe late on purpose, so no keepalive before that.
    fn tick(&mut self, out: &mut Vec<String>) {
        if self.started {
            out.push("PING".into());
        }
    }

    fn decode(
        &mut self,
        frame: &str,
        events: &mut Vec<Event>,
        _out: &mut Vec<String>,
    ) -> anyhow::Result<()> {
        match frame.trim_start().as_bytes().first() {
            None => {}
            Some(b'[') => {
                for m in serde_json::from_str::<Vec<Message>>(frame)? {
                    m.decode(events)?;
                }
            }
            Some(b'{') => serde_json::from_str::<Message>(frame)?.decode(events)?,
            Some(_) => {} // PONG
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct Message<'a> {
    event_type: &'a str,
    #[serde(default)]
    asset_id: &'a str,
    #[serde(default)]
    hash: &'a str,
    #[serde(default)]
    timestamp: &'a str,
    #[serde(default, borrow)]
    bids: Vec<Quote<'a>>,
    #[serde(default, borrow)]
    asks: Vec<Quote<'a>>,
    #[serde(default, borrow)]
    price_changes: Vec<Change<'a>>,
    #[serde(default, borrow)]
    assets_ids: Vec<&'a str>,
    #[serde(default, borrow)]
    clob_token_ids: Vec<&'a str>,
}

#[derive(Deserialize)]
struct Quote<'a> {
    price: &'a str,
    size: &'a str,
}

#[derive(Deserialize)]
struct Change<'a> {
    asset_id: &'a str,
    price: &'a str,
    size: &'a str,
    side: &'a str,
    #[serde(default)]
    hash: &'a str,
}

impl Message<'_> {
    fn decode(self, events: &mut Vec<Event>) -> anyhow::Result<()> {
        let ts = stamp(self.timestamp);
        let quotes = |v: &[Quote]| {
            v.iter()
                .map(|q| Ok((fixed(q.price)?, fixed(q.size)?)))
                .collect::<anyhow::Result<Vec<_>>>()
        };
        match self.event_type {
            "book" => events.push(Event::Book {
                head: Head {
                    sym: self.asset_id.into(),
                    seq: Seq::Hash(h64(self.hash)),
                    ts,
                },
                bids: quotes(&self.bids)?,
                asks: quotes(&self.asks)?,
            }),
            // One message updates several books (an outcome and its complement), possibly
            // interleaved. All of an asset's changes form one transition, to the book named by
            // the asset's hash, so group per asset rather than per run.
            "price_change" => {
                let mut groups: Vec<(&str, &str, Vec<Level>)> = Vec::with_capacity(2);
                for c in &self.price_changes {
                    let side = match c.side {
                        "BUY" => Side::Buy,
                        "SELL" => Side::Sell,
                        other => bail!("unknown side {other:?}"),
                    };
                    let level = Level {
                        side,
                        px: fixed(c.price)?,
                        qty: fixed(c.size)?,
                    };
                    match groups.iter_mut().find(|g| g.0 == c.asset_id) {
                        Some(g) => {
                            g.1 = c.hash;
                            g.2.push(level);
                        }
                        None => groups.push((c.asset_id, c.hash, vec![level])),
                    }
                }
                for (asset, hash, mut levels) in groups {
                    // Only the transition's end state is real, so normalise it: last size per
                    // level wins (sizes are absolute), and removals go first, so a reader never
                    // sees a level added at a price whose opposite level is still standing.
                    let mut seen = std::collections::HashSet::new();
                    levels.reverse();
                    levels.retain(|l| seen.insert((l.side, l.px)));
                    levels.reverse();
                    levels.sort_by_key(|l| l.qty != 0);
                    events.push(Event::Delta {
                        head: Head {
                            sym: asset.into(),
                            seq: Seq::Hash(h64(hash)),
                            ts,
                        },
                        rel: false,
                        levels,
                    });
                }
            }
            "new_market" => events.push(Event::Live(self.assets().collect())),
            "market_resolved" => events.push(Event::Dead(self.assets().collect())),
            _ => {} // last_trade_price, tick_size_change, best_bid_ask, and anything new
        }
        Ok(())
    }

    fn assets(&self) -> impl Iterator<Item = Sym> + '_ {
        self.assets_ids
            .iter()
            .chain(&self.clob_token_ids)
            .map(|s| Sym::from(*s))
    }
}

fn h64(s: &str) -> u64 {
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

fn stamp(s: &str) -> Option<DateTime<Utc>> {
    s.parse()
        .ok()
        .and_then(from_millis)
        .or_else(|| DateTime::parse_from_rfc3339(s).ok().map(|t| t.to_utc()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_book_and_grouped_price_changes() {
        let mut s = PolySession {
            started: true,
            lifecycle: false,
        };
        let (mut ev, mut out) = (Vec::new(), Vec::new());
        s.decode(r#"[{"event_type":"book","market":"0x1","asset_id":"7","timestamp":"1782753357257","hash":"ab","bids":[{"price":"0.08","size":"33343.4"}],"asks":[{"price":"0.99","size":"218442.27"}]}]"#, &mut ev, &mut out).unwrap();
        s.decode(r#"{"event_type":"price_change","market":"0x1","price_changes":[{"asset_id":"7","price":"0.09","size":"5","side":"BUY","hash":"h1"},{"asset_id":"8","price":"0.92","size":"5","side":"SELL","hash":"h2"},{"asset_id":"7","price":"0.08","size":"3","side":"BUY","hash":"h1"},{"asset_id":"7","price":"0.08","size":"0","side":"BUY","hash":"h1"}],"timestamp":"1782753357300"}"#, &mut ev, &mut out).unwrap();
        s.decode("PONG", &mut ev, &mut out).unwrap();
        assert_eq!(ev.len(), 3);
        let Event::Book { head, bids, asks } = &ev[0] else {
            panic!()
        };
        assert_eq!(
            (bids.as_slice(), asks.as_slice()),
            (
                &[(80_000, 33_343_400_000)][..],
                &[(990_000, 218_442_270_000)][..]
            )
        );
        assert_eq!(head.ts.unwrap().timestamp_millis(), 1782753357257);
        let Event::Delta { head, levels, rel } = &ev[1] else {
            panic!()
        };
        assert_eq!((&*head.sym, levels.len(), *rel), ("7", 2, false));
        // Interleaved changes regrouped; 0.08's last size (removal) wins and goes first.
        assert_eq!(
            (levels[0].px, levels[0].qty, levels[1].px),
            (80_000, 0, 90_000)
        );
        let Event::Delta { head, levels, .. } = &ev[2] else {
            panic!()
        };
        assert_eq!(
            (&*head.sym, levels[0]),
            (
                "8",
                Level {
                    side: Side::Sell,
                    px: 920_000,
                    qty: 5_000_000
                }
            )
        );
    }
}
