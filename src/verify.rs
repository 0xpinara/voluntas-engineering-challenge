//! `verify`: replays collector files (in order, books carried across rotated files) exactly as
//! a backtest would, and checks them.
//!
//! Every update must apply to a book introduced by a snapshot, a removal must name a level that
//! exists, and
//! every periodic snapshot must equal the book rebuilt from the previous snapshot plus the updates
//! since. In a healthy run every snapshot after a symbol's first is "consistent"; "reseeds" are
//! the collector's announced discontinuities.

use std::{collections::HashMap, path::PathBuf};

use chrono::{DateTime, Utc};
use serde_json::json;

use crate::book::Book;
use crate::util::dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain};
use crate::util::{AlignedSbeReader, OrderbookSchema};
use crate::venue::Level;

#[derive(Default)]
struct Tally {
    snapshots: u64,
    updates: u64,
    symbols: u64,
    consistent: u64,
    reseeds: u64,
    orphan_updates: u64,
    noop_removals: u64,
    crossed_after_update: u64,
    time_went_backwards: u64,
}

pub fn run(paths: &[PathBuf], dump: bool) -> anyhow::Result<()> {
    let mut books: HashMap<String, (Book, i64)> = HashMap::new();
    let mut t = Tally::default();
    let (mut first, mut last): (Option<DateTime<Utc>>, Option<DateTime<Utc>>) = (None, None);
    for path in paths {
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
        while let Some(msg) = reader.read_msg()? {
            match msg {
                AnyMessage::Snapshot(d) => {
                    let s = SnapshotDomain::try_from_decoder(d)?;
                    t.snapshots += 1;
                    let book = Book::new(
                        s.bids.iter().map(|l| (l.price_level, l.quantity)).collect(),
                        s.asks.iter().map(|l| (l.price_level, l.quantity)).collect(),
                    );
                    if dump {
                        println!(
                            "S {} {} {} bids={:?} asks={:?}",
                            s.received_at, s.exchange_timestamp, s.symbol, book.bids, book.asks
                        );
                    }
                    first.get_or_insert(s.received_at);
                    last = Some(s.received_at);
                    let ts = s.received_at.timestamp_micros();
                    match books.get_mut(&s.symbol) {
                        Some((b, prev_ts)) => {
                            if *b == book {
                                t.consistent += 1
                            } else {
                                t.reseeds += 1
                            }
                            t.time_went_backwards += (ts < *prev_ts) as u64;
                            (*b, *prev_ts) = (book, ts);
                        }
                        None => {
                            t.symbols += 1;
                            books.insert(s.symbol, (book, ts));
                        }
                    }
                }
                AnyMessage::PriceLevelUpdate(d) => {
                    let u = PriceLevelUpdateDomain::try_from_decoder(d)?;
                    t.updates += 1;
                    if dump {
                        println!(
                            "U {} {} {} {} px={} qty={}",
                            u.backtest_timestamp,
                            u.exchange_timestamp,
                            u.symbol,
                            u.side,
                            u.price_level,
                            u.quantity
                        );
                    }
                    last = Some(u.backtest_timestamp);
                    let Some((book, prev_ts)) = books.get_mut(&u.symbol) else {
                        t.orphan_updates += 1;
                        continue;
                    };
                    let ts = u.backtest_timestamp.timestamp_micros();
                    t.time_went_backwards += (ts < *prev_ts) as u64;
                    *prev_ts = ts;
                    let (side, px, qty) = (u.side, u.price_level, u.quantity);
                    if qty == 0 && book.qty(side, px) == 0 {
                        t.noop_removals += 1;
                        continue;
                    }
                    book.apply(&Level { side, px, qty }, false)?;
                    t.crossed_after_update += book.crossed() as u64;
                }
                _ => anyhow::bail!("unknown message type"),
            }
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "files": paths, "from": first, "to": last,
            "snapshots": t.snapshots, "updates": t.updates, "symbols": t.symbols,
            "snapshots_consistent_with_replay": t.consistent, "reseeds": t.reseeds,
            "orphan_updates": t.orphan_updates, "noop_removals": t.noop_removals,
            "crossed_after_update": t.crossed_after_update, "time_went_backwards": t.time_went_backwards,
        }))?
    );
    let clean = t.orphan_updates == 0 && t.noop_removals == 0;
    anyhow::ensure!(clean, "file failed verification");
    Ok(())
}
