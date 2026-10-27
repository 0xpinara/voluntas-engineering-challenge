//! Output. A dedicated thread owns the challenge's [`AlignedSbeWriter`]; the collector hands it
//! cheap messages so encoding and disk I/O never stall book processing.

use std::{cell::Cell, thread::JoinHandle};

use anyhow::Context;
use chrono::{DateTime, Utc};
use crossbeam::channel::{Receiver, Sender, unbounded};

use crate::Side;
use crate::book::Book;
use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};
use crate::venue::Sym;

/// `AlignedSbeReader::open` only reads 64 MiB chunks, so that is what we must write.
pub const CHUNK: usize = 64 << 20;

pub enum Out {
    Snapshot {
        sym: Sym,
        recv: DateTime<Utc>,
        exch: DateTime<Utc>,
        book: Book,
    },
    /// The level's new absolute size; 0 removes it.
    Update {
        sym: Sym,
        recv: DateTime<Utc>,
        exch: DateTime<Utc>,
        side: Side,
        px: i64,
        qty: i64,
    },
    /// A full snapshot round starts here; the file may rotate first.
    Round(DateTime<Utc>),
}

pub struct Writer {
    tx: Option<Sender<Out>>,
    thread: Option<JoinHandle<anyhow::Result<()>>>,
    dead: Cell<bool>,
    /// Updates and snapshots written so far.
    written: Cell<(u64, u64)>,
}

impl Writer {
    /// `pattern` is a path, optionally with strftime fields (`data/kalshi-%Y%m%d-%H.sbe`). A new
    /// file starts whenever the rendered name changes, always right before a snapshot round, so
    /// every file is replayable on its own.
    pub fn spawn(pattern: String) -> anyhow::Result<Self> {
        let path = render(&pattern, Utc::now());
        let w = AlignedSbeWriter::create(&path, CHUNK).with_context(|| format!("open {path}"))?;
        let (tx, rx) = unbounded();
        let thread = std::thread::Builder::new()
            .name("writer".into())
            .spawn(move || write(pattern, path, w, rx))?;
        Ok(Self {
            tx: Some(tx),
            thread: Some(thread),
            dead: Cell::new(false),
            written: Cell::new((0, 0)),
        })
    }

    pub fn send(&self, o: Out) {
        let (u, s) = self.written.get();
        match o {
            Out::Update { .. } => self.written.set((u + 1, s)),
            Out::Snapshot { .. } => self.written.set((u, s + 1)),
            Out::Round(_) => {}
        }
        if self.tx.as_ref().is_none_or(|tx| tx.send(o).is_err()) {
            self.dead.set(true);
        }
    }

    pub fn dead(&self) -> bool {
        self.dead.get() || self.thread.as_ref().is_none_or(|t| t.is_finished())
    }

    pub fn written(&self) -> (u64, u64) {
        self.written.get()
    }

    pub fn backlog(&self) -> usize {
        self.tx.as_ref().map_or(0, |tx| tx.len())
    }

    /// Writes everything queued, flushes the final chunk and closes the file.
    pub fn close(&mut self) -> anyhow::Result<()> {
        drop(self.tx.take());
        match self.thread.take() {
            Some(t) => t.join().map_err(|_| anyhow::anyhow!("writer panicked"))?,
            None => Ok(()),
        }
    }
}

fn write(
    pattern: String,
    mut path: String,
    mut w: AlignedSbeWriter,
    rx: Receiver<Out>,
) -> anyhow::Result<()> {
    for o in rx {
        match o {
            Out::Round(t) => {
                let next = render(&pattern, t);
                if next != path {
                    w.flush()?;
                    w = AlignedSbeWriter::create(&next, CHUNK)
                        .with_context(|| format!("open {next}"))?;
                    crate::log!("rotated output {path} -> {next}");
                    path = next;
                }
            }
            Out::Snapshot {
                sym,
                recv,
                exch,
                book,
            } => w.write_msg(&SnapshotDomain {
                received_at: recv,
                exchange_timestamp: exch,
                // Best price first on both sides.
                bids: book
                    .bids
                    .iter()
                    .rev()
                    .map(|&(price_level, quantity)| SnapshotBidsEntryDomain {
                        price_level,
                        quantity,
                    })
                    .collect(),
                asks: book
                    .asks
                    .iter()
                    .map(|&(price_level, quantity)| SnapshotAsksEntryDomain {
                        price_level,
                        quantity,
                    })
                    .collect(),
                symbol: sym.to_string(),
            })?,
            Out::Update {
                sym,
                recv,
                exch,
                side,
                px,
                qty,
            } => w.write_msg(&PriceLevelUpdateDomain {
                backtest_timestamp: recv,
                exchange_timestamp: exch,
                price_level: px,
                quantity: qty,
                side,
                symbol: sym.to_string(),
            })?,
        }
    }
    // All senders are gone: write the last partial chunk; dropping `w` closes the file.
    w.flush()
}

fn render(pattern: &str, t: DateTime<Utc>) -> String {
    if pattern.contains('%') {
        t.format(pattern).to_string()
    } else {
        pattern.to_owned()
    }
}
