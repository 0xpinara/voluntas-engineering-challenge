//! The collector: owns every book, supervises the sockets and writes the output. How replicas
//! are merged into the output book is in [`books`].

mod books;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use crossbeam::channel::{Receiver, RecvTimeoutError, Sender};
use serde_json::json;
use tokio::{runtime::Handle, sync::mpsc, task::JoinHandle};

use crate::book::Book;
use crate::conn::{self, Cmd};
use crate::log;
use crate::output::{Out, Writer};
use crate::venue::{Event, Sym, Venue};
use books::State;

pub type ConnId = u32;
pub type Tx = Sender<Msg>;

/// When a frame reached us: wall clock for the output, monotonic for everything we measure.
#[derive(Clone, Copy)]
pub struct Stamp {
    pub wall: DateTime<Utc>,
    pub mono: Instant,
}

impl Stamp {
    pub fn now() -> Self {
        Self {
            wall: Utc::now(),
            mono: Instant::now(),
        }
    }
}

pub enum Msg {
    Event {
        conn: ConnId,
        at: Stamp,
        ev: Event,
    },
    Up(ConnId),
    Down(ConnId, String),
    Live(Vec<Sym>),
    Dead(Vec<Sym>),
    /// Discovery starts / finishes a full scan; symbols it didn't announce in between are gone.
    ScanStart,
    ScanDone,
    Shutdown,
}

#[derive(Clone, Debug)]
pub struct Config {
    /// Sockets per shard, each a full replica of the shard's books.
    pub replicas: usize,
    pub shard_size: usize,
    pub snapshot_every: Duration,
    /// How long an in-step socket may trail one that is ahead before we re-seed from the latter.
    pub takeover_after: Duration,
    /// A replica that hasn't reached where the output was this long ago is resubscribed.
    pub stale_after: Duration,
    /// Re-request a symbol whose snapshot hasn't arrived after this long (doubling per retry).
    pub sync_timeout: Duration,
    /// Rebuild every socket this often (jittered), make-before-break.
    pub recycle_every: Duration,
    pub stats_every: Duration,
}

struct Replica {
    conn: ConnId,
    /// `None` until a snapshot syncs it.
    state: Option<State>,
    book: Book,
    progress: u64,
    ts: Option<DateTime<Utc>>,
    /// The output's progress when this replica (re)subscribed: a fresh snapshot can't be older
    /// unless the exchange reset its clocks (Kalshi maintenance).
    floor: u64,
    /// The output's progress at `since`; a synced replica still below it `stale_after` later is stuck.
    mark: u64,
    since: Instant,
    /// When its book last came from a snapshot.
    synced: Instant,
    /// Consecutive retries: snapshots that never came, and resyncs that left it still stuck.
    tries: u32,
    stuck: u32,
}

impl Replica {
    fn new(conn: ConnId, floor: u64, now: Instant) -> Self {
        Self {
            conn,
            state: None,
            book: Book::default(),
            progress: 0,
            ts: None,
            floor,
            mark: 0,
            since: now,
            synced: now,
            tries: 0,
            stuck: 0,
        }
    }
}

struct Market {
    out: Book,
    state: Option<State>,
    progress: u64,
    ts: Option<DateTime<Utc>>,
    /// The last receive time written for this symbol; see [`Market::stamp`].
    stamped: DateTime<Utc>,
    /// When the output book last changed.
    at: Instant,
    reps: Vec<Replica>,
    shard: usize,
    seen: Instant,
    /// Since when a replica has been ahead of the output.
    ahead: Option<Instant>,
    /// When the output was last seeded from a snapshot (`None`: never), and how many cross-check
    /// mismatches in a row were blamed on replicas.
    seeded: Option<Instant>,
    strikes: u8,
    /// EWMA of the seconds between output updates, and the update count.
    gap: f32,
    n: u32,
    probe: Instant,
}

struct Shard {
    syms: HashSet<Sym>,
    conns: Vec<ConnId>,
}

struct Conn {
    shard: usize,
    slot: usize,
    tx: mpsc::UnboundedSender<Cmd>,
    task: JoinHandle<()>,
    up: bool,
    /// Symbols not yet synced on this socket.
    pending: usize,
    born: Instant,
    recycle_at: Instant,
    /// The socket this one is warming up to replace.
    replaces: Option<ConnId>,
    wins: u64,
    dups: u64,
    lag: Hist,
}

/// Log2 histogram of lags in microseconds.
#[derive(Clone, Default)]
struct Hist([u32; 25]);

impl Hist {
    fn add(&mut self, d: Duration) {
        let us = (d.as_micros() as u64).max(1);
        self.0[(63 - us.leading_zeros() as usize).min(24)] += 1;
    }
    fn merge(&mut self, o: &Hist) {
        self.0.iter_mut().zip(o.0).for_each(|(a, b)| *a += b);
    }
    fn count(&self) -> u32 {
        self.0.iter().sum()
    }
    /// Approximate quantile in milliseconds (geometric bucket midpoint).
    fn q(&self, q: f64) -> Option<f64> {
        let n = self.count();
        let mut acc = 0;
        for (k, c) in self.0.iter().enumerate() {
            acc += c;
            if n > 0 && acc as f64 >= q * n as f64 {
                return Some(
                    ((1u64 << k) as f64 * std::f64::consts::SQRT_2 / 1e3 * 100.0).round() / 100.0,
                );
            }
        }
        None
    }
    fn decay(&mut self) {
        self.0.iter_mut().for_each(|c| *c /= 2);
    }
}

#[derive(Default, Clone)]
struct Stats {
    events: u64,
    updates: u64,
    snapshots: u64,
    takeovers: u64,
    resyncs: u64,
    mismatches: u64,
    downs: u64,
    recycles: u64,
    resync_why: std::collections::BTreeMap<&'static str, u64>,
}

pub struct Collector<V: Venue> {
    venue: Arc<V>,
    rt: Handle,
    tx: Tx,
    cfg: Config,
    out: Writer,
    markets: HashMap<Sym, Market>,
    shards: Vec<Shard>,
    conns: HashMap<ConnId, Conn>,
    next_id: ConnId,
    scan: Option<Instant>,
    watch: Vec<Sym>,
    stats: Stats,
    last: Stats,
    last_at: Instant,
    closing: bool,
    /// Subscriptions held back to stagger replicas, and resubscriptions waiting to go out.
    later: Vec<(Instant, ConnId, Vec<Sym>)>,
    resubs: HashMap<ConnId, Vec<Sym>>,
    /// How far behind real time we are processing (now - receive time of the latest event), and
    /// the worst since the last report.
    lag: Duration,
    lag_max: Duration,
}

impl<V: Venue> Collector<V> {
    pub fn new(venue: Arc<V>, rt: Handle, tx: Tx, cfg: Config, out: Writer) -> Self {
        Self {
            venue,
            rt,
            tx,
            cfg,
            out,
            markets: HashMap::new(),
            shards: Vec::new(),
            conns: HashMap::new(),
            next_id: 0,
            scan: None,
            watch: Vec::new(),
            stats: Stats::default(),
            last: Stats::default(),
            last_at: Instant::now(),
            closing: false,
            later: Vec::new(),
            resubs: HashMap::new(),
            lag: Duration::ZERO,
            lag_max: Duration::ZERO,
        }
    }

    /// Runs until `Msg::Shutdown`: then closes every socket, drains, and closes the output file.
    pub fn run(mut self, rx: Receiver<Msg>) -> anyhow::Result<()> {
        let t0 = Instant::now();
        let (mut fast, mut slow, mut report) = (t0, t0, t0 + self.cfg.stats_every);
        let mut round = next_round(self.cfg.snapshot_every);
        let mut deadline = None;
        loop {
            let now = Instant::now();
            if self.out.dead() {
                self.out.close()?; // surfaces the writer's own error (disk full, ...)
                anyhow::bail!("output writer stopped");
            }
            if let Some(d) = deadline {
                if now >= d || self.conns.values().all(|c| c.task.is_finished()) {
                    break;
                }
            } else {
                // Every timeout below compares receive times with now. While we are behind (a burst
                // of snapshots after a mass reconnect), the "missing" data is sitting in our own
                // queue, and acting on it would resubscribe the world and feed the burst.
                let behind = self.lag > Duration::from_millis(250);
                if now >= fast && !behind {
                    self.flush_later(now);
                    self.sweep_ahead(now);
                    fast = now + Duration::from_millis(50);
                }
                if now >= slow && !behind {
                    self.sweep(now);
                    self.supervise(now);
                    slow = now + Duration::from_secs(1);
                }
                if now >= round {
                    self.round();
                    round = next_round(self.cfg.snapshot_every);
                }
                if now >= report {
                    self.report();
                    report = now + self.cfg.stats_every;
                }
            }
            for (id, v) in self.resubs.drain() {
                if let Some(c) = self.conns.get(&id) {
                    let _ = c.tx.send(Cmd::Resub(v));
                }
            }
            match rx.recv_deadline(
                fast.min(slow)
                    .min(round)
                    .min(report)
                    .max(now + Duration::from_millis(1)),
            ) {
                Ok(Msg::Shutdown) if deadline.is_none() => {
                    log!("shutting down: closing {} sockets", self.conns.len());
                    self.closing = true;
                    self.conns
                        .values()
                        .for_each(|c| drop(c.tx.send(Cmd::Close)));
                    deadline = Some(now + Duration::from_secs(3));
                }
                Ok(msg) => self.handle(msg),
                Err(RecvTimeoutError::Timeout) => self.lag = Duration::ZERO, // queue drained
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        self.report();
        self.out.close()?;
        log!("output closed");
        Ok(())
    }

    fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::Event { conn, at, ev } => {
                self.stats.events += 1;
                self.lag = at.mono.elapsed();
                self.lag_max = self.lag_max.max(self.lag);
                match ev {
                    Event::Book { head, bids, asks } => {
                        self.on_book(conn, at, head, Book::new(bids, asks))
                    }
                    Event::Delta { head, rel, levels } => {
                        self.on_delta(conn, at, head, rel, &levels)
                    }
                    Event::Live(v) => self.live(v),
                    Event::Dead(v) => self.dead(v),
                }
            }
            Msg::Up(id) => {
                if let Some(c) = self.conns.get_mut(&id) {
                    c.up = true;
                }
            }
            Msg::Down(id, why) => self.down(id, &why),
            Msg::Live(v) => self.live(v),
            Msg::Dead(v) => self.dead(v),
            Msg::ScanStart => self.scan = Some(Instant::now()),
            Msg::ScanDone => self.scan_done(),
            Msg::Shutdown => {}
        }
    }

    // ---------------------------------------------------------------- universe

    fn live(&mut self, syms: Vec<Sym>) {
        let now = Instant::now();
        let mut subs: HashMap<ConnId, Vec<Sym>> = HashMap::new();
        for sym in syms {
            if let Some(m) = self.markets.get_mut(&sym) {
                m.seen = now;
                continue;
            }
            let shard = self.shard_with_room(now);
            self.shards[shard].syms.insert(sym.clone());
            let mut reps = Vec::with_capacity(self.cfg.replicas);
            for &id in &self.shards[shard].conns {
                reps.push(Replica::new(id, 0, now));
                self.conns.get_mut(&id).unwrap().pending += 1;
                subs.entry(id).or_default().push(sym.clone());
            }
            let m = Market {
                out: Book::default(),
                state: None,
                progress: 0,
                ts: None,
                at: now,
                reps,
                shard,
                seen: now,
                ahead: None,
                seeded: None,
                strikes: 0,
                gap: 600.0,
                n: 0,
                probe: now,
                stamped: DateTime::<Utc>::MIN_UTC,
            };
            self.markets.insert(sym, m);
        }
        // Replica k subscribes k seconds after replica 0. Polymarket can drop the transition in
        // flight while a subscription starts; replicas subscribing in the same instant could drop
        // the same one, agree with each other, and the cross-check would never see it.
        for (id, v) in subs {
            let delay = Duration::from_secs(self.conns[&id].slot as u64);
            self.later.push((now + delay, id, v));
        }
        self.flush_later(now);
    }

    fn flush_later(&mut self, now: Instant) {
        let (due, later) = std::mem::take(&mut self.later)
            .into_iter()
            .partition(|l| l.0 <= now);
        self.later = later;
        for (_, id, v) in due {
            if let Some(c) = self.conns.get(&id) {
                let _ = c.tx.send(Cmd::Sub(v));
            }
        }
    }

    fn dead(&mut self, syms: Vec<Sym>) {
        let mut unsubs: HashMap<ConnId, Vec<Sym>> = HashMap::new();
        for sym in syms {
            let Some(m) = self.markets.remove(&sym) else {
                continue;
            };
            self.shards[m.shard].syms.remove(&sym);
            self.later
                .iter_mut()
                .for_each(|l| l.2.retain(|s| *s != sym));
            for r in m.reps {
                if let Some(c) = self.conns.get_mut(&r.conn) {
                    if r.state.is_none() {
                        c.pending = c.pending.saturating_sub(1);
                    }
                    unsubs.entry(r.conn).or_default().push(sym.clone());
                }
            }
        }
        for (id, v) in unsubs {
            let _ = self.conns[&id].tx.send(Cmd::Unsub(v));
        }
    }

    fn scan_done(&mut self) {
        let Some(start) = self.scan.take() else {
            return;
        };
        let gone: Vec<Sym> = self
            .markets
            .iter()
            .filter(|(_, m)| m.seen < start)
            .map(|(s, _)| s.clone())
            .collect();
        // A truncated scan (REST glitch) must not unsubscribe half the universe.
        if gone.len() * 2 > self.markets.len() {
            log!(
                "scan would drop {}/{} symbols; ignoring it",
                gone.len(),
                self.markets.len()
            );
        } else if !gone.is_empty() {
            log!("scan: {} symbols no longer listed", gone.len());
            self.dead(gone);
        }
    }

    fn shard_with_room(&mut self, now: Instant) -> usize {
        if let Some(i) = self
            .shards
            .iter()
            .position(|s| s.syms.len() < self.cfg.shard_size)
        {
            return i;
        }
        let shard = self.shards.len();
        self.shards.push(Shard {
            syms: HashSet::new(),
            conns: Vec::new(),
        });
        for slot in 0..self.cfg.replicas {
            // First recycle spread uniformly over the period, so sockets don't recycle in lockstep.
            let phase = (shard * self.cfg.replicas + slot) as f64 * 0.618_034 % 1.0;
            self.spawn(
                shard,
                slot,
                None,
                now + self.cfg.recycle_every.mul_f64(0.5 + phase),
            );
        }
        shard
    }

    // ---------------------------------------------------------------- connections

    fn spawn(
        &mut self,
        shard: usize,
        slot: usize,
        replaces: Option<ConnId>,
        recycle_at: Instant,
    ) -> ConnId {
        let (id, now) = (self.next_id, Instant::now());
        self.next_id += 1;
        let (tx, rx) = mpsc::unbounded_channel();
        let syms: Vec<Sym> = self.shards[shard].syms.iter().cloned().collect();
        for sym in &syms {
            let m = self.markets.get_mut(sym).unwrap();
            let floor = m.progress;
            m.reps.push(Replica::new(id, floor, now));
        }
        let pending = syms.len();
        if !syms.is_empty() {
            let _ = tx.send(Cmd::Sub(syms));
        }
        let task = self.rt.spawn(conn::run(
            self.venue.clone(),
            id,
            shard,
            slot,
            rx,
            self.tx.clone(),
        ));
        self.conns.insert(
            id,
            Conn {
                shard,
                slot,
                tx,
                task,
                up: false,
                pending,
                born: now,
                recycle_at,
                replaces,
                wins: 0,
                dups: 0,
                lag: Hist::default(),
            },
        );
        self.shards[shard].conns.push(id);
        id
    }

    fn retire(&mut self, id: ConnId) {
        let Some(c) = self.conns.remove(&id) else {
            return;
        };
        let _ = c.tx.send(Cmd::Close);
        let shard = &mut self.shards[c.shard];
        shard.conns.retain(|&x| x != id);
        for sym in &shard.syms {
            if let Some(m) = self.markets.get_mut(sym) {
                m.reps.retain(|r| r.conn != id);
            }
        }
    }

    fn down(&mut self, id: ConnId, why: &str) {
        let Some(c) = self.conns.get_mut(&id) else {
            return;
        };
        self.stats.downs += 1;
        c.up = false;
        log!("conn {id} (shard {} slot {}) down: {why}", c.shard, c.slot);
        let now = Instant::now();
        for sym in &self.shards[c.shard].syms {
            let Some(m) = self.markets.get_mut(sym) else {
                continue;
            };
            let floor = m.progress;
            if let Some(r) = m.reps.iter_mut().find(|r| r.conn == id) {
                if r.state.take().is_some() {
                    c.pending += 1;
                }
                (r.floor, r.since) = (floor, now);
            }
        }
    }

    /// Make-before-break recycling: a replacement socket syncs the whole shard before the old
    /// one is closed, so redundancy never drops.
    fn supervise(&mut self, now: Instant) {
        if self.closing {
            return;
        }
        let (mut swap, mut abort, mut start) = (Vec::new(), Vec::new(), Vec::new());
        for (&id, c) in &self.conns {
            if let Some(old) = c.replaces {
                // A few stragglers can finish syncing later; a replacement that failed outright
                // is dropped instead, so a healthy socket is never traded for a broken one.
                let timed_out = now.duration_since(c.born) > self.cfg.sync_timeout * 2;
                if c.pending == 0
                    || (timed_out && c.up && c.pending * 100 <= self.shards[c.shard].syms.len())
                {
                    swap.push((id, old));
                } else if timed_out {
                    abort.push((id, old));
                }
                continue;
            }
            let busy = self.shards[c.shard].conns.iter().any(|x| {
                self.conns
                    .get(x)
                    .is_some_and(|o| o.replaces.is_some() || o.pending > 0 || !o.up)
            });
            if now >= c.recycle_at && !busy {
                start.push((id, c.shard, c.slot));
            }
        }
        for (new, old) in swap {
            self.retire(old);
            if let Some(c) = self.conns.get_mut(&new) {
                c.replaces = None;
            }
        }
        for (new, old) in abort {
            log!("replacement conn {new} for {old} failed to sync; keeping {old}");
            self.retire(new);
            if let Some(c) = self.conns.get_mut(&old) {
                c.recycle_at = now + Duration::from_secs(300);
            }
        }
        let mut started = HashSet::new();
        for (old, shard, slot) in start {
            if started.insert(shard) {
                self.stats.recycles += 1;
                let next = now
                    + self
                        .cfg
                        .recycle_every
                        .mul_f64(0.8 + 0.4 * (old as f64 * 0.618_034 % 1.0));
                self.spawn(shard, slot, Some(old), next);
            }
        }
    }

    // ---------------------------------------------------------------- output & stats

    fn round(&mut self) {
        let now = Utc::now();
        self.out.send(Out::Round(now));
        for (sym, m) in self.markets.iter_mut() {
            if m.state.is_some() {
                self.out.send(Out::Snapshot {
                    sym: sym.clone(),
                    recv: m.stamp(now),
                    exch: m.ts.unwrap_or(now),
                    book: m.out.clone(),
                });
            }
        }
        self.conns.values_mut().for_each(|c| c.lag.decay());
    }

    fn report(&mut self) {
        (self.stats.updates, self.stats.snapshots) = self.out.written();
        let (s, l, secs) = (
            &self.stats,
            &self.last,
            self.last_at.elapsed().as_secs_f64().max(1e-9),
        );
        let mut slots: Vec<(u64, u64, Hist)> = vec![(0, 0, Hist::default()); self.cfg.replicas];
        for c in self.conns.values() {
            let e = &mut slots[c.slot.min(self.cfg.replicas - 1)];
            (e.0, e.1) = (e.0 + c.wins, e.1 + c.dups);
            e.2.merge(&c.lag);
        }
        let total: u64 = slots.iter().map(|e| e.0).sum::<u64>().max(1);
        let race: Vec<_> = slots.iter().enumerate().map(|(i, (w, d, h))| json!({
            "slot": i, "win_share": (*w as f64 / total as f64 * 1e3).round() / 1e3, "dups": d,
            "lag_ms_p50": h.q(0.5), "lag_ms_p90": h.q(0.9), "lag_ms_p99": h.q(0.99),
        })).collect();
        let synced = self.markets.values().filter(|m| m.state.is_some()).count();
        let pending: usize = self.conns.values().map(|c| c.pending).sum();
        eprintln!(
            "{}",
            json!({
                "t": Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                "symbols": self.markets.len(), "synced": synced, "pending_subs": pending,
                "conns": self.conns.len(), "up": self.conns.values().filter(|c| c.up).count(),
                "events_per_s": ((s.events - l.events) as f64 / secs).round(),
                "updates_per_s": ((s.updates - l.updates) as f64 / secs).round(),
                "snapshots": s.snapshots, "takeovers": s.takeovers, "resyncs": s.resyncs,
                "mismatches": s.mismatches, "downs": s.downs, "recycles": s.recycles,
                "core_lag_ms": self.lag_max.as_millis(), "writer_backlog": self.out.backlog(), "resync_why": s.resync_why, "race": race,
            })
        );
        (self.last, self.last_at, self.lag_max) =
            (self.stats.clone(), Instant::now(), Duration::ZERO);
    }
}

/// Next wall-clock multiple of `every`, so snapshot rounds land on the minute.
fn next_round(every: Duration) -> Instant {
    let ms = every.as_millis().max(1) as i64;
    let now = Utc::now().timestamp_millis();
    Instant::now() + Duration::from_millis((ms - now.rem_euclid(ms)) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hist_quantiles() {
        let mut h = Hist::default();
        (0..99).for_each(|_| h.add(Duration::from_micros(1000)));
        h.add(Duration::from_millis(100));
        // Log2 buckets: estimates are within a factor of sqrt(2) of the truth.
        assert!((0.7..1.42).contains(&h.q(0.5).unwrap()));
        assert!(h.q(1.0).unwrap() > 90.0);
    }
}
