//! How redundant sockets are merged into one output book per symbol.
//!
//! Each symbol has one *output* book (exactly what a reader of our file reconstructs) and one
//! *replica* per socket subscribed to it. A replica's update is applied to the output only if the
//! replica was in exactly the output's state just before it, with states named by the exchange
//! (Kalshi `clock`, Polymarket `hash`). That makes this exact A/B line arbitration: whichever
//! socket delivers an update first wins, later copies are dropped (and cross-checked), and a socket
//! dying, stalling or lagging costs nothing while another one is in step. Only when no in-step
//! socket can continue is the output re-seeded from the freshest replica, and that discontinuity
//! is announced with a snapshot.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};

use super::{Collector, Conn, ConnId, Market, Stamp, Stats};
use crate::book::Book;
use crate::log;
use crate::output::{Out, Writer};
use crate::venue::{Head, Level, Seq, Sym, Venue};

/// A book state as named by the exchange: `n` messages of transition `id` applied, where `id`
/// is the Kalshi clock or the Polymarket book hash. Both venues split one transition over
/// several messages sharing that id (in the same order on every socket), so the count makes the
/// name exact. `done` once the transition is known complete (a snapshot at it, or a later id
/// arrived); `n == 0` means the count is unknown (the state came from a snapshot).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct State {
    id: u64,
    n: u32,
    done: bool,
}

impl State {
    fn snapshot(seq: Seq) -> Self {
        let (Seq::Clock(id) | Seq::Hash(id)) = seq;
        Self {
            id,
            n: 0,
            done: true,
        }
    }
}

/// The replica's (state before, state after) for an update, or `None` if the stream broke its
/// own rules: a clock went backwards, or a delta arrived for a clock its snapshot already holds.
fn advance(s: State, seq: Seq) -> Option<(State, State)> {
    let (id, ordered) = match seq {
        Seq::Clock(c) => (c, true),
        Seq::Hash(h) => (h, false),
    };
    match (id == s.id, s.done) {
        (true, false) => Some((s, State { n: s.n + 1, ..s })),
        // Polymarket can send the tail of a transition after a full book of it: redundant.
        (true, true) if !ordered => Some((s, s)),
        (false, _) if !ordered || id > s.id => Some((
            State { done: true, ..s },
            State {
                id,
                n: 1,
                done: false,
            },
        )),
        _ => None,
    }
}

/// Can an update whose pre-state is `prev` be applied to a book in state `cur`? An update that
/// opens a new transition needs `cur` to hold all of the previous one.
fn follows(cur: State, prev: State) -> bool {
    if prev.done {
        cur.id == prev.id && (cur.done || (prev.n > 0 && cur.n == prev.n))
    } else {
        cur == prev
    }
}

/// Monotone exchange-side progress: the clock, or exchange time (ms) for hash-named states.
fn progress(seq: Seq, ts: Option<DateTime<Utc>>, prev: u64) -> u64 {
    match seq {
        Seq::Clock(c) => c,
        Seq::Hash(_) => ts.map_or(prev, |t| t.timestamp_millis().max(0) as u64),
    }
}

impl Market {
    /// Receive times written for a symbol never go backwards. Two sockets' tasks can hand us
    /// frames in a different order than they read them, which could otherwise stamp an update a
    /// few microseconds before its predecessor; a backtest must never see that.
    pub(super) fn stamp(&mut self, recv: DateTime<Utc>) -> DateTime<Utc> {
        self.stamped = self.stamped.max(recv);
        self.stamped
    }

    fn touch(&mut self, state: State, progress: u64, ts: Option<DateTime<Utc>>, at: Instant) {
        let gap = at.saturating_duration_since(self.at).as_secs_f32();
        self.gap += (gap - self.gap) / 8.0;
        self.n = self.n.saturating_add(1);
        (self.state, self.progress, self.at) = (Some(state), progress.max(self.progress), at);
        self.ts = ts.or(self.ts);
    }
}

/// What a transition may touch besides its market, borrowed apart from the collector so the
/// market can be borrowed mutably alongside.
struct Fx<'a> {
    out: &'a Writer,
    conns: &'a mut HashMap<ConnId, Conn>,
    stats: &'a mut Stats,
    watch: &'a mut Vec<Sym>,
    resubs: &'a mut HashMap<ConnId, Vec<Sym>>,
}

impl<V: Venue> Collector<V> {
    fn parts(&mut self) -> (&mut HashMap<Sym, Market>, Fx<'_>) {
        let fx = Fx {
            out: &self.out,
            conns: &mut self.conns,
            stats: &mut self.stats,
            watch: &mut self.watch,
            resubs: &mut self.resubs,
        };
        (&mut self.markets, fx)
    }

    pub(super) fn on_delta(
        &mut self,
        conn: ConnId,
        t: Stamp,
        head: Head,
        rel: bool,
        levels: &[Level],
    ) {
        let (Head { sym, seq, ts }, recv, at) = (head, t.wall, t.mono);
        let (markets, mut fx) = self.parts();
        let Some(m) = markets.get_mut(&sym) else {
            return;
        };
        let Some(i) = m.reps.iter().position(|r| r.conn == conn) else {
            return;
        };
        let r = &mut m.reps[i];
        let Some(cur) = r.state else { return }; // not synced yet; its snapshot will cover this
        let Some((prev, next)) = advance(cur, seq) else {
            return fx.resync(&sym, m, i, at, "stream broke clock order");
        };
        if levels.iter().any(|l| r.book.apply(l, rel).is_err()) {
            return fx.resync(&sym, m, i, at, "delta did not apply");
        }
        (r.state, r.progress, r.ts) = (Some(next), progress(seq, ts, r.progress), ts.or(r.ts));
        let Some(o) = m.state else { return };
        if follows(o, prev) {
            // First socket to deliver this transition: it goes out.
            let (recv, exch) = (m.stamp(recv), ts.or(m.ts).unwrap_or(recv));
            if !fx.emit(&sym, &mut m.out, levels, rel, recv, exch) {
                return fx.seed(&sym, m, i, t, "output book rejected an in-step delta");
            }
            let (p, rts) = (m.reps[i].progress, m.reps[i].ts);
            m.touch(next, p, rts, at);
            if let Some(c) = fx.conns.get_mut(&conn) {
                c.wins += 1;
            }
        } else if o == next {
            // A slower socket delivering a transition already out: measure how late, and check
            // its book agrees with ours, since two independent streams must converge exactly.
            if let Some(c) = fx.conns.get_mut(&conn) {
                c.dups += 1;
                c.lag.add(at.saturating_duration_since(m.at));
            }
            if m.reps[i].book == m.out {
                m.strikes = 0;
            } else {
                // One of the two missed something. A book goes bad right after it is taken (a
                // venue without sequence numbers can serve a stale snapshot), so blame the side
                // that synced more recently; if the replica was already blamed once, or the
                // output is younger, rebuild the output from a fresh snapshot instead, emitting
                // nothing for this symbol until then.
                fx.stats.mismatches += 1;
                if m.seeded.is_none_or(|s| m.reps[i].synced > s) && m.strikes == 0 {
                    m.strikes = 1;
                } else {
                    log!(
                        "{sym}: output disagrees with conn {conn}; rebuilding it from a fresh snapshot"
                    );
                    (m.state, m.strikes) = (None, 0);
                }
                fx.resync(&sym, m, i, at, "cross-check mismatch");
            }
        } else if m.reps[i].progress > m.progress {
            fx.ahead(&sym, m, i, t);
        }
    }

    pub(super) fn on_book(&mut self, conn: ConnId, t: Stamp, head: Head, book: Book) {
        let (Head { sym, seq, ts }, recv, at) = (head, t.wall, t.mono);
        let (markets, mut fx) = self.parts();
        let Some(m) = markets.get_mut(&sym) else {
            return;
        };
        let Some(i) = m.reps.iter().position(|r| r.conn == conn) else {
            return;
        };
        let (state, o) = (State::snapshot(seq), m.state);
        let r = &mut m.reps[i];
        let was = r.state.replace(state);
        if was.is_none() {
            r.tries = 0;
            if let Some(c) = fx.conns.get_mut(&conn) {
                c.pending = c.pending.saturating_sub(1);
            }
        }
        (r.book, r.progress, r.ts) = (book, progress(seq, ts, r.progress), ts.or(r.ts));
        (r.mark, r.since, r.synced) = (r.progress.max(m.progress), at, at);
        // A snapshot requested after the output reached `floor` can't be older than it within
        // one clock epoch, so an older one means the exchange restarted its clocks (Kalshi
        // maintenance). Only the first such snapshot re-seeds; later ones meet the new output.
        let clock_reset = matches!(seq, Seq::Clock(_))
            && was.is_none()
            && r.progress < r.floor
            && m.progress >= r.floor;
        match o {
            None => fx.seed(&sym, m, i, t, "rebuilt after a mismatch"),
            // A socket at the output's transition sent a whole new book mid-stream (Polymarket
            // does after trades): an ordinary transition, emitted as the level diff. A full book
            // is a complete state, so unlike a delta it needs only the same transition, not the
            // same message count.
            Some(o) if was.is_some_and(|p| p.id == o.id) => {
                let mut diff = Vec::new();
                m.out.diff(&m.reps[i].book, &mut diff);
                diff.sort_by_key(|l| l.qty != 0); // removals first, as for deltas
                let (recv, exch) = (m.stamp(recv), ts.or(m.ts).unwrap_or(recv));
                if fx.emit(&sym, &mut m.out, &diff, false, recv, exch) {
                    let (p, rts) = (m.reps[i].progress, m.reps[i].ts);
                    m.touch(state, p, rts, at);
                } else {
                    fx.seed(&sym, m, i, t, "output book rejected an in-step book");
                }
            }
            // Same exchange state as the output: an independent audit of our book, for free.
            // (The output may be mid-transition for a few ms; only an idle one is wrong.)
            Some(o) if o.id == state.id => {
                if m.out == m.reps[i].book {
                    m.state = Some(State { done: true, ..o });
                } else if at.duration_since(m.at) > Duration::from_millis(100) {
                    fx.stats.mismatches += 1;
                    fx.seed(&sym, m, i, t, "fresh snapshot disagrees with output");
                }
            }
            Some(_) if clock_reset => fx.seed(&sym, m, i, t, "exchange clock reset"),
            Some(_) if m.reps[i].progress > m.progress => fx.ahead(&sym, m, i, t),
            Some(_) => {} // behind: it catches up through deltas
        }
    }

    /// Re-seeds outputs that have trailed an ahead replica for `takeover_after`.
    pub(super) fn sweep_ahead(&mut self, now: Instant) {
        let (wait, t) = (
            self.cfg.takeover_after,
            Stamp {
                wall: Utc::now(),
                mono: now,
            },
        );
        let (markets, mut fx) = self.parts();
        for sym in std::mem::take(fx.watch) {
            let Some(m) = markets.get_mut(&sym) else {
                continue;
            };
            let Some(since) = m.ahead else { continue };
            let synced = m.reps.iter().enumerate().filter(|(_, r)| r.state.is_some());
            let best = synced
                .max_by_key(|(_, r)| r.progress)
                .map(|(i, r)| (i, r.progress));
            match best {
                Some((i, p)) if p > m.progress && now.duration_since(since) >= wait => {
                    fx.seed(&sym, m, i, t, "in-step sockets fell behind")
                }
                Some((_, p)) if p > m.progress => fx.watch.push(sym),
                _ => m.ahead = None,
            }
        }
    }

    /// Once a second: snapshots that never came, replicas stuck behind, silent markets.
    pub(super) fn sweep(&mut self, now: Instant) {
        let (sync_timeout, stale_after) = (self.cfg.sync_timeout, self.cfg.stale_after);
        let (markets, mut fx) = self.parts();
        for (sym, m) in markets.iter_mut() {
            for i in 0..m.reps.len() {
                let r = &mut m.reps[i];
                if !fx.conns.get(&r.conn).is_some_and(|c| c.up) {
                    continue;
                }
                match r.state {
                    None if now.duration_since(r.since)
                        > sync_timeout * 2u32.pow(r.tries.min(6)) =>
                    {
                        r.tries += 1;
                        fx.resync(sym, m, i, now, "snapshot never arrived");
                    }
                    // Stuck: not at the output's state, and still short of where the output was
                    // when last checked. Backs off, because a venue can keep serving the same
                    // stale snapshot (Polymarket does, for quiet markets).
                    Some(rs)
                        if now.duration_since(r.since)
                            >= stale_after * 2u32.pow(r.stuck.min(7)) =>
                    {
                        let in_step = m.state.is_some_and(|o| o.id == rs.id);
                        if in_step {
                            r.stuck = 0;
                        }
                        if !in_step && r.progress < r.mark {
                            r.stuck += 1;
                            fx.resync(sym, m, i, now, "replica stuck behind");
                        } else {
                            (r.mark, r.since) = (m.progress, now);
                        }
                    }
                    _ => {}
                }
            }
            // Adaptive silence probe: an active market quiet for 20x its usual update gap on
            // *every* socket may be an exchange-side stall none of them can see. Resubscribe one
            // socket; its fresh snapshot is audited against the output.
            let silent = now.duration_since(m.at).as_secs_f32();
            if m.n >= 50
                && now >= m.probe
                && silent > (20.0 * m.gap).max(300.0)
                && !m.reps.is_empty()
            {
                m.probe = now + Duration::from_secs_f32(silent * 2.0);
                let i = m.n as usize % m.reps.len();
                fx.resync(sym, m, i, now, "silence probe");
            }
        }
    }
}

impl Fx<'_> {
    /// Applies an in-step transition to the output book and writes the levels it changed. Returns
    /// false if the output book rejected a change: it is corrupt, and the caller re-seeds it.
    fn emit(
        &self,
        sym: &Sym,
        book: &mut Book,
        levels: &[Level],
        rel: bool,
        recv: DateTime<Utc>,
        exch: DateTime<Utc>,
    ) -> bool {
        for l in levels {
            let before = book.qty(l.side, l.px);
            let Ok(qty) = book.apply(l, rel) else {
                return false;
            };
            // Skip no-ops, e.g. a venue re-sending an unchanged size.
            if qty != before {
                let (sym, side, px) = (sym.clone(), l.side, l.px);
                self.out.send(Out::Update {
                    sym,
                    recv,
                    exch,
                    side,
                    px,
                    qty,
                });
            }
        }
        true
    }

    /// Re-seeds the output book from replica `i` and announces the discontinuity with a snapshot.
    fn seed(&mut self, sym: &Sym, m: &mut Market, i: usize, t: Stamp, why: &str) {
        let (recv, at) = (m.stamp(t.wall), t.mono);
        let r = &m.reps[i];
        if m.seeded.is_some() {
            self.stats.takeovers += 1;
            log!("{sym}: re-seeding from conn {} ({why})", r.conn);
        }
        (m.out, m.state, m.progress, m.ahead) = (r.book.clone(), r.state, r.progress, None);
        (m.ts, m.at, m.seeded) = (r.ts.or(m.ts), at, Some(at));
        let (sym, exch, book) = (sym.clone(), m.ts.unwrap_or(recv), m.out.clone());
        self.out.send(Out::Snapshot {
            sym,
            recv,
            exch,
            book,
        });
    }

    /// Replica `i` is ahead of the output. If another replica is exactly in step it will most
    /// likely deliver the missing updates within milliseconds (keeping every intermediate state),
    /// so give it `takeover_after`; with nobody in step, re-seed now.
    fn ahead(&mut self, sym: &Sym, m: &mut Market, i: usize, t: Stamp) {
        if m.ahead.is_some() {
            return;
        }
        let mut reps = m.reps.iter().enumerate();
        if reps.any(|(j, r)| j != i && r.state.is_some() && r.state == m.state) {
            m.ahead = Some(t.mono);
            self.watch.push(sym.clone());
        } else {
            self.seed(sym, m, i, t, "no socket in step");
        }
    }

    /// Drops replica `i`'s state and queues a fresh snapshot request for its socket. Requests
    /// are sent in one batch per socket, so a socket whose stream stalled costs a few commands,
    /// not one per symbol (Kalshi rate-limits commands).
    fn resync(&mut self, sym: &Sym, m: &mut Market, i: usize, now: Instant, why: &'static str) {
        let floor = m.progress;
        let r = &mut m.reps[i];
        (r.state, r.floor, r.since) = (None, floor, now);
        self.stats.resyncs += 1;
        *self.stats.resync_why.entry(why).or_default() += 1;
        self.resubs.entry(r.conn).or_default().push(sym.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn st(id: u64, n: u32, done: bool) -> State {
        State { id, n, done }
    }

    #[test]
    fn clock_states_line_up_across_sockets() {
        // Replica from a snapshot at 10, output from a snapshot at 10: a delta at 12 applies.
        let (prev, next) = advance(st(10, 0, true), Seq::Clock(12)).unwrap();
        assert!(follows(st(10, 0, true), prev));
        assert_eq!(next, st(12, 1, false));
        // Second delta at the same clock must follow the first exactly.
        let (prev2, next2) = advance(next, Seq::Clock(12)).unwrap();
        assert!(follows(st(12, 1, false), prev2) && !follows(st(12, 2, false), prev2));
        // Opening clock 15: the output must hold all deltas of 12 (counts agree, or a snapshot).
        let (prev3, _) = advance(next2, Seq::Clock(15)).unwrap();
        assert!(follows(st(12, 2, false), prev3));
        assert!(!follows(st(12, 1, false), prev3));
        assert!(follows(st(12, 0, true), prev3));
        // A replica that only has a snapshot at 12 can't vouch for an output that counted deltas.
        let (prev4, _) = advance(st(12, 0, true), Seq::Clock(15)).unwrap();
        assert!(!follows(st(12, 2, false), prev4));
        // A delta at an already complete clock, or going backwards, breaks the stream.
        assert!(advance(st(12, 0, true), Seq::Clock(12)).is_none());
        assert!(advance(st(12, 3, false), Seq::Clock(11)).is_none());
        // Mid-action deltas are duplicates for an output holding the complete clock.
        assert!(!follows(st(12, 0, true), st(12, 1, false)));
    }

    #[test]
    fn hash_states_count_messages_of_a_transition() {
        // Book at hash 1; a transition to hash 2 arrives in two messages, both carrying hash 2.
        let (p1, n1) = advance(State::snapshot(Seq::Hash(1)), Seq::Hash(2)).unwrap();
        let (p2, n2) = advance(n1, Seq::Hash(2)).unwrap();
        assert!(follows(st(1, 0, true), p1) && follows(n1, p2));
        assert_eq!(n2, st(2, 2, false));
        // A socket that has only the first part is not yet at the output's state.
        assert_ne!(n1, n2);
        // Hashes are unordered: any new hash opens a transition; a tail after a full book is a no-op.
        assert!(advance(st(9, 3, false), Seq::Hash(4)).is_some());
        assert_eq!(
            advance(st(9, 0, true), Seq::Hash(9)).unwrap().1,
            st(9, 0, true)
        );
    }
}
