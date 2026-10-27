//! One websocket: connect, subscribe, timestamp and decode frames, and rebuild on any failure.
//! The task keeps its own copy of its symbol set so a reconnect resubscribes everything without
//! asking the collector.

use std::{
    collections::HashSet,
    hash::{BuildHasher, RandomState},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    sync::mpsc::UnboundedReceiver,
    time::{Instant, sleep, timeout},
};
use tokio_tungstenite::{connect_async_with_config, tungstenite::Message};

use crate::collector::{ConnId, Msg, Stamp, Tx};
use crate::venue::{Session, Sym, Venue};

pub enum Cmd {
    Sub(Vec<Sym>),
    Unsub(Vec<Sym>),
    /// Unsubscribe then subscribe again: forces a fresh snapshot.
    Resub(Vec<Sym>),
    Close,
}

/// Kalshi pings every 10s and Polymarket answers our 10s PING, so 30s of silence is a dead socket.
const IDLE: Duration = Duration::from_secs(30);
const TICK: Duration = Duration::from_secs(5);

pub async fn run<V: Venue>(
    venue: Arc<V>,
    id: ConnId,
    shard: usize,
    slot: usize,
    mut cmds: UnboundedReceiver<Cmd>,
    tx: Tx,
) {
    let mut syms = HashSet::new();
    let mut backoff = Duration::from_millis(250);
    loop {
        let started = Instant::now();
        let why = match session(&*venue, id, shard, slot, &mut syms, &mut cmds, &tx).await {
            Ok(()) => return,
            Err(e) => e,
        };
        if tx.send(Msg::Down(id, format!("{why:#}"))).is_err() {
            return;
        }
        if started.elapsed() > Duration::from_secs(60) {
            backoff = Duration::from_millis(250);
        }
        // Jittered, so a venue-wide drop doesn't reconnect every socket in the same instant.
        // Commands keep flowing meanwhile, so the symbol set stays current and Close is instant.
        let jitter = RandomState::new().hash_one(id) as f64 / u64::MAX as f64;
        let wake = sleep(backoff.mul_f64(0.5 + jitter));
        tokio::pin!(wake);
        loop {
            tokio::select! {
                _ = &mut wake => break,
                cmd = cmds.recv() => {
                    if !cmd.is_some_and(|c| absorb(c, &mut syms)) {
                        return;
                    }
                }
            }
        }
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

fn absorb(cmd: Cmd, syms: &mut HashSet<Sym>) -> bool {
    match cmd {
        Cmd::Sub(v) => syms.extend(v),
        Cmd::Unsub(v) => v.iter().for_each(|s| {
            syms.remove(s);
        }),
        Cmd::Resub(_) => {}
        Cmd::Close => return false,
    }
    true
}

async fn session<V: Venue>(
    venue: &V,
    id: ConnId,
    shard: usize,
    slot: usize,
    syms: &mut HashSet<Sym>,
    cmds: &mut UnboundedReceiver<Cmd>,
    tx: &Tx,
) -> anyhow::Result<()> {
    let req = venue.request(slot)?;
    let (ws, _) = timeout(
        Duration::from_secs(15),
        connect_async_with_config(req, None, true),
    )
    .await
    .context("connect timed out")??;
    let (mut sink, mut stream) = ws.split();
    tx.send(Msg::Up(id))?;

    let mut s = venue.session(shard);
    let (mut events, mut out) = (Vec::new(), Vec::new());
    while let Ok(cmd) = cmds.try_recv() {
        if !absorb(cmd, syms) {
            return Ok(());
        }
    }
    s.subscribe(&syms.iter().cloned().collect::<Vec<_>>(), &mut out);
    let mut tick = tokio::time::interval(TICK);
    let mut last = Instant::now();
    loop {
        for f in out.drain(..) {
            sink.send(Message::text(f)).await?;
        }
        tokio::select! {
            frame = stream.next() => {
                // Timestamp first: everything after this line is our own latency, not the exchange's.
                let at = Stamp::now();
                last = Instant::now();
                match frame.context("stream ended")?? {
                    Message::Text(t) => {
                        s.decode(&t, &mut events, &mut out)?;
                        for ev in events.drain(..) {
                            tx.send(Msg::Event { conn: id, at, ev })?;
                        }
                    }
                    Message::Close(c) => anyhow::bail!("closed by venue: {c:?}"),
                    _ => {} // tungstenite answers pings itself
                }
            }
            cmd = cmds.recv() => match cmd {
                Some(Cmd::Sub(v)) => {
                    s.subscribe(&v, &mut out);
                    syms.extend(v);
                }
                Some(Cmd::Unsub(v)) => {
                    s.unsubscribe(&v, &mut out);
                    v.iter().for_each(|x| { syms.remove(x); });
                }
                Some(Cmd::Resub(v)) => {
                    s.unsubscribe(&v, &mut out);
                    s.subscribe(&v, &mut out);
                }
                Some(Cmd::Close) | None => {
                    // Close handshake, bounded so shutdown never hangs on a dead peer.
                    let _ = timeout(Duration::from_secs(1), async {
                        sink.send(Message::Close(None)).await.ok();
                        while let Some(Ok(m)) = stream.next().await {
                            if m.is_close() { break; }
                        }
                    }).await;
                    return Ok(());
                }
            },
            _ = tick.tick() => {
                anyhow::ensure!(last.elapsed() < IDLE, "no frames for {IDLE:?}");
                s.tick(&mut out);
            }
        }
    }
}
