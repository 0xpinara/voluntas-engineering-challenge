//! End to end against a mock Kalshi exchange speaking the real protocol (subscribe, sid, seq,
//! clock, update_subscription), with faults injected on the sockets:
//!   socket 0: silently stops sending one market's deltas (only a resubscribe revives it)
//!   socket 1: delivers everything late, in 40ms batches (a lagging replica)
//!   socket 2: drops one message, leaving a seq gap
//!   socket 3: is cut abruptly mid-stream
//! The output, replayed like a backtest, must reproduce the exchange's final books exactly. It
//! runs twice: with Kalshi's documented `clock` (exact merging of the sockets), and without it, as
//! Kalshi's demo actually behaves (one socket per market, switching with a snapshot).

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};

use engineering_challenge::book::Book;
use engineering_challenge::collector::{Collector, Config, Msg};
use engineering_challenge::output::Writer;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use engineering_challenge::venue::{Level, ONE, kalshi::Kalshi};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::broadcast};
use tokio_tungstenite::tungstenite::Message;

const MARKETS: usize = 20;

/// The exchange: per market, YES and NO bid ladders in cents -> fixed point contracts.
#[derive(Default)]
struct Exchange {
    clocks: bool,
    clock: HashMap<String, u64>,
    yes: HashMap<String, BTreeMap<i64, i64>>,
    no: HashMap<String, BTreeMap<i64, i64>>,
}

#[derive(Clone, Debug)]
struct Delta {
    ticker: String,
    clock: u64,
    cents: i64,
    delta: i64,
    yes: bool,
}

fn tickers() -> Vec<String> {
    (0..MARKETS).map(|i| format!("MOCK-{i}")).collect()
}

fn dollars(cents: i64) -> String {
    format!("0.{cents:02}00")
}

fn contracts(fp: i64) -> String {
    format!("{}.{:02}", fp / ONE, (fp % ONE).abs() / 10_000)
}

impl Exchange {
    fn snapshot(&self, t: &str) -> Value {
        let side = |m: &BTreeMap<i64, i64>| {
            m.iter()
                .map(|(c, q)| json!([dollars(*c), contracts(*q)]))
                .collect::<Vec<_>>()
        };
        json!({"market_ticker": t, "market_id": "x", "clock": self.clock.get(t).copied().unwrap_or(0),
               "yes_dollars_fp": side(&self.yes[t]), "no_dollars_fp": side(&self.no[t])})
    }

    /// Kalshi's demo sends no `clock`; with `clocks` off the mock does the same.
    fn publish(&self, mut msg: Value) -> Value {
        if !self.clocks {
            msg.as_object_mut().unwrap().remove("clock");
        }
        msg
    }

    /// The YES-centric book the collector should publish.
    fn book(&self, t: &str) -> Book {
        Book::new(
            self.yes[t].iter().map(|(c, q)| (c * 10_000, *q)).collect(),
            self.no[t]
                .iter()
                .map(|(c, q)| (ONE - c * 10_000, *q))
                .collect(),
        )
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// One action: 1-3 deltas sharing a clock; clocks skip values. Prices stay in 1..=45 cents on
/// both ladders, so the YES book never crosses.
fn act(ex: &mut Exchange, rng: &mut Rng) -> Vec<Delta> {
    let t = format!("MOCK-{}", rng.next(MARKETS as u64));
    let clock = ex.clock[&t] + 1 + rng.next(3);
    ex.clock.insert(t.clone(), clock);
    (0..1 + rng.next(3))
        .map(|_| {
            let (yes, cents) = (rng.next(2) == 0, 1 + rng.next(45) as i64);
            let ladder = if yes {
                ex.yes.get_mut(&t).unwrap()
            } else {
                ex.no.get_mut(&t).unwrap()
            };
            let cur = ladder.get(&cents).copied().unwrap_or(0);
            let delta = if cur > 0 && rng.next(2) == 0 {
                -(cur.min((1 + rng.next(4) as i64) * ONE))
            } else {
                (1 + rng.next(9) as i64) * ONE
            };
            match cur + delta {
                0 => ladder.remove(&cents),
                q => ladder.insert(cents, q),
            };
            Delta {
                ticker: t.clone(),
                clock,
                cents,
                delta,
                yes,
            }
        })
        .collect()
}

async fn serve(
    listener: TcpListener,
    ex: Arc<Mutex<Exchange>>,
    feed: broadcast::Sender<Vec<Delta>>,
    accepted: Arc<AtomicUsize>,
) {
    loop {
        let (tcp, _) = listener.accept().await.unwrap();
        let k = accepted.fetch_add(1, SeqCst);
        let (ex, rx) = (ex.clone(), feed.subscribe());
        tokio::spawn(async move {
            let _ = socket(k, tcp, ex, rx).await;
        });
    }
}

async fn socket(
    k: usize,
    tcp: tokio::net::TcpStream,
    ex: Arc<Mutex<Exchange>>,
    mut feed: broadcast::Receiver<Vec<Delta>>,
) -> anyhow::Result<()> {
    let ws = tokio_tungstenite::accept_async(tcp).await?;
    let (mut sink, mut stream) = ws.split();
    let (sid, mut seq, mut sent) = (100 + k as u64, 0u64, 0usize);
    let mut subs: HashMap<String, u64> = HashMap::new(); // ticker -> clock of its snapshot
    let mut stalled: HashSet<String> = HashSet::new();
    let mut outbox: Vec<String> = Vec::new();
    let mut tick = tokio::time::interval(Duration::from_millis(40));
    let born = tokio::time::Instant::now();
    loop {
        let flush = tokio::select! {
            cmd = stream.next() => {
                let Some(Ok(Message::Text(t))) = cmd else { return Ok(()) };
                let c: Value = serde_json::from_str(&t)?;
                let (id, p) = (c["id"].as_u64().unwrap(), &c["params"]);
                let names: Vec<String> = p["market_tickers"].as_array().unwrap().iter().map(|v| v.as_str().unwrap().to_owned()).collect();
                let add = p["action"].as_str() != Some("delete_markets");
                if c["cmd"] == "subscribe" {
                    outbox.push(json!({"id": id, "type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": sid}}).to_string());
                } else {
                    seq += 1;
                    outbox.push(json!({"id": id, "sid": sid, "seq": seq, "type": "ok", "msg": {"market_tickers": names}}).to_string());
                }
                let ex = ex.lock().unwrap();
                for n in names {
                    if !add {
                        subs.remove(&n);
                        continue;
                    }
                    stalled.remove(&n);
                    subs.insert(n.clone(), ex.clock[&n]);
                    seq += 1;
                    outbox.push(json!({"type": "orderbook_snapshot", "sid": sid, "seq": seq, "msg": ex.publish(ex.snapshot(&n))}).to_string());
                }
                k != 1
            }
            batch = feed.recv() => {
                for d in batch? {
                    if subs.get(&d.ticker).is_none_or(|&c| d.clock <= c) || stalled.contains(&d.ticker) {
                        continue;
                    }
                    seq += 1;
                    sent += 1;
                    if k == 0 && sent == 400 { stalled.insert(d.ticker.clone()); } // fault: silent stall from here on
                    if k == 2 && sent == 300 { continue; }                         // fault: lost message
                    if k == 3 && born.elapsed() > Duration::from_millis(1500) { return Ok(()); } // fault: cut
                    let msg = json!({"market_ticker": d.ticker, "market_id": "x", "clock": d.clock, "price_dollars": dollars(d.cents),
                        "delta_fp": contracts(d.delta), "side": if d.yes { "yes" } else { "no" }, "ts_ms": chrono::Utc::now().timestamp_millis()});
                    let msg = ex.lock().unwrap().publish(msg);
                    outbox.push(json!({"type": "orderbook_delta", "sid": sid, "seq": seq, "msg": msg}).to_string());
                }
                k != 1
            }
            _ = tick.tick() => true, // socket 1 only delivers on this 40ms beat: a lagging replica
        };
        if flush {
            for m in outbox.drain(..) {
                sink.send(Message::text(m)).await?;
            }
        }
    }
}

#[test]
fn survives_faults_and_reproduces_the_exchange_exactly() {
    scenario(true);
}

#[test]
fn survives_faults_without_exchange_clocks() {
    scenario(false);
}

fn scenario(clocks: bool) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ex = Arc::new(Mutex::new(Exchange {
        clocks,
        ..Exchange::default()
    }));
    for t in tickers() {
        let mut e = ex.lock().unwrap();
        e.clock.insert(t.clone(), 1000);
        e.yes.insert(t.clone(), BTreeMap::from([(30, 5 * ONE)]));
        e.no.insert(t, BTreeMap::from([(40, 7 * ONE)]));
    }
    let (feed, _) = broadcast::channel(1 << 20);
    let accepted = Arc::new(AtomicUsize::new(0));
    let listener = rt.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
    let url = format!("ws://{}/trade-api/ws/v2", listener.local_addr().unwrap());
    rt.spawn(serve(listener, ex.clone(), feed.clone(), accepted.clone()));

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.sbe");
    let cfg = Config {
        replicas: 2,
        shard_size: 10,
        snapshot_every: Duration::from_secs(1),
        takeover_after: Duration::from_millis(200),
        stale_after: Duration::from_secs(1),
        sync_timeout: Duration::from_secs(2),
        recycle_every: Duration::from_secs(3600),

        stats_every: Duration::from_secs(2),
    };
    let venue = Arc::new(Kalshi::new(false, vec![url]).unwrap());
    let (tx, rx) = crossbeam::channel::unbounded();
    let collector = Collector::new(
        venue,
        rt.handle().clone(),
        tx.clone(),
        cfg,
        Writer::spawn(path.to_str().unwrap().into()).unwrap(),
    );
    tx.send(Msg::Live(tickers().into_iter().map(Into::into).collect()))
        .unwrap();
    let running = std::thread::spawn(move || collector.run(rx));

    // ~6s of trading at ~600 actions/s, then quiet so every socket can drain and resync.
    std::thread::sleep(Duration::from_millis(500));
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for _ in 0..3600 {
        // Publish under the lock, so the feed's order is the exchange's order.
        let mut e = ex.lock().unwrap();
        let _ = feed.send(act(&mut e, &mut rng));
        drop(e);
        std::thread::sleep(Duration::from_micros(1500));
    }
    std::thread::sleep(Duration::from_secs(4));
    tx.send(Msg::Shutdown).unwrap();
    running.join().unwrap().unwrap();
    assert!(
        accepted.load(SeqCst) >= 6,
        "the gap and the cut must have forced reconnects"
    );

    // Replay like a backtest: snapshots reset, updates apply.
    let mut books: HashMap<String, Book> = HashMap::new();
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path).unwrap();
    let (mut updates, mut snapshots) = (0, 0);
    while let Some(m) = reader.read_msg().unwrap() {
        match m {
            AnyMessage::Snapshot(d) => {
                let s = SnapshotDomain::try_from_decoder(d).unwrap();
                snapshots += 1;
                books.insert(
                    s.symbol,
                    Book::new(
                        s.bids.iter().map(|l| (l.price_level, l.quantity)).collect(),
                        s.asks.iter().map(|l| (l.price_level, l.quantity)).collect(),
                    ),
                );
            }
            AnyMessage::PriceLevelUpdate(d) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(d).unwrap();
                updates += 1;
                let b = books
                    .get_mut(&u.symbol)
                    .expect("update before any snapshot");
                b.apply(
                    &Level {
                        side: u.side,
                        px: u.price_level,
                        qty: u.quantity,
                    },
                    false,
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
    }
    let ex = ex.lock().unwrap();
    for t in tickers() {
        assert_eq!(books[&t], ex.book(&t), "{t} diverged from the exchange");
    }
    assert!(
        updates > 5000 && snapshots > MARKETS,
        "updates={updates} snapshots={snapshots}"
    );
    engineering_challenge::verify::run(&[path], false).unwrap();
}
