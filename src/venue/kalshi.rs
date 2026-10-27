//! Kalshi: REST discovery of open non-MVE markets, and the `orderbook_delta` channel.
//!
//! A Kalshi book is two bid ladders, YES and NO. A NO bid at `p` is a YES ask at `1 - p`, so the
//! symbol is the market ticker and its book is the YES contract's.

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, bail, ensure};
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use chrono::Utc;
use ring::{rand::SystemRandom, signature};
use serde::Deserialize;
use serde_json::{json, value::RawValue};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, handshake::client::Request};

use super::{
    Event, Head, Level, ONE, Seq, Session, Side, Sym, Venue, fixed, from_millis, get_json, http,
};
use crate::{
    collector::{Msg, Tx},
    log,
};

const PROD_WS: [&str; 2] = [
    "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
    "wss://api.elections.kalshi.com/trade-api/ws/v2",
];
const DEMO_WS: [&str; 2] = [
    "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2",
    "wss://demo-api.kalshi.co/trade-api/ws/v2",
];
const PROD_REST: &str = "https://external-api.kalshi.com/trade-api/v2";
const DEMO_REST: &str = "https://external-api.demo.kalshi.co/trade-api/v2";
/// Tickers per subscribe / add_markets command.
const BATCH: usize = 500;
const POLL: Duration = Duration::from_secs(5);
const RESCAN: Duration = Duration::from_secs(3600);

pub struct Kalshi {
    ws: Vec<String>,
    rest: String,
    auth: Option<Auth>,
}

impl Kalshi {
    /// Credentials come from `KALSHI_API_KEY_ID` and `KALSHI_PRIVATE_KEY_PATH` (or the PEM itself
    /// in `KALSHI_PRIVATE_KEY`). They are required unless `ws` points somewhere else (a mock).
    pub fn new(demo: bool, ws: Vec<String>) -> anyhow::Result<Self> {
        let custom = !ws.is_empty();
        let ws = if custom {
            ws
        } else {
            (if demo { DEMO_WS } else { PROD_WS })
                .map(String::from)
                .to_vec()
        };
        let rest = std::env::var("KALSHI_REST_URL")
            .unwrap_or_else(|_| (if demo { DEMO_REST } else { PROD_REST }).into());
        let auth = match std::env::var("KALSHI_API_KEY_ID") {
            Ok(id) => Some(Auth::from_env(id)?),
            Err(_) if custom => None,
            Err(_) => bail!(
                "Kalshi's orderbook channel needs an API key: set KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH (or use --venue polymarket)"
            ),
        };
        Ok(Self { ws, rest, auth })
    }

    async fn pages(
        &self,
        http: &reqwest::Client,
        query: &str,
        mut f: impl FnMut(Vec<RestMarket>) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mut cursor = String::new();
        loop {
            let url = format!(
                "{}/markets?limit=1000&{query}{}{cursor}",
                self.rest,
                if cursor.is_empty() { "" } else { "&cursor=" }
            );
            let page: Page = get_json(http, &url).await?;
            f(page.markets)?;
            match page.cursor {
                Some(c) if !c.is_empty() => cursor = c,
                _ => return Ok(()),
            }
        }
    }

    /// A full scan of open markets, streamed page by page so subscriptions start immediately.
    async fn scan(&self, http: &reqwest::Client, tx: &Tx) -> anyhow::Result<()> {
        tx.send(Msg::ScanStart)?;
        self.pages(http, "status=open&mve_filter=exclude", |ms| {
            Ok(tx.send(Msg::Live(
                ms.into_iter()
                    .filter(|m| m.status == "active")
                    .map(|m| m.ticker.into())
                    .collect(),
            ))?)
        })
        .await?;
        Ok(tx.send(Msg::ScanDone)?)
    }
}

impl Venue for Kalshi {
    type Session = KalshiSession;

    fn request(&self, slot: usize) -> anyhow::Result<Request> {
        let mut req = self.ws[slot % self.ws.len()]
            .as_str()
            .into_client_request()?;
        if let Some(a) = &self.auth {
            let ts = Utc::now().timestamp_millis().to_string();
            let sig = a.sign(&format!("{ts}GET{}", req.uri().path()))?;
            let h = req.headers_mut();
            h.insert("KALSHI-ACCESS-KEY", a.id.parse()?);
            h.insert("KALSHI-ACCESS-SIGNATURE", sig.parse()?);
            h.insert("KALSHI-ACCESS-TIMESTAMP", ts.parse()?);
        }
        Ok(req)
    }

    fn session(&self, _shard: usize) -> KalshiSession {
        KalshiSession::default()
    }

    /// Every 5s, markets updated since the last poll: new listings, openings (activation bumps
    /// `updated_time`) and closings. An hourly full scan runs alongside to reconcile.
    async fn discover(self: Arc<Self>, tx: Tx) -> anyhow::Result<()> {
        let http = http()?;
        let scanner = {
            let (me, tx, http) = (self.clone(), tx.clone(), http.clone());
            tokio::spawn(async move {
                loop {
                    match me.scan(&http, &tx).await {
                        Err(e) if e.is::<crossbeam::channel::SendError<Msg>>() => return,
                        Err(e) => log!("kalshi scan failed: {e:#}"),
                        Ok(()) => {}
                    }
                    tokio::time::sleep(RESCAN).await;
                }
            })
        };
        let _scanner = AbortOnDrop(scanner);
        let mut since = Utc::now().timestamp() - 60;
        loop {
            let t = Utc::now().timestamp();
            let (mut live, mut dead) = (Vec::new(), Vec::new());
            let query = format!("min_updated_ts={}&mve_filter=exclude", since - 10);
            self.pages(&http, &query, |ms| {
                for m in ms {
                    match m.status.as_str() {
                        "active" => live.push(m.ticker.into()),
                        "initialized" => {} // not open yet; it comes back here when it opens
                        _ => dead.push(m.ticker.into()),
                    }
                }
                Ok(())
            })
            .await?;
            since = t;
            if !live.is_empty() {
                tx.send(Msg::Live(live))?;
            }
            if !dead.is_empty() {
                tx.send(Msg::Dead(dead))?;
            }
            tokio::time::sleep(POLL).await;
        }
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Deserialize)]
struct Page {
    markets: Vec<RestMarket>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct RestMarket {
    ticker: String,
    status: String,
}

// ------------------------------------------------------------------------------------ auth

struct Auth {
    id: String,
    key: Key,
}

enum Key {
    Rsa(signature::RsaKeyPair),
    Ed25519(signature::Ed25519KeyPair),
}

impl Auth {
    fn from_env(id: String) -> anyhow::Result<Self> {
        let pem = match std::env::var("KALSHI_PRIVATE_KEY") {
            Ok(pem) => pem,
            Err(_) => {
                let path = std::env::var("KALSHI_PRIVATE_KEY_PATH")
                    .context("set KALSHI_PRIVATE_KEY_PATH")?;
                std::fs::read_to_string(&path).with_context(|| format!("read {path}"))?
            }
        };
        Ok(Self {
            id,
            key: Key::from_pem(&pem)?,
        })
    }

    /// Kalshi signs `timestamp_ms + METHOD + path` with RSA-PSS-SHA256 (salt = digest length,
    /// which is what ring's RSA_PSS_SHA256 uses) or Ed25519, base64 encoded.
    fn sign(&self, msg: &str) -> anyhow::Result<String> {
        Ok(B64.encode(match &self.key {
            Key::Rsa(k) => {
                let mut sig = vec![0; k.public().modulus_len()];
                k.sign(
                    &signature::RSA_PSS_SHA256,
                    &SystemRandom::new(),
                    msg.as_bytes(),
                    &mut sig,
                )
                .map_err(|e| anyhow::anyhow!("rsa sign: {e}"))?;
                sig
            }
            Key::Ed25519(k) => k.sign(msg.as_bytes()).as_ref().to_vec(),
        }))
    }
}

impl Key {
    fn from_pem(pem: &str) -> anyhow::Result<Self> {
        let start = pem.find("-----BEGIN ").context("private key is not PEM")? + 11;
        let label_end = start + pem[start..].find("-----").context("bad PEM header")?;
        let end = pem.find("-----END").context("bad PEM footer")?;
        let der = B64.decode(
            pem[label_end + 5..end]
                .split_whitespace()
                .collect::<String>(),
        )?;
        let rejected =
            |e: ring::error::KeyRejected| anyhow::anyhow!("unsupported private key: {e}");
        match &pem[start..label_end] {
            "RSA PRIVATE KEY" => Ok(Key::Rsa(
                signature::RsaKeyPair::from_der(&der).map_err(rejected)?,
            )),
            "PRIVATE KEY" => signature::RsaKeyPair::from_pkcs8(&der)
                .map(Key::Rsa)
                .or_else(|_| {
                    signature::Ed25519KeyPair::from_pkcs8_maybe_unchecked(&der).map(Key::Ed25519)
                })
                .map_err(rejected),
            other => bail!("unsupported PEM block {other:?}"),
        }
    }
}

// ------------------------------------------------------------------------------------ session

/// One subscription (`sid`) per socket, grown and shrunk with `update_subscription`. A rejected
/// command is only logged: its symbols stay unsynced, and the collector's retry re-requests each
/// one on its own, so a bad ticker holds up the rest for one retry at most.
#[derive(Default)]
pub struct KalshiSession {
    next_id: u64,
    sid: Option<u64>,
    /// Id of the in-flight `subscribe`; adds and deletes wait for its `sid`.
    subscribing: Option<u64>,
    queued: Vec<(bool, Vec<Sym>)>,
    seqs: HashMap<u64, u64>,
}

impl KalshiSession {
    fn command(&mut self, out: &mut Vec<String>, cmd: &str, params: serde_json::Value) -> u64 {
        self.next_id += 1;
        out.push(json!({"id": self.next_id, "cmd": cmd, "params": params}).to_string());
        self.next_id
    }

    fn update(&mut self, out: &mut Vec<String>, add: bool, syms: &[Sym]) {
        let Some(sid) = self.sid else {
            return self.queued.push((add, syms.to_vec()));
        };
        let action = if add { "add_markets" } else { "delete_markets" };
        let params = json!({"sids": [sid], "market_tickers": syms, "action": action});
        self.command(out, "update_subscription", params);
    }
}

impl Session for KalshiSession {
    fn subscribe(&mut self, syms: &[Sym], out: &mut Vec<String>) {
        for chunk in syms.chunks(BATCH) {
            if self.sid.is_none() && self.subscribing.is_none() {
                let params = json!({"channels": ["orderbook_delta"], "market_tickers": chunk});
                self.subscribing = Some(self.command(out, "subscribe", params));
            } else {
                self.update(out, true, chunk);
            }
        }
    }

    fn unsubscribe(&mut self, syms: &[Sym], out: &mut Vec<String>) {
        for chunk in syms.chunks(BATCH) {
            self.update(out, false, chunk);
        }
    }

    fn decode(
        &mut self,
        frame: &str,
        events: &mut Vec<Event>,
        out: &mut Vec<String>,
    ) -> anyhow::Result<()> {
        let f: Frame =
            serde_json::from_str(frame).with_context(|| format!("bad frame {frame:.200}"))?;
        // seq numbers every message of a subscription; a gap means this socket's books are suspect.
        if let (Some(sid), Some(seq)) = (f.sid, f.seq)
            && let Some(prev) = self.seqs.insert(sid, seq)
        {
            ensure!(seq == prev + 1, "seq gap on sid {sid}: {prev} -> {seq}");
        }
        let sent = f.sending_ts_ms.and_then(from_millis);
        let msg = f.msg.map_or("null", RawValue::get);
        match f.kind {
            "orderbook_snapshot" => {
                let s: Snapshot = parse(f.kind, msg)?;
                let side = |v: &[(&str, &str)], no: bool| -> anyhow::Result<Vec<(i64, i64)>> {
                    v.iter()
                        .map(|(p, q)| Ok((if no { ONE - fixed(p)? } else { fixed(p)? }, fixed(q)?)))
                        .collect()
                };
                events.push(Event::Book {
                    head: Head {
                        sym: s.market_ticker.into(),
                        seq: s.clock.map_or(Seq::None, Seq::Clock),
                        ts: sent,
                    },
                    bids: side(&s.yes_dollars_fp, false)?,
                    asks: side(&s.no_dollars_fp, true)?,
                });
            }
            "orderbook_delta" => {
                let d: Delta = parse(f.kind, msg)?;
                let (px, qty) = (fixed(d.price_dollars)?, fixed(d.delta_fp)?);
                let level = match d.side {
                    "yes" => Level {
                        side: Side::Buy,
                        px,
                        qty,
                    },
                    "no" => Level {
                        side: Side::Sell,
                        px: ONE - px,
                        qty,
                    },
                    other => bail!("unknown side {other:?}"),
                };
                events.push(Event::Delta {
                    head: Head {
                        sym: d.market_ticker.into(),
                        seq: d.clock.map_or(Seq::None, Seq::Clock),
                        ts: d.ts_ms.and_then(from_millis).or(sent),
                    },
                    rel: true,
                    levels: vec![level],
                });
            }
            "subscribed" => {
                let s: Subscribed = parse(f.kind, msg)?;
                if s.channel == "orderbook_delta" {
                    (self.sid, self.subscribing) = (Some(s.sid), None);
                    for (add, syms) in std::mem::take(&mut self.queued) {
                        self.update(out, add, &syms);
                    }
                }
            }
            "error" => {
                let e: Error = parse(f.kind, msg)?;
                if f.id.is_some() && f.id == self.subscribing {
                    self.subscribing = None; // the next subscribe opens a fresh subscription
                }
                match e.code {
                    25 => bail!("subscription buffer overflow (error 25): {}", e.msg),
                    26 => log!(
                        "kalshi subscription market limit reached (error 26): lower --shard-size"
                    ),
                    code => log!("kalshi error {code} for command {:?}: {}", f.id, e.msg),
                }
            }
            _ => {} // ok, unsubscribed, and message types added later
        }
        Ok(())
    }
}

fn parse<'a, T: Deserialize<'a>>(kind: &str, msg: &'a str) -> anyhow::Result<T> {
    serde_json::from_str(msg).with_context(|| format!("bad {kind} {msg:.300}"))
}

#[derive(Deserialize)]
struct Frame<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    id: Option<u64>,
    sid: Option<u64>,
    seq: Option<u64>,
    sending_ts_ms: Option<i64>,
    #[serde(borrow)]
    msg: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct Snapshot<'a> {
    market_ticker: &'a str,
    /// Documented as required on both messages; Kalshi's demo sends it on neither.
    clock: Option<u64>,
    #[serde(default, borrow)]
    yes_dollars_fp: Vec<(&'a str, &'a str)>,
    #[serde(default, borrow)]
    no_dollars_fp: Vec<(&'a str, &'a str)>,
}

#[derive(Deserialize)]
struct Delta<'a> {
    market_ticker: &'a str,
    clock: Option<u64>,
    price_dollars: &'a str,
    delta_fp: &'a str,
    side: &'a str,
    ts_ms: Option<i64>,
}

#[derive(Deserialize)]
struct Subscribed<'a> {
    channel: &'a str,
    sid: u64,
}

#[derive(Deserialize)]
struct Error {
    code: i64,
    #[serde(default)]
    msg: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(s: &mut KalshiSession, frame: &str) -> anyhow::Result<Vec<Event>> {
        let (mut ev, mut out) = (Vec::new(), Vec::new());
        s.decode(frame, &mut ev, &mut out).map(|_| ev)
    }

    #[test]
    fn decodes_documented_messages() {
        let mut s = KalshiSession::default();
        let ev = decode(&mut s, r#"{"type":"orderbook_snapshot","sending_ts_ms":1669149841234,"sid":2,"seq":2,"msg":{"market_ticker":"FED-23DEC-T3.00","market_id":"9b0f6b43","clock":4182057,"yes_dollars_fp":[["0.0800","300.00"],["0.2200","333.00"]],"no_dollars_fp":[["0.5400","20.00"],["0.5600","146.00"]]}}"#).unwrap();
        let Event::Book { head, bids, asks } = &ev[0] else {
            panic!()
        };
        assert_eq!(
            (&*head.sym, head.seq),
            ("FED-23DEC-T3.00", Seq::Clock(4182057))
        );
        assert_eq!(bids, &[(80_000, 300_000_000), (220_000, 333_000_000)]);
        assert_eq!(asks, &[(460_000, 20_000_000), (440_000, 146_000_000)]); // NO bids are YES asks at 1 - p
        let ev = decode(&mut s, r#"{"type":"orderbook_delta","sending_ts_ms":1669149841123,"sid":2,"seq":3,"msg":{"market_ticker":"FED-23DEC-T3.00","market_id":"9b0f","clock":4182061,"price_dollars":"0.9600","delta_fp":"-54.00","side":"yes","ts":"2022-11-22T20:44:01Z","ts_ms":1669149841000}}"#).unwrap();
        let Event::Delta { head, rel, levels } = &ev[0] else {
            panic!()
        };
        assert_eq!((head.seq, *rel), (Seq::Clock(4182061), true));
        assert_eq!(
            levels,
            &[Level {
                side: Side::Buy,
                px: 960_000,
                qty: -54_000_000
            }]
        );
        assert_eq!(head.ts.unwrap().timestamp_millis(), 1669149841000);
        // Kalshi's demo sends no clock at all: those states have no shared name.
        let ev = decode(&mut s, r#"{"type":"orderbook_delta","sid":2,"seq":4,"msg":{"market_ticker":"FED-23DEC-T3.00","price_dollars":"0.3900","delta_fp":"-10.00","side":"no","ts_ms":1793126658365}}"#).unwrap();
        let Event::Delta { head, levels, .. } = &ev[0] else {
            panic!()
        };
        assert_eq!((head.seq, levels[0].px), (Seq::None, 610_000));
        // Gaps are fatal for the socket, unknown types are not.
        assert!(decode(&mut s, r#"{"type":"something_new","sid":2,"seq":5}"#).is_ok());
        assert!(decode(&mut s, r#"{"type":"ok","sid":2,"seq":7,"id":9}"#).is_err());
    }

    #[test]
    fn adds_wait_for_the_sid_and_a_rejected_subscribe_starts_over() {
        let mut s = KalshiSession::default();
        let (mut ev, mut out) = (Vec::new(), Vec::new());
        let syms: Vec<Sym> = ["A", "B", "C", "D"].map(Sym::from).to_vec();
        s.subscribe(&syms[..2], &mut out);
        s.subscribe(&syms[2..], &mut out);
        assert_eq!(out.len(), 1, "second batch waits for the sid");
        s.decode(
            r#"{"id":1,"type":"error","msg":{"code":11,"msg":"bad ticker"}}"#,
            &mut ev,
            &mut out,
        )
        .unwrap();
        s.subscribe(&syms[..1], &mut out); // the collector's retry
        assert!(out[1].contains(r#""cmd":"subscribe""#), "{out:?}");
        s.decode(
            r#"{"id":2,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":7}}"#,
            &mut ev,
            &mut out,
        )
        .unwrap();
        assert!(
            out[2].contains("add_markets")
                && out[2].contains("[7]")
                && out[2].contains(r#"["C","D"]"#),
            "{out:?}"
        );
    }

    #[test]
    fn signs_with_rsa_pss_and_ed25519() {
        let ed = signature::Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let pem = format!(
            "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----\n",
            B64.encode(ed.as_ref())
        );
        let auth = Auth {
            id: "k".into(),
            key: Key::from_pem(&pem).unwrap(),
        };
        let sig = B64
            .decode(auth.sign("1700000000000GET/trade-api/ws/v2").unwrap())
            .unwrap();
        let Key::Ed25519(k) = &auth.key else { panic!() };
        use signature::KeyPair;
        signature::UnparsedPublicKey::new(&signature::ED25519, k.public_key().as_ref())
            .verify(b"1700000000000GET/trade-api/ws/v2", &sig)
            .unwrap();
    }

    /// RSA-PSS checked by an independent implementation, with Kalshi's exact parameters
    /// (SHA-256, MGF1-SHA-256, salt length 32). Needs the `openssl` CLI; skipped without it.
    #[test]
    fn rsa_pss_signature_verifies_with_openssl() {
        use std::process::Command;
        let dir = tempfile::tempdir().unwrap();
        let (key, msg, sig) = (
            dir.path().join("k.pem"),
            dir.path().join("m"),
            dir.path().join("s"),
        );
        let ok = |c: &mut Command| c.status().is_ok_and(|s| s.success());
        for (format, args) in [("PKCS#8", &[][..]), ("PKCS#1", &["-traditional"][..])] {
            if !ok(Command::new("openssl")
                .args(["genrsa", "-out"])
                .arg(&key)
                .args(args)
                .arg("2048")
                .stderr(std::process::Stdio::null()))
            {
                return eprintln!("openssl unavailable; skipping");
            }
            let auth = Auth {
                id: "k".into(),
                key: Key::from_pem(&std::fs::read_to_string(&key).unwrap()).unwrap(),
            };
            let text = "1700000000000GET/trade-api/ws/v2";
            std::fs::write(&msg, text).unwrap();
            std::fs::write(&sig, B64.decode(auth.sign(text).unwrap()).unwrap()).unwrap();
            let verified = Command::new("openssl")
                .args([
                    "dgst",
                    "-sha256",
                    "-sigopt",
                    "rsa_padding_mode:pss",
                    "-sigopt",
                    "rsa_pss_saltlen:32",
                    "-sigopt",
                    "rsa_mgf1_md:sha256",
                    "-prverify",
                ])
                .arg(&key)
                .arg("-signature")
                .arg(&sig)
                .arg(&msg)
                .output()
                .unwrap();
            assert!(
                String::from_utf8_lossy(&verified.stdout).contains("Verified OK"),
                "{format}: {verified:?}"
            );
        }
    }
}
