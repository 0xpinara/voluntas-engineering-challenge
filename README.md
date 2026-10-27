# Order book collector: Kalshi and Polymarket

Subscribes to every live (non-MVE) market, keeps an order book per symbol on **redundant
websockets**, and writes price-level updates plus a full snapshot every minute in the challenge's
SBE format. Most of the design is about one property: **the file must be complete and exactly
right, even when sockets drop, stall, lag or silently lose messages.**

```
cargo build --release
./target/release/engineering-challenge run --venue polymarket > pm.sbe        # no key needed
KALSHI_API_KEY_ID=... KALSHI_PRIVATE_KEY_PATH=key.pem \
  ./target/release/engineering-challenge run > kalshi.sbe                      # --demo for demo env
./target/release/engineering-challenge verify pm.sbe                           # replay + check
```

Ctrl-C (SIGINT) or SIGTERM closes every websocket with a close handshake, then flushes and closes
the output file (about 0.25 s). A second Ctrl-C forces an exit. Logs and one JSON stats line every
10 s go to stderr, so stdout carries only the binary feed.

Useful flags: `-o 'data/kalshi-%Y%m%dT%H.sbe'` rotates files (strftime). `--symbols A,B` collects
only those symbols. `--replicas 2` and `--shard-size 2500` set the socket layout. `--demo` uses
Kalshi's demo environment. `--ws-url` overrides the endpoints. `run --help` lists the timing knobs.

## How it works

```
 discovery (REST) ──Live/Dead──▶ ┌─────────────────────────────┐ ──Update/Snapshot──▶ writer thread
                                 │ collector (one thread)      │                       (AlignedSbeWriter,
 socket A ─┐ shard 0             │  per symbol:                │                        rotation, flush)
 socket B ─┘  (2 replicas)       │   output book (what we emit)│
 socket C ─┐ shard 1  ──events──▶│   replica book per socket   │◀── sweeps: stale, silence,
 socket D ─┘                     │  arbitration, health        │    takeover, recycling
   ...                           └─────────────────────────────┘
```

- **Shards and replicas.** Symbols are packed into shards of `--shard-size`. Each shard gets
  `--replicas` sockets, and each socket holds a complete replica of the shard's books. Kalshi
  replicas alternate between its two production hosts (`external-api-ws.kalshi.com`, a direct AWS
  ELB, and `api.elections.kalshi.com`, CloudFront), so the two copies take different network paths.
- **Socket tasks** (`conn.rs`) timestamp each frame *before* parsing it, decode it with the venue
  codec, and forward events. On any anomaly (a sequence gap, a buffer overflow, 30 s of silence, a
  close) the task rebuilds the socket and resubscribes its own symbol set, with jittered backoff.
- **The collector** (`collector/`) owns every book on a single thread, so there are no locks. It
  hands the writer thread cheap messages, so encoding and disk I/O never stall the books. The merge
  rule below lives in `collector/books.rs`; supervision of sockets and symbols is in
  `collector/mod.rs`.

### Redundant sockets (Extension 1)

When a venue names each book state identically on every connection, the sockets are merged
exactly. Polymarket does, with the book `hash` (verified below). Kalshi's docs say its `clock` does,
and that it "can match updates across redundant connections"; Kalshi's **demo sends no `clock` at
all** (found in the live test), so the collector handles both cases:

- **With state names** (Polymarket; Kalshi when `clock` is present): exact first-arrival merging,
  described below.
- **Without them** (Kalshi's demo): each market follows one socket. When that socket dies, stalls
  or falls more than 1 s behind the other one (judged by Kalshi's own `ts_ms`), the collector
  switches to the other and writes a fresh **snapshot**, which is the challenge's rule. The other
  socket is watched the same way and resubscribed if it falls behind.

Each venue can split one transition across several messages that share its name, so a state is
`(id, n)`: `n` messages of transition `id` have been applied.

With state names, the rule is: **apply a replica's update to the output book only if, just before
it, the replica was in exactly the output's state.** The consequences:

- Whichever socket delivers an update **first** wins. The output's latency is the minimum over its
  sockets.
- Later copies are dropped. Their arrival lag is measured, and their book is **cross-checked** with
  ours at the same exchange state.
- A socket that dies, stalls or lags costs nothing as long as another is in step. There's no
  failover, and no snapshot is needed.
- A full book that arrives mid-stream (Polymarket sends one after trades) is a complete state. It
  is applied as the level diff to that state, provided the socket was at the output's transition.
- If no in-step socket can continue (all of them died, or the in-step one fell more than 1 s
  behind one that is ahead), the output is re-seeded from the freshest replica. That
  discontinuity is written as a **snapshot**, which is the challenge's "switch source, emit a
  snapshot" rule applied exactly when the copies could actually differ.

### Monitoring (Extension 2)

| Failure | Detection | Action |
|---|---|---|
| Socket drop, Kalshi `seq` gap, error 25, 30 s silence | in the socket task | rebuild the socket and resubscribe |
| One symbol silently stalled on one socket | it hasn't reached where the output was 5 s ago, and isn't at the output's state | resubscribe that symbol on that socket, backing off to about 10 min if the venue keeps serving a stale snapshot |
| Snapshot never arrives | 30 s, doubling per retry | resubscribe |
| A market silent on *every* socket | quiet for >20× its own EWMA update gap (and ≥5 min) after ≥50 updates: adaptive, so illiquid markets aren't churned | resubscribe one socket; its fresh snapshot is audited against the output |
| Two copies disagree at the same exchange state | the cross-check above | resync the younger side (see the findings below) |
| Socket aging | every socket rebuilt hourly (jittered) | **make-before-break**: the replacement syncs the whole shard before the old socket closes, so redundancy never drops |
| **We** are the slow one (a snapshot burst after a mass reconnect) | processing lag (now minus the receive time of the event in hand) is above 250 ms | every timeout above is suspended. They compare receive times with now, so while we're behind they would blame data sitting in our own queue and resubscribe the world. `core_lag_ms` is in the stats |

### Venue-generic (Extension 3)

`venue/mod.rs` defines `Venue` (REST discovery and a handshake per replica slot) and `Session`
(subscribe and unsubscribe frames, keepalive, `decode` to normalised `Event`s). `kalshi.rs` and
`polymarket.rs` implement them. Books, arbitration, monitoring and output never see venue
specifics. Exchanges usually name states either by a per-book sequence (Binance, Coinbase,
Kalshi) or by a checksum (OKX, Kraken, Polymarket), and the core handles both.

### Latency (Extension 4)

See [LATENCY.md](LATENCY.md) for measurements and what they imply. The collector reports
per-slot win share and lag percentiles live in its stats line.

## Output format

The format is the challenge's SBE schema, written through the challenge's `AlignedSbeWriter`.
`src/util`, `src/side.rs` and `sbe_schemas` are byte-identical to the template's latest commit
(`97360ae`, "Add Side to PriceLevelUpdate", schema version 2).

- **Chunks are 64 MiB**, because `AlignedSbeReader::open` can only read 64 MiB chunks
  (`open_with_chunk_size` is private). Output therefore reaches stdout in 64 MiB steps.
- `PriceLevelUpdate` carries the template's `side` (`Buy` = bid, `Sell` = ask) and the level's new
  absolute `quantity`; `0` removes the level.
- **Kalshi** books are the YES contract. YES bids are bids; a NO bid at `p` is an ask at `1 - p`.
  **Polymarket** books are per outcome token (`asset_id`).
- Prices and sizes are exact fixed point (1e6). Decimal strings are parsed without floats, and
  anything finer than 6 dp is rejected, never rounded.
- `backtest_timestamp` / `received_at` is our receive time of the **first** socket to deliver the
  update, taken before parsing. It never decreases per symbol: two sockets' tasks can hand
  over frames in a different order than they read them, so a stamp is clamped to the
  symbol's previous one, by microseconds at most. `exchange_timestamp` is the exchange's: Kalshi
  `ts_ms` (or `sending_ts_ms` for snapshots), Polymarket's message `timestamp`.
- Snapshots of every book are written on the wall-clock minute, and whenever a book is
  (re)seeded. Within a transition, removals are written before additions.
- **Rotation** happens just before a snapshot round, so every file starts with a full snapshot set
  and replays on its own.

`verify FILES...` replays files in order the way a backtest would. It checks that every update
applies to a book a snapshot introduced, that every removal names a level that exists, that time
never goes backwards per symbol, and, most usefully, that **every periodic snapshot equals the book
rebuilt from the previous snapshot plus the updates since**. `verify --dump` prints messages as
text.

## What I learned about the venues

These are things a naive collector gets silently wrong.

- **Polymarket's subscription snapshot can be one transition stale.** I subscribed 500 active
  assets mid-stream on one socket while a reference socket streamed them from the start. For
  **12 of 468 (2.6%)**, the stream resumed one transition after the snapshot, so exactly one change
  was in neither, with no sequence number to reveal it (`experiments/subscribe_race.mjs`).
  Instrumenting the collector showed this isn't only a race with a message in flight. Quiet
  markets were served snapshots missing a change from about 10 s *before* the subscription, and the
  same stale snapshot again on resubscribe. A single-socket collector would silently keep those
  books wrong. The cross-check found this in production, and it shapes four choices:
  - On a disagreement the *younger* side is resynced. Corruption enters right after a book is
    taken. A second strike, or a younger output, rebuilds the output from a fresh snapshot instead.
  - Replica `k` subscribes new symbols `k` seconds after replica 0. Replicas subscribing in the
    same instant could lose the *same* transition, agree with each other, and the cross-check
    would never see it.
  - The output only ever takes *updates* from replicas, which are absolute sizes and safe to take
    from a replica whose book is wrong. It copies a whole replica book only when re-seeding.
  - Resubscribing a stuck replica backs off exponentially, from 5 s to about 10 min. The venue may
    keep returning the same stale snapshot, so retrying fast would only churn.
- **Polymarket splits one transition across several messages that carry the same hash**, which is
  why a state is `(hash, n)` and not just a hash. Without the count, a slower socket looks as if
  it disagrees mid-transition.
- **The Polymarket hash includes time.** In 218 of 218 cases where a book returned to an earlier
  set of levels, it got a new hash, so hashes are safe state names. Framing is identical across
  sockets: 45,238 of 45,238 transitions matched in count and content.
- **Polymarket closes a socket whose first frame isn't a subscription**, including a keepalive.
  It also restarts servers mid-session (`1001 server shutting down`). I saw both during the
  full-universe runs, and both were absorbed with no gap because the other replica kept the books.
- **Kalshi's demo sends no `clock`**, although the docs list it as required on every snapshot and
  update and describe it as the way to match redundant connections. Parsing it as required, as
  documented, rejected every message; this was found only by running live. See Extension 1 above.
- **Kalshi's universe is about 145k open non-MVE markets** (226k on the demo). Discovery streams a paginated scan
  (subscriptions start with the first page), then polls `min_updated_ts` every 5 s, which is about
  135 status changes a minute in one request. Opening a market bumps its `updated_time` (true for
  all 215 markets that opened in one 15-minute sample), so new markets are subscribed within 5 s of
  opening. Unauthenticated
  REST returns HTTP 429 above a few requests a second, so all REST calls share a 4/s limiter.

## Testing

- **Unit tests** cover exact fixed-point parsing, books, the `(id, n)` state algebra, both codecs
  against the documented message examples, and how Kalshi commands wait for the `sid`. Kalshi
  RSA-PSS signing (PKCS#1 and PKCS#8 keys) is verified by **openssl** with Kalshi's exact
  parameters (SHA-256, MGF1, salt 32). Ed25519 signing is tested too.
- **`tests/kalshi_mock.rs`** runs end to end against an in-process mock exchange that speaks
  Kalshi's protocol (`sid`, `seq`, `clock`, `update_subscription`). It runs 20 markets on 4
  sockets, with one lagging, one losing a message (seq gap), one cut mid-stream, and one silently
  stalling a market. The output, replayed, must equal the exchange's final books **exactly**. It
  does, every run, both with `clock` (exact merging) and without it, as Kalshi's demo behaves
  (26 socket switches in one run, each announced with a snapshot).
- **Live Polymarket, 300 busy assets:** 0 mismatches, and 1,224/1,224 periodic snapshots equal to
  the replayed updates (current v2 format).
- **Live Polymarket, full universe:** 265–305k assets on 214–244 sockets, 35–40k events/s, about
  0.6–0.75 cores and 600–800 MB RSS on an M2 laptop. Collector lag stays at 0.1–0.3 s; the one
  exception was an 8 s stall while the laptop was 11 GB into swap. On the final build, `verify` over
  every rotated file (5.2M updates) found 0 orphan updates, 0 no-op removals and 0 timestamp
  regressions. Under 0.001% of updates left a book briefly crossed, all of them in books the
  collector then re-seeded while repairing a stale Polymarket snapshot.
- **Stop-the-world:** I froze the process with SIGSTOP for 60 s at full scale. On resume, 100
  dead sockets were rebuilt and every stale book was re-seeded with an announced snapshot. The
  collector was 1.2 s behind at worst and back to about 50 ms within about 20 s. An earlier,
  accidental version of this test (the laptop slept for six hours mid-run) is what exposed the
  timeout feedback loop that backpressure-aware supervision now prevents: 438k needless
  resyncs then, 1.8k now.
- **Live Kalshi (demo environment, Ed25519 API key), 3.5 minutes:** discovered and synced all
  226,227 open non-MVE markets on 182 sockets (both of Kalshi's hosts), with 0 socket drops and 0
  mismatches, and wrote about 2,150 live updates a second. It dropped markets as they closed, and
  once switched a market to its other socket with a snapshot when the first fell behind.
  `verify`: 445,136 updates, and **all 719,138 periodic snapshots equal the replayed updates**,
  with 0 orphan updates, no-op removals, crossed books or timestamp regressions.

**Not verified:** Kalshi *production* order books. A production account requires a US Social
Security Number, so I tested on Kalshi's demo, which runs the same API on Kalshi's servers with
play money. Production runs the same way, without `--demo`.

## Limits and next steps

- **A process crash loses up to one 64 MiB chunk** that hasn't been written yet. That's inherent
  to the chunk format. Run a second collector on another host and merge offline. `verify` already
  rebuilds books, so arbitrating between two files is a small step from it.
- Kalshi's per-subscription market limit isn't documented. 2,500 is the default and configurable.
  A rejected command is logged and its markets are retried one at a time 30 s later, so a bad
  ticker can delay its batch by one retry at most.
- Receive times use `CLOCK_REALTIME`. In production, discipline the clock (chrony or PTP) and
  consider kernel receive timestamps (see LATENCY.md).
