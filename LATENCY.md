# Latency notes (Extension 4)

All measurements were taken on 2026-10-27 from an M2 MacBook on a home connection, roughly 90 ms
from AWS us-east-2. Absolute numbers here are mostly geography. What carries over to a colocated
box are the *relative* effects: socket against socket, path against path.

## 1. Where the venues are

| Endpoint | Resolves to | TCP connect | TLS done | WS handshake |
|---|---|---|---|---|
| `external-api-ws.kalshi.com` | AWS ELB `…us-east-2.elb.amazonaws.com`, about 6 rotating IPs (one per AZ node) | 92–149 ms | 184–242 ms | 270–332 ms |
| `api.elections.kalshi.com` | CloudFront (`18.155.192.x`) | 55–85 ms | 119–133 ms | 218–235 ms |
| `ws-subscriptions-clob.polymarket.com` | Cloudflare anycast (`172.64.x`, `104.18.x`) | n/a | n/a | n/a |

From here, CloudFront wins the handshake because its edge is nearer. Every message still travels
edge to us-east-2 to edge, though. **From a box in us-east-2 the direct ELB host is the shorter
path**, and connecting to the ELB node in your own AZ avoids a cross-AZ hop (roughly 0.5–1 ms). The
collector puts its two replicas on the two hosts. That gives path diversity (one CDN or ELB
incident can't take out both copies) rather than speed.

## 2. Redundant sockets: the same update, different arrival times

**Two sockets, same 300 assets, 45 s** (an early probe): both received all 3,236 transitions.
Socket A arrived first 91% of the time. Socket B minus socket A was p50 +5.9 ms, p90 +16 ms and
**p99 +154 ms**. The median gap is small; the tail is large.

**Is speed a property of the socket?** `experiments/race.mjs` opens N sockets on the same busy
assets and splits the run in halves:

| Run | Fastest socket's share of first arrivals, 1st half → 2nd half |
|---|---|
| 8 sockets, 120 s | **73% → 74%** (the other 7 shared the rest) |
| 4 sockets, 40 s | **56% → 60%** |

The fastest socket stays fastest, so a connection's backend placement sticks for its lifetime.
"Spam connections until you find a fast one" is therefore a real effect on Polymarket, not noise.

**At full scale, inside the collector** (Polymarket: 305k assets, 244 sockets, two per shard), from the
`race` field of the stats line: the two slots split wins about 50/50. When a socket lost the race
it was behind by p50 **0.4–0.7 ms**, p90 **6–12 ms** and p99 **46–185 ms**. First-arrival
arbitration means the output pays none of that: each update goes out at the minimum over its
sockets. A single-socket collector would carry the full lag distribution in its timestamps, plus
the outages.

**Exchange to us:** receive time minus Polymarket's message timestamp was p50 about 70 ms and p99
about 575 ms here, which includes any offset between our clock and theirs. The tail is fat even at
the median connection, which is another argument for taking the minimum over several sockets
rather than picking one.

## 3. What the collector does with this

- **First arrival wins, exactly,** where the venue names its states (Polymarket's `hash`;
  Kalshi's `clock` when sent). Taking the earliest copy of each update then never risks applying
  one twice or skipping one. Kalshi's demo sends no `clock`, so there each market follows one
  socket and its latency is that socket's; picking the faster socket per market from the measured
  lag is the natural next step.
- **Timestamps come first.** The receive time is taken the moment the websocket yields a frame,
  before JSON parsing. Book work happens on another thread, and SBE encoding and disk on a third.
  `TCP_NODELAY` is set on every socket.
- **Every socket is measured.** The stats line reports each slot's share of first arrivals and
  its lag percentiles when it loses. Because speed sticks to a connection, that's enough to act on
  (see item 2 below); the collector reports it rather than acting on it.
- **Path diversity** across Kalshi's two hosts for the two replicas.

## 4. What I'd try next, and why

1. **Colocate in AWS us-east-2 for Kalshi.** Both Kalshi hosts terminate there. Then resolve the
   ELB, time a TCP connect to each AZ node, and pin sockets to the local node (SNI and Host stay
   `external-api-ws.kalshi.com`). Expected: sub-millisecond RTT against about 90 ms here.
2. **Race at connect time.** Open `k > replicas` sockets per shard, score each by first-arrival
   share over a minute of identical traffic (the stats already compute it), and keep the best
   `replicas`. The 73% → 74% persistence above says the score predicts.
3. **Vary the 4-tuple.** ELBs and Cloudflare hash connections onto backends, so a new source port
   is a new draw. Racing over source ports is item 2 done deliberately.
4. **Split Kalshi latency into network and server queueing.** Kalshi stamps `sending_ts_ms` (when
   the message was queued at its network layer) next to `ts_ms` (when the change happened). With
   a disciplined clock, `sending - ts` is Kalshi's queueing and `recv - sending` is the wire. A
   socket that is slow on queueing is a server-side slow consumer; recycle it.
5. **Reconnect right after maintenance.** Kalshi pauses every Thursday 03:00–05:00 ET and drops
   sessions. Being first back on fresh backends is cheap to try, since the collector already
   rebuilds and resyncs on its own.
6. **Kernel timestamps and pinning.** `SO_TIMESTAMPING` would remove scheduler jitter from receive
   times, and pinning the socket threads and the collector thread would cut tail wakeup latency.
   Both matter once the network is no longer 90 ms.

Reproduce the Polymarket numbers with Node 22 or later and no dependencies:
`node experiments/race.mjs 8 120 150` and `node experiments/subscribe_race.mjs`.
