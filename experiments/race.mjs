// N sockets subscribe the same busy assets. For every book transition (asset + hash) record which
// socket delivered it first and how late the others were. Splitting the run in halves shows
// whether a socket's speed is persistent (worth racing / recycling for) or per-message noise.
//   node experiments/race.mjs [sockets=8] [seconds=120] [markets=150]
import { busiestAssets, socket, quantile } from "./common.mjs";
const [N, secs, markets] = [+(process.argv[2] ?? 8), +(process.argv[3] ?? 120), +(process.argv[4] ?? 150)];
const assets = await busiestAssets(markets);
const t0 = performance.now(), first = new Map();
const wins = Array.from({ length: N }, () => [0, 0]), lag = Array.from({ length: N }, () => [[], []]);
for (let c = 0; c < N; c++)
  socket(assets, (m, t) => {
    if (m.event_type !== "price_change") return;
    const half = t - t0 < secs * 500 ? 0 : 1, k = m.price_changes[0].asset_id + m.price_changes[0].hash;
    const f = first.get(k);
    if (f === undefined) { first.set(k, t); wins[c][half]++; } else lag[c][half].push(t - f);
  });
setTimeout(() => {
  console.log(`${assets.length} assets, ${first.size} transitions`);
  console.log("socket | first-arrival wins (1st half / 2nd half) | lag when not first, ms p50/p90/p99 (1st half) | (2nd half)");
  for (let c = 0; c < N; c++)
    console.log(c, "|", wins[c].join(" / "), "|", [0.5, 0.9, 0.99].map((q) => quantile(lag[c][0], q)).join("/"), "|", [0.5, 0.9, 0.99].map((q) => quantile(lag[c][1], q)).join("/"));
  process.exit(0);
}, secs * 1000);
