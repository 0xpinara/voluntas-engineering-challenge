// Does a subscription made on an already-busy socket lose the transition in flight?
// Reference socket R streams set S2 from the start; test socket T streams S1, then subscribes S2
// mid-stream. For each S2 asset, locate T's snapshot in R's hash sequence and check that T's next
// change is the very next transition.   node experiments/subscribe_race.mjs
import { busiestAssets, socket } from "./common.mjs";
const all = await busiestAssets(500), S1 = all.slice(0, 500), S2 = all.slice(500, 1000), inS2 = new Set(S2);
const ref = new Map(), seen = new Map();
const push = (map, a, h) => { const l = map.get(a) ?? []; if (l.at(-1) !== h) l.push(h); map.set(a, l); };
const hashes = (m) => (m.event_type === "book" ? [[m.asset_id, m.hash]] : m.event_type === "price_change" ? m.price_changes.map((c) => [c.asset_id, c.hash]) : []);
socket(S2, (m) => hashes(m).forEach(([a, h]) => push(ref, a, h)));
const T = socket(S1, (m) => {
  for (const [a, h] of hashes(m)) {
    if (!inS2.has(a)) continue;
    const s = seen.get(a) ?? { book: null, after: [] };
    seen.set(a, s);
    if (m.event_type === "book" && !s.book) s.book = h; else if (s.book && s.after.at(-1) !== h) s.after.push(h);
  }
});
setTimeout(() => T.send(JSON.stringify({ assets_ids: S2, operation: "subscribe" })), 20_000);
setTimeout(() => {
  let clean = 0, lost = 0, quiet = 0, unknown = 0;
  for (const [a, s] of seen) {
    const r = ref.get(a) ?? [], i = r.lastIndexOf(s.book);
    if (i < 0) unknown++; else if (!s.after.length) quiet++; else if (r.indexOf(s.after[0], i) === i + 1) clean++; else lost++;
  }
  console.log({ subscribedMidStream: S2.length, continuedCleanly: clean, lostTransitionsAfterSnapshot: lost, noChangesAfterSnapshot: quiet, snapshotNotOnReference: unknown });
  process.exit(0);
}, 80_000);
