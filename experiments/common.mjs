// Shared helpers for the latency / integrity experiments (Node >= 22: built-in fetch + WebSocket).
export const WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/** Token ids of the `markets` busiest Polymarket markets (both outcomes each). */
export async function busiestAssets(markets) {
  const out = [];
  for (let offset = 0; out.length < 2 * markets; offset += 100) {
    const r = await fetch(`https://gamma-api.polymarket.com/markets?closed=false&order=volume24hr&ascending=false&limit=100&offset=${offset}`);
    const page = await r.json();
    if (!page.length) break;
    for (const m of page) if (m.enableOrderBook && m.acceptingOrders && m.clobTokenIds) out.push(...JSON.parse(m.clobTokenIds));
  }
  return out.slice(0, 2 * markets);
}

/** Opens a market-channel socket, subscribes `assets` in 500s, keeps it alive, calls onMsg(msg, t). */
export function socket(assets, onMsg) {
  const ws = new WebSocket(WS);
  ws.onopen = () => {
    for (let i = 0; i < assets.length; i += 500)
      ws.send(JSON.stringify(i ? { assets_ids: assets.slice(i, i + 500), operation: "subscribe" } : { assets_ids: assets.slice(0, 500), type: "market" }));
    setInterval(() => ws.send("PING"), 10_000);
  };
  ws.onmessage = (ev) => {
    const t = performance.now();
    if (ev.data === "PONG") return;
    const d = JSON.parse(ev.data);
    for (const m of Array.isArray(d) ? d : [d]) onMsg(m, t);
  };
  return ws;
}

export const quantile = (xs, q) => { const s = [...xs].sort((a, b) => a - b); return s.length ? +s[Math.floor(q * (s.length - 1))].toFixed(2) : null; };
