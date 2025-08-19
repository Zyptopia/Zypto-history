// ===============================
// File: scripts/ingestors/zyptoIngestHourly.js
// Purpose: Fetch a consolidated $Zypto price once per hour and store it.
// Minimal MVP uses DexScreener (robust, no key). We can add Uniswap v3 & others later.
// Run locally: node scripts/ingestors/zyptoIngestHourly.js
// ===============================

const { init: initAdmin2 } = require('../firebaseAdmin');

// --- Basic config (can be overridden by env vars) ---
const TOKEN_ADDRESS = (process.env.ZYPTO_ADDR || '0x7a65cb87f596caf31a4932f074c59c0592be77d7').toLowerCase();
const UNI_PAIR = (process.env.ZYPTO_UNI_PAIR || '0x1ecb460a532c1d76937bedbadf7d333da30255a4').toLowerCase();

// Prefer Node 18+ global fetch
async function getJSON(url, opts = {}) {
  const ctl = new AbortController();
  const timeout = setTimeout(() => ctl.abort(), opts.timeout || 15000);
  try {
    const res = await fetch(url, { signal: ctl.signal, headers: opts.headers });
    if (!res.ok) throw new Error(`${res.status}`);
    return await res.json();
  } finally {
    clearTimeout(timeout);
  }
}

function pickDexPair(json) {
  const pairs = Array.isArray(json?.pairs) ? json.pairs : (json?.pair ? [json.pair] : []);
  if (!pairs.length) return null;
  // 1) exact pair match
  const byPair = pairs.find(p => (p?.pairAddress || '').toLowerCase() === UNI_PAIR);
  if (byPair) return byPair;
  // 2) any ethereum pair containing our token
  const t = TOKEN_ADDRESS;
  const byToken = pairs.find(p => (
    (p?.chainId === 'ethereum' || p?.chain === 'ethereum') &&
    ((p?.baseToken?.address || '').toLowerCase() === t || (p?.quoteToken?.address || '').toLowerCase() === t)
  ));
  if (byToken) return byToken;
  // 3) otherwise first
  return pairs[0];
}

async function pullDexScreenerQuote() {
  // Try pair endpoint first, then token, then search
  const tries = [
    `https://api.dexscreener.com/latest/dex/pairs/ethereum/${UNI_PAIR}`,
    `https://api.dexscreener.com/latest/dex/pairs/${UNI_PAIR}`,
    `https://api.dexscreener.com/latest/dex/tokens/${TOKEN_ADDRESS}`,
    `https://api.dexscreener.com/latest/dex/search?q=${encodeURIComponent(TOKEN_ADDRESS)}`,
  ];
  let lastErr = null;
  for (const url of tries) {
    try {
      const j = await getJSON(url, { timeout: 12000 });
      const pair = pickDexPair(j);
      if (!pair) { lastErr = 'no pair in response'; continue; }
      const priceUSD = Number(pair.priceUsd);
      const volUSD = Number(pair.volume?.h24 || pair.volume || 0);
      if (!isFinite(priceUSD) || priceUSD <= 0) { lastErr = 'invalid price'; continue; }
      return { provider: 'dexscreener', priceUSD, volumeUSD: volUSD, pairAddress: pair.pairAddress };
    } catch (e) {
      lastErr = e.message || String(e);
    }
  }
  throw new Error(`dexscreener_failed: ${lastErr || 'unknown'}`);
}

function ymd(date = new Date()) {
  const d = new Date(date);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function ymdh(date = new Date()) {
  const d = new Date(date);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  const h = String(d.getUTCHours()).padStart(2, '0');
  return `${y}-${m}-${day}-${h}`; // safe for doc id
}

async function writeHourlyAndDaily(db, quote) {
  const now = new Date();
  const hourId = ymdh(now);
  const dayId = ymd(now);
  const ts = Date.now();

  // --- Hourly point ---
  const hourlyRef = db.collection('zypto_prices_hourly').doc(hourId);
  await hourlyRef.set({
    ts, provider: quote.provider, priceUSD: quote.priceUSD, volumeUSD: quote.volumeUSD || null,
    token: TOKEN_ADDRESS, pair: quote.pairAddress || null,
  }, { merge: true });

  // --- Daily OHLC aggregator (very light) ---
  const dailyRef = db.collection('zypto_prices_daily').doc(dayId);
  const snap = await dailyRef.get();
  if (!snap.exists) {
    await dailyRef.set({
      date: dayId, open: quote.priceUSD, high: quote.priceUSD, low: quote.priceUSD, close: quote.priceUSD,
      volumeUSD: quote.volumeUSD || 0, firstTs: ts, lastTs: ts,
    });
  } else {
    const d = snap.data();
    const high = Math.max(d.high, quote.priceUSD);
    const low = Math.min(d.low, quote.priceUSD);
    const volumeUSD = (d.volumeUSD || 0) + (quote.volumeUSD || 0);
    await dailyRef.set({ high, low, close: quote.priceUSD, volumeUSD, lastTs: ts }, { merge: true });
  }
}

(async () => {
  try {
    const { db } = initAdmin2();
    const quote = await pullDexScreenerQuote();
    await writeHourlyAndDaily(db, quote);
    console.log('[ingest] ok:', quote);
    process.exit(0);
  } catch (e) {
    console.error('[ingest] failed:', e);
    process.exit(1);
  }
})();