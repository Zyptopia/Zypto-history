// scripts/backfill/backfillFromCoinGecko.js
// One-off (or occasional) backfill that fetches full daily price history from CoinGecko
// and writes it into Firestore at collection `zypto_prices_daily` keyed by YYYY-MM-DD.
//
// Inputs (via env):
//   FIREBASE_PROJECT_ID, FIREBASE_CLIENT_EMAIL, FIREBASE_PRIVATE_KEY
//   ZYPTO_ADDR (ERC-20 address, lowercase)
//   ZYPTO_CHAIN (default 'ethereum')
//   COINGECKO_API_KEY (optional; helps avoid 401/429)

import admin from 'firebase-admin';

const projectId = process.env.FIREBASE_PROJECT_ID;
const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;
let privateKey = process.env.FIREBASE_PRIVATE_KEY || '';
if (privateKey && privateKey.includes('\\n')) privateKey = privateKey.replace(/\\n/g, '\n');

const tokenAddress = (process.env.ZYPTO_ADDR || '').toLowerCase();
const chain = process.env.ZYPTO_CHAIN || 'ethereum';
const cgKey = process.env.COINGECKO_API_KEY || '';

if (!projectId || !clientEmail || !privateKey) {
  console.error('[backfill] missing Firebase service account envs');
  process.exit(1);
}
if (!tokenAddress) {
  console.error('[backfill] missing ZYPTO_ADDR');
  process.exit(1);
}

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert({ projectId, clientEmail, privateKey }),
    projectId,
  });
}
const db = admin.firestore();

function ymdUTC(tsMs) {
  const d = new Date(tsMs);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const da = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${da}`;
}

async function fetchAllDaily() {
  const base = 'https://api.coingecko.com/api/v3';
  const url = `${base}/coins/${chain}/contract/${tokenAddress}/market_chart?vs_currency=usd&days=max&precision=6&interval=daily`;
  const headers = cgKey ? { 'x-cg-pro-api-key': cgKey } : {};
  const res = await fetch(url, { headers });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`coingecko ${res.status}: ${text.slice(0, 200)}`);
  }
  const json = await res.json();
  if (!Array.isArray(json?.prices)) throw new Error('coingecko: no prices[]');
  return json.prices; // [ [tsMs, price], ... ]
}

function groupToDailyClose(pricePairs) {
  // CG is already daily with interval=daily, but we defensively group by day
  const map = new Map(); // day -> { ts, price }
  for (const [ts, price] of pricePairs) {
    const day = ymdUTC(ts);
    const prev = map.get(day);
    if (!prev || ts > prev.ts) map.set(day, { ts, price });
  }
  return Array.from(map.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([day, { ts, price }]) => ({ day, ts, priceUSD: Number(price) }));
}

async function writeDaily(rows) {
  const batchSize = 400; // Firestore hard limit is 500
  let i = 0, wrote = 0;
  while (i < rows.length) {
    const batch = db.batch();
    const chunk = rows.slice(i, i + batchSize);
    for (const r of chunk) {
      const ref = db.collection('zypto_prices_daily').doc(r.day);
      batch.set(ref, {
        t: r.ts,
        priceUSD: r.priceUSD,
        source: 'coingecko',
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
    }
    await batch.commit();
    wrote += chunk.length;
    i += batchSize;
    console.log(`[backfill] wrote ${wrote}/${rows.length}`);
  }
}

(async () => {
  try {
    console.log(`[backfill] start for ${tokenAddress} on ${chain}`);
    const pairs = await fetchAllDaily();
    const daily = groupToDailyClose(pairs);
    console.log(`[backfill] fetched ${daily.length} days from CG`);
    await writeDaily(daily);
    console.log('[backfill] done ✔');
  } catch (e) {
    console.error('[backfill] ERROR', e?.message || e);
    process.exit(1);
  }
})();
