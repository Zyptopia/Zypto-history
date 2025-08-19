// scripts/backfill/backfillFromCoinGecko.js
// Backfills daily prices into Firestore from CoinGecko.
// - Works with DEMO key (free): falls back to last 365 days
// - Will use PRO endpoint if you later set COINGECKO_TIER=pro
//
// Required ENV (set as GitHub repo secrets when run in Actions):
//   FIREBASE_PROJECT_ID, FIREBASE_CLIENT_EMAIL, FIREBASE_PRIVATE_KEY
//   ZYPTO_ADDR (token address, lowercase)
//   COINGECKO_API_KEY (your demo/pro key)
// Optional:
//   CHAIN (default 'ethereum')
//   COINGECKO_TIER ('demo' | 'pro')

import admin from 'firebase-admin';

// ---- Firebase Admin init (service account from env) ----
const projectId = process.env.FIREBASE_PROJECT_ID;
const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;
let privateKey = process.env.FIREBASE_PRIVATE_KEY || '';
if (privateKey?.includes('\\n')) privateKey = privateKey.replace(/\\n/g, '\n');

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert({ projectId, clientEmail, privateKey }),
  });
}
const db = admin.firestore();

// ---- Config ----
const ZYPTO_ADDR = (process.env.ZYPTO_ADDR || '').toLowerCase();
const CHAIN = process.env.CHAIN || 'ethereum';
const CG_KEY = process.env.COINGECKO_API_KEY || '';
const CG_TIER = (process.env.COINGECKO_TIER || 'demo').toLowerCase(); // 'demo' or 'pro'

const CG_BASE = CG_TIER === 'pro' ? 'https://pro-api.coingecko.com' : 'https://api.coingecko.com';
const CG_HEADER_NAME = CG_TIER === 'pro' ? 'x-cg-pro-api-key' : 'x-cg-demo-api-key';

function dayKeyUTC(tsMs) {
  return new Date(tsMs).toISOString().slice(0, 10); // YYYY-MM-DD
}

async function fetchCG(contract, { days = 'max' } = {}) {
  const url = `${CG_BASE}/api/v3/coins/${CHAIN}/contract/${contract}/market_chart?vs_currency=usd&days=${days}&precision=6&interval=daily`;
  const res = await fetch(url, { headers: CG_KEY ? { [CG_HEADER_NAME]: CG_KEY } : undefined });
  const text = await res.text();
  if (!res.ok) {
    let body; try { body = JSON.parse(text); } catch { body = text; }
    const msg = typeof body === 'string' ? body : JSON.stringify(body);
    throw new Error(`coingecko ${res.status}: ${msg}`);
  }
  try {
    return JSON.parse(text);
  } catch (e) {
    throw new Error(`coingecko parse error: ${String(e?.message || e)}`);
  }
}

async function getCGPricesDaysMax(contract) {
  try {
    // First try full history
    return await fetchCG(contract, { days: 'max' });
  } catch (e) {
    const msg = String(e?.message || e);
    // Demo key historical limit -> auto-fallback to 365 days
    if (/past 365 days/i.test(msg) || /10012/.test(msg) || /401/.test(msg)) {
      console.log('[backfill] falling back to 365 days (demo key limit)');
      return await fetchCG(contract, { days: '365' });
    }
    throw e;
  }
}

async function backfillDaily() {
  if (!ZYPTO_ADDR) throw new Error('ZYPTO_ADDR missing');
  console.log(`[backfill] start for ${ZYPTO_ADDR} on ${CHAIN}`);

  const data = await getCGPricesDaysMax(ZYPTO_ADDR);
  const prices = Array.isArray(data?.prices) ? data.prices : [];
  if (!prices.length) throw new Error('coingecko returned no prices');

  // Reduce to { YYYY-MM-DD: { ts, close } } using the *last* sample of the day as close
  const byDay = new Map();
  for (const [tsMs, price] of prices) {
    const k = dayKeyUTC(tsMs);
    const prev = byDay.get(k);
    if (!prev || tsMs > prev.ts) byDay.set(k, { ts: tsMs, close: Number(price) });
  }

  // Write to Firestore in small batches to avoid timeouts
  const entries = Array.from(byDay.entries()).sort(([a], [b]) => a.localeCompare(b));
  let wrote = 0;
  while (entries.length) {
    const chunk = entries.splice(0, 400);
    const batch = db.batch();
    for (const [day, { ts, close }] of chunk) {
      if (!Number.isFinite(close)) continue;
      const ref = db.collection('zypto_prices_daily').doc(day);
      batch.set(ref, {
        day,
        priceUSD: close,
        source: 'coingecko',
        chain: CHAIN,
        contract: ZYPTO_ADDR,
        ts,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
    }
    await batch.commit();
    wrote += chunk.length;
  }

  console.log(`[backfill] wrote days: ${wrote}`);
}

backfillDaily().catch((err) => {
  console.error('[backfill] ERROR', String(err?.message || err));
  process.exit(1);
});
