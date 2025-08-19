// scripts/backfill/backfillFromGeckoTerminal.js
// ESM version. Pulls daily OHLCV from GeckoTerminal (public tier) and
// upserts into Firestore. Soft‑handles the 180‑day limit (HTTP 401) as success.

import admin from "firebase-admin";

// --- Env ---
const PROJECT_ID = process.env.FIREBASE_PROJECT_ID;
const CLIENT_EMAIL = process.env.FIREBASE_CLIENT_EMAIL;
const PRIVATE_KEY = (process.env.FIREBASE_PRIVATE_KEY || "").replace(/\\n/g, "\n");

const GT_KEY = process.env.GECKOTERMINAL_API_KEY; // required
const GT_NETWORK = process.env.GT_NETWORK || "eth"; // e.g. "eth", "bsc", "base"

// CLI args (token not currently used for GT daily; we backfill by pair)
const [,, tokenArg, pairArg] = process.argv; // token optional, pair required
const PAIR = (pairArg || process.env.UNIV2_PAIR || "").toLowerCase();

if (!PROJECT_ID || !CLIENT_EMAIL || !PRIVATE_KEY) {
  console.error("[gt] missing FIREBASE_* envs");
  process.exit(1);
}
if (!GT_KEY) {
  console.error("[gt] missing GECKOTERMINAL_API_KEY env");
  process.exit(1);
}
if (!PAIR) {
  console.error("[gt] missing pair address (arg2 or UNIV2_PAIR)");
  process.exit(1);
}

// --- Firebase Admin ---
if (admin.apps.length === 0) {
  admin.initializeApp({
    credential: admin.credential.cert({
      projectId: PROJECT_ID,
      clientEmail: CLIENT_EMAIL,
      privateKey: PRIVATE_KEY,
    }),
  });
}
const db = admin.firestore();

// --- Helpers ---
function ymd(tsMs) {
  const d = new Date(tsMs);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

async function fetchGT_Daily({ network, pair, limit = 365 }) {
  // Public tier: up to ~180 days; we'll request 365 and accept partial
  const url = `https://api.geckoterminal.com/api/v2/networks/${network}/pools/${pair}/ohlcv/day?limit=${limit}`;
  const res = await fetch(url, {
    headers: { "accept": "application/json", "x-api-key": GT_KEY },
  });

  if (res.status === 401) {
    // Public tier hard‑limit hit; treat as soft success with empty/partial data
    const txt = await res.text().catch(() => "");
    console.warn("[gt] 401 soft‑limit:", txt.slice(0, 200));
    return { candles: [], softLimited: true };
  }

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`[geckoterminal ${res.status}] ${txt}`);
  }

  const json = await res.json();
  // Shape: { data: { attributes: { ohlcv_list: [[ts, o,h,l,c,v], ...] }}}
  const list = json?.data?.attributes?.ohlcv_list || [];
  const candles = list.map((row) => {
    const [tsSec, o, h, l, c, v] = row;
    return { ts: Number(tsSec) * 1000, o: +o, h: +h, l: +l, c: +c, v: +v };
  }).filter(c => Number.isFinite(c.ts) && Number.isFinite(c.c));

  return { candles, softLimited: false };
}

async function upsertDaily(candles) {
  if (!candles.length) return 0;
  const batchSize = 450; // stay under 500 writes/batch
  let written = 0;

  for (let i = 0; i < candles.length; i += batchSize) {
    const slice = candles.slice(i, i + batchSize);
    const batch = db.batch();
    for (const k of slice) {
      const docId = ymd(k.ts);
      const ref = db.collection("zypto_prices_daily").doc(docId);
      batch.set(ref, {
        close: k.c,
        high: k.h,
        low: k.l,
        open: k.o,
        volumeUSD: k.v,
        source_gt: true,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
      written++;
    }
    await batch.commit();
  }
  return written;
}

(async function main() {
  try {
    console.log("[uni-v2→gt] backfill start pair=", PAIR, "network=", GT_NETWORK);

    const { candles, softLimited } = await fetchGT_Daily({ network: GT_NETWORK, pair: PAIR, limit: 365 });

    if (candles.length) {
      // Oldest first so last write is the newest (useful for client caches)
      candles.sort((a, b) => a.ts - b.ts);
      const wrote = await upsertDaily(candles);
      console.log(`[gt] wrote ${wrote} rows (total ${candles.length}) — up to ${ymd(candles.at(-1).ts)}`);
    } else {
      console.log("[gt] no candles returned (likely limit)");
    }

    if (softLimited) {
      console.log("[gt] reached public‑tier window; treating as success");
    }

    process.exit(0);
  } catch (e) {
    console.error("[gt] ERROR", String(e?.message || e));
    process.exit(1);
  }
})();
