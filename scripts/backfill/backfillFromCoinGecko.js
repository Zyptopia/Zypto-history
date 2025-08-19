// scripts/backfill/backfillFromCoinGecko.js
// Fetch full-price history from CoinGecko and store missing daily docs in Firestore
// Works with both Demo and Pro keys. Set env COINGECKO_IS_PRO="true" for Pro, otherwise Demo.

import admin from "firebase-admin";

// --- Env ---
const PROJECT_ID = process.env.FIREBASE_PROJECT_ID;
const CLIENT_EMAIL = process.env.FIREBASE_CLIENT_EMAIL;
const PRIVATE_KEY = (process.env.FIREBASE_PRIVATE_KEY || "").replace(/\\n/g, "\n");

const ADDR = (process.env.ZYPTO_ADDR || "").toLowerCase();
const CHAIN = process.env.ZYPTO_CHAIN || "ethereum";

const CG_KEY = process.env.COINGECKO_API_KEY || "";
const CG_IS_PRO = (process.env.COINGECKO_IS_PRO || "false").toLowerCase() === "true";
const CG_BASE = process.env.COINGECKO_BASE || (CG_IS_PRO
  ? "https://pro-api.coingecko.com"
  : "https://api.coingecko.com");

if (!PROJECT_ID || !CLIENT_EMAIL || !PRIVATE_KEY) {
  console.error("[backfill] Missing Firebase credentials env (FIREBASE_*)");
  process.exit(1);
}
if (!ADDR) {
  console.error("[backfill] Missing ZYPTO_ADDR env");
  process.exit(1);
}

// --- Firebase Admin ---
if (!admin.apps.length) {
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
const toDateKey = (ms) => new Date(ms).toISOString().slice(0, 10); // YYYY-MM-DD (UTC)

async function getCGPricesDaysMax({ chain, addr }) {
  const url = `${CG_BASE}/api/v3/coins/${encodeURIComponent(chain)}/contract/${addr}/market_chart?vs_currency=usd&days=max&precision=6&interval=daily`;
  const headers = {};
  if (CG_KEY) {
    // CoinGecko requires different header names for Demo vs Pro
    headers[CG_IS_PRO ? "x-cg-pro-api-key" : "x-cg-demo-api-key"] = CG_KEY;
  }
  const res = await fetch(url, { headers });
  if (!res.ok) {
    const text = await res.text();
    console.error("[backfill] ERROR coingecko", res.status + ":", text);
    throw new Error("coingecko " + res.status);
  }
  const json = await res.json();
  // Expect arrays: prices[[ts, price]...], total_volumes[[ts, vol]...]
  const prices = Array.isArray(json?.prices) ? json.prices : [];
  const vols = Array.isArray(json?.total_volumes) ? json.total_volumes : [];

  // Build map by date (UTC)
  const byDate = new Map();
  for (const [ts, price] of prices) {
    const k = toDateKey(ts);
    const exist = byDate.get(k) || {};
    exist.priceUSD = Number(price);
    byDate.set(k, exist);
  }
  for (const [ts, vol] of vols) {
    const k = toDateKey(ts);
    const exist = byDate.get(k) || {};
    exist.volumeUSD = Number(vol);
    byDate.set(k, exist);
  }
  return byDate; // Map<YYYY-MM-DD, {priceUSD, volumeUSD?}>
}

async function backfillDaily() {
  console.log(`[backfill] start for ${ADDR} on ${CHAIN}`);
  const byDate = await getCGPricesDaysMax({ chain: CHAIN, addr: ADDR });

  // Fetch existing docs (keys) to avoid rewriting
  const snap = await db.collection("zypto_prices_daily").get();
  const have = new Set();
  snap.forEach((d) => have.add(d.id));

  let written = 0;
  let batch = db.batch();
  let ops = 0;

  for (const [k, v] of byDate.entries()) {
    if (have.has(k)) continue; // skip existing day
    const ref = db.collection("zypto_prices_daily").doc(k);
    batch.set(ref, {
      priceUSD: v.priceUSD,
      volumeUSD: v.volumeUSD ?? null,
      source: "coingecko",
      ts: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    ops++;
    if (ops >= 450) { // keep well under 500 op limit
      await batch.commit();
      written += ops;
      ops = 0;
      batch = db.batch();
    }
  }

  if (ops > 0) {
    await batch.commit();
    written += ops;
  }

  console.log(`[backfill] done. wrote ${written} new days (existing skipped: ${have.size})`);
}

backfillDaily().catch((e) => {
  console.error(e);
  process.exit(1);
});
