// scripts/backfill/backfillFromCoinGecko.js
// Backfill daily history from CoinGecko into Firestore.
// - Auto-uses PRO base when a COINGECKO_API_KEY is present.
// - Idempotent and chunked commits (<=490 writes per batch).
// - Writes: collection `zypto_prices_daily` with docs keyed by YYYY-MM-DD.

import admin from "firebase-admin";

const {
  FIREBASE_PROJECT_ID,
  FIREBASE_CLIENT_EMAIL,
  FIREBASE_PRIVATE_KEY,
  COINGECKO_API_KEY,
} = process.env;

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert({
      projectId: FIREBASE_PROJECT_ID,
      clientEmail: FIREBASE_CLIENT_EMAIL,
      privateKey: (FIREBASE_PRIVATE_KEY || "").replace(/\\n/g, "\n"),
    }),
  });
}

const db = admin.firestore();


const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function cgBase() {
  // If a PRO key is provided, we must use the pro-api hostname.
  return COINGECKO_API_KEY && COINGECKO_API_KEY.trim()
    ? "https://pro-api.coingecko.com"
    : "https://api.coingecko.com";
}

async function cgDaily({ chain, addr }) {
  const base = cgBase();
  const url = `${base}/api/v3/coins/${encodeURIComponent(
    chain
  )}/contract/${addr}/market_chart?vs_currency=usd&days=max&interval=daily&precision=6`;
  const headers = { Accept: "application/json" };
  if (COINGECKO_API_KEY) headers["x-cg-pro-api-key"] = COINGECKO_API_KEY;

  let last;
  for (let attempt = 0; attempt < 5; attempt++) {
    const res = await fetch(url, { headers });
    if (res.ok) return res.json();

    last = { status: res.status, text: await res.text() };
    // 400/401 likely indicate wrong base or bad key → fail fast
    if (res.status === 400 || res.status === 401) {
      throw new Error(`coingecko ${res.status}: ${last.text}`);
    }
    if (res.status === 429 || res.status >= 500) {
      await sleep((attempt + 1) * 800);
      continue;
    }
    break;
  }
  throw new Error(`coingecko ${last?.status}: ${last?.text}`);
}

function ymd(ts) {
  const d = new Date(ts);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

export async function main() {
  const addr = (process.env.ZYPTO_ADDR || process.env.INPUT_TOKEN_ADDRESS || "")
    .toLowerCase();
  const chain = process.env.INPUT_CHAIN || "ethereum";
  if (!addr) throw new Error("missing ZYPTO_ADDR/INPUT_TOKEN_ADDRESS");

  console.log("[backfill] start for %s on %s", addr, chain);

  const json = await cgDaily({ chain, addr });
  if (!json?.prices?.length) throw new Error("no prices array from CG");

  // json.prices = [ [ms, price], ... ] ascending order (daily)
  // Write the day's CLOSE (CG returns one datapoint per day for interval=daily)
  let wrote = 0;
  let pending = 0;
  let batch = db.batch();

  for (const [ms, price] of json.prices) {
    const day = ymd(ms);
    const ref = db.collection("zypto_prices_daily").doc(day);

    batch.set(
      ref,
      {
        t: admin.firestore.Timestamp.fromDate(
          new Date(Date.UTC(
            Number(day.slice(0, 4)),
            Number(day.slice(5, 7)) - 1,
            Number(day.slice(8, 10))
          ))
        ),
        priceUSD: Number(price),
        source: "coingecko",
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    wrote++;
    pending++;

    if (pending >= 490) {
      await batch.commit();
      console.log("[backfill] committed chunk up to", day);
      batch = db.batch();
      pending = 0;
    }
  }

  if (pending > 0) {
    await batch.commit();
  }

  console.log("[backfill] wrote %d daily docs", wrote);
}

if (process.argv[1]?.endsWith("backfillFromCoinGecko.js")) {
  main().catch((e) => {
    console.error("[backfill] ERROR", e?.message || e);
    process.exit(1);
  });
}
