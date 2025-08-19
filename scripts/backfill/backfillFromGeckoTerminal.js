// ESM version — works with "type": "module" in package.json
// Usage (via GH Actions): node scripts/backfill/backfillFromGeckoTerminal.js <tokenAddress> <pairAddress>

import { initializeApp, cert } from "firebase-admin/app";
import { getFirestore, FieldValue } from "firebase-admin/firestore";

const projectId = process.env.FIREBASE_PROJECT_ID;
const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;
const privateKey = (process.env.FIREBASE_PRIVATE_KEY || "").replace(/\\n/g, "\n");

if (!projectId || !clientEmail || !privateKey) {
  console.error("[gt] missing Firebase envs");
  process.exit(1);
}

initializeApp({ credential: cert({ projectId, clientEmail, privateKey }) });
const db = getFirestore();

const token = (process.argv[2] || process.env.ZYPTO_ADDR || "").toLowerCase();
const pair = (process.argv[3] || process.env.UNIV2_PAIR || "").toLowerCase();
const network = (process.env.GT_NETWORK || "eth").toLowerCase();
const apiKey = process.env.GECKOTERMINAL_API_KEY || ""; // optional, helps with rate limits

if (!pair) {
  console.error("[gt] missing pair address");
  process.exit(1);
}

const GT_BASE = "https://api.geckoterminal.com/api/v2";

async function fetchPage(beforeTs) {
  const u = new URL(`${GT_BASE}/networks/${network}/pools/${pair}/ohlcv/day`);
  u.searchParams.set("aggregate", "1");
  u.searchParams.set("limit", "1000");
  if (beforeTs) u.searchParams.set("before_timestamp", String(beforeTs));

  const headers = { Accept: "application/json" };
  if (apiKey) headers["X-API-KEY"] = apiKey;

  const res = await fetch(u, { headers });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`geckoterminal ${res.status}: ${text}`);
  }
  const json = await res.json();
  const list = json?.data?.attributes?.ohlcv_list || [];
  return list;
}

function isoDay(tsSec) {
  return new Date(tsSec * 1000).toISOString().slice(0, 10);
}

async function writeBatch(rows) {
  const batch = db.batch();
  for (const row of rows) {
    const [ts, o, h, l, c, v] = row; // [unix, open, high, low, close, volume]
    const day = isoDay(ts);
    const ref = db.collection("zypto_prices_daily").doc(day);
    batch.set(
      ref,
      {
        geckoTerminal: { o, h, l, c, v, ts, pair, token, network },
        _updatedAt: FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
  }
  await batch.commit();
}

(async () => {
  try {
    console.log("[uni-v2→gt] backfill start pair=", pair, "network=", network);
    let before = Math.floor(Date.now() / 1000);
    let total = 0;

    while (true) {
      const rows = await fetchPage(before);
      if (!rows.length) break;
      await writeBatch(rows);
      total += rows.length;

      const lastTs = rows[rows.length - 1][0];
      console.log(
        `[gt] wrote ${rows.length} rows (total ${total}) — up to ${isoDay(lastTs)}`
      );

      before = lastTs - 1; // page backwards
      if (total >= 20000) break; // safety cap
      await new Promise((r) => setTimeout(r, 250)); // be polite
    }

    console.log(`[gt] done. total rows: ${total}`);
  } catch (e) {
    console.error("[gt] ERROR", e?.message || e);
    process.exit(1);
  }
})();
