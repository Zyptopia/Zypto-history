// scripts/backfill/backfillFromGeckoTerminal.js
// Backfill daily OHLCV using GeckoTerminal Onchain API (CoinGecko).
// - Works by paging backwards with `before_timestamp` until no more data.
// - Writes NON-DESTRUCTIVE fields under `geckoTerminal` in `zypto_prices_daily/{YYYY-MM-DD}`.
//
// Env needed (GitHub Actions → Repo secrets):
//   FIREBASE_PROJECT_ID
//   FIREBASE_CLIENT_EMAIL
//   FIREBASE_PRIVATE_KEY   (use the multiline value, with \n preserved)
// Optional:
//   GECKOTERMINAL_API_KEY  (you can reuse your CoinGecko key; if missing, it will try without)
//
// Usage (local):
//   node scripts/backfill/backfillFromGeckoTerminal.js 0xPAIRADDRESS
// Usage (GH Actions): see .github/workflows/backfill-geckoterminal.yml

const admin = require("firebase-admin");

const TOKEN_ADDR = (process.argv[2] || process.env.ZYPTO_ADDR || "0x7a65cb87f596caf31a4932f074c59c0592be77d7").toLowerCase();
const PAIR_ADDR = (process.argv[3] || process.env.UNIV2_PAIR || "0x1ecb460a532c1d76937bedbadf7d333da30255a4").toLowerCase();
const NETWORK_SLUG = process.env.GT_NETWORK || "eth"; // Ethereum mainnet on GeckoTerminal is usually "eth"

function initFirebase() {
  if (admin.apps.length) return admin.app();
  const projectId = process.env.FIREBASE_PROJECT_ID;
  const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;
  let privateKey = process.env.FIREBASE_PRIVATE_KEY;
  if (!projectId || !clientEmail || !privateKey) {
    throw new Error("Missing FIREBASE_* envs");
  }
  // Fix escaped newlines if needed
  if (privateKey.includes("\\n")) privateKey = privateKey.replace(/\\n/g, "\n");

  admin.initializeApp({
    credential: admin.credential.cert({ projectId, clientEmail, privateKey }),
  });
  return admin.app();
}

function ymdFromUnix(tsSec) {
  const d = new Date(tsSec * 1000);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

async function fetchCandlesDay(pair, beforeTs) {
  const params = new URLSearchParams({ aggregate: "1", limit: "1000" });
  if (beforeTs) params.set("before_timestamp", String(beforeTs));
  const url = `https://api.geckoterminal.com/api/v2/networks/${NETWORK_SLUG}/pools/${pair}/ohlcv/day?${params.toString()}`;

  const headers = { Accept: "application/json" };
  const k = process.env.GECKOTERMINAL_API_KEY;
  if (k) headers["x-api-key"] = k;

  const res = await fetch(url, { headers });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`geckoterminal ${res.status}: ${text.slice(0, 200)}`);
  }
  const json = await res.json();

  // Shape: data.attributes.ohlcv_list is commonly [[ts, o, h, l, c, v], ...]
  const attrs = json && json.data && json.data.attributes;
  const raw = (attrs && (attrs.ohlcv_list || attrs.ohlcv || attrs.candles)) || [];

  const rows = raw.map((row) => {
    if (Array.isArray(row)) {
      const [ts, o, h, l, c, v] = row;
      return { ts: Number(ts), o: Number(o), h: Number(h), l: Number(l), c: Number(c), v: Number(v || 0) };
    }
    // object-style safety
    return {
      ts: Number(row.timestamp || row.t || 0),
      o: Number(row.open || row.o),
      h: Number(row.high || row.h),
      l: Number(row.low || row.l),
      c: Number(row.close || row.c),
      v: Number(row.volume || row.v || 0),
    };
  }).filter(r => r.ts && isFinite(r.c));

  return rows;
}

async function backfillDaily() {
  console.log("[gt] backfill start pair=", PAIR_ADDR, "network=", NETWORK_SLUG);
  initFirebase();
  const db = admin.firestore();

  let before = Math.floor(Date.now() / 1000);
  let total = 0;

  while (true) {
    const batch = await fetchCandlesDay(PAIR_ADDR, before);
    if (!batch.length) break;

    // Next page: set before to (oldest ts - 1)
    const oldest = batch[batch.length - 1].ts;
    before = oldest - 1;

    const writes = batch.map((row) => {
      const ymd = ymdFromUnix(row.ts);
      const ref = db.collection("zypto_prices_daily").doc(ymd);
      return ref.set({
        geckoTerminal: {
          pair: PAIR_ADDR,
          token: TOKEN_ADDR,
          o: row.o,
          h: row.h,
          l: row.l,
          c: row.c,
          v: row.v,
          ts: row.ts,
          network: NETWORK_SLUG,
        },
        // do not overwrite your canonical price if you have one already
        _updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
    });

    await Promise.all(writes);
    total += batch.length;
    console.log(`[gt] wrote ${batch.length} rows (total ${total}) — up to ${ymdFromUnix(before+1)}`);

    // Small polite delay to avoid any rate limiting
    await new Promise(r => setTimeout(r, 500));
  }

  console.log(`[gt] done. total rows: ${total}`);
}

backfillDaily().catch((e) => {
  console.error(e);
  process.exit(1);
});
