// backfillFromUniswapV2.js
// Free daily history via Uniswap v2 hosted subgraph (no API key).
// - Queries tokenDayDatas for the ERC‑20 token
// - Upserts into Firestore collection: `zypto_prices_daily/<YYYY-MM-DD>`
// - Merges alongside any CoinGecko fields (cgUSD) and computes `priceUSD` as the average
//
// Usage (locally):
//   FIREBASE_PROJECT_ID=... FIREBASE_CLIENT_EMAIL=... FIREBASE_PRIVATE_KEY=... 
//   node scripts/backfill/backfillFromUniswapV2.js 0xTOKEN
//
// Usage (GitHub Actions): see workflow backfill-uniswap-v2.yml

import admin from "firebase-admin";

// ----- Firestore init from env (same pattern you used already) -----
function initFirestore() {
  const projectId = process.env.FIREBASE_PROJECT_ID;
  const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;
  let privateKey = process.env.FIREBASE_PRIVATE_KEY;
  if (!projectId || !clientEmail || !privateKey) {
    throw new Error("Missing FIREBASE_* env vars");
  }
  // GH/Actions often stores the key with literal \n; normalize to real newlines
  privateKey = privateKey.replace(/\\n/g, "\n");

  if (!admin.apps.length) {
    admin.initializeApp({
      credential: admin.credential.cert({ projectId, clientEmail, privateKey }),
    });
  }
  return admin.firestore();
}

// ----- Graph helpers -----
const UNISWAP_V2_HOSTED = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2";

async function gql(query, variables = {}) {
  const res = await fetch(UNISWAP_V2_HOSTED, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`uniswap-v2 gql ${res.status}: ${txt}`);
  }
  const json = await res.json();
  if (json.errors) {
    throw new Error(`uniswap-v2 gql errors: ${JSON.stringify(json.errors)}`);
  }
  return json.data;
}

const Q_TOKEN_DAY_DATAS = `#graphql
  query TDD($token: String!, $skip: Int!) {
    tokenDayDatas(
      first: 1000,
      skip: $skip,
      orderBy: date,
      orderDirection: asc,
      where: { token: $token }
    ) {
      date
      priceUSD
      totalLiquidityToken
      totalLiquidityUSD
    }
  }
`;

function tsToDay(ts) {
  const d = new Date(ts * 1000);
  // YYYY-MM-DD
  return d.toISOString().slice(0, 10);
}

function mean(arr) {
  const xs = arr.filter((n) => typeof n === "number" && isFinite(n));
  if (!xs.length) return undefined;
  return xs.reduce((a, b) => a + b, 0) / xs.length;
}

async function upsertDaily(db, day, updates) {
  const ref = db.collection("zypto_prices_daily").doc(day);
  const snap = await ref.get();
  const prev = snap.exists ? snap.data() : {};

  const cg = typeof prev.cgUSD === "number" ? prev.cgUSD : undefined;
  const uni = typeof updates.uniV2USD === "number" ? updates.uniV2USD : undefined;
  const priceUSD = mean([cg, uni]);

  const sources = new Set([...(prev.sources || [])]);
  if (cg != null) sources.add("coingecko");
  if (uni != null) sources.add("uniswap-v2");

  const payload = {
    ...prev,
    uniV2USD: uni,
    // Keep a canonical "priceUSD" for consumers (avg if two sources, else the one we have)
    ...(priceUSD != null ? { priceUSD } : {}),
    sources: Array.from(sources),
    updatedAt: Date.now(),
  };

  await ref.set(payload, { merge: true });
}

async function backfillFromUniswapV2(tokenAddr) {
  const token = String(tokenAddr).toLowerCase();
  const db = initFirestore();
  console.log(`[uni-v2] backfill start token=${token}`);

  let skip = 0;
  let total = 0;
  while (true) {
    const data = await gql(Q_TOKEN_DAY_DATAS, { token, skip });
    const rows = data?.tokenDayDatas || [];
    if (!rows.length) break;

    // Batch in chunks to keep Firestore writes snappy
    for (const r of rows) {
      const day = tsToDay(r.date);
      const uniV2USD = Number(r.priceUSD);
      if (uniV2USD && isFinite(uniV2USD)) {
        await upsertDaily(db, day, { uniV2USD });
        total++;
      }
    }
    skip += rows.length;
  }
  console.log(`[uni-v2] backfill wrote days: ${total}`);
}

// ---- main ----
const token = process.argv[2] || process.env.ZYPTO_ADDR;
if (!token) {
  console.error("Usage: node scripts/backfill/backfillFromUniswapV2.js <erc20 token addr>");
  process.exit(1);
}
backfillFromUniswapV2(token).catch((e) => {
  console.error("[uni-v2] ERROR", e?.message || e);
  process.exit(1);
});
