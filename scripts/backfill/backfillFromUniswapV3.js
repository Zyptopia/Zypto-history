// scripts/backfill/backfillFromUniswapV3.js
// Fetch full daily price history from Uniswap v3 subgraph (via The Graph Gateway)
// and upsert into Firestore under collection `zypto_prices_daily`.
//
// USAGE (GitHub Action or local):
//   node scripts/backfill/backfillFromUniswapV3.js <erc20_token_address_lowercase>
//
// Requires env secrets (GH Actions -> repo "Secrets and variables" -> Actions):
//   FIREBASE_PROJECT_ID
//   FIREBASE_CLIENT_EMAIL
//   FIREBASE_PRIVATE_KEY           (use \n for newlines if set in GH Secrets)
//   THEGRAPH_API_KEY
//   UNIV3_SUBGRAPH_ID              (e.g. 5zvR82Qo...)

import admin from "firebase-admin";

const token = (process.argv[2] || process.env.ZYPTO_ADDR || "").toLowerCase();
if (!token) {
  console.error("[uni-v3] missing token address arg");
  process.exit(1);
}

const projectId = process.env.FIREBASE_PROJECT_ID;
const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;
const privateKeyRaw = process.env.FIREBASE_PRIVATE_KEY || "";
const privateKey = privateKeyRaw.replace(/\\n/g, "\n");
const graphKey = process.env.THEGRAPH_API_KEY;
const v3Id = process.env.UNIV3_SUBGRAPH_ID;

if (!projectId || !clientEmail || !privateKey || !graphKey || !v3Id) {
  console.error("[uni-v3] missing one of required env vars: FIREBASE_*, THEGRAPH_API_KEY, UNIV3_SUBGRAPH_ID");
  process.exit(1);
}

// Initialize Admin SDK (idempotent if already initialized)
if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert({
      projectId,
      clientEmail,
      privateKey,
    }),
  });
}
const db = admin.firestore();

const GW_BASE = `https://gateway.thegraph.com/api/${graphKey}/subgraphs/id/${v3Id}`;

async function gql(query, variables) {
  const res = await fetch(GW_BASE, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });
  const json = await res.json();
  if (!res.ok || json.errors) {
    console.error("[uni-v3] gql error:", json.errors || json);
    throw new Error("uniswap-v3 gql error");
  }
  return json.data;
}

const Q = /* GraphQL */ `#graphql
  query TokenDays($token: String!, $skip: Int!) {
    tokenDayDatas(
      first: 1000
      skip: $skip
      orderBy: date
      orderDirection: asc
      where: { token_: { id: $token } }
    ) {
      date
      priceUSD
      volumeUSD
    }
  }
`;

function yyyyMmDd(ts) {
  const d = new Date(ts);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const da = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${da}`;
}

async function backfill() {
  console.log("[uni-v3] backfill start token=", token);
  let all = [];
  let skip = 0;
  while (true) {
    const data = await gql(Q, { token, skip });
    const chunk = data?.tokenDayDatas || [];
    all = all.concat(chunk);
    console.log(`[uni-v3] fetched ${chunk.length} rows (total ${all.length})`);
    if (chunk.length < 1000) break;
    skip += 1000;
  }
  if (!all.length) {
    console.log("[uni-v3] no rows returned");
    return;
  }

  // Firestore batched upserts (max 500 per batch)
  const BATCH_LIMIT = 500;
  let batch = db.batch();
  let ops = 0;

  for (const row of all) {
    const ts = Number(row.date) * 1000;
    const id = yyyyMmDd(ts); // daily doc id
    const ref = db.collection("zypto_prices_daily").doc(id);
    batch.set(
      ref,
      {
        ts,
        uniV3: { priceUSD: Number(row.priceUSD) || null, volumeUSD: Number(row.volumeUSD) || null },
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
    ops++;
    if (ops % BATCH_LIMIT === 0) {
      await batch.commit();
      batch = db.batch();
      console.log(`[uni-v3] committed ${ops} docs...`);
    }
  }
  if (ops % BATCH_LIMIT !== 0) {
    await batch.commit();
  }
  console.log(`[uni-v3] backfill complete. wrote ${ops} daily docs.`);
}

backfill().catch((e) => {
  console.error("[uni-v3] ERROR", e?.message || e);
  process.exit(1);
});
