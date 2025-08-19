// =============================
// File: scripts/sanity/localSanityCheck.js (ESM)
// Purpose: Quick test to confirm Admin init works and Firestore is writable.
// Usage (locally):
//   1) Put your serviceAccount.json in ./scripts/secrets/ (gitignored)
//   2) node scripts/sanity/localSanityCheck.js
// =============================

import { initAdmin } from '../firebaseAdmin.js';

const { admin, db } = initAdmin();
const ref = db.collection('sanity').doc('hello');
await ref.set({ ts: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
const snap = await ref.get();
console.log('[sanity] wrote + read:', snap.data());
