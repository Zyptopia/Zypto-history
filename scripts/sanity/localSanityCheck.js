// ===============================
// File: scripts/sanity/localSanityCheck.js
// Purpose: Quick local smoke test: writes & reads one document.
// Run: node scripts/sanity/localSanityCheck.js
// ===============================

const { init: initAdmin } = require('../firebaseAdmin');

(async () => {
  try {
    const { db } = initAdmin();
    const now = new Date();
    const id = now.toISOString();
    const ref = db.collection('zypto_history_sanity').doc(id);
    await ref.set({ ts: Date.now(), note: 'hello from local sanity' });
    const snap = await ref.get();
    console.log('[sanity] wrote + read:', snap.data());
    process.exit(0);
  } catch (e) {
    console.error('[sanity] failed:', e);
    process.exit(1);
  }
})();
