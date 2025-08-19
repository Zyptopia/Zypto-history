// =============================
// File: scripts/firebaseAdmin.js (ESM)
// Purpose: Initialize Firebase Admin SDK from ENV (GitHub Actions) OR from a
//          local service-account JSON (dev). Includes the "newline fix" when
//          using env vars so multi-line PEM keys work.
// =============================

import admin from 'firebase-admin';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

let inited = false;

function loadLocalServiceAccount() {
  try {
    const __filename = fileURLToPath(import.meta.url);
    const __dirname = path.dirname(__filename);
    const localPath = path.join(__dirname, 'secrets', 'serviceAccount.json');
    const json = fs.readFileSync(localPath, 'utf8');
    return JSON.parse(json);
  } catch (e) {
    throw new Error(`serviceAccount.json missing or unreadable at scripts/secrets/. ${e?.message || e}`);
  }
}

export function initAdmin() {
  if (!inited && !admin.apps.length) {
    const pid = process.env.FIREBASE_PROJECT_ID;
    const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;
    let privateKey = process.env.FIREBASE_PRIVATE_KEY;

    if (pid && clientEmail && privateKey) {
      // GitHub/ENV secrets often contain literal "\\n" sequences. Convert to real newlines.
      if (privateKey.includes('\\n')) privateKey = privateKey.replace(/\\n/g, '\n');
      admin.initializeApp({
        credential: admin.credential.cert({ projectId: pid, clientEmail, privateKey }),
      });
    } else {
      // Local dev fallback (./scripts/secrets/serviceAccount.json)
      const serviceAccount = loadLocalServiceAccount();
      admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
      });
    }
    inited = true;
  }
  const db = admin.firestore();
  return { admin, db };
}

// Optional alias so existing code importing { init } still works
export { initAdmin as init };
export default initAdmin;