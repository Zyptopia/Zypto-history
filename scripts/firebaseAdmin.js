// ===============================
// File: scripts/firebaseAdmin.js
// Purpose: Initialize Firebase Admin from either (A) env secrets (GitHub) or (B) local JSON (your new file).
// ===============================

const admin = require('firebase-admin');

let app;

function getCreds() {
  // Prefer environment vars (GitHub Actions). Fall back to local JSON for dev.
  const pid = process.env.FIREBASE_PROJECT_ID;
  const clientEmail = process.env.FIREBASE_CLIENT_EMAIL;
  const pk = process.env.FIREBASE_PRIVATE_KEY;

  if (pid && clientEmail && pk) {
    return {
      projectId: pid,
      clientEmail,
      // IMPORTANT: when the key is stored in a GitHub secret, newlines are often \n. Fix them.
      privateKey: pk.replace(/\\n/g, '\n'),
    };
  }

  // Local development: use the file you just placed at scripts/secrets/serviceAccount.json
  // (Make sure scripts/secrets/ is in .gitignore)
  // eslint-disable-next-line import/no-dynamic-require, global-require
  const local = require('./secrets/serviceAccount.json');
  return {
    projectId: local.project_id,
    clientEmail: local.client_email,
    privateKey: local.private_key,
  };
}

function init() {
  if (!admin.apps.length) {
    const creds = getCreds();
    app = admin.initializeApp({
      credential: admin.credential.cert(creds),
    });
  } else {
    app = admin.app();
  }
  const db = admin.firestore();
  return { admin, app, db };
}

module.exports = { init };