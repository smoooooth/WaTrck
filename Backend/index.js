// functions/index.js
const functions = require('firebase-functions');
const admin = require('firebase-admin');
const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');
const express = require('express');
const cors = require('cors');
const axios = require('axios');

admin.initializeApp();
const db = admin.firestore();
const secretClient = new SecretManagerServiceClient();


const CHATWOOT_WEBHOOK_URL = "https://wa-chat.auresealtd.com/webhooks/whatsapp/+971585927034";



// ---------- CONFIG ---------- Allowed Websites - allowed domains
const DEFAULT_LEAD_VALUE = 70; // default immediate conversion value
const ALLOWED_ORIGINS = [
  'https://aidattrxta.webflow.io',
  'https://www.davinci-tower-pagani.com',
  'https://davtowerdubai.webflow.io',
  'https://aida-muscat-oman.com'
];
// ----------------------------

// Helper: normalize token
function normalizeToken(t) {
  if (!t || typeof t !== 'string') return '';
  return t.toUpperCase().replace(/[^A-Z0-9]/g, '');
}

// Helper: simple PII redaction (optional; not used by default)
function redactPII(text) {
  if (!text || typeof text !== 'string') return '';
  text = text.replace(/\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/ig, '[email]');
  text = text.replace(
    /(\+?\d{1,3}[-.\s]?)?(\(?\d{2,4}\)?[-.\s]?){1,3}\d{3,4}/g,
    '[phone]'
  );
  return text.replace(/\s{2,}/g, ' ').trim();
}

// Timestamp -> ISO string helper
function tsToIso(ts) {
  if (!ts) return new Date().toISOString();
  if (typeof ts === 'object' && ts._seconds !== undefined) {
    const millis =
      Number(ts._seconds) * 1000 + Math.floor((ts._nanoseconds || 0) / 1e6);
    return new Date(millis).toISOString();
  }
  if (ts && typeof ts.toDate === 'function') return ts.toDate().toISOString();
  try {
    return new Date(ts).toISOString();
  } catch (e) {
    return new Date().toISOString();
  }
}

// Secret Manager: get verify token (caches in memory)
let cachedVerifyToken = null;
async function getVerifyToken() {
  if (cachedVerifyToken) return cachedVerifyToken;
  const projectId = process.env.GCP_PROJECT || process.env.GCLOUD_PROJECT;
  if (!projectId) throw new Error('GCP project id not set in env');
  const name = `projects/${projectId}/secrets/whatsapp_verify_token/versions/latest`;
  const [version] = await secretClient.accessSecretVersion({ name });
  const payload = version.payload.data.toString('utf8');
  cachedVerifyToken = payload;
  return cachedVerifyToken;
}

// Secret Manager helper for exports secret (caches in memory)
let cachedExportSecret = null;
async function getExportSecret() {
  if (cachedExportSecret) return cachedExportSecret;
  const projectId = process.env.GCP_PROJECT || process.env.GCLOUD_PROJECT;
  if (!projectId) throw new Error('GCP project id not set in env');
  const name = `projects/${projectId}/secrets/conversions_exports_secrect/versions/latest`;
  const [version] = await secretClient.accessSecretVersion({ name });
  const payload = version.payload.data.toString('utf8');
  cachedExportSecret = payload;
  return cachedExportSecret;
}

// --------------------- SAVE TOKEN ---------------------
exports.saveToken = functions.https.onRequest(async (req, res) => {
  const origin = req.get('origin') || '';
  if (ALLOWED_ORIGINS.includes(origin)) {
    res.set('Access-Control-Allow-Origin', origin);
    res.set('Access-Control-Allow-Credentials', 'true');
  } else {
    res.set('Access-Control-Allow-Origin', 'null');
  }
  res.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', 'Content-Type, Accept, X-Requested-With');

  if (req.method === 'OPTIONS') return res.status(204).send('');

  try {
    if (req.method !== 'POST')
      return res.status(405).json({ error: 'Method not allowed' });

    const body =
      req.body && typeof req.body === 'object'
        ? req.body
        : JSON.parse(req.rawBody ? req.rawBody.toString() : '{}');

    const rawToken = body.token || '';
    const token =
      normalizeToken(rawToken) ||
      Math.random().toString(36).slice(2, 9).toUpperCase();
    const gclid = (body.gclid && String(body.gclid)) || null;
    const ts = body.ts || admin.firestore.FieldValue.serverTimestamp();
    const page = body.page || null;
    const projectId =
      (body.projectId && String(body.projectId).trim()) || 'default';
    const ctaId = body.ctaId || null;
    const leadValueEstimate =
      typeof body.lead_value_estimate === 'number'
        ? body.lead_value_estimate
        : null;
    const conversion_name =
      body.conversion_name && String(body.conversion_name).trim()
        ? String(body.conversion_name).trim()
        : null;
    const usedFlag = body.used === true;
    const campaignId = (body.campaign_id && String(body.campaign_id)) || null;

    const clickRef = db.doc(`projects/${projectId}/clicks/${token}`);

    const docData = {
      token,
      gclid,
      page,
      ts,
      projectId,
      ctaId,
      used: usedFlag ? true : false,
      lead_value_estimate: leadValueEstimate,
      conversion_name: conversion_name || null,
      conversion_value_initial: null,
      conversion_value_final: null,
      conversion_value_uploaded: false,
      conversion_value_uploaded_at: null,
      conversion_value_source: null,
      upload_version: 0,
      google_campaign_id: campaignId
    };

    if (usedFlag) {
      const initialVal =
        typeof body.conversion_value_initial === 'number'
          ? body.conversion_value_initial
          : typeof leadValueEstimate === 'number' && leadValueEstimate > 0
          ? leadValueEstimate
          : DEFAULT_LEAD_VALUE;

      docData.used_at = admin.firestore.FieldValue.serverTimestamp();
      docData.conversion_value_initial = initialVal;
      docData.conversion_value_source = 'form_submission';
      docData.conversion_value_uploaded = false;
    }

    await clickRef.set(docData, { merge: true });

    await db.doc(`tokenIndex/${token}`).set(
      {
        token,
        projectId,
        clickPath: `projects/${projectId}/clicks/${token}`,
        created_at: admin.firestore.FieldValue.serverTimestamp()
      },
      { merge: true }
    );

    return res.status(200).json({ ok: true, token, projectId });
  } catch (err) {
    console.error('saveToken error', err);
    return res.status(500).json({ error: String(err) });
  }
});

// --------------------- WHATSAPP WEBHOOK ---------------------
// --------------------- WHATSAPP WEBHOOK (FINAL - ORGANIZED BACKUP) ---------------------
exports.whatsappWebhook = functions.https.onRequest(async (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');

  // 1. GET Request (Verification) - Keeps Meta happy
  if (req.method === 'GET') {
    const tokenFromMeta = req.query['hub.verify_token'];
    const challenge = req.query['hub.challenge'];
    try {
      const expected = await getVerifyToken();
      if (tokenFromMeta && expected && tokenFromMeta === expected) {
        return res.status(200).send(challenge);
      } else {
        return res.status(403).send('verification failed');
      }
    } catch (err) {
      console.error('whatsappWebhook verification error', err);
      return res.status(500).send('server error');
    }
  }

  // 2. POST Request (Incoming Message)
  if (req.method === 'POST') {
    try {
      // --- A. CHATWOOT BRIDGE (Forwarding) ---
      try {
        const signature = req.get('x-hub-signature-256');
        if (req.rawBody && CHATWOOT_WEBHOOK_URL) {
           // Fire and forget (don't await) to keep things fast
           axios.post(CHATWOOT_WEBHOOK_URL, req.rawBody, {
            headers: { 'Content-Type': 'application/json', 'X-Hub-Signature-256': signature }
          }).catch(err => console.error('Chatwoot Forward Error (Ignored):', err.message));
        }
      } catch (e) { console.error('Bridge Setup Error:', e); }


      // --- B. PARSE MESSAGE ---
      const body = req.body && typeof req.body === 'object' ? req.body : JSON.parse(req.rawBody ? req.rawBody.toString() : '{}');
      
      let messages = [];
      if (Array.isArray(body.entry)) {
        for (const entry of body.entry) {
          if (!entry.changes) continue;
          for (const ch of entry.changes) {
            const val = ch.value || {};
            if (Array.isArray(val.messages)) messages = messages.concat(val.messages);
          }
        }
      }

      if (!messages.length) return res.status(200).send('no messages');

      // --- C. PROCESS LOOP ---
      for (const msg of messages) {
        try {
          const from = msg.from || 'unknown'; // The Phone Number
          const msgId = msg.id || msg._id || 'no-id';
          const text = (msg.text && msg.text.body) || '[Media/Other]';
          const type = msg.type || 'unknown';
          const timestamp = msg.timestamp || Date.now() / 1000;

          // <--- 1. ALERT TEST TRIGGER (Keep this for testing) --->
          if (text === 'ForceTestError123') {
             console.error("Manual Test Error Triggered");
             throw new Error("Alert System Test");
          }

          // <--- 2. ORGANIZED FIRESTORE BACKUP (The Smart Way) --->
          if (msgId && from !== 'unknown') {
            const userDocRef = db.collection('whatsapp_conversations').doc(from);
            const messagesCollection = userDocRef.collection('messages');

            // Batch write: Update the parent "User" doc AND save the "Message"
            const batch = db.batch();

            // 1. Update the "Phone Book" entry (Parent Doc)
            // This lets you see a list of users and when they last messaged
            batch.set(userDocRef, {
                last_active: admin.firestore.FieldValue.serverTimestamp(),
                last_message_preview: text.substring(0, 50), // First 50 chars
                phone_number: from
            }, { merge: true });

            // 2. Save the actual message in the sub-collection
            const messageDocRef = messagesCollection.doc(msgId);
            batch.set(messageDocRef, {
                from: from,
                msg_id: msgId,
                text_body: text,
                message_type: type,
                meta_timestamp: timestamp,
                stored_at: admin.firestore.FieldValue.serverTimestamp(),
                raw_payload: msg // Full backup of raw data
            }, { merge: true });

            await batch.commit();
          }

          // <--- 3. YOUR EXISTING TOKEN LOGIC (Keep this exactly as is) --->
          const m = text.match(/#([A-Z0-9]{4,12})/i);
          const rawToken = m ? m[1] : null;
          const token = rawToken ? normalizeToken(rawToken) : null;

          if (!token) {
            // Only log if it's NOT media/status update to reduce noise
            if(type === 'text') console.log('no token in message, skipping logic');
            continue; 
          }

          const indexRef = db.doc(`tokenIndex/${token}`);
          const indexSnap = await indexRef.get();
          if (!indexSnap.exists) continue;
          
          const indexData = indexSnap.data();
          const projectIdFound = indexData.projectId;
          if (!projectIdFound) continue;

          const clickRef = db.doc(`projects/${projectIdFound}/clicks/${token}`);
          
          await clickRef.update({
            used: true,
            used_at: admin.firestore.FieldValue.serverTimestamp(),
            whatsapp_from: from,
            whatsapp_msg_id: msgId,
            conversion_value_source: 'whatsapp_message'
          });
          
          console.log('Token logic executed for', token);

        } catch (innerErr) {
          console.error('Error processing single message:', innerErr);
        }
      }

      return res.status(200).send('EVENT_RECEIVED');
    } catch (err) {
      console.error('Webhook Fatal Error:', err);
      return res.status(500).send('server error');
    }
  }

  return res.status(405).send('method not allowed');
});






// --------------------- EXPRESS APP (exports API) ---------------------
const app = express();
app.use(cors({ origin: false }));

async function requireSecret(req, res, next) {
  try {
    const provided = (
      req.query.secret ||
      req.headers['x-export-secret'] ||
      ''
    ).toString();
    const configured = await getExportSecret();
    if (!configured) {
      console.error('EXPORT secret not found in Secret Manager');
      return res.status(500).json({ error: 'Server misconfigured' });
    }
    if (!provided || provided !== configured) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    return next();
  } catch (err) {
    console.error('requireSecret error', err);
    return res.status(500).json({ error: 'Server error' });
  }
}



/**
 * GET /exports/pending?project=<project>&secret=<secret>
 * Returns list of pending conversions to append to sheet.
 * Returns docs that need initial upload or adjustments (conversion_value_uploaded !== true)
 */

app.get('/exports/pending', requireSecret, async (req, res) => {
  try {
    // project may be optional now
    const project = req.query.project || null;
    const limit = parseInt(req.query.limit || '500', 10);
    const debugMode = (req.query.debug === '1' || req.query.debug === 'true');

    let snapshot;
    let totalSeen = 0;
    let totalKept = 0;
    const results = [];
    const debugRows = [];

    if (project) {
      // per-project query (existing behavior)
      const basePath = `projects/${project}/clicks`;
      const colRef = db.collection(basePath)
        .where('used', '==', true)
        .orderBy('used_at', 'asc')
        .limit(limit);
      snapshot = await colRef.get();

      snapshot.forEach(doc => {
        totalSeen++;
        const data = doc.data();
        if (!data) {
          if (debugMode) debugRows.push({ id: doc.id, reason: 'no-data' });
          return;
        }
        // (keep the same inclusion logic you already have)
        const hasSalesTrigger = (data.sales_sheet_quality_uploaded === false) && (typeof data.sales_sheet_updated_quality === 'number');
        const wantBase = data.conversion_value_uploaded !== true;
        if (!wantBase && !hasSalesTrigger) {
          if (debugMode) debugRows.push({ id: doc.id, wantBase: !!wantBase, hasSalesTrigger: !!hasSalesTrigger });
          return;
        }
        // Build convInitial etc (same as existing)
        const convInitial =
          (typeof data.conversion_value_initial === 'number' && !isNaN(data.conversion_value_initial)) ? data.conversion_value_initial :
          (typeof data.lead_value_estimate === 'number' && !isNaN(data.lead_value_estimate)) ? data.lead_value_estimate :
          DEFAULT_LEAD_VALUE;

        results.push({
          project: project,
          gclid: (typeof data.gclid === 'string') ? data.gclid : '',
          conversion_time: tsToIso(data.used_at || data.ts || null),
          conversion_value_initial: convInitial,
          conversion_value: convInitial,
          conversion_value_final: (data.conversion_value_final !== undefined ? data.conversion_value_final : null),
          conversion_name: (data.conversion_name && String(data.conversion_name).trim()) ? String(data.conversion_name).trim() : null,
          upload_version: (typeof data.upload_version === 'number') ? data.upload_version : (data.upload_version || 0),
          conversion_value_source: (data.conversion_value_source || null),
          original_conversion_time: (data.original_conversion_time ? tsToIso(data.original_conversion_time) : null),
          order_id: doc.id,
          conversion_name_sales: (data.conversion_name_sales && String(data.conversion_name_sales).trim()) ? String(data.conversion_name_sales).trim() : null,
          sales_sheet_updated_quality: (typeof data.sales_sheet_updated_quality === 'number') ? data.sales_sheet_updated_quality : null,
          sales_sheet_quality_uploaded: (typeof data.sales_sheet_quality_uploaded === 'boolean') ? data.sales_sheet_quality_uploaded : null,
          quality_status: (data.quality_status || null),
          _wantBase: !!wantBase,
          _wantSales: !!hasSalesTrigger
        });
        totalKept++;
      });

      console.log(`exports/pending (per-project): project=${project} scanned=${totalSeen} returned=${totalKept}`);
      if (debugMode) return res.json({ debug: true, project, scanned: totalSeen, returned: totalKept, sample_debug_rows: debugRows.slice(0,200), results_count: results.length });
      return res.json(results);
    } else {
      // global query across all projects using collectionGroup
      // NOTE: collectionGroup requires proper indexing if you orderBy used_at
      const colRef = db.collectionGroup('clicks')
        .where('used', '==', true)
        .orderBy('used_at', 'asc')
        .limit(limit);

      snapshot = await colRef.get();
      snapshot.forEach(doc => {
        totalSeen++;
        const data = doc.data();
        if (!data) {
          if (debugMode) debugRows.push({ id: doc.id, reason: 'no-data', path: doc.ref.path });
          return;
        }

        // Inclusion logic must be the same as before:
        const hasSalesTrigger = (data.sales_sheet_quality_uploaded === false) && (typeof data.sales_sheet_updated_quality === 'number');
        const wantBase = data.conversion_value_uploaded !== true;
        if (!wantBase && !hasSalesTrigger) {
          if (debugMode) debugRows.push({
            id: doc.id,
            path: doc.ref.path,
            wantBase: !!wantBase,
            hasSalesTrigger: !!hasSalesTrigger,
            conversion_value_uploaded: data.conversion_value_uploaded
          });
          return;
        }

        // determine project id: try doc path first, fallback to data.projectId
        let projectIdFromDoc = null;
        try {
          // doc.ref.path like "projects/{projectId}/clicks/{docId}"
          const parent = doc.ref.parent; // clicks collection ref
          const projRef = parent ? parent.parent : null;
          projectIdFromDoc = projRef && projRef.id ? projRef.id : null;
        } catch (e) {
          projectIdFromDoc = null;
        }
        if (!projectIdFromDoc && data.projectId) projectIdFromDoc = data.projectId;

        const convInitial =
          (typeof data.conversion_value_initial === 'number' && !isNaN(data.conversion_value_initial)) ? data.conversion_value_initial :
          (typeof data.lead_value_estimate === 'number' && !isNaN(data.lead_value_estimate)) ? data.lead_value_estimate :
          DEFAULT_LEAD_VALUE;

        results.push({
          project: projectIdFromDoc,
          gclid: (typeof data.gclid === 'string') ? data.gclid : '',
          conversion_time: tsToIso(data.used_at || data.ts || null),
          conversion_value_initial: convInitial,
          conversion_value: convInitial,
          conversion_value_final: (data.conversion_value_final !== undefined ? data.conversion_value_final : null),
          conversion_name: (data.conversion_name && String(data.conversion_name).trim()) ? String(data.conversion_name).trim() : null,
          upload_version: (typeof data.upload_version === 'number') ? data.upload_version : (data.upload_version || 0),
          conversion_value_source: (data.conversion_value_source || null),
          original_conversion_time: (data.original_conversion_time ? tsToIso(data.original_conversion_time) : null),
          order_id: doc.id,
          conversion_name_sales: (data.conversion_name_sales && String(data.conversion_name_sales).trim()) ? String(data.conversion_name_sales).trim() : null,
          sales_sheet_updated_quality: (typeof data.sales_sheet_updated_quality === 'number') ? data.sales_sheet_updated_quality : null,
          sales_sheet_quality_uploaded: (typeof data.sales_sheet_quality_uploaded === 'boolean') ? data.sales_sheet_quality_uploaded : null,
          quality_status: (data.quality_status || null),
          _wantBase: !!wantBase,
          _wantSales: !!hasSalesTrigger
        });
        totalKept++;
      });

      console.log(`exports/pending (global): scanned=${totalSeen} returned=${totalKept}`);
      if (debugMode) {
        return res.json({ debug: true, scanned: totalSeen, returned: totalKept, sample_debug_rows: debugRows.slice(0,200), results_count: results.length });
      }
      return res.json(results);
    }
  } catch (err) {
    console.error('exports/pending error', err);
    return res.status(500).json({ error: String(err) });
  }
});














/**
 * GET /exports/adjustments-pending?project=<project>&secret=<secret>
 * Returns docs that need value adjustments exported (per upload_version).
 */
// Replace the existing handler for /exports/adjustments-pending with this function

app.get('/exports/adjustments-pending', requireSecret, async (req, res) => {
  try {
    // project is optional now; when missing we query across all projects (collectionGroup)
    const project = req.query.project || null;
    const limit = parseInt(req.query.limit || '500', 10);
    const debugMode = (req.query.debug === '1' || req.query.debug === 'true');

    let snapshot;
    if (project) {
      // existing single-project behavior
      const basePath = `projects/${project}/clicks`;
      const colRef = db.collection(basePath);
      const q = colRef
        .where('used', '==', true)
        .orderBy('used_at', 'asc')
        .limit(limit);
      snapshot = await q.get();
    } else {
      // global behavior: collectionGroup across all 'clicks' subcollections
      // NOTE: ordering by used_at across collectionGroup may require an index in Firestore console
      const q = db.collectionGroup('clicks')
        .where('used', '==', true)
        .orderBy('used_at', 'asc')
        .limit(limit);
      snapshot = await q.get();
    }

    const results = [];
    const debugRows = [];
    let totalSeen = 0;
    let totalKept = 0;

    snapshot.forEach(doc => {
      totalSeen++;
      const data = doc.data();
      if (!data) {
        if (debugMode) debugRows.push({ id: doc.id, reason: 'no-data' });
        return;
      }

      // determine projectId: if single-project requested, use it; else derive from path
      let projectIdFound = project;
      if (!projectIdFound) {
        // doc.ref.path is like "projects/<projectId>/clicks/<docId>"
        const parts = (doc.ref && doc.ref.path) ? doc.ref.path.split('/') : [];
        if (parts.length >= 2 && parts[0] === 'projects') {
          projectIdFound = parts[1];
        } else {
          projectIdFound = null;
        }
      }

      // must have gclid for adjustments endpoint (sheet only cares about gclid-based rows)
      const gclid = typeof data.gclid === 'string' ? data.gclid.trim() : '';
      if (!gclid) {
        if (debugMode) debugRows.push({ id: doc.id, reason: 'no-gclid', project: projectIdFound });
        return;
      }

      // parse numeric values robustly
      const parseNum = v => {
        if (v === null || v === undefined || v === '') return null;
        if (typeof v === 'number' && Number.isFinite(v)) return v;
        const p = Number(v);
        return Number.isFinite(p) ? p : null;
      };

      const uploadVersion =
        typeof data.upload_version === 'number'
          ? data.upload_version
          : (data.upload_version ? Number(data.upload_version) : 0);

      const finalVal = parseNum(data.conversion_value_final);
      const initialVal = parseNum(data.conversion_value_initial !== undefined ? data.conversion_value_initial : data.lead_value_estimate || data.conversion_value);

      // Only include rows that have a numeric final or are explicitly flagged (same rules as sheet)
      const hasFinal = finalVal !== null;
      const wantSales = (typeof data.sales_sheet_updated_quality === 'number' && data.sales_sheet_quality_uploaded === false) || !!data._wantSales;
      const wantBase = !!data._wantBase;

      if (!hasFinal && !wantSales && !wantBase) {
        if (debugMode) debugRows.push({ id: doc.id, reason: 'no-final-and-no-flag', project: projectIdFound });
        return;
      }

      // Only export when upload_version > last_adjustment_version_exported (if that logic exists)
      const lastAdj =
        typeof data.last_adjustment_version_exported === 'number'
          ? data.last_adjustment_version_exported
          : 0;
      if (uploadVersion <= lastAdj) {
        if (debugMode) debugRows.push({ id: doc.id, reason: 'already-adjusted', uploadVersion, lastAdj, project: projectIdFound });
        return;
      }

      // Build response item (shape expected by sheet script)
      const item = {
        project: projectIdFound || null,
        projectId: projectIdFound || null,
        gclid: gclid,
        conversion_time: tsToIso(data.used_at || data.ts || null),
        original_conversion_time: data.original_conversion_time ? tsToIso(data.original_conversion_time) : null,
        conversion_value_initial: (initialVal !== null ? initialVal : null),
        conversion_value: (initialVal !== null ? initialVal : null),
        conversion_value_final: (hasFinal ? finalVal : null),
        conversion_name: (data.conversion_name && String(data.conversion_name).trim()) ? String(data.conversion_name).trim() : null,
        conversion_name_sales: (data.conversion_name_sales && String(data.conversion_name_sales).trim()) ? String(data.conversion_name_sales).trim() : null,
        conversion_name_sales_qualified: (data.conversion_name_sales_qualified && String(data.conversion_name_sales_qualified).trim()) ? String(data.conversion_name_sales_qualified).trim() : null,
        sales_sheet_updated_quality: (typeof data.sales_sheet_updated_quality === 'number') ? data.sales_sheet_updated_quality : null,
        sales_sheet_quality_uploaded: (typeof data.sales_sheet_quality_uploaded === 'boolean') ? data.sales_sheet_quality_uploaded : null,
        upload_version: uploadVersion,
        order_id: doc.id
      };

      results.push(item);
      totalKept++;

      if (debugMode) {
        debugRows.push({
          id: doc.id,
          project: projectIdFound,
          gclid,
          upload_version: uploadVersion,
          conversion_value_final: item.conversion_value_final,
          conversion_value_initial: item.conversion_value_initial
        });
      }
    });

    console.log(`exports/adjustments-pending: project=${project || '(all)'} scanned=${totalSeen} returned=${totalKept}`);

    if (debugMode) {
      return res.json({
        debug: true,
        project: project || null,
        scanned: totalSeen,
        returned: totalKept,
        sample_debug_rows: debugRows.slice(0, 200),
        results_count: results.length,
        items: results
      });
    }

    return res.json(results);
  } catch (err) {
    console.error('exports/adjustments-pending error', err);
    return res.status(500).json({ error: String(err) });
  }
});
















// POST /exports/mark-exported?project=<project>&secret=<secret>
// Body: { order_ids: ['A','B'] }
// Marks conversion_value_uploaded = true and sets timestamps
// ALSO marks sales_sheet_quality_uploaded = true so sales-trigger is cleared.
app.post('/exports/mark-exported', requireSecret, express.json(), async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: 'Missing project' });

    const orderIds = (req.body && Array.isArray(req.body.order_ids)) ? req.body.order_ids : [];
    if (!orderIds.length) return res.status(400).json({ error: 'No order_ids provided' });

    const basePath = `projects/${project}/clicks`;
    const batch = db.batch();
    const now = admin.firestore.FieldValue.serverTimestamp();
    const updated = [];

    // primary batched update
    for (const id of orderIds) {
      const docRef = db.doc(`${basePath}/${id}`);
      batch.update(docRef, {
        conversion_value_uploaded: true,
        conversion_value_uploaded_at: now,
        google_sheet_exported: true,
        google_sheet_export_job: `apps-script-${Date.now()}`,
        exported_once: true,
        // 👇 NEW: clear sales trigger for the current state
        sales_sheet_quality_uploaded: true,
        sales_sheet_last_uploaded_at: now
      });
      updated.push(id);
    }

    try {
      await batch.commit();
      console.log(`mark-exported: batch commit success for project=${project} count=${updated.length}`);
      return res.json({ success: true, updated, errors: [] });
    } catch (batchErr) {
      console.error('mark-exported: batch commit failed, falling back to per-doc updates', batchErr);

      // fallback: per-doc updates
      const fallbackUpdated = [];
      const fallbackErrors = [];
      for (const id of orderIds) {
        const docRef = db.doc(`${basePath}/${id}`);
        try {
          await docRef.update({
            conversion_value_uploaded: true,
            conversion_value_uploaded_at: admin.firestore.FieldValue.serverTimestamp(),
            google_sheet_exported: true,
            google_sheet_export_job: `apps-script-${Date.now()}`,
            exported_once: true,
            // 👇 NEW: clear sales trigger for the current state
            sales_sheet_quality_uploaded: true,
            sales_sheet_last_uploaded_at: admin.firestore.FieldValue.serverTimestamp()
          });
          fallbackUpdated.push(id);
        } catch (e) {
          fallbackErrors.push({ id, error: String(e) });
          console.error('mark-exported single update error', id, e);
        }
      }
      const success = fallbackErrors.length === 0;
      return res.json({ success, updated: fallbackUpdated, errors: fallbackErrors });
    }
  } catch (err) {
    console.error('exports/mark-exported error', err);
    return res.status(500).json({ error: String(err) });
  }
});








/**
 * POST /exports/mark-adjustments-exported?project=<project>&secret=<secret>
 * Body: { items: [ { order_id: '...', upload_version: 3 }, ... ] }
 * Marks last_adjustment_version_exported for each doc.
 */
app.post('/exports/mark-adjustments-exported', requireSecret, express.json(), async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: 'Missing project' });

    const items = (req.body && Array.isArray(req.body.items)) ? req.body.items : [];
    if (!items.length) return res.status(400).json({ error: 'No items provided' });

    const basePath = `projects/${project}/clicks`;
    const batch = db.batch();
    const updated = [];
    const errors = [];

    for (const item of items) {
      const id = item.order_id;
      const v = (typeof item.upload_version === 'number') ? item.upload_version : null;
      if (!id || v === null) {
        errors.push({ id, error: 'Missing order_id or upload_version' });
        continue;
      }
      const docRef = db.doc(`${basePath}/${id}`);
      batch.update(docRef, {
        last_adjustment_version_exported: v
      });
      updated.push({ id, upload_version: v });
    }

    await batch.commit();
    return res.json({ success: true, updated, errors });
  } catch (err) {
    console.error('mark-adjustments-exported error', err);
    return res.status(500).json({ error: String(err) });
  }
});















/**
 * POST /sales/quality-update?secret=<secret>
 * Body: { token: 'ABC1234', quality: 'qualified'|'unqualified'|'closed', value?: number, conversion_name?: 'Qualified Conversion Name' }
 * Updates sales fields on the click doc and triggers exporter via flags.
 */
// POST /sales/quality-update?secret=<secret>
app.post('/sales/quality-update', requireSecret, express.json(), async (req, res) => {
  try {
    const body = req.body || {};
    const rawToken   = body.token;
    const rawQuality = body.quality;
    const rawValue   = body.value;
    const rawConvName = body.conversion_name;


    const token = rawToken && String(rawToken).trim().toUpperCase();
    if (!token) return res.status(400).json({ error: 'Missing token' });


    const quality = rawQuality && String(rawQuality).trim().toLowerCase();
    const allowedQualities = ['qualified', 'unqualified', 'closed'];
    if (!quality || !allowedQualities.includes(quality)) {
      return res.status(400).json({ error: 'Invalid quality; must be qualified|unqualified|closed' });
    }


    const qualityMap = { unqualified: 0, qualified: 1, closed: 2 };
    const qualityCode = qualityMap[quality];


    // parse numeric value if present
    let valueNum = null;
    if (rawValue !== undefined && rawValue !== null && String(rawValue).trim() !== '') {
      if (typeof rawValue === 'number' && !Number.isNaN(rawValue)) {
        valueNum = rawValue;
      } else {
        const parsed = parseFloat(String(rawValue));
        if (!Number.isNaN(parsed)) valueNum = parsed;
      }
    }


    // REQUIRE value for qualified/closed (server-side validation requested)
    if ((quality === 'qualified' || quality === 'closed') && valueNum === null) {
      return res.status(400).json({ error: 'Value required when marking qualified or closed' });
    }


    const indexRef = db.doc(`tokenIndex/${token}`);
    const indexSnap = await indexRef.get();
    if (!indexSnap.exists) return res.status(404).json({ error: 'tokenIndex entry not found for token' });
    const indexData = indexSnap.data() || {};
    const projectId = indexData.projectId;
    if (!projectId) return res.status(500).json({ error: 'tokenIndex missing projectId' });


    const clickRef = db.doc(`projects/${projectId}/clicks/${token}`);
    const clickSnap = await clickRef.get();
    if (!clickSnap.exists) return res.status(404).json({ error: 'click doc not found for token' });
    const clickData = clickSnap.data() || {};
    const currentVersion =
      (typeof clickData.upload_version === 'number' && Number.isFinite(clickData.upload_version))
        ? clickData.upload_version
        : 0;


    const update = {
      quality_status: quality,
      sales_sheet_updated_quality: qualityCode,
      sales_sheet_quality_uploaded: false,
      updated_by_sales_at: admin.firestore.FieldValue.serverTimestamp(),
      conversion_value_uploaded: false,
      upload_version: currentVersion + 1
    };


    // if a conversion_name was explicitly provided by sales, use it for the "sales" conversion name
    if (rawConvName && String(rawConvName).trim()) {
      update.conversion_name_sales = String(rawConvName).trim();
    }


    // If quality was previously 'qualified' then when we mark closed we want to ensure the
    // qualified conversion action name is preserved for adjustments. Store it in a dedicated field.
    if (quality === 'closed') {
      // if document already had a sales conversion name that represented 'qualified',
      // preserve it into conversion_name_sales_qualified so adjustments will include it.
      if (clickData.conversion_name_sales && clickData.sales_sheet_updated_quality === 1) {
        update.conversion_name_sales_qualified = String(clickData.conversion_name_sales).trim();
      } else if (clickData.conversion_name_sales_qualified) {
        // preserve previously stored qualified name (idempotent)
        update.conversion_name_sales_qualified = clickData.conversion_name_sales_qualified;
      }
    }


    // set final/zero values
    if (quality === 'unqualified') {
      update.conversion_value_final = 0;
    } else if (quality === 'qualified' || quality === 'closed') {
      if (valueNum !== null) {
        update.conversion_value_final = valueNum;
      }
    }


    await clickRef.update(update);


    return res.json({
      ok: true,
      token,
      projectId,
      quality,
      quality_code: qualityCode,
      value: valueNum,
      upload_version: update.upload_version,
      flags: {
        base_restate_needed: true,
        sales_sheet_quality_uploaded: false
      }
    });
  } catch (err) {
    console.error('sales/quality-update error', err);
    return res.status(500).json({ error: String(err) });
  }
});












/**
 * POST /queueAdjustment?project=<project>&secret=<secret>
 * Body: { order_id: "<DOC_ID>", conversion_value_final: 1000, source: "manual_adjustment" }
 * Transactionally sets conversion_value_final, upload_version++, conversion_value_uploaded=false
 */
app.post('/queueAdjustment', requireSecret, express.json(), async (req, res) => {
  try {
    const project = req.query.project;
    if (!project) return res.status(400).json({ error: 'Missing project' });

    const body = req.body || {};
    const orderId = body.order_id || body.token;
    if (!orderId) return res.status(400).json({ error: 'Missing order_id' });

    const finalVal = typeof body.conversion_value_final === 'number' ? body.conversion_value_final : null;
    if (finalVal === null) return res.status(400).json({ error: 'conversion_value_final (number) required' });

    const source = body.source || 'manual_adjustment';
    const basePath = `projects/${project}/clicks`;
    const docRef = db.doc(`${basePath}/${orderId}`);

    await db.runTransaction(async (tx) => {
      const doc = await tx.get(docRef);
      if (!doc.exists) throw new Error('Document not found: ' + docRef.path);
      const data = doc.data();
      const currentVersion = typeof data.upload_version === 'number' ? data.upload_version : (data.upload_version ? Number(data.upload_version) : 0);
      const newVersion = currentVersion + 1;
      tx.update(docRef, {
        conversion_value_final: finalVal,
        conversion_value_uploaded: false,
        conversion_value_source: source,
        upload_version: newVersion
      });
    });

    return res.json({ success: true, order_id: orderId });
  } catch (err) {
    console.error('queueAdjustment error', err);
    return res.status(500).json({ error: String(err) });
  }
});

// Export the express app as a single Cloud Function endpoint
exports.exportsApi = functions.https.onRequest(app);



