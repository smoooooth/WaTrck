/**
 * TEST CONVERSION SEEDER (Zigzag & Batch Modes)
 * * Usage:
 * node seed-test-conversions.js
 */

const admin = require('firebase-admin');
const crypto = require('crypto');

if (!admin.apps.length) {
  admin.initializeApp();
}

const db = admin.firestore();

/* ======================================================================== node seed-test-conversions.js
   ========================= 1. GENERAL CONFIG ============================
   ======================================================================== */

// The Projects to cycle through
const PROJECT_IDS = [
  //'aida',
 // 'DVT_Pagani'
  'AIDA_Muscat',
  'DaVinci_Tower'
  //'project_test1',
  //'project_test2'
  //'project_test3',
  //'project_test4'
];

// MASTER TOGGLE: Choose which mode to run
// true  = New Zigzag mode (Round-robin with timer)
// false = Old Batch mode (Instant, sequential per project) node seed-test-conversions.js
const USE_ZIGZAG_MODE = true; 

const TEST_MARKER = '_testproduction_';


/* ========================================================================
   ========================= 2. ZIGZAG CONFIG ============================= node seed-test-conversions.js
   (Only used if USE_ZIGZAG_MODE = true)
   ======================================================================== */

// How many STANDARD (GCLID) conversions to create per project
const ZIGZAG_GCLID_COUNT_PER_PROJECT = 1;

// NO-GCLID Settings
const ZIGZAG_INCLUDE_NO_GCLID = false;      // Toggle creation of No-GCLID docs
const ZIGZAG_NO_GCLID_COUNT = 10;           // How many No-GCLID docs per project

// Timer delay between EVERY document creation (in milliseconds)
const ZIGZAG_DELAY_MS = 1000; 


/* ======================================================================== node seed-test-conversions.js
   ========================= 3. BATCH CONFIG ==============================
   (Only used if USE_ZIGZAG_MODE = false)
   ======================================================================== */
const BATCH_COUNT_PER_PROJECT = 10;
const BATCH_INCLUDE_EMPTY_GCLID = true;   
const BATCH_EMPTY_GCLID_COUNT = 5;         


/* ========================================================================
   =========================== HELPERS ====================================
   ======================================================================== */

function randString(len = 7) {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let out = '';
  const arr = crypto.randomBytes(len);
  for (let i = 0; i < len; i++) out += chars[arr[i] % chars.length];
  return out;
}

// Generates a delay promise
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Creates a single document in Firestore mimicking REAL conversion structure
async function createConversionDoc(projectId, isNoGclid) {
  const now = new Date(); // Javascript Date object
  const firestoreTimestamp = admin.firestore.Timestamp.now(); // Firestore Timestamp
  const token = randString(7); // Random 7-char token like "BYQQ4QF"

  // -- PAYLOAD CONSTRUCTION --
  let gclidValue = null;
  let campaignIdValue = null;
  let convName = "";

  if (isNoGclid) {
    // Case: No GCLID
    // Per requirements: gclid is null, campaign_id is marker
    gclidValue = null;
    campaignIdValue = TEST_MARKER; 
    convName = `${projectId}_WA_Contact`; // mimic real naming
  } else {
    // Case: Standard GCLID
    // Per requirements: gclid has marker, campaign_id is null
    // gclid format: _testproduction_-<random>
    gclidValue = `${TEST_MARKER}-${randString(20)}`;
    campaignIdValue = null;
    convName = `${projectId}_WA_Contact`;
  }

  const docPath = `projects/${projectId}/clicks/${token}`;
  
  const payload = {
    conversion_name: convName,
    conversion_value_final: null,
    conversion_value_initial: null,
    conversion_value_source: "whatsapp_message",
    conversion_value_uploaded: false,
    conversion_value_uploaded_at: null,
    ctaId: "hero_sec_cta", // hardcoded test value
    gclid: gclidValue,
    google_campaign_id: campaignIdValue,
    google_sheet_exported: false,
    lead_value_estimate: 70,
    page: `${projectId}.test.com/`,
    projectId: projectId,
    token: token,
    ts: now.toISOString(), // String ISO
    upload_version: 0,
    used: true,
    used_at: firestoreTimestamp, // Timestamp object
    whatsapp_from: "201277358620",
    whatsapp_msg_id: `wamid.TEST${randString(20)}`
  };

  // Log what we are doing
  console.log(`   [${isNoGclid ? 'NO-GCLID' : 'GCLID'}] Creating doc in ${projectId}...`);
  
  // Write to DB (Click Doc + Index)
  const batch = db.batch();
  
  const clickRef = db.doc(docPath);
  batch.set(clickRef, payload);

  const indexRef = db.doc(`tokenIndex/${token}`);
  batch.set(indexRef, {
    token: token,
    projectId: projectId,
    clickPath: docPath,
    created_at: firestoreTimestamp
  });

  await batch.commit();
}


/* ========================================================================
   =========================== MAIN LOGIC =================================
   ======================================================================== */

(async () => {
  try {
    if (USE_ZIGZAG_MODE) {
      await runZigzagMode();
    } else {
      await runBatchMode();
    }
    process.exit(0);
  } catch (err) {
    console.error('❌ Script failed:', err);
    process.exit(1);
  }
})();


/* ---------------------------------------------------------
   MODE 1: ZIGZAG (Round Robin with Timer)
   --------------------------------------------------------- */
async function runZigzagMode() {
  console.log(`\n🚀 STARTING ZIGZAG MODE`);
  console.log(`   Projects: ${PROJECT_IDS.join(', ')}`);
  console.log(`   Delay: ${ZIGZAG_DELAY_MS}ms`);

  // --- PHASE 1: Standard GCLID Conversions ---
  const totalStandard = PROJECT_IDS.length * ZIGZAG_GCLID_COUNT_PER_PROJECT;
  console.log(`\n--- Phase 1: Creating ${totalStandard} Standard GCLID Conversions ---`);
  
  for (let i = 0; i < totalStandard; i++) {
    // Round Robin Logic: i % length gives us 0, 1, 0, 1...
    const projectIndex = i % PROJECT_IDS.length;
    const projectId = PROJECT_IDS[projectIndex];

    await sleep(ZIGZAG_DELAY_MS);
    await createConversionDoc(projectId, false); // false = has gclid
  }

  // --- PHASE 2: No-GCLID Conversions (if enabled) ---
  if (ZIGZAG_INCLUDE_NO_GCLID) {
    const totalNoGclid = PROJECT_IDS.length * ZIGZAG_NO_GCLID_COUNT;
    console.log(`\n--- Phase 2: Creating ${totalNoGclid} No-GCLID Conversions ---`);

    for (let i = 0; i < totalNoGclid; i++) {
      const projectIndex = i % PROJECT_IDS.length;
      const projectId = PROJECT_IDS[projectIndex];

      await sleep(ZIGZAG_DELAY_MS);
      await createConversionDoc(projectId, true); // true = no gclid
    }
  } else {
    console.log(`\n--- Phase 2 Skipped (Toggle OFF) ---`);
  }

  console.log(`\n✅ Zigzag Test Complete.`);
}


/* ---------------------------------------------------------
   MODE 2: BATCH (Original Logic)
   --------------------------------------------------------- */
async function runBatchMode() {
  console.log(`\n📦 STARTING BATCH MODE (Original)`);
  const BATCH_SIZE = 500;
  const now = admin.firestore.FieldValue.serverTimestamp();
  let totalCreated = 0;

  for (const projectId of PROJECT_IDS) {
    let createdForProject = 0;
    console.log(`   Processing Project: ${projectId}`);

    for (let i = 0; i < BATCH_COUNT_PER_PROJECT; i += BATCH_SIZE) {
      const batch = db.batch();
      const slice = Math.min(BATCH_SIZE, BATCH_COUNT_PER_PROJECT - i);

      for (let j = 0; j < slice; j++) {
        const order = createdForProject + 1;
        // Old ID format retained for batch mode
        const testId = `${TEST_MARKER}-${projectId}-${randString(5)}-${order}`;
        const token = testId;

        // Determine GCLID based on old batch config
        let gclid = testId;
        if (BATCH_INCLUDE_EMPTY_GCLID && order <= BATCH_EMPTY_GCLID_COUNT) {
          gclid = ""; 
        }

        const clickRef = db.doc(`projects/${projectId}/clicks/${token}`);
        batch.set(clickRef, {
          token,
          projectId,
          gclid, 
          conversion_name: 'TEST_CONVERSION_BATCH',
          used: true,
          used_at: now,
          ts: now,
          page: 'test-batch',
          lead_value_estimate: 10,
          conversion_value_uploaded: false,
          google_sheet_exported: false,
          upload_version: 0,
          whatsapp_from: null,
          google_campaign_id: null
        });

        const indexRef = db.doc(`tokenIndex/${token}`);
        batch.set(indexRef, {
          token,
          projectId,
          clickPath: `projects/${projectId}/clicks/${token}`,
          created_at: now
        });

        createdForProject++;
        totalCreated++;
      }
      await batch.commit();
    }
    console.log(`   -> Created ${createdForProject} docs.`);
  }
  console.log(`✅ Batch Test Complete. Total: ${totalCreated}`);
}


