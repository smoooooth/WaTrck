/**
 * WaTrck - TEST CONVERSION SEEDER
 *
 * Modes:
 * 1. "zigzag" = round-robin conversions between 2 or more projects
 * 2. "bulk"   = bulk conversions into one single project
 *
 * Run:
 * node seed-test-conversions.js
 */

const { initializeApp, applicationDefault } = require('firebase-admin/app');
const { getFirestore, Timestamp } = require('firebase-admin/firestore');
const crypto = require('crypto');

initializeApp({
  credential: applicationDefault(),
  projectId: 'aida-muscat-wa-tracking'
});

const db = getFirestore();


/* =========================================================
   MASTER CONFIG
   ========================================================= */

// "zigzag" or "bulk"
const TEST_MODE = "bulk";

// All valid projects in the system
const PROJECT_IDS = [
  "AIDA_Oman",
  "Trump Plaza Jeddah",
  "Trump Park Residences"
];

// Marker recognized by the testing conversions exporter
const TEST_MARKER = "_testproduction_";

// Default conversion value
const LEAD_VALUE_ESTIMATE = 70;


/* =========================================================
   ZIGZAG CONFIG
   ========================================================= */

// Add/remove projects here.
// Zigzag supports 2 or more projects.
const ZIGZAG_PROJECT_IDS = [
  "AIDA_Oman",
  "Trump Plaza Jeddah",
  "Trump Park Residences"
];

// Number of conversions created PER PROJECT
const ZIGZAG_COUNT_PER_PROJECT = 25;

// Delay between each conversion
// 1000 = 1 second
const ZIGZAG_DELAY_MS = 1000;


/* =========================================================
   BULK CONFIG
   ========================================================= */

// ONE project only
const BULK_PROJECT_ID = "Trump Park Residences";

// Total conversions to create
const BULK_COUNT = 100;

// Every conversion = 2 Firestore writes:
// click document + tokenIndex document
// Keep this at 200 or less.
const BULK_CONVERSIONS_PER_BATCH = 200;


/* =========================================================
   HELPERS
   ========================================================= */

function randString(len = 7) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  let output = "";
  const bytes = crypto.randomBytes(len);

  for (let i = 0; i < len; i++) {
    output += chars[bytes[i] % chars.length];
  }

  return output;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function validateProject(projectId) {
  if (!PROJECT_IDS.includes(projectId)) {
    throw new Error("Unknown project ID: " + projectId);
  }
}

function getTestConversionName(projectId) {
  return projectId + " - TEST Contact";
}

function createTestData(projectId) {
  validateProject(projectId);

  const token = randString(7);
  const gclid = TEST_MARKER + "-" + randString(24);
  const now = new Date();
  const firestoreTimestamp = Timestamp.now();

  const docPath =
    "projects/" +
    projectId +
    "/clicks/" +
    token;

  const payload = {
    token: token,
    projectId: projectId,

    gclid: gclid,
    google_campaign_id: null,

    conversion_name: getTestConversionName(projectId),

    conversion_value_initial: LEAD_VALUE_ESTIMATE,
    conversion_value_final: null,
    conversion_value_source: "whatsapp_message",

    conversion_value_uploaded: false,
    conversion_value_uploaded_at: null,

    lead_value_estimate: LEAD_VALUE_ESTIMATE,
    upload_version: 0,

    used: true,
    used_at: firestoreTimestamp,

    ts: now.toISOString(),

    ctaId: "stress_test",
    page: "stress-test/" + projectId
  };

  const indexPayload = {
    token: token,
    projectId: projectId,
    clickPath: docPath,
    created_at: firestoreTimestamp
  };

  return {
    token,
    gclid,
    docPath,
    payload,
    indexPayload
  };
}


/* =========================================================
   CREATE ONE CONVERSION
   ========================================================= */

async function createSingleConversion(projectId, number) {
  const test = createTestData(projectId);
  const batch = db.batch();

  const clickRef = db.doc(test.docPath);
  const indexRef = db.doc("tokenIndex/" + test.token);

  batch.set(clickRef, test.payload);
  batch.set(indexRef, test.indexPayload);

  await batch.commit();

  console.log(
    "[" +
    String(number).padStart(3, "0") +
    "] " +
    projectId +
    " | token=" +
    test.token
  );

  return test.token;
}


/* =========================================================
   MODE 1 - ZIGZAG
   ========================================================= */

async function runZigzagMode() {
  if (ZIGZAG_PROJECT_IDS.length < 2) {
    throw new Error(
      "ZIGZAG_PROJECT_IDS must contain at least TWO projects."
    );
  }

  const uniqueProjects = new Set(ZIGZAG_PROJECT_IDS);

  if (uniqueProjects.size !== ZIGZAG_PROJECT_IDS.length) {
    throw new Error(
      "ZIGZAG_PROJECT_IDS contains the same project more than once."
    );
  }

  ZIGZAG_PROJECT_IDS.forEach(validateProject);

  const total =
    ZIGZAG_PROJECT_IDS.length *
    ZIGZAG_COUNT_PER_PROJECT;

  console.log("");
  console.log("============================================");
  console.log("WaTrck ZIGZAG STRESS TEST");
  console.log("============================================");
  console.log("Projects:", ZIGZAG_PROJECT_IDS.join(" -> "));
  console.log("Conversions per project:", ZIGZAG_COUNT_PER_PROJECT);
  console.log("Total conversions:", total);
  console.log("Delay:", ZIGZAG_DELAY_MS + "ms");
  console.log("");

  const createdTokens = [];

  for (let i = 0; i < total; i++) {
    const projectId =
      ZIGZAG_PROJECT_IDS[
        i % ZIGZAG_PROJECT_IDS.length
      ];

    if (i > 0) {
      await sleep(ZIGZAG_DELAY_MS);
    }

    const token =
      await createSingleConversion(
        projectId,
        i + 1
      );

    createdTokens.push(token);
  }

  console.log("");
  console.log("============================================");
  console.log("ZIGZAG TEST COMPLETE");
  console.log("Created:", createdTokens.length, "conversions");
  console.log("============================================");
  console.log("");

  console.log("Tokens created:");
  console.log(createdTokens.join("\n"));
}


/* =========================================================
   MODE 2 - BULK
   ========================================================= */

async function runBulkMode() {
  validateProject(BULK_PROJECT_ID);

  if (BULK_CONVERSIONS_PER_BATCH > 200) {
    throw new Error(
      "BULK_CONVERSIONS_PER_BATCH must be 200 or less."
    );
  }

  if (BULK_COUNT < 1) {
    throw new Error(
      "BULK_COUNT must be at least 1."
    );
  }

  console.log("");
  console.log("============================================");
  console.log("WaTrck BULK STRESS TEST");
  console.log("============================================");
  console.log("Project:", BULK_PROJECT_ID);
  console.log("Conversions:", BULK_COUNT);
  console.log("");

  let totalCreated = 0;
  const createdTokens = [];

  while (totalCreated < BULK_COUNT) {
    const remaining =
      BULK_COUNT - totalCreated;

    const thisBatchCount =
      Math.min(
        BULK_CONVERSIONS_PER_BATCH,
        remaining
      );

    const batch = db.batch();
    const batchTokens = [];

    for (let i = 0; i < thisBatchCount; i++) {
      const test =
        createTestData(BULK_PROJECT_ID);

      const clickRef =
        db.doc(test.docPath);

      const indexRef =
        db.doc(
          "tokenIndex/" +
          test.token
        );

      batch.set(
        clickRef,
        test.payload
      );

      batch.set(
        indexRef,
        test.indexPayload
      );

      batchTokens.push(
        test.token
      );
    }

    await batch.commit();

    totalCreated += thisBatchCount;

    createdTokens.push(
      ...batchTokens
    );

    console.log(
      "Committed batch: " +
      thisBatchCount +
      " conversions | Total: " +
      totalCreated +
      "/" +
      BULK_COUNT
    );
  }

  console.log("");
  console.log("============================================");
  console.log("BULK TEST COMPLETE");
  console.log("Project:", BULK_PROJECT_ID);
  console.log("Created:", totalCreated);
  console.log("============================================");
  console.log("");

  console.log("Tokens created:");
  console.log(createdTokens.join("\n"));
}


/* =========================================================
   MAIN
   ========================================================= */

(async function main() {
  try {
    console.log("");
    console.log(
      "Firebase project: aida-muscat-wa-tracking"
    );
    console.log(
      "Test mode:",
      TEST_MODE
    );

    if (TEST_MODE === "zigzag") {
      await runZigzagMode();
    } else if (TEST_MODE === "bulk") {
      await runBulkMode();
    } else {
      throw new Error(
        'TEST_MODE must be either "zigzag" or "bulk".'
      );
    }

    process.exit(0);

  } catch (error) {
    console.error("");
    console.error("TEST FAILED:");
    console.error(error);

    process.exit(1);
  }
})();