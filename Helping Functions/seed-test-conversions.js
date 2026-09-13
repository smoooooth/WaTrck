/**
 * WaTrck - TEST CONVERSION SEEDER
 * ==========================================
 *
 * Two test modes:
 *
 * 1. ZIGZAG
 *    Alternates conversions between EXACTLY two projects.
 *
 *    Example:
 *    Project A
 *    Project B
 *    Project A
 *    Project B
 *    ...
 *
 * 2. BULK
 *    Creates many conversions inside ONE project only.
 *
 *
 * RUN:
 *
 * node seed-test-conversions.js
 *
 *
 * IMPORTANT:
 *
 * All generated GCLIDs contain:
 *
 * _testproduction_
 *
 * so the global Conversions exporter should route them into:
 *
 * TESTING_Conversions_Sheet
 *
 * NOT the normal production conversions sheet.
 */


/* =========================================================
   FIREBASE
   ========================================================= */

const {
  initializeApp,
  applicationDefault
} = require('firebase-admin/app');

const {
  getFirestore,
  Timestamp
} = require('firebase-admin/firestore');

const crypto = require('crypto');


initializeApp({
  credential: applicationDefault(),
  projectId: 'aida-muscat-wa-tracking'
});

const db = getFirestore();


/* =========================================================
   1. MASTER TEST CONFIG
   ========================================================= */
/*
 * Choose:
 *
 * "zigzag"
 *
 * OR
 *
 * "bulk"
 */

const TEST_MODE = "zigzag";
/*
 * These are the three current WaTrck project IDs.
 *
 * Project IDs must match Firestore exactly.
 */

const PROJECT_IDS = [

  "AIDA_Oman",

  "Trump Plaza Jeddah",

  "Trump Park Residences"

];

/*
 * IMPORTANT:
 *
 * These conversion names are TEST conversion names.
 *
 * They do NOT need to exist in Google Ads because
 * _testproduction_ conversions are isolated into the
 * TESTING conversions sheet.
 *
 * We deliberately make the conversion name unique
 * for each project so it is easy to see which project
 * produced each row.
 */

function getTestConversionName(projectId) {

  return projectId + " - TEST Contact";

}


/*
 * Marker recognized by the global Conversions exporter.
 *
 * DO NOT CHANGE.
 */

const TEST_MARKER =
  "_testproduction_";


/*
 * Conversion value used by generated test leads.
 */

const LEAD_VALUE_ESTIMATE = 70;


/* =========================================================
   2. ZIGZAG CONFIG
   ========================================================= */


/*
 * Pick EXACTLY TWO projects from PROJECT_IDS.
 *
 * The script will alternate:
 *
 * project 1
 * project 2
 * project 1
 * project 2
 * ...
 */

const ZIGZAG_PROJECT_IDS = [

  "AIDA_Oman",
  "Trump Plaza Jeddah",
  "Trump Park Residences"

];


/*
 * Number of conversions PER PROJECT.
 *
 * Example:
 *
 * 10 means:
 *
 * AIDA = 10
 * Trump Plaza = 10
 *
 * Total = 20
 */

const ZIGZAG_COUNT_PER_PROJECT = 5;


/*
 * Delay between each created conversion.
 *
 * 1000 = 1 second.
 *
 * This is useful for challenging the global exporter
 * while conversions arrive one after another.
 */

const ZIGZAG_DELAY_MS = 1000;


/* =========================================================
   3. BULK CONFIG
   ========================================================= */


/*
 * ONE project only.
 *
 * Change this when you want to bulk-test another project.
 */

const BULK_PROJECT_ID =
  "Trump Park Residences";


/*
 * Total conversions to create in that single project.
 */

const BULK_COUNT = 100;


/*
 * Firestore allows a limited number of writes per batch.
 *
 * Every conversion uses TWO writes:
 *
 * 1. projects/<project>/clicks/<token>
 * 2. tokenIndex/<token>
 *
 * 200 conversions = 400 writes.
 *
 * Keep this <= 200.
 */

const BULK_CONVERSIONS_PER_BATCH = 200;


/* =========================================================
   HELPERS
   ========================================================= */

function randString(len = 7) {

  const chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

  let output = "";

  const bytes =
    crypto.randomBytes(len);

  for (
    let i = 0;
    i < len;
    i++
  ) {

    output +=
      chars[
        bytes[i] %
        chars.length
      ];

  }

  return output;

}


function sleep(ms) {

  return new Promise(
    resolve =>
      setTimeout(resolve, ms)
  );

}


function validateProject(projectId) {

  if (
    !PROJECT_IDS.includes(projectId)
  ) {

    throw new Error(
      "Unknown project ID: " +
      projectId
    );

  }

}


function createTestData(projectId) {

  validateProject(projectId);


  const token =
    randString(7);


  /*
   * The test marker MUST be inside the GCLID.
   *
   * The Conversions Apps Script uses this marker
   * to isolate these rows from production.
   */

  const gclid =
    TEST_MARKER +
    "-" +
    randString(24);


  const now =
    new Date();


  const firestoreTimestamp =
    Timestamp.now();


  const docPath =
    "projects/" +
    projectId +
    "/clicks/" +
    token;


  const payload = {

    token:
      token,

    projectId:
      projectId,

    gclid:
      gclid,

    google_campaign_id:
      null,

    conversion_name:
      getTestConversionName(
        projectId
      ),

    conversion_value_initial:
      LEAD_VALUE_ESTIMATE,

    conversion_value_final:
      null,

    conversion_value_source:
      "whatsapp_message",

    conversion_value_uploaded:
      false,

    conversion_value_uploaded_at:
      null,

    lead_value_estimate:
      LEAD_VALUE_ESTIMATE,

    upload_version:
      0,

    used:
      true,

    used_at:
      firestoreTimestamp,

    ts:
      now.toISOString(),

    ctaId:
      "stress_test",

    page:
      "stress-test/" +
      projectId

  };


  const indexPayload = {

    token:
      token,

    projectId:
      projectId,

    clickPath:
      docPath,

    created_at:
      firestoreTimestamp

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

async function createSingleConversion(
  projectId,
  number
) {

  const test =
    createTestData(
      projectId
    );


  const batch =
    db.batch();


  const clickRef =
    db.doc(
      test.docPath
    );


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
   MODE 1 — ZIGZAG
   ========================================================= */

async function runZigzagMode() {


  if (
    ZIGZAG_PROJECT_IDS.length !== 2
  ) {

    throw new Error(
      "ZIGZAG_PROJECT_IDS must contain exactly TWO projects."
    );

  }


  validateProject(
    ZIGZAG_PROJECT_IDS[0]
  );

  validateProject(
    ZIGZAG_PROJECT_IDS[1]
  );


  if (
    ZIGZAG_PROJECT_IDS[0] ===
    ZIGZAG_PROJECT_IDS[1]
  ) {

    throw new Error(
      "Zigzag projects must be different."
    );

  }


  const total =
    ZIGZAG_COUNT_PER_PROJECT *
    2;


  console.log("");
  console.log(
    "============================================"
  );
  console.log(
    "WaTrck ZIGZAG STRESS TEST"
  );
  console.log(
    "============================================"
  );

  console.log(
    "Project A:",
    ZIGZAG_PROJECT_IDS[0]
  );

  console.log(
    "Project B:",
    ZIGZAG_PROJECT_IDS[1]
  );

  console.log(
    "Conversions per project:",
    ZIGZAG_COUNT_PER_PROJECT
  );

  console.log(
    "Total conversions:",
    total
  );

  console.log(
    "Delay:",
    ZIGZAG_DELAY_MS + "ms"
  );

  console.log("");


  const createdTokens = [];


  for (
    let i = 0;
    i < total;
    i++
  ) {

    /*
     * Round robin:
     *
     * 0 → project A
     * 1 → project B
     * 2 → project A
     * 3 → project B
     */

    const projectId =
      ZIGZAG_PROJECT_IDS[
        i % 2
      ];


    /*
     * No delay before the first record.
     */

    if (i > 0) {

      await sleep(
        ZIGZAG_DELAY_MS
      );

    }


    const token =
      await createSingleConversion(
        projectId,
        i + 1
      );


    createdTokens.push(
      token
    );

  }


  console.log("");
  console.log(
    "============================================"
  );

  console.log(
    "ZIGZAG TEST COMPLETE"
  );

  console.log(
    "Created:",
    createdTokens.length,
    "conversions"
  );

  console.log(
    "============================================"
  );

  console.log("");


  console.log(
    "Tokens created:"
  );

  console.log(
    createdTokens.join("\n")
  );

}


/* =========================================================
   MODE 2 — BULK
   ========================================================= */

async function runBulkMode() {


  validateProject(
    BULK_PROJECT_ID
  );


  if (
    BULK_CONVERSIONS_PER_BATCH >
    200
  ) {

    throw new Error(
      "BULK_CONVERSIONS_PER_BATCH must be 200 or less."
    );

  }


  console.log("");
  console.log(
    "============================================"
  );

  console.log(
    "WaTrck BULK STRESS TEST"
  );

  console.log(
    "============================================"
  );

  console.log(
    "Project:",
    BULK_PROJECT_ID
  );

  console.log(
    "Conversions:",
    BULK_COUNT
  );

  console.log("");


  let totalCreated = 0;

  const createdTokens = [];


  while (
    totalCreated <
    BULK_COUNT
  ) {


    const remaining =
      BULK_COUNT -
      totalCreated;


    const thisBatchCount =
      Math.min(
        BULK_CONVERSIONS_PER_BATCH,
        remaining
      );


    const batch =
      db.batch();


    const batchTokens = [];


    for (
      let i = 0;
      i < thisBatchCount;
      i++
    ) {


      const test =
        createTestData(
          BULK_PROJECT_ID
        );


      const clickRef =
        db.doc(
          test.docPath
        );


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


    totalCreated +=
      thisBatchCount;


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
  console.log(
    "============================================"
  );

  console.log(
    "BULK TEST COMPLETE"
  );

  console.log(
    "Project:",
    BULK_PROJECT_ID
  );

  console.log(
    "Created:",
    totalCreated
  );

  console.log(
    "============================================"
  );

  console.log("");


  console.log(
    "Tokens created:"
  );

  console.log(
    createdTokens.join("\n")
  );

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


    if (
      TEST_MODE ===
      "zigzag"
    ) {

      await runZigzagMode();

    }

    else if (
      TEST_MODE ===
      "bulk"
    ) {

      await runBulkMode();

    }

    else {

      throw new Error(
        'TEST_MODE must be either "zigzag" or "bulk".'
      );

    }


    process.exit(0);


  } catch (error) {


    console.error("");
    console.error(
      "TEST FAILED:"
    );

    console.error(
      error
    );


    process.exit(1);

  }

})();