/**
 * WaTrck - TEST / TOKEN CLEANUP
 *
 * PRIMARY USE:
 * Delete every test conversion whose GCLID contains:
 *
 *   _testproduction_
 *
 * SECONDARY USE:
 * Delete specific records by token.
 *
 * Deletes:
 *   - projects/<projectId>/clicks/<token>
 *   - tokenIndex/<token>
 *
 * NEVER deletes:
 *   - WhatsApp conversations
 *   - WhatsApp messages
 *   - whatsappSenderIndex
 *   - Google Sheets rows
 *
 * Run:
 *   node delete_docs_by_token_v2.js
 */

const {
  initializeApp,
  applicationDefault,
  getApps
} = require('firebase-admin/app');

const { getFirestore } = require('firebase-admin/firestore');
const readline = require('readline');


/* =========================================================
   FIREBASE
   ========================================================= */

if (!getApps().length) {
  initializeApp({
    credential: applicationDefault(),
    projectId: 'aida-muscat-wa-tracking'
  });
}

const db = getFirestore();


/* =========================================================
   CONFIG
   ========================================================= */

// All current WaTrck projects.
// Add future projects here.
const PROJECT_IDS = [
  "AIDA_Oman",
  "Trump Plaza Jeddah",
  "Trump Park Residences"
];


// true = delete ALL conversions whose GCLID contains TEST_MARKER
// false = do not perform automatic test cleanup
const DELETE_TEST_CONVERSIONS = true;


// Marker created by seed-test-conversions.js
const TEST_MARKER = "_testproduction_";


// OPTIONAL:
//
// Add individual tokens here if you specifically want
// to delete certain records.
//
// This works in addition to DELETE_TEST_CONVERSIONS.
//
// Leave empty for normal bulk-test cleanup.
const TOKENS_TO_DELETE = [
  // "ABC1234",
  // "XYZ5678"
];


// true  = preview only
// false = actually delete
const DRY_RUN = false;


// false = ask "yes" before deleting
// true  = delete immediately
const SKIP_CONFIRMATION = true;


// Every conversion can create two deletes:
// click + tokenIndex.
//
// 200 conversions = max ~400 writes per batch.
const DELETE_BATCH_SIZE = 200;


/* =========================================================
   HELPERS
   ========================================================= */

function normalizeToken(value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '');
}


function isTestGclid(value) {
  if (value === null || value === undefined) {
    return false;
  }

  return String(value).includes(TEST_MARKER);
}


async function getDocumentIfExists(path) {
  try {
    const snap = await db.doc(path).get();
    return snap.exists ? snap : null;
  } catch (error) {
    console.warn(
      `Could not read ${path}: ${error.message || error}`
    );
    return null;
  }
}


/* =========================================================
   FIND ALL TEST CONVERSIONS
   ========================================================= */

async function findTestConversions() {
  const found = new Map();

  console.log("");
  console.log("Scanning projects for test conversions...");
  console.log(`Marker: ${TEST_MARKER}`);
  console.log("");

  for (const projectId of PROJECT_IDS) {
    const collectionRef =
      db.collection(`projects/${projectId}/clicks`);

    const snapshot =
      await collectionRef.get();

    let matched = 0;

    snapshot.forEach(doc => {
      const data = doc.data() || {};

      if (!isTestGclid(data.gclid)) {
        return;
      }

      const token =
        normalizeToken(
          data.token || doc.id
        );

      found.set(doc.ref.path, {
        token,
        projectId,
        clickRef: doc.ref,
        gclid: data.gclid || ""
      });

      matched++;
    });

    console.log(
      `${projectId}: ${matched} test conversion(s) found`
    );
  }

  return Array.from(found.values());
}


/* =========================================================
   FIND CLICK DOCS FOR ONE SPECIFIC TOKEN
   ========================================================= */

async function resolveToken(token) {
  const clickDocs = new Map();

  const tokenIndexRef =
    db.doc(`tokenIndex/${token}`);

  const indexSnap =
    await tokenIndexRef.get();

  let indexData = null;

  if (indexSnap.exists) {
    indexData =
      indexSnap.data() || {};

    if (indexData.clickPath) {
      const snap =
        await getDocumentIfExists(
          indexData.clickPath
        );

      if (snap) {
        clickDocs.set(
          snap.ref.path,
          snap
        );
      }
    }

    if (indexData.projectId) {
      const path =
        `projects/${indexData.projectId}/clicks/${token}`;

      const snap =
        await getDocumentIfExists(path);

      if (snap) {
        clickDocs.set(
          snap.ref.path,
          snap
        );
      }
    }
  }


  /*
   * Fallback:
   * directly check all known projects.
   *
   * No collectionGroup token index required.
   */

  const projectsToCheck =
    new Set(PROJECT_IDS);

  if (
    indexData &&
    indexData.projectId
  ) {
    projectsToCheck.add(
      indexData.projectId
    );
  }

  const reads =
    Array.from(projectsToCheck)
      .map(async projectId => {
        const path =
          `projects/${projectId}/clicks/${token}`;

        return getDocumentIfExists(path);
      });

  const snapshots =
    await Promise.all(reads);

  snapshots.forEach(snap => {
    if (!snap) return;

    clickDocs.set(
      snap.ref.path,
      snap
    );
  });


  return {
    token,
    tokenIndexRef,
    tokenIndexExists:
      indexSnap.exists,

    clickDocs:
      Array.from(
        clickDocs.values()
      )
  };
}


/* =========================================================
   BUILD COMPLETE DELETE LIST
   ========================================================= */

async function buildDeleteList() {
  /*
   * Map keyed by click-document path.
   *
   * This prevents duplicates when:
   *
   * - a test record is found by marker
   * - AND its token is also in TOKENS_TO_DELETE
   */

  const clickMap = new Map();

  const tokens =
    new Set();


  /* ---------------------------------------------------------
     1. TEST MARKER CLEANUP
     --------------------------------------------------------- */

  if (DELETE_TEST_CONVERSIONS) {
    const testConversions =
      await findTestConversions();

    testConversions.forEach(item => {
      clickMap.set(
        item.clickRef.path,
        item.clickRef
      );

      if (item.token) {
        tokens.add(item.token);
      }
    });
  }


  /* ---------------------------------------------------------
     2. OPTIONAL TOKEN CLEANUP
     --------------------------------------------------------- */

  const manualTokens =
    [
      ...new Set(
        TOKENS_TO_DELETE
          .map(normalizeToken)
          .filter(Boolean)
      )
    ];


  for (const token of manualTokens) {
    const resolved =
      await resolveToken(token);

    tokens.add(token);

    resolved.clickDocs.forEach(snap => {
      clickMap.set(
        snap.ref.path,
        snap.ref
      );
    });
  }


  return {
    clickRefs:
      Array.from(
        clickMap.values()
      ),

    tokens:
      Array.from(tokens),

    manualTokens
  };
}


/* =========================================================
   CONFIRMATION
   ========================================================= */

async function askConfirmation(deletePlan) {
  if (
    DRY_RUN ||
    SKIP_CONFIRMATION
  ) {
    return true;
  }


  const rl =
    readline.createInterface({
      input: process.stdin,
      output: process.stdout
    });


  console.log("");
  console.log("--------------------------------------------");
  console.log("DELETE PLAN");
  console.log("--------------------------------------------");

  if (DELETE_TEST_CONVERSIONS) {
    console.log(
      `Test marker cleanup: YES (${TEST_MARKER})`
    );
  } else {
    console.log(
      "Test marker cleanup: NO"
    );
  }

  console.log(
    `Specific tokens configured: ${deletePlan.manualTokens.length}`
  );

  console.log(
    `Click documents to delete: ${deletePlan.clickRefs.length}`
  );

  console.log(
    `tokenIndex documents targeted: ${deletePlan.tokens.length}`
  );

  console.log("");
  console.log(
    "WhatsApp conversations/messages: PRESERVED"
  );

  console.log(
    "Google Sheets rows: PRESERVED"
  );


  const answer =
    await new Promise(resolve => {
      rl.question(
        "\nType yes to continue: ",
        value => {
          rl.close();
          resolve(value);
        }
      );
    });


  return (
    String(answer)
      .trim()
      .toLowerCase() ===
    "yes"
  );
}


/* =========================================================
   DELETE
   ========================================================= */

async function executeDeletePlan(deletePlan) {
  const clickRefs =
    deletePlan.clickRefs;

  const tokens =
    deletePlan.tokens;


  let clicksDeleted = 0;
  let tokenIndexesDeleted = 0;


  /*
   * Process a limited number of conversions per batch.
   *
   * Each conversion can generate:
   *
   * 1 click deletion
   * 1 tokenIndex deletion
   */

  for (
    let offset = 0;
    offset < clickRefs.length;
    offset += DELETE_BATCH_SIZE
  ) {
    const clickChunk =
      clickRefs.slice(
        offset,
        offset + DELETE_BATCH_SIZE
      );


    /*
     * Determine tokens belonging to this chunk.
     */

    const chunkTokens =
      new Set();

    for (const clickRef of clickChunk) {
      const token =
        normalizeToken(clickRef.id);

      if (token) {
        chunkTokens.add(token);
      }
    }


    const batch =
      db.batch();


    clickChunk.forEach(ref => {
      batch.delete(ref);
    });


    for (const token of chunkTokens) {
      batch.delete(
        db.doc(`tokenIndex/${token}`)
      );
    }


    if (!DRY_RUN) {
      await batch.commit();
    }


    clicksDeleted +=
      clickChunk.length;

    tokenIndexesDeleted +=
      chunkTokens.size;


    console.log(
      `${DRY_RUN ? "Would process" : "Deleted"} batch: ` +
      `${clickChunk.length} click(s), ` +
      `${chunkTokens.size} tokenIndex doc(s)`
    );
  }


  /*
   * Handle manually specified tokens for which
   * tokenIndex exists but the click document was already gone.
   */

  const clickTokens =
    new Set(
      clickRefs.map(ref =>
        normalizeToken(ref.id)
      )
    );


  const leftoverTokens =
    tokens.filter(
      token =>
        !clickTokens.has(token)
    );


  if (leftoverTokens.length) {
    for (
      let offset = 0;
      offset < leftoverTokens.length;
      offset += 400
    ) {
      const chunk =
        leftoverTokens.slice(
          offset,
          offset + 400
        );

      const batch =
        db.batch();

      chunk.forEach(token => {
        batch.delete(
          db.doc(`tokenIndex/${token}`)
        );
      });

      if (!DRY_RUN) {
        await batch.commit();
      }

      tokenIndexesDeleted +=
        chunk.length;
    }
  }


  return {
    clicksDeleted,
    tokenIndexesDeleted
  };
}


/* =========================================================
   MAIN
   ========================================================= */

async function main() {
  console.log("");
  console.log("============================================");
  console.log("WaTrck TEST / TOKEN CLEANUP");
  console.log("============================================");

  console.log(
    `Delete test conversions: ${
      DELETE_TEST_CONVERSIONS
        ? "YES"
        : "NO"
    }`
  );

  console.log(
    `Test marker: ${TEST_MARKER}`
  );

  console.log(
    `Manual tokens: ${TOKENS_TO_DELETE.length}`
  );

  console.log(
    `Dry run: ${DRY_RUN ? "YES" : "NO"}`
  );

  console.log(
    "WhatsApp chats/messages: ALWAYS PRESERVED"
  );


  if (
    !DELETE_TEST_CONVERSIONS &&
    TOKENS_TO_DELETE.length === 0
  ) {
    console.log("");
    console.log(
      "Nothing configured for deletion."
    );

    console.log(
      "Enable DELETE_TEST_CONVERSIONS or add tokens."
    );

    return;
  }


  const deletePlan =
    await buildDeleteList();


  console.log("");
  console.log("============================================");
  console.log("SCAN COMPLETE");
  console.log("============================================");

  console.log(
    `Click docs found: ${deletePlan.clickRefs.length}`
  );

  console.log(
    `Tokens found: ${deletePlan.tokens.length}`
  );


  if (
    !deletePlan.clickRefs.length &&
    !deletePlan.tokens.length
  ) {
    console.log("");
    console.log(
      "Nothing matched. No deletion needed."
    );

    return;
  }


  const confirmed =
    await askConfirmation(
      deletePlan
    );


  if (!confirmed) {
    console.log("");
    console.log("Cancelled.");
    return;
  }


  const result =
    await executeDeletePlan(
      deletePlan
    );


  console.log("");
  console.log("============================================");
  console.log("SUMMARY");
  console.log("============================================");


  if (DRY_RUN) {
    console.log(
      `Would delete click docs: ${result.clicksDeleted}`
    );

    console.log(
      `Would delete tokenIndex docs: ${result.tokenIndexesDeleted}`
    );

    console.log("");
    console.log(
      "DRY RUN ONLY - nothing was deleted."
    );

  } else {
    console.log(
      `Click docs deleted: ${result.clicksDeleted}`
    );

    console.log(
      `tokenIndex docs deleted: ${result.tokenIndexesDeleted}`
    );

    console.log("");
    console.log(
      "Cleanup finished successfully."
    );
  }


  console.log("");
  console.log(
    "WhatsApp conversations/messages were preserved."
  );

  console.log(
    "Google Sheets rows were preserved."
  );
}


/* =========================================================
   RUN
   ========================================================= */

if (require.main === module) {
  main().catch(error => {
    console.error("");
    console.error(
      "CLEANUP FAILED:",
      error.stack || error
    );

    process.exitCode = 1;
  });
}