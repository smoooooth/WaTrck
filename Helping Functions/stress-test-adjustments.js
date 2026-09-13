/**
 * WaTrck - ADJUSTMENTS EXPORT STRESS TEST
 * =======================================
 *
 * Purpose:
 * Stress-test:
 *
 * Firestore sales-style updates
 *   -> adjustment_pending
 *   -> /exports/adjustments-pending
 *   -> Adjustments Apps Script
 *   -> Adjusted_Conversions_Sheet
 *   -> /exports/mark-adjustments-exported
 *   -> adjustment_pending=false
 *
 * IMPORTANT:
 * - Uses ONLY _testproduction_ GCLIDs.
 * - Does NOT touch WhatsApp conversations.
 * - Creates brand-new test click docs + tokenIndex docs.
 * - Waits for Phase 1 to be fully acknowledged before Phase 2.
 *
 * Run:
 * node stress-test-adjustments.js
 */

const {
  initializeApp,
  applicationDefault
} = require('firebase-admin/app');

const {
  getFirestore,
  Timestamp,
  FieldValue
} = require('firebase-admin/firestore');

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');


/* =========================================================
   FIREBASE
   ========================================================= */

initializeApp({
  credential: applicationDefault(),
  projectId: 'aida-muscat-wa-tracking'
});

const db = getFirestore();


/* =========================================================
   MASTER CONFIG
   ========================================================= */

const PROJECT_IDS = [
  'AIDA_Oman',
  'Trump Plaza Jeddah',
  'Trump Park Residences'
];

// Exact marker recognized by the WaTrck test exporters / cleanup helper.
const TEST_MARKER = '_testproduction_';

// Each set creates 6 leads PER PROJECT.
//
// One complete set eventually produces:
//   9 adjustment rows per project.
//
// With:
//   3 projects
//   3 sets/project
//
// Expected final adjustment rows:
//   3 × 3 × 9 = 81
const SETS_PER_PROJECT = 3;

// Base Contact value.
const INITIAL_VALUE = 70;

// Small delay between Firestore updates.
// This makes logs easier to follow and produces a realistic zigzag.
const UPDATE_DELAY_MS = 100;

// How often we check whether Adjustments exporter acknowledged everything.
const POLL_INTERVAL_MS = 5000;

// Maximum time to wait for each export phase.
// 15 minutes gives Cloud Scheduler plenty of time.
const EXPORT_TIMEOUT_MS = 15 * 60 * 1000;

// Manifest written beside this script.
const MANIFEST_FILENAME =
  'adjustments-stress-last-run.json';


/* =========================================================
   SCENARIOS
   ========================================================= */

/**
 * Six leads are created in every set.
 *
 * Phase 1:
 *
 * U       New -> Unqualified
 * Q       New -> Qualified
 * C       New -> Closed
 * Q2C     New -> Qualified
 * QEDIT   New -> Qualified
 * CEDIT   New -> Closed
 *
 * Phase 2:
 *
 * Q2C     Qualified -> Closed
 * QEDIT   Qualified -> Qualified, NEW VALUE
 * CEDIT   Closed -> Closed, NEW VALUE
 *
 *
 * Therefore each set/project produces:
 *
 * Contact adjustments:   1
 * Qualified adjustments: 4
 * Closed adjustments:    4
 *
 * Total:                  9
 */
const SCENARIOS = [
  'U',
  'Q',
  'C',
  'Q2C',
  'QEDIT',
  'CEDIT'
];


/* =========================================================
   SMALL HELPERS
   ========================================================= */

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function randString(len = 7) {
  const chars =
    'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';

  const bytes =
    crypto.randomBytes(len);

  let result = '';

  for (let i = 0; i < len; i++) {
    result +=
      chars[
        bytes[i] %
        chars.length
      ];
  }

  return result;
}

function makeRunId() {
  const now = new Date();

  return (
    now
      .toISOString()
      .replace(/[-:.TZ]/g, '')
      .slice(0, 14) +
    '-' +
    randString(4)
  );
}

function validateConfig() {
  if (PROJECT_IDS.length < 2) {
    throw new Error(
      'PROJECT_IDS must contain at least 2 projects.'
    );
  }

  const unique =
    new Set(PROJECT_IDS);

  if (unique.size !== PROJECT_IDS.length) {
    throw new Error(
      'PROJECT_IDS contains duplicates.'
    );
  }

  if (
    !Number.isInteger(SETS_PER_PROJECT) ||
    SETS_PER_PROJECT < 1
  ) {
    throw new Error(
      'SETS_PER_PROJECT must be at least 1.'
    );
  }
}

function contactName(projectId) {
  return projectId + ' - TEST Contact';
}

function qualifiedName(projectId) {
  return projectId + ' - TEST Qualified';
}

function closedName(projectId) {
  return projectId + ' - TEST Closed';
}

function qualityCode(quality) {
  if (quality === 'unqualified') return 0;
  if (quality === 'qualified') return 1;
  if (quality === 'closed') return 2;

  throw new Error(
    'Unknown quality: ' + quality
  );
}

function conversionNameForQuality(
  projectId,
  quality
) {
  if (quality === 'qualified') {
    return qualifiedName(projectId);
  }

  if (quality === 'closed') {
    return closedName(projectId);
  }

  return null;
}


/* =========================================================
   VALUES
   ========================================================= */

/**
 * Generate deterministic values so rows are easier
 * to recognize visually in the Google Sheet.
 */
function getValue(
  projectIndex,
  setIndex,
  scenario,
  phase
) {
  const offset =
    projectIndex * 100 +
    setIndex * 10;

  if (scenario === 'U') {
    return 0;
  }

  if (scenario === 'Q') {
    return 500 + offset;
  }

  if (scenario === 'C') {
    return 2000 + offset;
  }

  if (scenario === 'Q2C') {
    if (phase === 1) {
      return 600 + offset;
    }

    return 3000 + offset;
  }

  if (scenario === 'QEDIT') {
    if (phase === 1) {
      return 700 + offset;
    }

    return 900 + offset;
  }

  if (scenario === 'CEDIT') {
    if (phase === 1) {
      return 2500 + offset;
    }

    return 3500 + offset;
  }

  throw new Error(
    'Unknown scenario: ' + scenario
  );
}


/* =========================================================
   EXPECTED ADJUSTMENT DESCRIPTION
   ========================================================= */

function getTargetConversionName(
  projectId,
  quality
) {
  if (quality === 'unqualified') {
    return contactName(projectId);
  }

  if (quality === 'qualified') {
    return qualifiedName(projectId);
  }

  if (quality === 'closed') {
    return closedName(projectId);
  }

  throw new Error(
    'Unknown quality: ' + quality
  );
}


/* =========================================================
   CREATE TEST LEADS
   ========================================================= */

function buildTestLead(
  runId,
  projectId,
  projectIndex,
  setIndex,
  scenario
) {
  const token =
    randString(7);

  const gclid =
    TEST_MARKER +
    '-adjustments-' +
    runId +
    '-' +
    randString(12);

  const now =
    new Date();

  const nowTs =
    Timestamp.now();

  const docPath =
    'projects/' +
    projectId +
    '/clicks/' +
    token;

  const payload = {
    token,
    projectId,

    gclid,
    google_campaign_id: null,

    conversion_name:
      contactName(projectId),

    conversion_name_sales: null,
    conversion_name_sales_qualified: null,

    conversion_value_initial:
      INITIAL_VALUE,

    conversion_value_final: null,

    conversion_value_source:
      'adjustments_stress_test',

    /*
     * Treat the original Contact conversion as already handled.
     *
     * A sales-style update below will reset this to false,
     * exactly like /sales/quality-update currently does.
     */
    conversion_value_uploaded: true,

    conversion_value_uploaded_at:
      nowTs,

    google_sheet_exported: true,
    exported_once: true,

    lead_value_estimate:
      INITIAL_VALUE,

    upload_version: 0,

    last_adjustment_version_exported: 0,

    adjustment_pending: false,

    sales_sheet_updated_quality: null,
    sales_sheet_quality_uploaded: true,
    quality_status: null,

    used: true,
    used_at: nowTs,

    ts: now.toISOString(),

    ctaId:
      'adjustments_stress_test',

    page:
      'adjustments-stress-test/' +
      projectId,

    stress_test_run_id:
      runId,

    stress_test_scenario:
      scenario,

    stress_test_set:
      setIndex + 1
  };

  const indexPayload = {
    token,
    projectId,
    clickPath: docPath,
    created_at: nowTs
  };

  return {
    runId,
    token,
    gclid,
    projectId,
    projectIndex,
    setIndex,
    scenario,
    docPath,
    payload,
    indexPayload
  };
}


async function createAllLeads(runId) {
  const leads = [];

  /*
   * Slot-major order deliberately alternates projects:
   *
   * AIDA U
   * Trump Plaza U
   * Trump Park U
   * AIDA Q
   * Trump Plaza Q
   * ...
   */
  for (
    let setIndex = 0;
    setIndex < SETS_PER_PROJECT;
    setIndex++
  ) {
    for (
      const scenario of SCENARIOS
    ) {
      for (
        let projectIndex = 0;
        projectIndex < PROJECT_IDS.length;
        projectIndex++
      ) {
        const projectId =
          PROJECT_IDS[projectIndex];

        leads.push(
          buildTestLead(
            runId,
            projectId,
            projectIndex,
            setIndex,
            scenario
          )
        );
      }
    }
  }

  console.log('');
  console.log(
    'Creating ' +
    leads.length +
    ' fresh test leads...'
  );

  /*
   * 2 writes per lead:
   * click + tokenIndex.
   *
   * Keep batches comfortably below Firestore's
   * maximum write count.
   */
  const LEADS_PER_BATCH = 200;

  for (
    let offset = 0;
    offset < leads.length;
    offset += LEADS_PER_BATCH
  ) {
    const chunk =
      leads.slice(
        offset,
        offset + LEADS_PER_BATCH
      );

    const batch =
      db.batch();

    for (const lead of chunk) {
      batch.set(
        db.doc(lead.docPath),
        lead.payload
      );

      batch.set(
        db.doc(
          'tokenIndex/' +
          lead.token
        ),
        lead.indexPayload
      );
    }

    await batch.commit();

    console.log(
      'Seeded ' +
      chunk.length +
      ' leads | total=' +
      Math.min(
        offset + chunk.length,
        leads.length
      ) +
      '/' +
      leads.length
    );
  }

  return leads;
}


/* =========================================================
   SALES-STYLE QUALITY UPDATE
   ========================================================= */

/**
 * Mirrors the important Firestore behavior of the current
 * /sales/quality-update backend:
 *
 * - quality status
 * - numeric quality code
 * - sales_sheet_quality_uploaded=false
 * - conversion_value_uploaded=false
 * - adjustment_pending=true
 * - upload_version++
 * - conversion_value_final
 * - conversion_name_sales
 * - preserve Qualified conversion name when moving
 *   Qualified -> Closed
 */
async function applyQualityUpdate(
  lead,
  quality,
  value
) {
  const ref =
    db.doc(lead.docPath);

  const code =
    qualityCode(quality);

  const salesConversionName =
    conversionNameForQuality(
      lead.projectId,
      quality
    );

  const result =
    await db.runTransaction(
      async tx => {
        const snap =
          await tx.get(ref);

        if (!snap.exists) {
          throw new Error(
            'Missing click doc: ' +
            lead.docPath
          );
        }

        const data =
          snap.data() || {};

        const currentVersion =
          typeof data.upload_version === 'number'
            ? data.upload_version
            : Number(
                data.upload_version || 0
              );

        const newVersion =
          currentVersion + 1;

        const update = {
          quality_status:
            quality,

          sales_sheet_updated_quality:
            code,

          sales_sheet_quality_uploaded:
            false,

          updated_by_sales_at:
            FieldValue.serverTimestamp(),

          conversion_value_uploaded:
            false,

          adjustment_pending:
            true,

          upload_version:
            newVersion,

          conversion_value_final:
            value
        };

        if (salesConversionName) {
          update.conversion_name_sales =
            salesConversionName;
        }

        /*
         * Same preservation behavior as the backend:
         *
         * Qualified -> Closed
         *
         * Remember the Qualified action name.
         */
        if (quality === 'closed') {
          if (
            data.conversion_name_sales &&
            data.sales_sheet_updated_quality === 1
          ) {
            update.conversion_name_sales_qualified =
              String(
                data.conversion_name_sales
              ).trim();
          } else if (
            data.conversion_name_sales_qualified
          ) {
            update.conversion_name_sales_qualified =
              data.conversion_name_sales_qualified;
          }
        }

        await tx.update(
          ref,
          update
        );

        return newVersion;
      }
    );

  return result;
}


/* =========================================================
   PHASE 1
   ========================================================= */

function getPhase1Quality(scenario) {
  if (scenario === 'U') {
    return 'unqualified';
  }

  if (
    scenario === 'Q' ||
    scenario === 'Q2C' ||
    scenario === 'QEDIT'
  ) {
    return 'qualified';
  }

  if (
    scenario === 'C' ||
    scenario === 'CEDIT'
  ) {
    return 'closed';
  }

  throw new Error(
    'Unknown scenario: ' +
    scenario
  );
}


async function runPhase1(
  leads,
  expectedRows
) {
  console.log('');
  console.log(
    '============================================'
  );
  console.log(
    'PHASE 1'
  );
  console.log(
    '============================================'
  );

  const expectations = [];

  for (
    let i = 0;
    i < leads.length;
    i++
  ) {
    const lead =
      leads[i];

    const quality =
      getPhase1Quality(
        lead.scenario
      );

    const value =
      getValue(
        lead.projectIndex,
        lead.setIndex,
        lead.scenario,
        1
      );

    const version =
      await applyQualityUpdate(
        lead,
        quality,
        value
      );

    expectations.push({
      lead,
      expectedVersion: version
    });

    expectedRows.push({
      phase: 1,
      project: lead.projectId,
      token: lead.token,
      gclid: lead.gclid,
      scenario: lead.scenario,
      quality,
      quality_code:
        qualityCode(quality),
      conversion_name:
        getTargetConversionName(
          lead.projectId,
          quality
        ),
      adjustment_value:
        value,
      upload_version:
        version
    });

    console.log(
      '[' +
      String(i + 1).padStart(3, '0') +
      '/' +
      String(leads.length).padStart(3, '0') +
      '] ' +
      lead.projectId +
      ' | ' +
      lead.scenario +
      ' | ' +
      quality +
      ' | value=' +
      value +
      ' | v' +
      version +
      ' | ' +
      lead.token
    );

    if (UPDATE_DELAY_MS > 0) {
      await sleep(
        UPDATE_DELAY_MS
      );
    }
  }

  return expectations;
}


/* =========================================================
   PHASE 2
   ========================================================= */

async function runPhase2(
  leads,
  expectedRows
) {
  console.log('');
  console.log(
    '============================================'
  );
  console.log(
    'PHASE 2 - SECOND UPDATE ON SAME LEADS'
  );
  console.log(
    '============================================'
  );

  const phase2Leads =
    leads.filter(
      lead =>
        lead.scenario === 'Q2C' ||
        lead.scenario === 'QEDIT' ||
        lead.scenario === 'CEDIT'
    );

  const expectations = [];

  for (
    let i = 0;
    i < phase2Leads.length;
    i++
  ) {
    const lead =
      phase2Leads[i];

    let quality;

    if (
      lead.scenario === 'Q2C'
    ) {
      quality = 'closed';
    } else if (
      lead.scenario === 'QEDIT'
    ) {
      quality = 'qualified';
    } else {
      quality = 'closed';
    }

    const value =
      getValue(
        lead.projectIndex,
        lead.setIndex,
        lead.scenario,
        2
      );

    const version =
      await applyQualityUpdate(
        lead,
        quality,
        value
      );

    expectations.push({
      lead,
      expectedVersion: version
    });

    expectedRows.push({
      phase: 2,
      project: lead.projectId,
      token: lead.token,
      gclid: lead.gclid,
      scenario: lead.scenario,
      quality,
      quality_code:
        qualityCode(quality),
      conversion_name:
        getTargetConversionName(
          lead.projectId,
          quality
        ),
      adjustment_value:
        value,
      upload_version:
        version
    });

    console.log(
      '[' +
      String(i + 1).padStart(3, '0') +
      '/' +
      String(phase2Leads.length).padStart(3, '0') +
      '] ' +
      lead.projectId +
      ' | ' +
      lead.scenario +
      ' | ' +
      quality +
      ' | value=' +
      value +
      ' | v' +
      version +
      ' | ' +
      lead.token
    );

    if (UPDATE_DELAY_MS > 0) {
      await sleep(
        UPDATE_DELAY_MS
      );
    }
  }

  return expectations;
}


/* =========================================================
   WAIT FOR REAL ADJUSTMENTS EXPORTER
   ========================================================= */

async function checkAcknowledgements(
  expectations
) {
  const refs =
    expectations.map(
      item =>
        db.doc(
          item.lead.docPath
        )
    );

  const snaps =
    await db.getAll(
      ...refs
    );

  const incomplete = [];

  for (
    let i = 0;
    i < snaps.length;
    i++
  ) {
    const snap =
      snaps[i];

    const expectation =
      expectations[i];

    if (!snap.exists) {
      incomplete.push({
        token:
          expectation.lead.token,
        reason:
          'document disappeared'
      });

      continue;
    }

    const data =
      snap.data() || {};

    const lastExported =
      typeof data.last_adjustment_version_exported ===
      'number'
        ? data.last_adjustment_version_exported
        : Number(
            data.last_adjustment_version_exported ||
              0
          );

    const pending =
      data.adjustment_pending === true;

    if (
      lastExported <
        expectation.expectedVersion ||
      pending
    ) {
      incomplete.push({
        token:
          expectation.lead.token,

        project:
          expectation.lead.projectId,

        scenario:
          expectation.lead.scenario,

        expectedVersion:
          expectation.expectedVersion,

        lastExported,

        adjustment_pending:
          data.adjustment_pending
      });
    }
  }

  return incomplete;
}


async function waitForAdjustmentsExporter(
  expectations,
  phaseName
) {
  const deadline =
    Date.now() +
    EXPORT_TIMEOUT_MS;

  console.log('');
  console.log(
    'Waiting for real Adjustments exporter: ' +
    phaseName
  );

  console.log(
    'Expected acknowledgements: ' +
    expectations.length
  );

  while (true) {
    const incomplete =
      await checkAcknowledgements(
        expectations
      );

    const done =
      expectations.length -
      incomplete.length;

    console.log(
      '[' +
      new Date().toLocaleTimeString() +
      '] acknowledged=' +
      done +
      '/' +
      expectations.length
    );

    if (
      incomplete.length === 0
    ) {
      console.log(
        phaseName +
        ' fully acknowledged.'
      );

      return;
    }

    if (
      Date.now() >= deadline
    ) {
      console.error('');
      console.error(
        'TIMEOUT waiting for ' +
        phaseName
      );

      console.error(
        'First incomplete records:'
      );

      console.error(
        JSON.stringify(
          incomplete.slice(0, 20),
          null,
          2
        )
      );

      throw new Error(
        phaseName +
        ' was not fully acknowledged before timeout. ' +
        'Check the Adjustments Apps Script execution log and Cloud Scheduler.'
      );
    }

    await sleep(
      POLL_INTERVAL_MS
    );
  }
}


/* =========================================================
   FINAL FIRESTORE VERIFICATION
   ========================================================= */

async function finalVerification(
  leads
) {
  console.log('');
  console.log(
    '============================================'
  );
  console.log(
    'FINAL FIRESTORE VERIFICATION'
  );
  console.log(
    '============================================'
  );

  const refs =
    leads.map(
      lead =>
        db.doc(
          lead.docPath
        )
    );

  const snaps =
    await db.getAll(
      ...refs
    );

  const failures = [];

  for (
    let i = 0;
    i < snaps.length;
    i++
  ) {
    const lead =
      leads[i];

    const snap =
      snaps[i];

    if (!snap.exists) {
      failures.push({
        token: lead.token,
        problem:
          'click doc missing'
      });

      continue;
    }

    const data =
      snap.data() || {};

    const uploadVersion =
      Number(
        data.upload_version || 0
      );

    const lastExported =
      Number(
        data.last_adjustment_version_exported ||
          0
      );

    if (
      data.adjustment_pending !== false
    ) {
      failures.push({
        token: lead.token,
        project:
          lead.projectId,
        problem:
          'adjustment_pending is not false',
        value:
          data.adjustment_pending
      });
    }

    if (
      lastExported !==
      uploadVersion
    ) {
      failures.push({
        token: lead.token,
        project:
          lead.projectId,
        problem:
          'version mismatch',
        uploadVersion,
        lastExported
      });
    }
  }

  if (failures.length) {
    console.error(
      JSON.stringify(
        failures,
        null,
        2
      )
    );

    throw new Error(
      failures.length +
      ' final Firestore verification failure(s).'
    );
  }

  console.log(
    'PASS: all ' +
    leads.length +
    ' test leads are fully acknowledged.'
  );

  console.log(
    'PASS: adjustment_pending=false on every lead.'
  );

  console.log(
    'PASS: last_adjustment_version_exported === upload_version on every lead.'
  );
}


/* =========================================================
   MANIFEST / EXPECTED RESULTS
   ========================================================= */

function buildSummary(
  expectedRows
) {
  const byProject = {};
  const byQuality = {
    unqualified: 0,
    qualified: 0,
    closed: 0
  };

  const byVersion = {};

  for (
    const row of expectedRows
  ) {
    if (!byProject[row.project]) {
      byProject[row.project] = {
        total: 0,
        contact: 0,
        qualified: 0,
        closed: 0
      };
    }

    const p =
      byProject[row.project];

    p.total++;

    if (
      row.quality === 'unqualified'
    ) {
      p.contact++;
    }

    if (
      row.quality === 'qualified'
    ) {
      p.qualified++;
    }

    if (
      row.quality === 'closed'
    ) {
      p.closed++;
    }

    byQuality[row.quality]++;

    const versionKey =
      'v' + row.upload_version;

    byVersion[versionKey] =
      (byVersion[versionKey] || 0) +
      1;
  }

  return {
    totalExpectedRows:
      expectedRows.length,

    byProject,

    byQuality,

    byVersion
  };
}


function writeManifest(
  runId,
  leads,
  expectedRows
) {
  const summary =
    buildSummary(
      expectedRows
    );

  const manifest = {
    generated_at:
      new Date().toISOString(),

    run_id:
      runId,

    test_marker:
      TEST_MARKER,

    projects:
      PROJECT_IDS,

    sets_per_project:
      SETS_PER_PROJECT,

    leads_created:
      leads.length,

    expected_adjustment_rows:
      expectedRows.length,

    summary,

    expected_rows:
      expectedRows
  };

  const filePath =
    path.join(
      __dirname,
      MANIFEST_FILENAME
    );

  fs.writeFileSync(
    filePath,
    JSON.stringify(
      manifest,
      null,
      2
    ),
    'utf8'
  );

  console.log('');
  console.log(
    'Manifest saved:'
  );

  console.log(
    filePath
  );

  return manifest;
}


function printExpectedSummary(
  manifest
) {
  console.log('');
  console.log(
    '============================================'
  );
  console.log(
    'EXPECTED ADJUSTMENTS SHEET RESULT'
  );
  console.log(
    '============================================'
  );

  console.log(
    'Run ID:',
    manifest.run_id
  );

  console.log(
    'Test leads:',
    manifest.leads_created
  );

  console.log(
    'EXPECTED SHEET ROWS:',
    manifest.expected_adjustment_rows
  );

  console.log('');

  for (
    const projectId of PROJECT_IDS
  ) {
    const p =
      manifest.summary.byProject[
        projectId
      ];

    console.log(
      projectId
    );

    console.log(
      '  Total:     ' +
      p.total
    );

    console.log(
      '  Contact:   ' +
      p.contact
    );

    console.log(
      '  Qualified: ' +
      p.qualified
    );

    console.log(
      '  Closed:    ' +
      p.closed
    );
  }

  console.log('');

  console.log(
    'All projects combined:'
  );

  console.log(
    '  Unqualified / Contact: ' +
    manifest.summary.byQuality
      .unqualified
  );

  console.log(
    '  Qualified:             ' +
    manifest.summary.byQuality
      .qualified
  );

  console.log(
    '  Closed:                ' +
    manifest.summary.byQuality
      .closed
  );

  console.log('');

  console.log(
    'Upload versions:'
  );

  Object.keys(
    manifest.summary.byVersion
  )
    .sort()
    .forEach(version => {
      console.log(
        '  ' +
        version +
        ': ' +
        manifest.summary.byVersion[
          version
        ]
      );
    });

  console.log('');
  console.log(
    'Filter the Adjusted_Conversions_Sheet for:'
  );

  console.log(
    TEST_MARKER +
    '-adjustments-' +
    manifest.run_id
  );

  console.log(
    '============================================'
  );
}


/* =========================================================
   MAIN
   ========================================================= */

(async function main() {
  try {
    validateConfig();

    const runId =
      makeRunId();

    const expectedRows = [];

    console.log('');
    console.log(
      '============================================'
    );

    console.log(
      'WaTrck ADJUSTMENTS STRESS TEST'
    );

    console.log(
      '============================================'
    );

    console.log(
      'Firebase project: aida-muscat-wa-tracking'
    );

    console.log(
      'Projects:',
      PROJECT_IDS.join(' -> ')
    );

    console.log(
      'Sets per project:',
      SETS_PER_PROJECT
    );

    console.log(
      'Run ID:',
      runId
    );

    console.log('');

    const leads =
      await createAllLeads(
        runId
      );

    /*
     * PHASE 1
     */
    const phase1 =
      await runPhase1(
        leads,
        expectedRows
      );

    /*
     * CRITICAL:
     *
     * Do not start Phase 2 until version 1
     * has been exported and acknowledged.
     *
     * Otherwise version 2 would legitimately
     * replace version 1 before the exporter
     * ever saw it.
     */
    await waitForAdjustmentsExporter(
      phase1,
      'PHASE 1'
    );

    /*
     * PHASE 2
     */
    const phase2 =
      await runPhase2(
        leads,
        expectedRows
      );

    await waitForAdjustmentsExporter(
      phase2,
      'PHASE 2'
    );

    await finalVerification(
      leads
    );

    const manifest =
      writeManifest(
        runId,
        leads,
        expectedRows
      );

    printExpectedSummary(
      manifest
    );

    console.log('');
    console.log(
      'STRESS TEST PASSED.'
    );

    console.log(
      'Now send me a screenshot of the filtered Adjusted_Conversions_Sheet.'
    );

    console.log('');

    process.exit(0);

  } catch (error) {
    console.error('');
    console.error(
      'STRESS TEST FAILED'
    );

    console.error(
      error &&
      error.stack
        ? error.stack
        : error
    );

    console.error('');

    process.exit(1);
  }
})();