// Adjustments Sheet Apps Script - global internal audit log + clean Google Ads adjustments feed
// Place this script inside the dedicated adjustments Google Sheet workbook.


// ---------------- CONFIG ----------------
const SHEET_FILE_ID = '14__57ycyK1QciUL3r7GyNiQGYlSivSc2hqtCPG_B4E0';
const ADJUSTMENTS_SHEET_NAME = 'Adjusted_Conversions_Sheet';
const GOOGLE_ADS_ADJUSTMENTS_SHEET_NAME = 'Google_Ads_Adjustments_Upload';


const PROJECT_ID = 'aida';
const EXPORT_BASE = 'https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/exportsApi/exports/adjustments-pending';
const MARK_BASE   = 'https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/exportsApi/exports/mark-adjustments-exported';


const CONVERSION_NAME = 'AIDA_WA_Contact_Leads';
const ADJUSTED_VALUE_CURRENCY = 'USD';
const TEST_GCLID_MARKER = '_testproduction_';


const BATCH_SIZE = 200;
const DEBUG = true; // set false to reduce logging


// ================ UTIL =================
function getSecret() {
  const s = PropertiesService.getScriptProperties().getProperty('EXPORT_SECRET');
  if (!s) throw new Error('EXPORT_SECRET not set in Script Properties.');
  return s;
}


function getProjectIdRuntime() {
  return PropertiesService.getScriptProperties().getProperty('PROJECT_ID') || PROJECT_ID;
}
function getSheetFileIdRuntime() {
  return PropertiesService.getScriptProperties().getProperty('SHEET_FILE_ID') || SHEET_FILE_ID;
}
function getConversionNameRuntime() {
  return PropertiesService.getScriptProperties().getProperty('CONVERSION_NAME') || CONVERSION_NAME;
}


function pad2(n){ return n < 10 ? '0' + n : n; }
function formatAsGoogleAdsDatetime(d) {
  const yyyy = d.getUTCFullYear();
  const mm = pad2(d.getUTCMonth() + 1);
  const dd = pad2(d.getUTCDate());
  const hh = pad2(d.getUTCHours());
  const mi = pad2(d.getUTCMinutes());
  const ss = pad2(d.getUTCSeconds());
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}+00:00`;
}
function convertIsoToGoogleAdsDatetime(isoStr) {
  if (!isoStr) return formatAsGoogleAdsDatetime(new Date());
  const d = new Date(isoStr);
  if (isNaN(d.getTime())) return isoStr;
  return formatAsGoogleAdsDatetime(d);
}
function normalizeStoredGoogleAdsDatetime(value) {
  if (!value) return '';
  if (Object.prototype.toString.call(value) === '[object Date]' && !isNaN(value.getTime())) {
    return formatAsGoogleAdsDatetime(value);
  }
  return String(value).trim();
}


// ================ MAIN runner =================


function runOnceFetchAdjustments() {
  const secret = getSecret();
  // GLOBAL fetch - no project filter (backend will return items for all projects)
  const url = EXPORT_BASE;


  Logger.log('runOnceFetchAdjustments: fetching URL: ' + url);


  const resp = UrlFetchApp.fetch(url, {
    method: 'get',
    muteHttpExceptions: true,
    headers: { 'x-export-secret': secret }
  });
  const code = resp.getResponseCode();
  const text = resp.getContentText();


  Logger.log('runOnceFetchAdjustments: response code: ' + code);
  Logger.log('runOnceFetchAdjustments: response body preview: ' + (text ? text.slice(0, 3000) : '(empty)'));


  if (code !== 200) {
    throw new Error('exports/adjustments-pending returned ' + code + ': ' + text);
  }


  const payload = JSON.parse(text);
  const items = Array.isArray(payload) ? payload : (payload.items || []);


  Logger.log('runOnceFetchAdjustments: fetched items count: ' + (items ? items.length : 0));


  if (!items || items.length === 0) {
    Logger.log('No pending docs returned.');
    return;
  }


  // select docs that actually require an adjustment
  // NOTE: rely on backend-provided flags (_wantSales/_wantBase) AND also accept numeric final values
  const adjustments = items.filter(it => {
    if (!it) return false;


    const hasGclid = it.gclid && String(it.gclid).trim() !== '';


    // backend hint: doc wants sales export or base export
    const wantSales = !!it._wantSales || ((typeof it.sales_sheet_updated_quality === 'number') && it.sales_sheet_quality_uploaded === false);
    const wantBase = !!it._wantBase;


    // robust numeric parsing of final/initial
    const parseNum = v => {
      if (v === null || v === undefined || v === '') return null;
      if (typeof v === 'number' && Number.isFinite(v)) return v;
      const p = Number(v);
      return Number.isFinite(p) ? p : null;
    };
    const finalVal = parseNum(it.conversion_value_final);
    const initialVal = parseNum(it.conversion_value_initial !== undefined ? it.conversion_value_initial : it.conversion_value);


    const hasFinal = finalVal !== null;
    const differs = hasFinal && (initialVal === null || Number(finalVal) !== Number(initialVal));


    // We want adjustments when:
    // - it has a GCLID AND
    // - final differs from initial (strong case)
    // - OR backend explicitly flagged sales export and there is a numeric final value (covers sales updates)
    if (!hasGclid) return false;
    if (differs) return true;
    if (wantSales && hasFinal) return true;


    // otherwise skip
    return false;
  });


  if (!adjustments.length) {
    Logger.log('No gclid-based adjustments to process (after filtering).');
    return;
  }


  Logger.log('Found ' + adjustments.length + ' gclid-based adjustment(s).');


  const allReadyItems = [];
  for (let i = 0; i < adjustments.length; i += BATCH_SIZE) {
    const batch = adjustments.slice(i, i + BATCH_SIZE);

    // Returns only items whose required sheet state is safely present:
    // - production: internal row + Google-facing row
    // - _testproduction_: internal row only (intentionally excluded from Google feed)
    const readyItems = appendAdjustmentRowsBatch(batch);

    if (readyItems && readyItems.length) {
      allReadyItems.push(...readyItems);


      // group ready items by project and call mark-adjustments-exported per project
      const byProject = {};
      readyItems.forEach(it => {
        const p = it.project || it.projectId || getProjectIdRuntime() || 'unknown';
        if (!byProject[p]) byProject[p] = [];
        byProject[p].push({ order_id: it.order_id, upload_version: it.upload_version });
      });


      for (const projKey in byProject) {
        try {
          markExportedForProject(byProject[projKey], projKey);
        } catch (e) {
          // Do not alter sheet rows on ACK failure.
          // A later retry will detect the existing internal row, reuse its Adjustment Time,
          // confirm/rebuild the Google-facing row, then retry the same backend ACK safely.
          Logger.log('mark-adjustments-exported failed for project=' + projKey + ' err=' + e);
        }
      }
    }
  }


  Logger.log('Finished. Ready/confirmed adjustments count: ' + allReadyItems.length);
}


// ================ APPEND / CONFIRM adjustments helper =================
function appendAdjustmentRowsBatch(rows) {
  if (!rows || rows.length === 0) return [];


  const ss = SpreadsheetApp.openById(getSheetFileIdRuntime());


  // ---------- Internal WaTrck audit sheet ----------
  let sheet = ss.getSheetByName(ADJUSTMENTS_SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(ADJUSTMENTS_SHEET_NAME);
    const headers = [
      'Project ID','Google Click ID','Conversion Name','Conversion Time','Adjustment Time',
      'Adjustment Type','Adjustment Value','Adjusted Value Currency','Upload Version'
    ];
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }


  // Map the stable internal dedupe key to its already-stored Adjustment Time.
  // Key: GCLID | Upload Version | Conversion Name
  const existingInternalTimes = new Map();
  const lastInternalRow = sheet.getLastRow();

  if (lastInternalRow >= 2) {
    const data = sheet.getRange(2, 1, lastInternalRow - 1, 9).getValues();
    for (let r = 0; r < data.length; r++) {
      const g = data[r][1] ? String(data[r][1]).trim() : '';      // Google Click ID: col2
      const conv = data[r][2] ? String(data[r][2]).trim() : '';   // Conversion Name: col3
      const adjTime = normalizeStoredGoogleAdsDatetime(data[r][4]); // Adjustment Time: col5
      const upv = (data[r][8] !== undefined && data[r][8] !== null)
        ? String(data[r][8])
        : '0';                                                    // Upload Version: col9

      if (g && conv) {
        existingInternalTimes.set(g + '|' + upv + '|' + conv, adjTime);
      }
    }
  }


  // ---------- Clean Google Ads adjustment feed ----------
  let googleSheet = ss.getSheetByName(GOOGLE_ADS_ADJUSTMENTS_SHEET_NAME);
  if (!googleSheet) {
    googleSheet = ss.insertSheet(GOOGLE_ADS_ADJUSTMENTS_SHEET_NAME, 0);
    const googleHeaders = [
      'Order ID',
      'Conversion Name',
      'Adjustment Time',
      'Adjustment Type',
      'Adjusted Value',
      'Adjusted Value Currency'
    ];
    googleSheet.getRange(1, 1, 1, googleHeaders.length).setValues([googleHeaders]);
  }


  // Stable Google-facing dedupe key:
  // Order ID | Conversion Name | Adjustment Time
  const existingGoogleKeys = new Set();
  const lastGoogleRow = googleSheet.getLastRow();

  if (lastGoogleRow >= 2) {
    const googleData = googleSheet.getRange(2, 1, lastGoogleRow - 1, 6).getValues();
    for (let r = 0; r < googleData.length; r++) {
      const orderId = googleData[r][0] ? String(googleData[r][0]).trim() : '';
      const conv = googleData[r][1] ? String(googleData[r][1]).trim() : '';
      const adjTime = normalizeStoredGoogleAdsDatetime(googleData[r][2]);

      if (orderId && conv && adjTime) {
        existingGoogleKeys.add(orderId + '|' + conv + '|' + adjTime);
      }
    }
  }


  const internalRowsToAppend = [];
  const googleRowsToAppend = [];
  const readyItems = [];


  rows.forEach(item => {
    try {
      const gclid = item.gclid ? String(item.gclid).trim() : '';
      if (!gclid) return;


      const uploadV =
        (typeof item.upload_version === 'number')
          ? item.upload_version
          : (item.upload_version ? Number(item.upload_version) : 0);


      const convPrimary = (item.conversion_name && String(item.conversion_name).trim())
        ? String(item.conversion_name).trim()
        : getConversionNameRuntime();


      const convSales = (item.conversion_name_sales && String(item.conversion_name_sales).trim())
        ? String(item.conversion_name_sales).trim()
        : null;


      const rawConvTime = item.original_conversion_time || item.conversion_time || '';
      const convTime = rawConvTime ? convertIsoToGoogleAdsDatetime(rawConvTime) : '';


      const adjustedValue =
        (typeof item.conversion_value_final === 'number')
          ? item.conversion_value_final
          : (item.conversion_value || 0);


      // project for this item (ensure something present so mark-exported can be called)
      const projectForItem = item.project || item.projectId || getProjectIdRuntime() || '';


      // Decide which EXISTING conversion action this adjustment belongs to.
      //
      // 0 = Unqualified → adjust original Contact conversion only.
      // 1 = Qualified   → adjust Qualified conversion only.
      // 2 = Closed      → adjust Closed conversion only.
      //
      // For legacy/manual adjustments with no sales quality,
      // preserve the old behavior of targeting the base conversion.
      const qualityCode =
        (typeof item.sales_sheet_updated_quality === 'number')
          ? item.sales_sheet_updated_quality
          : null;


      let targetConversionName = null;


      if (qualityCode === 0) {
        // Unqualified: zero the original Contact conversion.
        targetConversionName = convPrimary;
      } else if (qualityCode === 1 || qualityCode === 2) {
        // Qualified / Closed: adjust only the CURRENT sales-stage conversion.
        targetConversionName = convSales;
      } else {
        // Legacy/manual adjustment fallback.
        targetConversionName = convPrimary;
      }


      if (!targetConversionName) {
        if (DEBUG) {
          Logger.log(
            'appendAdjustmentRowsBatch: no target conversion for order_id=%s quality=%s',
            item.order_id || '',
            qualityCode
          );
        }
        return;
      }


      const trimmed = String(targetConversionName).trim();
      const internalKey = gclid + '|' + uploadV + '|' + trimmed;


      // CRITICAL RETRY RULE:
      // If this adjustment already exists internally, reuse its exact existing Adjustment Time.
      // If it is new, generate the time once and persist/use that same value in both sheets.
      let adjTime = '';

      if (existingInternalTimes.has(internalKey)) {
        adjTime = existingInternalTimes.get(internalKey) || '';

        if (!adjTime) {
          Logger.log(
            'appendAdjustmentRowsBatch: existing internal row has no Adjustment Time; not acknowledging key=%s',
            internalKey
          );
          return;
        }

        if (DEBUG) {
          Logger.log(
            'appendAdjustmentRowsBatch: reusing existing Adjustment Time for key=%s time=%s',
            internalKey,
            adjTime
          );
        }
      } else {
        adjTime = formatAsGoogleAdsDatetime(new Date());

        internalRowsToAppend.push([
          projectForItem,
          gclid,
          trimmed,
          convTime,
          adjTime,
          'RESTATE',
          adjustedValue,
          ADJUSTED_VALUE_CURRENCY,
          uploadV
        ]);

        // Update the in-memory map immediately so duplicate items in the same fetched batch
        // reuse the exact same Adjustment Time and do not create a second internal row.
        existingInternalTimes.set(internalKey, adjTime);
      }


      const orderId = item.order_id ? String(item.order_id).trim() : '';

      // Order ID is required for safe Google-facing adjustment identity AND backend ACK.
      // We still keep/write the internal audit row, but we deliberately leave the item pending.
      if (!orderId) {
        Logger.log(
          'appendAdjustmentRowsBatch: missing order_id; internal row may be logged but item will NOT be Google-fed or acknowledged. key=%s',
          internalKey
        );
        return;
      }


      const isTestProduction = gclid.toLowerCase().indexOf(TEST_GCLID_MARKER) !== -1;
      //const isTestProduction = false; // TEMPORARY: allow stress-test rows into Google Ads sheet


      if (isTestProduction) {
        // Stress/seeding rows remain visible in the internal audit sheet but are intentionally
        // isolated from the clean Google Ads upload feed.
        if (DEBUG) {
          Logger.log(
            'appendAdjustmentRowsBatch: test GCLID isolated from Google Ads feed order_id=%s key=%s',
            orderId,
            internalKey
          );
        }

        readyItems.push({
          order_id: orderId,
          upload_version: uploadV,
          project: projectForItem
        });
        return;
      }


      const googleKey = orderId + '|' + trimmed + '|' + adjTime;


      if (!existingGoogleKeys.has(googleKey)) {
        googleRowsToAppend.push([
          orderId,
          trimmed,
          adjTime,
          'RESTATE',
          adjustedValue,
          ADJUSTED_VALUE_CURRENCY
        ]);

        // Prevent duplicates inside the same fetched batch.
        existingGoogleKeys.add(googleKey);
      } else if (DEBUG) {
        Logger.log(
          'appendAdjustmentRowsBatch: Google-facing row already exists key=%s',
          googleKey
        );
      }


      // Production items reach this point only when their internal state exists/planned
      // and their Google-facing state exists/planned. The function does not return these
      // items until BOTH writes have completed and SpreadsheetApp.flush() succeeds.
      readyItems.push({
        order_id: orderId,
        upload_version: uploadV,
        project: projectForItem
      });
    } catch (inner) {
      Logger.log('appendAdjustmentRowsBatch: row processing error: ' + inner);
    }
  });


  // 1) Internal audit row must exist first.
  if (internalRowsToAppend.length) {
    sheet
      .getRange(sheet.getLastRow() + 1, 1, internalRowsToAppend.length, 9)
      .setValues(internalRowsToAppend);

    if (DEBUG) {
      Logger.log(
        'appendAdjustmentRowsBatch: appended %d internal adjustment row(s)',
        internalRowsToAppend.length
      );
    }
  } else if (DEBUG) {
    Logger.log('appendAdjustmentRowsBatch: no new internal rows needed.');
  }


  // 2) Then the clean Google Ads row must exist for real production adjustments.
  if (googleRowsToAppend.length) {
    googleSheet
      .getRange(googleSheet.getLastRow() + 1, 1, googleRowsToAppend.length, 6)
      .setValues(googleRowsToAppend);

    if (DEBUG) {
      Logger.log(
        'appendAdjustmentRowsBatch: appended %d Google Ads adjustment row(s)',
        googleRowsToAppend.length
      );
    }
  } else if (DEBUG) {
    Logger.log('appendAdjustmentRowsBatch: no new Google Ads rows needed.');
  }


  // 3) Force pending sheet writes to complete BEFORE returning anything eligible for ACK.
  // If either setValues() or flush() throws, this function never returns readyItems,
  // therefore the backend is not acknowledged and the retry remains safe.
  SpreadsheetApp.flush();


  return readyItems;
}


// ================ MARK EXPORTED =================
function markExportedForProject(items, project, job_id) {
  if (!items || !items.length) return;
  const secret = getSecret();
  const url = MARK_BASE + '?project=' + encodeURIComponent(project);
  const body = { items: items };
  if (job_id) body.export_job_id = job_id;
  const payload = JSON.stringify(body);
  const opt = {
    method: 'post',
    contentType: 'application/json',
    payload: payload,
    headers: { 'x-export-secret': secret },
    muteHttpExceptions: true
  };
  const resp = UrlFetchApp.fetch(url, opt);
  const code = resp.getResponseCode();
  const txt = resp.getContentText();
  Logger.log('markExportedForProject(adjustments): url=%s project=%s code=%s body=%s', url, project, code, txt);
  if (code !== 200) {
    throw new Error(project + ' mark-adjustments-exported failed: ' + code + ' - ' + txt);
  }
}


// ---------- INSTALLER & SETUP ----------
const RUNNER_FN = 'runOnceFetchAdjustments';


function installProjectConfig(projectId, conversionName, exportSecret) {
  const props = PropertiesService.getScriptProperties();
  props.setProperties({
    PROJECT_ID: String(projectId),
    CONVERSION_NAME: String(conversionName),
    EXPORT_SECRET: String(exportSecret),
    SHEET_FILE_ID: String(SpreadsheetApp.getActiveSpreadsheet().getId())
  });
  createFiveMinuteAdjustmentsTrigger_clean();
  Logger.log('Installed config for: ' + projectId);
}


function deleteRunnerTriggers() {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === RUNNER_FN) ScriptApp.deleteTrigger(t);
  });
}


function setupProject() {
  const ui = SpreadsheetApp.getUi();
  const pid   = ui.prompt('Project ID', 'Enter the Firestore projectId:', ui.ButtonSet.OK).getResponseText();
  const cname = ui.prompt('Conversion Action Name', 'Enter the Google Ads Conversion Name:', ui.ButtonSet.OK).getResponseText();
  const secret = ui.prompt('Export Secret', 'Enter your EXPORT_SECRET:', ui.ButtonSet.OK).getResponseText();
  const props = PropertiesService.getScriptProperties();
  props.setProperty('PROJECT_ID', pid);
  props.setProperty('CONVERSION_NAME', cname);
  props.setProperty('EXPORT_SECRET', secret);
  props.setProperty('SHEET_FILE_ID', SpreadsheetApp.getActiveSpreadsheet().getId());
  createFiveMinuteAdjustmentsTrigger_clean();
  ui.alert('Setup complete! Properties saved + triggers created.');
}


function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Project Setup')
    .addItem('Run setup (prompts)', 'setupProject')
    .addItem('Create 5-min trigger', 'createFiveMinuteAdjustmentsTrigger_clean')
    .addItem('Delete runner triggers', 'deleteRunnerTriggers')
    .addToUi();
}


function createFiveMinuteAdjustmentsTrigger_clean() {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === RUNNER_FN) ScriptApp.deleteTrigger(t);
  });
  ScriptApp.newTrigger(RUNNER_FN)
    .timeBased()
    .everyMinutes(5)
    .create();
}


function installProjectConfigFromEditor() {
  const PROJECT_ID = 'REPLACE_WITH_PROJECT_ID';
  const CONVERSION_NAME = 'REPLACE_WITH_CONVERSION_NAME';
  const EXPORT_SECRET = 'REPLACE_WITH_EXPORT_SECRET';
  installProjectConfig(PROJECT_ID, CONVERSION_NAME, EXPORT_SECRET);
}





// ================ CLOUD SCHEDULER WEB APP =================

function jsonResponse(object) {
  return ContentService
    .createTextOutput(JSON.stringify(object))
    .setMimeType(ContentService.MimeType.JSON);
}

function runAdjustmentsFromScheduler() {
  const lock = LockService.getScriptLock();

  if (!lock.tryLock(30000)) {
    throw new Error(
      'Could not acquire adjustments exporter lock within 30 seconds. Another export is probably running.'
    );
  }

  try {
    runOnceFetchAdjustments();

    return {
      ok: true
    };
  } finally {
    lock.releaseLock();
  }
}

function doGet(e) {
  return jsonResponse(
    runAdjustmentsFromScheduler()
  );
}

function doPost(e) {
  return jsonResponse(
    runAdjustmentsFromScheduler()
  );
}
