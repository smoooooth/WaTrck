



// ================= CONFIG =================
// Workbook file (one workbook for all projects)
// The script will prefer Script Properties set by setupProject() or installProjectConfig()
const SHEET_FILE_ID = 'SHEET_FILE_ID_IN_URL';


// Single shared sheet names
const SHEET_NAME = 'Conversions_Sheet';       // tab for new conversions (all projects)
const NO_GCLID_SHEET_NAME = 'NoGCLID_Review'; // tab to store conversions missing gclid


// Endpoints (base) - keep as-is or edit if your backend URL changes
const EXPORT_BASE = 'https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/exportsApi/exports/pending';
const MARK_BASE   = 'https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/exportsApi/exports/mark-exported';


const CURRENCY = 'USD';


// Safety / performance
const BATCH_SIZE = 200; // rows per sheet write and per markExport call (tune as needed)


// ================ UTIL: secret =================
function getSecret() {
  const s = PropertiesService.getScriptProperties().getProperty('EXPORT_SECRET');
  if (!s) throw new Error('EXPORT_SECRET not set in Script Properties.');
  return s;
}


// ================ RUNTIME CONFIG HELPERS =================
// We keep helpers for compatibility, but runOnceFetch does not rely on PROJECT_ID to filter anymore.
function getProjectIdRuntime() {
  return PropertiesService.getScriptProperties().getProperty('PROJECT_ID') || null;
}
function getSheetFileIdRuntime() {
  return PropertiesService.getScriptProperties().getProperty('SHEET_FILE_ID') || SHEET_FILE_ID;
}
function getConversionNameRuntime() {
  return PropertiesService.getScriptProperties().getProperty('CONVERSION_NAME') || null;
}


// ================ RUNNER: main loop (global multi-project) =================
function runOnceFetch() {
  let totalAppended = 0;
  let totalNoGclidAppended = 0;
  try {
    // Fetch globally (pass null to fetchPendingConversionsForProject to request all projects)
    const fetchResp = fetchPendingConversionsForProject(null);
    const allRows = Array.isArray(fetchResp) ? fetchResp : (fetchResp.items || []);
    const job_id = (!Array.isArray(fetchResp) && fetchResp.job_id) ? fetchResp.job_id : null;


    if (!allRows || allRows.length === 0) {
      Logger.log(`Global fetch: no pending conversions.`);
      return;
    }


    Logger.log(`Global fetch: fetched ${allRows.length} pending rows.`);


    // Build order->project mapping for marking exported later
    const orderToProject = {};
    allRows.forEach(item => {
      const id = (item.order_id || item.token || '') + '';
      if (!id) return;
      orderToProject[id] = (item.project || item.projectId || null);
    });


    // Split into withGclid / noGclid
    const withGclid = allRows.filter(r => {
      const g = (r.gclid || '').toString().trim();
      return g !== '';
    });
    const noGclid = allRows.filter(r => {
      const g = (r.gclid || '').toString().trim();
      return g === '';
    });


    Logger.log(`Global fetch: withGclid=${withGclid.length}, noGclid=${noGclid.length}`);


    // 1) Handle rows that have GCLID (normal flow) in batches
    for (let i = 0; i < withGclid.length; i += BATCH_SIZE) {
      const batch = withGclid.slice(i, i + BATCH_SIZE);


      // Append rows to the single sheet. We pass null as defaultConversionName to avoid fallback.
      const res = appendRowsToSheet(batch, SHEET_NAME, null);
      const batchAppended = (res && res.appendedOrderIds) ? res.appendedOrderIds : [];
      const batchSkipped  = (res && res.skippedOrderIds)  ? res.skippedOrderIds  : [];


      // Group appended & skipped IDs by project
      const groupedToMark = {};
      batchAppended.concat(batchSkipped).forEach(id => {
        if (!id) return;
        const proj = orderToProject[id] || null;
        if (!proj) {
          Logger.log(`runOnceFetch: skipping markExport for order_id=${id} because project is missing in backend payload.`);
          return;
        }
        if (!groupedToMark[proj]) groupedToMark[proj] = [];
        groupedToMark[proj].push(id);
      });


      // Call mark-exported per project group
      for (const projKey in groupedToMark) {
        const ids = groupedToMark[projKey];
        if (!ids || !ids.length) continue;
        try {
          markExportedForProject(ids, projKey, job_id);
        } catch (markErr) {
          Logger.log(`runOnceFetch: markExported failed for project=${projKey} ids=${ids.length} err=${markErr}`);
        }
      }


      totalAppended += batchAppended.length;
    }


    // 2) Handle no-GCLID rows: append to admin review sheet and mark exported
    if (noGclid && noGclid.length) {
      for (let i = 0; i < noGclid.length; i += BATCH_SIZE) {
        const batch = noGclid.slice(i, i + BATCH_SIZE);
        try {
          // appendNoGclidRows will dedupe by Order ID and return appended order ids
          const appendedIds = appendNoGclidRows(batch, NO_GCLID_SHEET_NAME) || [];
          totalNoGclidAppended += appendedIds.length;


          // Group appendedIds by project and mark exported per project
          const grouped = {};
          appendedIds.forEach(id => {
            const proj = orderToProject[id] || null;
            if (!proj) {
              Logger.log(`runOnceFetch: noGclid order ${id} missing project in payload; skipping mark-export.`);
              return;
            }
            if (!grouped[proj]) grouped[proj] = [];
            grouped[proj].push(id);
          });


          for (const projKey in grouped) {
            const ids = grouped[projKey];
            if (!ids || !ids.length) continue;
            try {
              markExportedForProject(ids, projKey, job_id);
            } catch (markErr) {
              Logger.log(`runOnceFetch: markExported failed for noGclid project=${projKey} ids=${ids.length} err=${markErr}`);
            }
          }
        } catch (e) {
          Logger.log(`runOnceFetch: failed to append noGclid batch: ${e}`);
        }
      }
    }


    Logger.log(`runOnceFetch finished. Total appended (GCLID rows): ${totalAppended}. Total no-GCLID appended: ${totalNoGclidAppended}. Job ID: ${job_id || '(none)'}`);
  } catch (err) {
    Logger.log(`runOnceFetch ERROR: ${err}`);
    throw err;
  }
}


// ================ FETCH from backend =================
/**
 * fetchPendingConversionsForProject(project)
 * - If `project` is provided (non-empty), calls /exports/pending?project=...
 * - If `project` is falsy (null/empty), calls /exports/pending (global) to fetch all projects.
 * - Returns either array or object { items: [...], job_id: '...' } as backend supports.
 */
function fetchPendingConversionsForProject(project) {
  const secret = getSecret();
  // build URL: only append ?project when project provided
  const url = project ? (EXPORT_BASE + '?project=' + encodeURIComponent(project)) : EXPORT_BASE;
  const opt = {
    method: 'get',
    muteHttpExceptions: true,
    headers: { 'x-export-secret': secret }
  };
  const resp = UrlFetchApp.fetch(url, opt);
  const code = resp.getResponseCode();
  if (code !== 200) {
    throw new Error(`exports/pending returned ${code} - ${resp.getContentText()}`);
  }
  const payload = JSON.parse(resp.getContentText());
  // Accept either array or object with items
  if (Array.isArray(payload)) return payload;
  if (payload && Array.isArray(payload.items)) return payload;
  return payload; // fallback
}


// ================ APPEND with dedupe & batching =================
/**
 * appendRowsToSheet(rows, sheetName, defaultConversionName)
 * - Dedupes against combination Google Click ID + ConversionName (columns B + C) before appending.
 * - Returns { appendedOrderIds: [...], skippedOrderIds: [...] }
 *
 * NEW: writes rows with columns:
 * [ Project, Google Click ID, Conversion Name, Conversion Time, Conversion Value, Conversion Value Currency, Order ID, Status, Uploaded At ]
 */
function appendRowsToSheet(rows, sheetName, defaultConversionName) {
  if (!rows || rows.length === 0) return { appendedOrderIds: [], skippedOrderIds: [] };


  const ss = SpreadsheetApp.openById(getSheetFileIdRuntime());
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) throw new Error('Sheet not found: ' + sheetName);


  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  if (headers.length < 9) throw new Error('Sheet must have at least 9 header columns. Found: ' + headers.length);


  // Build set of existing gclid|conversionName keys from columns B (2) and C (3)
  const existingKeys = new Set();
  const lastRow = sheet.getLastRow();
  if (lastRow >= 2) {
    // read columns 2 & 3 (Google Click ID and Conversion Name)
    const existingData = sheet.getRange(2, 2, lastRow - 1, 2).getValues(); // cols B,C
    for (let i = 0; i < existingData.length; i++) {
      const g = existingData[i][0] ? String(existingData[i][0]).trim() : '';
      const cn = existingData[i][1] ? String(existingData[i][1]).trim() : '';
      if (g && cn) existingKeys.add(g + '|' + cn);
    }
  }


  const toAppend = [];
  const appendedOrderIds = [];
  const skippedOrderIds = [];


  rows.forEach(r => {
    const gclid = (r.gclid || '').toString().trim();
    const orderId = (r.order_id || r.token || '').toString().trim();


    if (!gclid) {
      // rows without gclid should be handled by appendNoGclidRows instead; skip here
      if (orderId) skippedOrderIds.push(orderId);
      return;
    }


    // --- Base conversion name (from backend item) ---
    // Do NOT fallback to a script-level default if backend doesn't provide conversion_name.
    const baseConvName = (r.conversion_name && String(r.conversion_name).trim())
      ? String(r.conversion_name).trim()
      : (defaultConversionName ? defaultConversionName : null);


    // --- Sales conversion name & quality (from backend) ---
    const salesConvName = (r.conversion_name_sales && String(r.conversion_name_sales).trim())
      ? String(r.conversion_name_sales).trim()
      : null;


    const salesQualityCode = (typeof r.sales_sheet_updated_quality === 'number')
      ? r.sales_sheet_updated_quality
      : null; // 0 = unqualified, 1 = qualified, 2 = closed


    // We consider sales conversion only when quality is qualified(1) or closed(2)
    const wantSalesConversion = !!salesConvName && (salesQualityCode === 1 || salesQualityCode === 2);


    // Common fields
    const convTime = convertIsoToGoogleAdsDatetime(r.conversion_time);
    const convInitial = (typeof r.conversion_value_initial === 'number')
      ? r.conversion_value_initial
      : ((typeof r.conversion_value === 'number')
          ? r.conversion_value
          : parseFloat(r.conversion_value || 0) || 0);


    // Final value from sales (if present)
    const convFinal = (typeof r.conversion_value_final === 'number')
      ? r.conversion_value_final
      : null;


    const nowIso = new Date().toISOString();
    const status = 'SENT';
    const uploadedAt = nowIso;


    let appendedForThisDoc = false;


    // ---------- 1) Base conversion row ----------
    if (baseConvName) {
      const baseKey = gclid + '|' + baseConvName;
      if (!existingKeys.has(baseKey)) {
        // Base conversion uses its initial value
        const baseValue = convInitial;


        toAppend.push([
          (r.project || r.projectId || ''), // Project
          gclid,                            // Google Click ID
          baseConvName,                     // Conversion Name
          convTime,                         // Conversion Time
          baseValue,                        // Conversion Value
          CURRENCY,                         // Conversion Value Currency
          orderId,                          // Order ID
          status,                           // Status
          uploadedAt                        // Uploaded At
        ]);


        existingKeys.add(baseKey);
        appendedForThisDoc = true;
      }
    }


    // ---------- 2) Sales-based conversion row (Qualified / Closed) ----------
    if (wantSalesConversion) {
      const salesKey = gclid + '|' + salesConvName;
      if (!existingKeys.has(salesKey)) {
        // Sales conversion uses final value if available, else fallback to initial
        const salesValue = (convFinal !== null && !isNaN(convFinal))
          ? convFinal
          : convInitial;


        toAppend.push([
          (r.project || r.projectId || ''), // Project
          gclid,                            // Google Click ID
          salesConvName,                    // Conversion Name (sales)
          convTime,
          salesValue,
          CURRENCY,
          orderId,
          status,
          uploadedAt
        ]);


        existingKeys.add(salesKey);
        appendedForThisDoc = true;
      }
    }


    // Track whether this Firestore doc produced any new rows
    if (orderId) {
      if (appendedForThisDoc) {
        appendedOrderIds.push(orderId);
      } else {
        skippedOrderIds.push(orderId);
      }
    }
  });


  if (toAppend.length) {
    const startRow = Math.max(sheet.getLastRow(), 1) + 1;
    sheet.getRange(startRow, 1, toAppend.length, toAppend[0].length).setValues(toAppend);
  }


  return { appendedOrderIds, skippedOrderIds };
}


/** Helper: reads existing order ids from Order ID column (now column 7) and returns a Set */
function getExistingOrderIdsSet(sheet) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return new Set(); // only header present
  // Read column 7 (Order ID) from row 2 to lastRow
  const range = sheet.getRange(2, 7, lastRow - 1, 1);
  const values = range.getValues();
  const s = new Set();
  for (let i = 0; i < values.length; i++) {
    const v = values[i][0];
    if (v !== null && v !== undefined && String(v).trim() !== '') {
      s.add(String(v).trim());
    }
  }
  return s;
}


// appendNoGclidRows doesn't change much; it writes into review sheet with its own headers.
function appendNoGclidRows(rows, reviewSheetName) {
  if (!rows || rows.length === 0) return [];


  const SS = SpreadsheetApp.openById(getSheetFileIdRuntime());
  const SHEET_NAME_REVIEW = reviewSheetName || NO_GCLID_SHEET_NAME;
  let reviewSheet = SS.getSheetByName(SHEET_NAME_REVIEW);


  // Create review sheet with headers if missing
  if (!reviewSheet) {
    reviewSheet = SS.insertSheet(SHEET_NAME_REVIEW);
      const headers = [
    'Project', 'Order ID', 'Conversion Time', 'Conversion Value', 'Source', 'Reason', 'Upload Version', 'Uploaded At', 'Note'
     ];
    reviewSheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }


  // Build existing set to dedupe by Order ID (so we don't append same missing row repeatedly)
  const existing = new Set();
  const lastRow = reviewSheet.getLastRow();
  if (lastRow >= 2) {
  const existingRange = reviewSheet.getRange(2, 2, lastRow - 1, 1).getValues();
  for (let i = 0; i < existingRange.length; i++) {
    const v = existingRange[i][0];
    if (v && String(v).trim() !== '') existing.add(String(v).trim());
    }
  }










  const nowIso = new Date().toISOString();
  const rowsToAppend = [];
  const appendedOrderIds = [];


  rows.forEach(r => {
    const orderId = (r.order_id || r.token || '').toString().trim();
    if (!orderId) return; // skip if no id
    if (existing.has(orderId)) return; // already present


    const convTime = convertIsoToGoogleAdsDatetime(r.conversion_time);
    const convValue = (typeof r.conversion_value === 'number') ? r.conversion_value : parseFloat(r.conversion_value || 0) || 0;


    // <--- fallback chain for Source: backend field -> r.source -> r.project -> getProjectIdRuntime() -> 'NO_SOURCE'
    const source = (r.conversion_value_source || r.source || r.project || getProjectIdRuntime() || 'NO_SOURCE');


    const reason = 'NO_GCLID';
    const uploadVersion = (typeof r.upload_version === 'number') ? r.upload_version : (r.upload_version || 0);
    rowsToAppend.push([
      (r.project || r.projectId || getProjectIdRuntime() || ''),
      orderId,
      convTime,
      convValue,
      source,
      reason,
      uploadVersion,
      nowIso,
      'REVIEW'
    ]);


    existing.add(orderId);
    appendedOrderIds.push(orderId);
  });


  if (rowsToAppend.length) {
    reviewSheet.getRange(reviewSheet.getLastRow() + 1, 1, rowsToAppend.length, rowsToAppend[0].length).setValues(rowsToAppend);
  }


  return appendedOrderIds;
}


// ================ MARK EXPORTED (per project, optionally with job_id) =================
/**
 * markExportedForProject(orderIds, project, job_id)
 * - Sends { order_ids: [...] } and optionally includes job_id as property.
 * - Requires `project` param (backend uses it to update docs under projects/<project>/clicks/<id>)
 */
function markExportedForProject(orderIds, project, job_id) {
  if (!orderIds || orderIds.length === 0) return;
  if (!project) {
    Logger.log('markExportedForProject: cannot mark exported without project param. Skipping.');
    return;
  }
  const secret = getSecret();
  const url = MARK_BASE + '?project=' + encodeURIComponent(project);
  const body = { order_ids: orderIds };
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
  // Log full response for debugging - remove later
  Logger.log('markExportedForProject: url=%s project=%s code=%s body=%s', url, project, code, txt);
  if (code !== 200) {
    throw new Error(`${project} markExported failed: ${code} - ${txt}`);
  }
  // try parse JSON and log success details if provided
  try {
    const parsed = JSON.parse(txt);
    Logger.log('markExportedForProject: parsed response: %s', JSON.stringify(parsed));
  } catch (e) {
    Logger.log('markExportedForProject: response not JSON');
  }
}


// ================ DATE helpers =================
function convertIsoToGoogleAdsDatetime(isoStr) {
  if (!isoStr) return formatAsGoogleAdsDatetime(new Date());
  const d = new Date(isoStr);
  if (isNaN(d.getTime())) return isoStr;
  return formatAsGoogleAdsDatetime(d);
}
function formatAsGoogleAdsDatetime(d) {
  function pad(n){ return n < 10 ? '0' + n : n; }
  const yyyy = d.getUTCFullYear();
  const mm = pad(d.getUTCMonth() + 1);
  const dd = pad(d.getUTCDate());
  const hh = pad(d.getUTCHours());
  const mi = pad(d.getUTCMinutes());
  const ss = pad(d.getUTCSeconds());
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}+00:00`;
}


// ================ Small test helpers =================
/* Local test runner to verify sheet mapping (no network) */
function runTestFetch() {
  const sample = [
    { project: 'aida', gclid: 'EAIaIQobChMI-test-gclid-1', conversion_time: '2025-11-17T12:34:56Z', conversion_value: 40.00, order_id: 'AIDA-TOKEN-TEST-001', conversion_name: 'AIDA_SLF_Offline' },
    { project: 'other', gclid: '', conversion_time: '2025-11-17T13:00:00+02:00', conversion_value: 25.5, order_id: 'AIDA-TOKEN-TEST-002' }
  ];
  // first item will append to main sheet, second will go to NoGCLID_Review
  const appendedMain = appendRowsToSheet([sample[0]], SHEET_NAME, null);
  const appendedNoGclid = appendNoGclidRows([sample[1]], NO_GCLID_SHEET_NAME);
  Logger.log('Test run complete. Appended main count: ' + appendedMain.appendedOrderIds.length + ', noGclid count: ' + appendedNoGclid.length);
}


/* Quick manual append test that ignores backend (useful to confirm write permissions) */
function appendTestRowNow() {
  const ss = SpreadsheetApp.openById(getSheetFileIdRuntime());
  const sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet) throw new Error('Sheet not found: ' + SHEET_NAME);
  const now = new Date().toISOString();
  // Match new column layout: Project, Google Click ID, Conversion Name, Conversion Time, Value, Currency, Order ID, Status, UploadedAt
  const testRow = ['test-project', 'TEST-GCLID', 'TEST_CONVERSION', now, 1, 'USD', 'TEST_ORDER_'+Date.now(), 'SENT', now];
  sheet.appendRow(testRow);
  Logger.log('appendTestRowNow: appended one test row.');
}


// ---------- Installer helpers for template ----------
const RUNNER_FN = 'runOnceFetch'; // runner function name used in this template


/**
 * installProjectConfig(projectId, conversionName, exportSecret)
 * - Writes Script Properties for this script project. Call once after copying the sheet.
 * Note: projectId is optional for this consolidated-sheet setup.
 */
function installProjectConfig(projectId, conversionName, exportSecret) {
  const props = PropertiesService.getScriptProperties();
  const values = {};
  if (projectId !== undefined && projectId !== null) values.PROJECT_ID = String(projectId);
  if (conversionName !== undefined && conversionName !== null) values.CONVERSION_NAME = String(conversionName);
  if (exportSecret !== undefined && exportSecret !== null) values.EXPORT_SECRET = String(exportSecret);
  // always set SHEET_FILE_ID to the active spreadsheet
  values.SHEET_FILE_ID = SpreadsheetApp.getActiveSpreadsheet().getId();
  props.setProperties(values);


  // create trigger (deletes any existing triggers for the runner first)
  createFiveMinuteFetchTrigger();


  Logger.log('Installed project config (consolidated sheet).');
}


/** createFiveMinuteFetchTrigger
 * Creates a single time-driven trigger for RUNNER_FN every 5 minutes.
 */
function createFiveMinuteFetchTrigger() {
  const projectTriggers = ScriptApp.getProjectTriggers();
  projectTriggers.forEach(t => {
    if (t.getHandlerFunction() === RUNNER_FN) {
      ScriptApp.deleteTrigger(t);
    }
  });
  ScriptApp.newTrigger(RUNNER_FN).timeBased().everyMinutes(5).create();
  Logger.log('Created 5-minute trigger for %s', RUNNER_FN);
}


/** deleteRunnerTriggers - utility to cleanup triggers */
function deleteRunnerTriggers() {
  const projectTriggers = ScriptApp.getProjectTriggers();
  projectTriggers.forEach(t => {
    if (t.getHandlerFunction() === RUNNER_FN) ScriptApp.deleteTrigger(t);
  });
  Logger.log('Deleted triggers for %s', RUNNER_FN);
}


/**
 * UI helper: prompt-based one-time setup.
 * Run this from the Apps Script editor or call it from the custom menu in the sheet.
 */
function setupProject() {
  const ui = SpreadsheetApp.getUi();
  // Prompt user for required values
  const secret = ui.prompt('Export Secret', 'Enter your EXPORT_SECRET (required):', ui.ButtonSet.OK).getResponseText();
  const props = PropertiesService.getScriptProperties();
  props.setProperty('EXPORT_SECRET', secret);
  props.setProperty('SHEET_FILE_ID', SpreadsheetApp.getActiveSpreadsheet().getId());
  // ProjectId/conversionName are optional in consolidated setup; leave blank if unused.
  createFiveMinuteFetchTrigger();
  ui.alert('Setup complete!\n\nExport secret saved + trigger created.');
}


/** Adds a simple custom menu for quick access after copy */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Project Setup')
    .addItem('Run setup (prompts)', 'setupProject')
    .addItem('Create 5-min trigger', 'createFiveMinuteFetchTrigger')
    .addItem('Delete runner triggers', 'deleteRunnerTriggers')
    .addToUi();
}


// Optional: expose a function to run install with direct parameters (for advanced usage)
function installProjectConfigFromEditor() {
  const PROJECT_ID = ''; // optional
  const CONVERSION_NAME = ''; // optional
  const EXPORT_SECRET = 'REPLACE_WITH_EXPORT_SECRET';
  installProjectConfig(PROJECT_ID, CONVERSION_NAME, EXPORT_SECRET);
}










