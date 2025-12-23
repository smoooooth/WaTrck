// Adjustments Sheet Apps Script - updated to use a single global adjustments sheet
// Place this script inside the dedicated adjustments Google Sheet workbook.


// ---------------- CONFIG ----------------
const SHEET_FILE_ID = '14__57ycyK1QciUL3r7GyNiQGYlSivSc2hqtCPG_B4E0';
const ADJUSTMENTS_SHEET_NAME = 'Adjusted_Conversions_Sheet';


const PROJECT_ID = 'aida';
const EXPORT_BASE = 'https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/exportsApi/exports/adjustments-pending';
const MARK_BASE   = 'https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/exportsApi/exports/mark-adjustments-exported';


const CONVERSION_NAME = 'AIDA_WA_Contact_Leads';
const ADJUSTED_VALUE_CURRENCY = 'USD';


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


  const allAppendedItems = [];
  for (let i = 0; i < adjustments.length; i += BATCH_SIZE) {
    const batch = adjustments.slice(i, i + BATCH_SIZE);
    // appendAdjustmentRowsBatch now returns appended items with project included
    const appendedItems = appendAdjustmentRowsBatch(batch);
    if (appendedItems && appendedItems.length) {
      allAppendedItems.push(...appendedItems);


      // group appended items by project and call mark-adjustments-exported per project
      const byProject = {};
      appendedItems.forEach(it => {
        const p = it.project || it.projectId || getProjectIdRuntime() || 'unknown';
        if (!byProject[p]) byProject[p] = [];
        byProject[p].push({ order_id: it.order_id, upload_version: it.upload_version });
      });


      for (const projKey in byProject) {
        try {
          markExportedForProject(byProject[projKey], projKey);
        } catch (e) {
          Logger.log('mark-adjustments-exported failed for project=' + projKey + ' err=' + e);
        }
      }
    }
  }


  Logger.log('Finished. Appended adjustments for items count: ' + allAppendedItems.length);
}


// ================ APPEND adjustments helper =================
function appendAdjustmentRowsBatch(rows) {
  if (!rows || rows.length === 0) return [];


  const ss = SpreadsheetApp.openById(getSheetFileIdRuntime());
  let sheet = ss.getSheetByName(ADJUSTMENTS_SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(ADJUSTMENTS_SHEET_NAME);
    const headers = [
      'Project ID','Google Click ID','Conversion Name','Conversion Time','Adjustment Time',
      'Adjustment Type','Adjustment Value','Adjusted Value Currency','Upload Version'
    ];
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }


  const lastRow = sheet.getLastRow();
  const existingKeys = new Set();
  if (lastRow >= 2) {
    // We now have 9 columns: Project ID (col1), Google Click ID (col2), Conversion Name (col3), ... Upload Version (col9)
    const data = sheet.getRange(2, 1, lastRow - 1, 9).getValues();
    for (let r = 0; r < data.length; r++) {
      const g = data[r][1] ? String(data[r][1]).trim() : '';      // Google Click ID at index 1 (col2)
      const conv = data[r][2] ? String(data[r][2]).trim() : '';   // Conversion Name at index 2 (col3)
      const upv = (data[r][8] !== undefined && data[r][8] !== null) ? String(data[r][8]) : '0'; // Upload version at index 8 (col9)
      if (g && conv) existingKeys.add(g + '|' + upv + '|' + conv);
    }
  }


  const rowsToAppend = [];
  const appendedItems = [];


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


      const adjTime = formatAsGoogleAdsDatetime(new Date());
      const adjustedValue =
        (typeof item.conversion_value_final === 'number')
          ? item.conversion_value_final
          : (item.conversion_value || 0);


      // project for this item (ensure something present so mark-exported can be called)
      const projectForItem = item.project || item.projectId || getProjectIdRuntime() || '';


      function pushRowForConv(convName) {
        // build list of conversion names we should push adjustments for (unique)
        const convNames = [];
        if (convPrimary) convNames.push(convPrimary);
        if (convSales && convSales !== '') convNames.push(convSales);
        if (item.conversion_name_sales_qualified && item.conversion_name_sales_qualified !== '') convNames.push(String(item.conversion_name_sales_qualified).trim());


        // dedupe and push each distinct conversion name (skip if same as primary already pushed)
        const seenConv = new Set();
        convNames.forEach(convName => {
          if (!convName) return;
          const trimmed = String(convName).trim();
          if (seenConv.has(trimmed)) return;
          seenConv.add(trimmed);
          const key = gclid + '|' + uploadV + '|' + trimmed;
          if (existingKeys.has(key)) {
            if (DEBUG) Logger.log('appendAdjustmentRowsBatch: skipping existing key %s', key);
            return;
          }
          // push with Project ID as first column
          rowsToAppend.push([
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
          existingKeys.add(key);
        });
      }


      // primary
      pushRowForConv(convPrimary);
      // sales conversion if present and different
      if (convSales && convSales !== convPrimary) pushRowForConv(convSales);


      if (item.order_id) {
        appendedItems.push({
          order_id: String(item.order_id).trim(),
          upload_version: uploadV,
          project: projectForItem
        });
      }
    } catch (inner) {
      Logger.log('appendAdjustmentRowsBatch: row processing error: ' + inner);
    }
  });


  if (rowsToAppend.length) {
    // write 9 columns now
    sheet.getRange(sheet.getLastRow() + 1, 1, rowsToAppend.length, 9).setValues(rowsToAppend);
    if (DEBUG) Logger.log('appendAdjustmentRowsBatch: appended %d rows', rowsToAppend.length);
  } else {
    if (DEBUG) Logger.log('appendAdjustmentRowsBatch: nothing to append (all keys present or no valid rows).');
  }


  return appendedItems;
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










