/**
 * WaTrck - Global Conversions Exporter
 * =====================================
 *
 * PRODUCTION ARCHITECTURE
 * - One Apps Script / Google Sheet for conversions across ALL projects.
 * - Triggered by Google Cloud Scheduler through this script's Web App URL.
 * - doGet()/doPost() -> runOnceFetch() -> global backend /exports/pending.
 * - DO NOT create Apps Script time-based triggers for this exporter.
 *
 * MERGED VERSION
 * This file combines:
 * 1) the newer GitHub funnel logic:
 * - base Contact conversion
 * - new Qualified / Closed conversion occurrences
 * - conversion_value_final for sales-stage conversions
 * - dedupe by GCLID + Conversion Name
 * 2) the useful production protections from the deployed Sheet:
 * - Cloud Scheduler Web App entry points
 * - script lock
 * - real/test separation
 * - No-GCLID review tabs
 * - retry logic when acknowledging exports
 *
 * IMPORTANT
 * - Unqualified does NOT create an "Unqualified" conversion.
 * The normal exporter simply acknowledges the existing base conversion;
 * the separate Adjustments exporter is responsible for RESTATE to $0.
 * - Qualified and Closed DO create new conversion occurrences when
 * conversion_name_sales is provided by the Sales Sheet/backend.
 */
// ================= CONFIG =================
const SHEET_FILE_ID = 'SHEET_FILE_ID_IN_URL'; // Script Property overrides this.
const MAIN_SHEET_NAME = 'Conversions_Sheet';
const NO_GCLID_SHEET_NAME = 'NoGCLID_Review';
const TESTING_MAIN_SHEET_NAME = 'TESTING_Conversions_Sheet';
const TESTING_NO_GCLID_SHEET_NAME = 'TESTING_NoGCLID_Review';
const BACKEND_BASE =
 'https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/exportsApi';
const ENDPOINT_PENDING = BACKEND_BASE + '/exports/pending';
const ENDPOINT_MARK = BACKEND_BASE + '/exports/mark-exported';
const CURRENCY = 'USD';
// Backend query size. Current backend defaults to 500; keeping it explicit makes
// the production behavior obvious.
const FETCH_LIMIT = 500;
// Maximum number of backend items processed in one sheet write/ack cycle.
const WRITE_BATCH_SIZE = 200;
// Marker used to isolate production tests from the real Google Ads feed.
const TEST_MARKER = '_testproduction_';
// Retry policy for /exports/mark-exported.
const MAX_MARK_RETRIES = 2;
// ================= SHEET SCHEMAS =================
// Keep these aligned with the current production workbook.
const MAIN_HEADERS = [
 'Project',
 'GCLID',
 'Conversion Name',
 'Conversion Time',
 'Value',
 'Currency',
 'Order ID',
 'Status',
 'Uploaded At'
];
const NO_GCLID_HEADERS = [
 'Project ID',
 'Token',
 'Conv. Time',
 'Conv. Value',
 'Source',
 'Reason',
 'Upload Version',
 'Uploaded At',
 'Note'
];
const TESTING_MAIN_HEADERS = MAIN_HEADERS.concat(['Test Marker']);
const TESTING_NO_GCLID_HEADERS = NO_GCLID_HEADERS.concat(['Test Marker']);
// ================= SCRIPT PROPERTIES =================
function getSecret() {
 const value =
 PropertiesService.getScriptProperties().getProperty('EXPORT_SECRET');
 if (!value) {
 throw new Error('EXPORT_SECRET not set in Script Properties.');
 }
 return value;
}
function getSheetId() {
 return (
 PropertiesService.getScriptProperties().getProperty('SHEET_FILE_ID') ||
 SHEET_FILE_ID
 );
}
// ================= SMALL HELPERS =================
function normalizeGclid(value) {
 if (value === null || value === undefined) return '';
 let text = String(value).trim();
 if (!text) return '';
 const lowered = text.toLowerCase();
 if (
 lowered === 'null' ||
 lowered === 'undefined' ||
 lowered === 'nan'
 ) {
 return '';
 }
 // Remove invisible Unicode characters that occasionally appear in copied IDs.
 text = text.replace(/[\u200B-\u200D\uFEFF]/g, '').trim();
 return text;
}
function cleanString(value) {
 if (value === null || value === undefined) return '';
 return String(value).trim();
}
function toFiniteNumber(value) {
 if (value === null || value === undefined || value === '') return null;
 if (typeof value === 'number') {
 return Number.isFinite(value) ? value : null;
 }
 const parsed = Number(value);
 return Number.isFinite(parsed) ? parsed : null;
}
function getQualityCode(item) {
 const raw = item ? item.sales_sheet_updated_quality : null;
 if (typeof raw === 'number' && Number.isFinite(raw)) {
 return raw;
 }
 if (raw === null || raw === undefined || raw === '') {
 return null;
 }
 const parsed = Number(raw);
 return Number.isFinite(parsed) ? parsed : null;
}
function getProject(item) {
 return cleanString(item && (item.project || item.projectId));
}
function getOrderId(item) {
 return cleanString(item && (item.order_id || item.token));
}
// Current backend returns gclid to /exports/pending.
// The google_campaign_id check is deliberately preserved so it becomes active
// automatically if that field is later added to the pending-export payload.
function getTestMarkerReason(item) {
 const gclid = cleanString(item && item.gclid);
 if (gclid.indexOf(TEST_MARKER) !== -1) {
 return 'TEST (gclid contains ' + TEST_MARKER + ')';
 }
 const campaignId = cleanString(item && item.google_campaign_id);
 if (campaignId && campaignId.indexOf(TEST_MARKER) !== -1) {
 return 'TEST (google_campaign_id contains ' + TEST_MARKER + ')';
 }
 return '';
}
function isTestConversion(item) {
 return getTestMarkerReason(item) !== '';
}
// ================= DATE HELPERS =================
function pad2(number) {
 return number < 10 ? '0' + number : String(number);
}
function formatAsGoogleAdsDatetime(date) {
 return (
 date.getUTCFullYear() +
 '-' +
 pad2(date.getUTCMonth() + 1) +
 '-' +
 pad2(date.getUTCDate()) +
 ' ' +
 pad2(date.getUTCHours()) +
 ':' +
 pad2(date.getUTCMinutes()) +
 ':' +
 pad2(date.getUTCSeconds()) +
 '+00:00'
 );
}
function toGoogleAdsDatetime(value) {
 if (!value) return formatAsGoogleAdsDatetime(new Date());
 const date = new Date(value);
 if (isNaN(date.getTime())) return value;
 return formatAsGoogleAdsDatetime(date);
}
// ================= BACKEND FETCH =================
/**
 * Fetch pending conversions GLOBALLY.
 *
 * Supports either backend response shape:
 * [ ...items ]
 * or:
 * { items: [ ...items ], ... }
 */
function fetchPendingConversions() {
 const url = ENDPOINT_PENDING + '?limit=' + encodeURIComponent(FETCH_LIMIT);
 const response = UrlFetchApp.fetch(url, {
 method: 'get',
 headers: { 'x-export-secret': getSecret() },
 muteHttpExceptions: true
 });
 const code = response.getResponseCode();
 const text = response.getContentText();
 if (code !== 200) {
 throw new Error(
 'Backend fetch failed: HTTP ' + code + ' - ' + text.slice(0, 1000)
 );
 }
 let payload;
 try {
 payload = JSON.parse(text);
 } catch (error) {
 throw new Error(
 'Backend returned invalid JSON: ' + String(error && error.message)
 );
 }
 if (Array.isArray(payload)) {
 return payload;
 }
 if (payload && Array.isArray(payload.items)) {
 return payload.items;
 }
 throw new Error('Backend returned an unexpected /exports/pending payload.');
}
// ================= SHEET HELPERS =================
function ensureSheet(spreadsheet, sheetName, headers) {
 let sheet = spreadsheet.getSheetByName(sheetName);
 if (!sheet) {
 sheet = spreadsheet.insertSheet(sheetName);
 sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
 }
 if (sheet.getMaxColumns() < headers.length) {
 sheet.insertColumnsAfter(
 sheet.getMaxColumns(),
 headers.length - sheet.getMaxColumns()
 );
 }
 return sheet;
}
/**
 * Normal conversion dedupe key:
 * GCLID + Conversion Name
 *
 * This is intentionally NOT Order ID/token. One lead may legitimately have:
 * same GCLID + Contact
 * same GCLID + Qualified
 * same GCLID + Closed
 */
function getExistingConversionKeys(sheet, isTesting) {
  const lastRow = sheet.getLastRow();
  const keys = new Set();

  if (lastRow < 2) return keys;

  // B = GCLID, C = Conversion Name, G = Order ID
  const values = sheet
    .getRange(2, 2, lastRow - 1, 6)
    .getValues();

  values.forEach(function (row) {
    const gclid = normalizeGclid(row[0]);      // B
    const conversionName = cleanString(row[1]); // C
    const orderId = cleanString(row[5]);       // G

    if (!gclid || !conversionName) return;

    if (isTesting) {
      if (orderId) {
        keys.add(
          gclid + '|' + conversionName + '|' + orderId
        );
      }
    } else {
      keys.add(
        gclid + '|' + conversionName
      );
    }
  });

  return keys;
}
function getExistingOrderIds(sheet, orderIdColumn) {
 const lastRow = sheet.getLastRow();
 const ids = new Set();
 if (lastRow < 2) return ids;
 const values = sheet
 .getRange(2, orderIdColumn, lastRow - 1, 1)
 .getValues();
 values.forEach(function (row) {
 const id = cleanString(row[0]);
 if (id) ids.add(id);
 });
 return ids;
}
// ================= CONVERSION ROW BUILDING =================
/**
 * Build base + sales-stage conversion rows for a batch.
 *
 * handledItems:
 * - safe to acknowledge to backend because every conversion occurrence expected
 * for the current state is either already in the sheet or has been queued for
 * this successful write.
 *
 * blockedItems:
 * - NOT acknowledged. This prevents malformed data from being silently marked
 * exported.
 */
function buildConversionRows(items, existingKeys, isTesting) {
 const rowsToAppend = [];
 const handledItems = [];
 const blockedItems = [];
 items.forEach(function (item) {
 const project = getProject(item);
 const orderId = getOrderId(item);
 const gclid = normalizeGclid(item.gclid);
 if (!project || !orderId || !gclid) {
 blockedItems.push({
 project: project,
 order_id: orderId,
 reason: 'Missing project, order ID, or GCLID'
 });
 return;
 }
 const baseConvName = cleanString(item.conversion_name);
 const salesConvName = cleanString(item.conversion_name_sales);
 const qualityCode = getQualityCode(item);
 // Backend sets _wantBase when the document is waiting for normal export.
 // On Sales updates it can also be true because Sales deliberately resets
 // conversion_value_uploaded=false.
 const needsBase = item._wantBase === true;
 const hasSalesTrigger =
 item._wantSales === true ||
 (item.sales_sheet_quality_uploaded === false &&
 qualityCode !== null);
 const wantSalesConversion =
 !!salesConvName && (qualityCode === 1 || qualityCode === 2);
 // If backend says the base conversion needs processing but the base action
 // name is absent, do NOT mark this document exported.
 if (needsBase && !baseConvName) {
 blockedItems.push({
 project: project,
 order_id: orderId,
 reason: 'Backend requested base export but conversion_name is missing'
 });
 return;
 }
 // Qualified/Closed must have a sales conversion action supplied by the
 // Sales Sheet Config tab. Without it, do not clear the sales trigger.
 if (
 hasSalesTrigger &&
 (qualityCode === 1 || qualityCode === 2) &&
 !salesConvName
 ) {
 blockedItems.push({
 project: project,
 order_id: orderId,
 reason:
 'Qualified/Closed sales update has no conversion_name_sales'
 });
 return;
 }
 const initialValueRaw =
 toFiniteNumber(item.conversion_value_initial) !== null
 ? toFiniteNumber(item.conversion_value_initial)
 : toFiniteNumber(item.conversion_value);
 const initialValue =
 initialValueRaw !== null ? initialValueRaw : 0;
 const finalValue = toFiniteNumber(item.conversion_value_final);
 const conversionTime = toGoogleAdsDatetime(item.conversion_time);
 const uploadedAt = new Date();
 const status = 'SENT';
 const testReason = isTesting ? getTestMarkerReason(item) : '';
 let expectedConversionCount = 0;
 const expectedForThisItem = new Set();
 function satisfyConversion(conversionName, value) {
 if (!conversionName) return;
const key = isTesting
  ? gclid + '|' + conversionName + '|' + orderId
  : gclid + '|' + conversionName;
 // Prevent two identical expected rows inside the same document state.
 if (expectedForThisItem.has(key)) return;
 expectedForThisItem.add(key);
 expectedConversionCount++;
 // Existing row already satisfies this conversion occurrence.
 if (existingKeys.has(key)) return;
 const row = [
 project,
 gclid,
 conversionName,
 conversionTime,
 value,
 CURRENCY,
 orderId,
 status,
 uploadedAt
 ];
 if (isTesting) {
 row.push(testReason);
 }
 rowsToAppend.push(row);
 existingKeys.add(key);
 }
 // 1) ORIGINAL / BASE conversion.
 // Keep its original estimated/initial value. Do not overwrite the base
 // conversion with the latest sales value here; value changes belong to the
 // separate Adjustments/RESTATE pipeline.
 if (baseConvName) {
 satisfyConversion(baseConvName, initialValue);
 }
 // 2) NEW SALES-STAGE conversion occurrence.
 // Qualified (1) and Closed (2) are distinct positive funnel milestones.
 // Unqualified (0) intentionally creates NO new conversion action.
 if (wantSalesConversion) {
 const salesValue =
 finalValue !== null ? finalValue : initialValue;
 satisfyConversion(salesConvName, salesValue);
 }
 if (expectedConversionCount === 0) {
 blockedItems.push({
 project: project,
 order_id: orderId,
 reason: 'No valid conversion action was available for this item'
 });
 return;
 }
 handledItems.push({
 project: project,
 order_id: orderId
 });
 });
 return {
 rowsToAppend: rowsToAppend,
 handledItems: handledItems,
 blockedItems: blockedItems
 };
}
/**
 * Process GCLID conversions for one destination sheet.
 *
 * Sheet write happens BEFORE backend acknowledgement.
 * If the write fails, the backend document remains pending.
 */
function processGclidBucket(items, sheet, isTesting) {
 if (!items || !items.length) {
 return {
 fetched: 0,
 rowsAppended: 0,
 handled: 0,
 blocked: 0
 };
 }
const existingKeys =
  getExistingConversionKeys(sheet, isTesting);
 let rowsAppended = 0;
 let handledCount = 0;
 let blockedCount = 0;
 for (let offset = 0; offset < items.length; offset += WRITE_BATCH_SIZE) {
 const batch = items.slice(offset, offset + WRITE_BATCH_SIZE);
 const result = buildConversionRows(batch, existingKeys, isTesting);
 if (result.blockedItems.length) {
 blockedCount += result.blockedItems.length;
 result.blockedItems.forEach(function (blocked) {
 Logger.log(
 'BLOCKED conversion export: project=%s order_id=%s reason=%s',
 blocked.project || '(missing)',
 blocked.order_id || '(missing)',
 blocked.reason
 );
 });
 }
 // Critical ordering: write first.
 if (result.rowsToAppend.length) {
 sheet
 .getRange(
 sheet.getLastRow() + 1,
 1,
 result.rowsToAppend.length,
 result.rowsToAppend[0].length
 )
 .setValues(result.rowsToAppend);
 rowsAppended += result.rowsToAppend.length;
 }
 SpreadsheetApp.flush();
 // Acknowledge only after rows were successfully written, or confirmed to
 // already exist by the GCLID + Conversion Name dedupe check.
 markHandledItems(result.handledItems);
 handledCount += result.handledItems.length;
 }
 return {
 fetched: items.length,
 rowsAppended: rowsAppended,
 handled: handledCount,
 blocked: blockedCount
 };
}
// ================= NO-GCLID REVIEW =================
function buildNoGclidRows(items, existingOrderIds, isTesting) {
 const rowsToAppend = [];
 const handledItems = [];
 const blockedItems = [];
 items.forEach(function (item) {
 const project = getProject(item);
 const orderId = getOrderId(item);
 if (!project || !orderId) {
 blockedItems.push({
 project: project,
 order_id: orderId,
 reason: 'Missing project or order ID'
 });
 return;
 }
 // If the token is already in the review sheet, there is no reason to append
 // it repeatedly. It is still safe to acknowledge this backend state.
 if (existingOrderIds.has(orderId)) {
 handledItems.push({
 project: project,
 order_id: orderId
 });
 return;
 }
 const conversionTime = toGoogleAdsDatetime(item.conversion_time);
 const conversionValueRaw = toFiniteNumber(item.conversion_value);
 const conversionValue =
 conversionValueRaw !== null ? conversionValueRaw : 0;
 const source =
 cleanString(item.conversion_value_source) ||
 cleanString(item.source) ||
 project ||
 'NO_SOURCE';
 const uploadVersionRaw = toFiniteNumber(item.upload_version);
 const uploadVersion =
 uploadVersionRaw !== null ? uploadVersionRaw : 0;
 const row = [
 project,
 orderId,
 conversionTime,
 conversionValue,
 source,
 'NO_GCLID',
 uploadVersion,
 new Date(),
 'REVIEW'
 ];
 if (isTesting) {
 row.push(getTestMarkerReason(item));
 }
 rowsToAppend.push(row);
 existingOrderIds.add(orderId);
 handledItems.push({
 project: project,
 order_id: orderId
 });
 });
 return {
 rowsToAppend: rowsToAppend,
 handledItems: handledItems,
 blockedItems: blockedItems
 };
}
function processNoGclidBucket(items, sheet, isTesting) {
 if (!items || !items.length) {
 return {
 fetched: 0,
 rowsAppended: 0,
 handled: 0,
 blocked: 0
 };
 }
 // Column B is Token/Order ID in both no-GCLID sheet layouts.
 const existingOrderIds = getExistingOrderIds(sheet, 2);
 let rowsAppended = 0;
 let handledCount = 0;
 let blockedCount = 0;
 for (let offset = 0; offset < items.length; offset += WRITE_BATCH_SIZE) {
 const batch = items.slice(offset, offset + WRITE_BATCH_SIZE);
 const result = buildNoGclidRows(
 batch,
 existingOrderIds,
 isTesting
 );
 if (result.blockedItems.length) {
 blockedCount += result.blockedItems.length;
 result.blockedItems.forEach(function (blocked) {
 Logger.log(
 'BLOCKED no-GCLID export: project=%s order_id=%s reason=%s',
 blocked.project || '(missing)',
 blocked.order_id || '(missing)',
 blocked.reason
 );
 });
 }
 if (result.rowsToAppend.length) {
 sheet
 .getRange(
 sheet.getLastRow() + 1,
 1,
 result.rowsToAppend.length,
 result.rowsToAppend[0].length
 )
 .setValues(result.rowsToAppend);
 rowsAppended += result.rowsToAppend.length;
 }
 SpreadsheetApp.flush();
 markHandledItems(result.handledItems);
 handledCount += result.handledItems.length;
 }
 return {
 fetched: items.length,
 rowsAppended: rowsAppended,
 handled: handledCount,
 blocked: blockedCount
 };
}
// ================= BACKEND ACKNOWLEDGEMENT =================
function groupHandledItemsByProject(items) {
 const grouped = {};
 (items || []).forEach(function (item) {
 const project = cleanString(item && item.project);
 const orderId = cleanString(item && item.order_id);
 if (!project || !orderId) return;
 if (!grouped[project]) {
 grouped[project] = new Set();
 }
 grouped[project].add(orderId);
 });
 return grouped;
}
function markHandledItems(items) {
 if (!items || !items.length) return;
 const grouped = groupHandledItemsByProject(items);
 Object.keys(grouped).forEach(function (project) {
 const orderIds = Array.from(grouped[project]);
 if (orderIds.length) {
 markExportedBatch(project, orderIds, 0);
 }
 });
}
/**
 * Mark backend documents exported.
 *
 * Retries:
 * - HTTP 429
 * - HTTP 5xx
 * - network/UrlFetch exceptions
 * - backend JSON response with success=false
 *
 * After retries are exhausted, this function THROWS so the Cloud Scheduler /
 * Apps Script execution visibly fails instead of silently pretending success.
 */
function markExportedBatch(project, orderIds, retryCount) {
 retryCount = retryCount || 0;
 if (!project || !orderIds || !orderIds.length) {
 throw new Error(
 'markExportedBatch requires project and at least one order ID.'
 );
 }
 const url =
 ENDPOINT_MARK + '?project=' + encodeURIComponent(project);
 const payload = JSON.stringify({
 order_ids: orderIds
 });
 try {
 Logger.log(
 'Marking %s item(s) exported for %s%s',
 orderIds.length,
 project,
 retryCount > 0 ? ' (retry ' + retryCount + ')' : ''
 );
 const response = UrlFetchApp.fetch(url, {
 method: 'post',
 contentType: 'application/json',
 headers: { 'x-export-secret': getSecret() },
 payload: payload,
 muteHttpExceptions: true
 });
 const code = response.getResponseCode();
 const text = response.getContentText();
 if (code === 200) {
 let parsed = null;
 try {
 parsed = JSON.parse(text);
 } catch (error) {
 throw new Error(
 'mark-exported returned HTTP 200 but invalid JSON: ' +
 text.slice(0, 500)
 );
 }
 if (parsed && parsed.success === true) {
 Logger.log(
 'Marked %s item(s) exported for %s.',
 parsed.updated ? parsed.updated.length : orderIds.length,
 project
 );
 return;
 }
 if (retryCount < MAX_MARK_RETRIES) {
 Utilities.sleep(2000 * (retryCount + 1));
 return markExportedBatch(
 project,
 orderIds,
 retryCount + 1
 );
 }
 throw new Error(
 'mark-exported returned success=false for ' +
 project +
 ': ' +
 text.slice(0, 1000)
 );
 }
 const retryable = code === 429 || (code >= 500 && code < 600);
 if (retryable && retryCount < MAX_MARK_RETRIES) {
 Utilities.sleep(2000 * (retryCount + 1));
 return markExportedBatch(
 project,
 orderIds,
 retryCount + 1
 );
 }
 throw new Error(
 'mark-exported failed for ' +
 project +
 ': HTTP ' +
 code +
 ' - ' +
 text.slice(0, 1000)
 );
 } catch (error) {
 // Do not retry errors we deliberately throw after exhausting retries.
 const message = String(error && (error.message || error));
 const isOurFinalError =
 message.indexOf('mark-exported returned success=false') === 0 ||
 message.indexOf('mark-exported failed for ') === 0 ||
 message.indexOf(
 'mark-exported returned HTTP 200 but invalid JSON'
 ) === 0;
 if (
 !isOurFinalError &&
 retryCount < MAX_MARK_RETRIES
 ) {
 Utilities.sleep(2000 * (retryCount + 1));
 return markExportedBatch(
 project,
 orderIds,
 retryCount + 1
 );
 }
 throw error;
 }
}
// ================= MAIN PROCESSOR =================
function processBatch() {
 const spreadsheet = SpreadsheetApp.openById(getSheetId());
 const items = fetchPendingConversions();
 if (!items.length) {
 Logger.log('No pending conversions.');
 return {
 fetched: 0,
 realRowsAppended: 0,
 testRowsAppended: 0,
 blocked: 0
 };
 }
 Logger.log('Fetched %s pending conversion document(s).', items.length);
 // Ensure all four destination tabs exist.
 const realMainSheet = ensureSheet(
 spreadsheet,
 MAIN_SHEET_NAME,
 MAIN_HEADERS
 );
 const realNoGclidSheet = ensureSheet(
 spreadsheet,
 NO_GCLID_SHEET_NAME,
 NO_GCLID_HEADERS
 );
 const testingMainSheet = ensureSheet(
 spreadsheet,
 TESTING_MAIN_SHEET_NAME,
 TESTING_MAIN_HEADERS
 );
 const testingNoGclidSheet = ensureSheet(
 spreadsheet,
 TESTING_NO_GCLID_SHEET_NAME,
 TESTING_NO_GCLID_HEADERS
 );
 const realWithGclid = [];
 const realNoGclid = [];
 const testingWithGclid = [];
 const testingNoGclid = [];
 items.forEach(function (item) {
 const gclid = normalizeGclid(item && item.gclid);
 const test = isTestConversion(item);
 if (test) {
 if (gclid) testingWithGclid.push(item);
 else testingNoGclid.push(item);
 } else {
 if (gclid) realWithGclid.push(item);
 else realNoGclid.push(item);
 }
 });
 const realMainResult = processGclidBucket(
 realWithGclid,
 realMainSheet,
 false
 );
 const realNoGclidResult = processNoGclidBucket(
 realNoGclid,
 realNoGclidSheet,
 false
 );
 const testingMainResult = processGclidBucket(
 testingWithGclid,
 testingMainSheet,
 true
 );
 const testingNoGclidResult = processNoGclidBucket(
 testingNoGclid,
 testingNoGclidSheet,
 true
 );
 const summary = {
 fetched: items.length,
 realWithGclidFetched: realMainResult.fetched,
 realWithGclidRowsAppended: realMainResult.rowsAppended,
 realNoGclidFetched: realNoGclidResult.fetched,
 realNoGclidRowsAppended: realNoGclidResult.rowsAppended,
 testWithGclidFetched: testingMainResult.fetched,
 testWithGclidRowsAppended: testingMainResult.rowsAppended,
 testNoGclidFetched: testingNoGclidResult.fetched,
 testNoGclidRowsAppended: testingNoGclidResult.rowsAppended,
 handled:
 realMainResult.handled +
 realNoGclidResult.handled +
 testingMainResult.handled +
 testingNoGclidResult.handled,
 blocked:
 realMainResult.blocked +
 realNoGclidResult.blocked +
 testingMainResult.blocked +
 testingNoGclidResult.blocked
 };
 Logger.log('Export summary: %s', JSON.stringify(summary));
 return summary;
}
// ================= SAFE RUNNER / CLOUD SCHEDULER =================
/**
 * Single public runner used by:
 * - Cloud Scheduler -> Web App -> doGet/doPost
 * - manual execution from Apps Script editor
 *
 * Lock prevents two scheduler/manual executions from overlapping.
 */
function runOnceFetch() {
 const lock = LockService.getScriptLock();
 if (!lock.tryLock(30000)) {
 throw new Error(
 'Could not acquire exporter lock within 30 seconds. Another export is probably running.'
 );
 }
 try {
 return processBatch();
 } catch (error) {
 Logger.log(
 'FATAL EXPORT ERROR: %s',
 String(error && (error.stack || error.message || error))
 );
 throw error;
 } finally {
 lock.releaseLock();
 }
}
function jsonResponse(object) {
 return ContentService
 .createTextOutput(JSON.stringify(object))
 .setMimeType(ContentService.MimeType.JSON);
}
/**
 * Google Cloud Scheduler production entry point.
 *
 * Intentionally lets runOnceFetch() exceptions propagate. That way a real
 * exporter failure is visible as a failed Web App execution instead of being
 * silently reported as {ok:true}.
 */
function doGet(e) {
 const summary = runOnceFetch();
 return jsonResponse({
 ok: true,
 summary: summary || {}
 });
}
function doPost(e) {
 const summary = runOnceFetch();
 return jsonResponse({
 ok: true,
 summary: summary || {}
 });
}
// ================= ONE-TIME CONFIG / MANUAL UI =================
/**
 * One-time config for the global production exporter.
 *
 * IMPORTANT:
 * Scheduling is handled by Google Cloud Scheduler.
 * This function deliberately creates NO Apps Script time trigger.
 */
function installGlobalConfig(exportSecret) {
 if (!exportSecret) {
 throw new Error('installGlobalConfig requires exportSecret.');
 }
 PropertiesService.getScriptProperties().setProperties({
 EXPORT_SECRET: String(exportSecret),
 SHEET_FILE_ID: String(
 SpreadsheetApp.getActiveSpreadsheet().getId()
 )
 });
 Logger.log('Global exporter Script Properties saved.');
}
function setupGlobalExporter() {
 const ui = SpreadsheetApp.getUi();
 const response = ui.prompt(
 'Export Secret',
 'Enter the EXPORT_SECRET used by the backend:',
 ui.ButtonSet.OK_CANCEL
 );
 if (response.getSelectedButton() !== ui.Button.OK) {
 return;
 }
 const secret = response.getResponseText().trim();
 if (!secret) {
 throw new Error('EXPORT_SECRET cannot be blank.');
 }
 installGlobalConfig(secret);
 ui.alert(
 'Setup complete.\n\n' +
 'EXPORT_SECRET and SHEET_FILE_ID were saved.\n' +
 'No Apps Script time trigger was created because production scheduling is handled by Google Cloud Scheduler.'
 );
}
function onOpen() {
 SpreadsheetApp.getUi()
 .createMenu('WaTrck Exporter')
 .addItem('Save production config', 'setupGlobalExporter')
 .addItem('Run export now', 'runOnceFetch')
 .addToUi();
}