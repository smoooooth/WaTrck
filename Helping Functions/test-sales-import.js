// Offline Sheets stub: validates preview, preservation, and retry behavior.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const values = Array.from({ length: 1117 }, () => Array(13).fill(''));
values[0] = ['Name', 'Phone number', 'Interest', 'Notes', 'Token', 'Date', 'Quality',
  'USD Value', 'SERVER FEEDBACK', 'Campaign', 'Ad group / Asset group', 'Ad', 'Campaign type'];
values[1] = ['Sales name', '001234', 'Interest', 'Human notes', 'OLD1234', '', 'Qualified', 500, 'Published', '', '', '', ''];
const notes = new Map();
const backgrounds = new Map();
let writes = 0;
function makeSheet(values) { return {
  getRange(row, column, height = 1, width = 1) {
    assert.ok(row >= 1 && row + height - 1 <= values.length);
    assert.ok(column >= 1 && column + width - 1 <= 13, 'must not access filtered output');
    const key = row + ':' + column;
    const assertWritable = () => assert.ok(column > 9 || column + width - 1 < 7, 'Importer must never write G:I');
    return {
      getValues: () => values.slice(row - 1, row - 1 + height).map(cells => cells.slice(column - 1, column - 1 + width)),
      getDisplayValues: () => values.slice(row - 1, row - 1 + height).map(cells => cells.slice(column - 1, column - 1 + width).map(String)),
      getFormulas: () => Array.from({ length: height }, () => Array(width).fill('')),
      getDataValidation: () => null,
      setNumberFormat() { assertWritable(); writes++; },
      setBackground(color) { assertWritable(); backgrounds.set(key, color); writes++; return this; },
      setFontColor(color) { assertWritable(); assert.equal(color, '#000000'); writes++; return this; },
      setValues(rows) { assertWritable(); rows.forEach((cells, offset) => values[row - 1 + offset].splice(column - 1, width, ...cells)); writes++; },
      setValue(value) { assertWritable(); values[row - 1][column - 1] = value; writes++; },
      getNote: () => notes.get(key) || '',
      setNote(value) { assertWritable(); notes.set(key, value); writes++; }
    };
  }
}; }
const sheet = makeSheet(values);
const otherValues = Array.from({ length: 6 }, () => Array(13).fill(''));
otherValues[0] = values[0].slice();
const otherSheet = makeSheet(otherValues);
const item = token => ({ project_id: 'Vision Iconic Tower', contact_conversion_name: 'Vision Contact', token, name: '=Unsafe name', phone: '001111',
  username: 'fallback_name', date: '2026-10-29T10:00:00Z', quality: 'qualified', value: 500, campaign_id: '1234567890123456789',
  adgroup_id: '222', ad_id: '333', assetgroup_id: null });
const properties = new Map([['EXPORT_SECRET', 'fake-offline-secret'], ['SALES_SHEET_FILE_ID', 'copied-workbook']]);
const configRows = [['TabName', 'QualifiedConversionName', 'ClosedConversionName', 'Contact_Conversion_Name'],
  ['Vision_Iconic_Tower', 'Vision Qualified', 'Vision Closed', 'Vision Contact'],
  ['Unprepared', 'Other Qualified', 'Other Closed', 'Other Contact']];
const config = { getLastRow: () => configRows.length,
  getRange: () => ({ getDisplayValues: () => configRows }) };
const metadata = { sheets: [{ properties: { title: 'Vision_Iconic_Tower' }, tables: [
  { range: { endColumnIndex: 13, endRowIndex: 1117 } },
  { range: { startColumnIndex: 13, startRowIndex: 6, endColumnIndex: 26, endRowIndex: 419 } }
] }] };
let feedItems = [item('OLD1234'), item('NEW1234')];
const workbook = {
  getId: () => 'copied-workbook',
  getSheetByName: name => name === 'Config' ? config : name === 'Vision_Iconic_Tower' ? sheet : name === 'ReadyOther' ? otherSheet : null
};
const context = vm.createContext({
  console: { log() {} },
  Sheets: { Spreadsheets: { get: id => { assert.equal(id, 'copied-workbook'); return metadata; } } },
  ContentService: {
    MimeType: { JSON: 'json' },
    createTextOutput: text => ({ setMimeType: () => JSON.parse(text) })
  },
  LockService: {
    getDocumentLock: () => ({ waitLock() {}, releaseLock() {} }),
    getScriptLock: () => ({ waitLock() {}, releaseLock() {} })
  },
  PropertiesService: { getScriptProperties: () => ({
    getProperty: key => properties.get(key) || null,
    setProperty: (key, value) => properties.set(key, value),
    deleteProperty: key => properties.delete(key)
  }) },
  ScriptApp: { getScriptId: () => 'offline-conversions-script' },
  SpreadsheetApp: {
    openById: id => { assert.equal(id, 'copied-workbook'); return workbook; },
    flush() {}, DataValidationCriteria: { VALUE_IN_LIST: 'LIST' }
  },
  UrlFetchApp: { fetch(url, options) {
    const include = properties.get('SALES_INCLUDE_TEST_USERNAME') === 'true';
    assert.equal(url, 'https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/exportsApi/sales/leads?limit=200' +
      (include ? '&include_test_username=1' : ''));
    assert.equal(options.headers['x-export-secret'], 'fake-offline-secret');
    return { getResponseCode: () => 200, getContentText: () => JSON.stringify({
      items: feedItems, testing: false, include_test_username: include, next_cursor: null
    }) };
  } }
});
vm.runInContext(fs.readFileSync(path.join(__dirname, '../Sales Sheet/Lead Import.js'), 'utf8'), context);
const before = JSON.stringify(values);
properties.set('SALES_INCLUDE_TEST_USERNAME', 'true');
context.previewSalesLeadImport();
assert.equal(writes, 0);
properties.set('SALES_INCLUDE_TEST_USERNAME', 'false');
context.previewSalesLeadImport();
assert.equal(writes, 0);
properties.delete('SALES_INCLUDE_TEST_USERNAME');
let result = context.previewSalesLeadImport();
assert.equal(result.newRows, 1);
assert.equal(result.existingRows, 1);
assert.equal(writes, 0);
assert.equal(JSON.stringify(values), before);
context.importSalesLeadsOnce();
assert.deepEqual(values[1].slice(0, 5), ['Sales name', '001234', 'Interest', 'Human notes', 'OLD1234']);
assert.deepEqual(values[1].slice(6, 9), ['Qualified', 500, 'Published']);
assert.equal(values[2][4], 'NEW1234');
assert.equal(values[2][0], "'=Unsafe name");
assert.equal(values[2][1], '001111');
assert.deepEqual(values[2].slice(2, 4), ['Fresh Lead — Review', 'Fresh Lead — Review']);
assert.deepEqual(values[2].slice(6, 9), ['', '', '']);
assert.equal(backgrounds.get('3:3'), '#fff2cc');
assert.equal(notes.get('3:10'), 'Google Ads ID: 1234567890123456789');
const imported = JSON.stringify(values);
context.importSalesLeadsOnce();
assert.equal(JSON.stringify(values), imported);
result = context.previewSalesLeadImport();
assert.equal(result.newRows, 0);
assert.equal(result.existingRows, 2);
// Config routing: unprepared tabs and unmapped names must not receive writes.
feedItems.push({ ...item('OTHER12'), project_id: 'Other', contact_conversion_name: 'Other Contact' },
  { ...item('UNKNOWN'), project_id: 'Unknown', contact_conversion_name: 'Unknown Contact' });
result = context.previewSalesLeadImport();
assert.equal(result.unmappedLeads, 1);
assert.equal(result.skippedTabs.length, 1);
assert.equal(result.skippedTabs[0].confirmedLeads, 1);
assert.equal(result.tabs.length, 1);
const beforeGuardWrites = writes;
configRows.push(['Another', '', '', 'Vision Contact']);
assert.throws(() => context.importSalesLeadsOnce(), /Duplicate Config/);
configRows.pop();
feedItems.push({ ...item('COLLIDE'), project_id: 'Wrong project' });
assert.throws(() => context.importSalesLeadsOnce(), /multiple or invalid project/);
feedItems.pop();
const range = metadata.sheets[0].tables[0].range;
range.endRowIndex = 2;
assert.throws(() => context.importSalesLeadsOnce(), /No empty rows/);
range.endRowIndex = 1117;
assert.equal(writes, beforeGuardWrites);
feedItems = [item('OLD1234'), item('NEW1234')];
// A new Config mapping needs no hard-coded project ID or row limit.
configRows.push(['ReadyOther', '', '', 'Ready Contact']);
metadata.sheets.push({ properties: { title: 'ReadyOther' }, tables: [
  { range: { endColumnIndex: 13, endRowIndex: 6 } }
] });
feedItems.push({ ...item('READY12'), phone: null, project_id: 'New project', contact_conversion_name: 'Ready Contact' });
result = context.importSalesLeadsOnce();
assert.equal(result.newRows, 1);
assert.equal(otherValues[1][4], 'READY12');
assert.equal(otherValues[1][1], 'fallback_name');
otherValues[1][2] = 'Sales changed interest';
otherValues[1][3] = 'Sales changed notes';
assert.equal(values[3][4], '', 'second project must not enter Vision');
assert.equal(context.importSalesLeadsOnce().newRows, 0);
assert.deepEqual(otherValues[1].slice(2, 4), ['Sales changed interest', 'Sales changed notes']);
configRows.pop();
metadata.sheets.pop();
feedItems.pop();
const savedContact = feedItems[0].contact_conversion_name;
delete feedItems[0].contact_conversion_name;
assert.throws(() => context.previewSalesLeadImport(), /Deploy the updated exportsApi/);
feedItems[0].contact_conversion_name = savedContact;
const duplicateRows = [values[1].slice(), values[1].slice()];
assert.throws(() => context.watrckPlanLeadImport_(duplicateRows, [Array(13).fill(''), Array(13).fill('')], [], 'Vision Iconic Tower'), /Duplicate token/);
const beforeInstallWrites = writes;
assert.equal(context.installVisionLeadAutoSync, undefined);
assert.equal(context.setupVisionCloudScheduler, undefined);
assert.equal(context.previewVisionLeadImport, undefined);
assert.equal(context.doGet, undefined, 'importer must not override existing Web App');
assert.equal(context.doPost, undefined);
properties.set('SHEET_FILE_ID', 'conversions-workbook');
properties.delete('SALES_SHEET_FILE_ID');
assert.throws(() => context.previewSalesLeadImport(), /SALES_SHEET_FILE_ID/);
properties.set('SALES_SHEET_FILE_ID', 'conversions-workbook');
assert.throws(() => context.previewSalesLeadImport(), /must differ/);
properties.set('SALES_SHEET_FILE_ID', 'copied-workbook');
context.previewSalesLeadImport();
assert.equal(writes, beforeInstallWrites);
// Load both files into one Apps Script-like namespace to catch collisions.
context.Logger = { log() {} };
vm.runInContext(fs.readFileSync(path.join(__dirname, '../Conversions/Conversions.js'), 'utf8'), context);
let held = false;
context.LockService.getScriptLock = () => ({
  tryLock() { assert.equal(held, false); held = true; return true; },
  waitLock() { assert.equal(held, false, 'nested lock acquisition'); held = true; },
  releaseLock() { assert.equal(held, true); held = false; }
});
let salesCalls = 0;
const salesRunner = context.runSalesImportForExistingJob_;
context.runSalesImportForExistingJob_ = () => { salesCalls++; return salesRunner(); };
context.processBatch = () => ({ fetched: 0, realRowsAppended: 0 });
assert.equal(context.runOnceFetch().fetched, 0);
assert.equal(salesCalls, 0, 'Sales defaults to disabled');
properties.set('SALES_IMPORT_ENABLED', 'true');
const response = context.doGet({});
assert.equal(response.ok, true);
assert.equal(response.summary.fetched, 0);
assert.equal(response.summary.salesImport.existingRows, 2);
assert.equal(response.summary.salesImport.newRows, 0);
let diagnostic = context.inspectLastWebExport();
assert.equal(diagnostic.status, 'completed');
assert.equal(diagnostic.salesWorkbookId, 'copied-workbook');
assert.equal(diagnostic.sales.existingRows, 2);
assert.equal(diagnostic.scriptId, 'offline-conversions-script');
assert.ok(!properties.get('LAST_WEB_EXPORT_DIAGNOSTIC').includes('fake-offline-secret'));
assert.equal(JSON.stringify(values), imported);
context.processBatch = () => { throw new Error('conversion failure'); };
assert.throws(() => context.runOnceFetch(), /Conversions: conversion failure/);
assert.equal(salesCalls, 2, 'Sales attempted even after conversion failure');
let conversionsCompleted = 0;
context.processBatch = () => { conversionsCompleted++; return { fetched: 1 }; };
context.runSalesImportForExistingJob_ = () => { throw new Error('sales failure'); };
assert.throws(() => context.doPost({}), /Sales import: sales failure/);
assert.equal(context.inspectLastWebExport().status, 'failed');
assert.equal(context.inspectLastWebExport().method, 'POST');
assert.equal(conversionsCompleted, 1);
context.processBatch = () => { throw new Error('conversion failure'); };
assert.throws(() => context.runOnceFetch(), /Conversions: conversion failure \| Sales import: sales failure/);
assert.equal(held, false);
assert.equal(context.syncVisionLeadsScheduled, undefined);
console.log('PASS: importer preservation/dedupe; shared namespace; default disabled; pinned Sales workbook; empty-conversion runs; independent failure paths; single lock ownership; existing GET/POST retained; retired trial helpers absent.');
