// Sales Sheet Apps Script - paste into your Sales sheet Apps Script project

// ---------- CONFIG ----------
const CONFIG_SHEET_NAME = 'Config';         // config tab (TabName | QualifiedConv | ClosedConv)
const EXPORT_SECRET_PROP = 'EXPORT_SECRET'; // script property name where secret is stored
const SALES_ENDPOINT = 'https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/exportsApi/sales/quality-update';

// Columns (1-based)
const COL_TOKEN = 5;
const COL_DATE = 6;
const COL_QUALITY = 7;
const COL_VALUE = 8;
const COL_MESSAGE = 9;
// ----------------------------

// Read secret from Script Properties (must be set)
function getExportSecretFromProps() {
  const s = PropertiesService.getScriptProperties().getProperty(EXPORT_SECRET_PROP);
  if (!s) throw new Error('EXPORT_SECRET not set in Script Properties.');
  return s;
}

// Read Config sheet into a Map { tabName -> { qualified, closed } }
function readConfigMap() {
  const ss = SpreadsheetApp.getActive();
  const cfg = ss.getSheetByName(CONFIG_SHEET_NAME);
  if (!cfg) return {};
  const data = cfg.getDataRange().getValues(); // read whole sheet, including header if present
  const map = {};
  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    // Ignore empty rows
    const tab = (row[0] || '').toString().trim();
    if (!tab) continue;
    const qualified = (row[1] || '').toString().trim();
    const closed = (row[2] || '').toString().trim();
    map[tab] = { qualified: qualified || null, closed: closed || null };
  }
  return map;
}

// Helper: normalize quality input to string enum: 'qualified'|'unqualified'|'closed'
function normalizeQuality(raw) {
  if (raw === null || raw === undefined) return null;
  if (typeof raw === 'number') {
    if (raw === 1) return 'qualified';
    if (raw === 0) return 'unqualified';
    if (raw === 2) return 'closed';
    return null;
  }
  const s = ('' + raw).trim().toLowerCase();
  if (!s) return null;
  if (s === '1' || s === 'qualified') return 'qualified';
  if (s === '0' || s === 'unqualified') return 'unqualified';
  if (s === '2' || s === 'closed' || s === 'closed sale') return 'closed';
  return null;
}

// Installable trigger handler. Must be installed as an **installable** onEdit trigger.
// Replace your existing onEditTrigger(e) with this function:
function onEditTrigger(e) {
  try {
    if (!e || !e.range) return;
    const range = e.range;
    const sheet = range.getSheet();
    if (!sheet) return;
    const sheetName = sheet.getName();

    // Only act on edits in Quality column
    if (range.getColumn() !== COL_QUALITY) return;

    // Read config
    const configMap = readConfigMap();
    if (!configMap[sheetName]) {
      return; // sheet not configured
    }
    const cfg = configMap[sheetName];

    // Read the whole row up to message column (so we can write back)
    const rowNum = range.getRow();
    const row = sheet.getRange(rowNum, 1, 1, Math.max(sheet.getLastColumn(), COL_MESSAGE)).getValues()[0];

    const token = (row[COL_TOKEN - 1] || '').toString().trim();
    const messageCell = sheet.getRange(rowNum, COL_MESSAGE);

    function restorePreviousQualitySafe() {
      try {
        if (
          e &&
          typeof e.oldValue !== 'undefined' &&
          e.oldValue !== null
        ) {
          sheet
            .getRange(rowNum, COL_QUALITY)
            .setValue(e.oldValue);
        } else {
          sheet
            .getRange(rowNum, COL_QUALITY)
            .setValue('');
        }
      } catch (_) {}
    }

    // capture previous/raw quality display for potential use (kept but not appended to errors)
    const prevRawQuality = (e && typeof e.oldValue !== 'undefined' && e.oldValue !== null) ? e.oldValue : (row[COL_QUALITY - 1] !== undefined && row[COL_QUALITY - 1] !== null ? row[COL_QUALITY - 1] : '');

    if (!token) {
      messageCell.setValue('ERROR. No token found in row; cannot update.');
      try { messageCell.setFontColor('red'); } catch (_) {}
      // clear both quality & value on error as requested
      restorePreviousQualitySafe();
      return;
    }

    const rawQuality = row[COL_QUALITY - 1];
    const quality = normalizeQuality(rawQuality);
    if (!quality) {
      messageCell.setValue('ERROR. Quality not recognized. Use qualified/unqualified/closed or 1/0/2');
      try { messageCell.setFontColor('red'); } catch (_) {}
      // clear both quality & value on error as requested
      restorePreviousQualitySafe();
      return;
    }

      // Prevent backward lead-quality transitions
      const prevQualityNorm = normalizeQuality(prevRawQuality);

      const invalidTransition =
        (
          prevQualityNorm === 'qualified' &&
          quality === 'unqualified'
        ) ||
        (
          prevQualityNorm === 'closed' &&
          (quality === 'qualified' || quality === 'unqualified')
        );

      if (invalidTransition) {
        messageCell.setValue(
          'ERROR. This lead quality cannot be moved backwards.'
        );

        try {
          messageCell.setFontColor('red');
        } catch (_) {}

        restorePreviousQualitySafe();
        return;
      }

    // Determine final value to send (client-side enforcement)
    const rawValue = row[COL_VALUE - 1];
    let sendValue = null;
    if (quality === 'unqualified') {
      sendValue = 0;
    } else {
      if (typeof rawValue === 'number' && !Number.isNaN(rawValue)) sendValue = rawValue;
      else if (rawValue !== null && rawValue !== undefined && String(rawValue).trim() !== '') {
        const p = parseFloat(String(rawValue).trim());
        if (!Number.isNaN(p)) sendValue = p;
      } else {
        sendValue = null;
      }
    }


    // === Client-side validation requested: block when value is required but missing ===
    if ((quality === 'qualified' || quality === 'closed') && sendValue === null) {
      messageCell.setValue('ERROR. Value required when marking qualified or closed');
      try { messageCell.setFontColor('red'); } catch (_) {}
      // clear both quality & value on error as requested
      restorePreviousQualitySafe();
      return;
    }

    // Choose conversion name from config per sheet/tab
    let conversionNameToSend = null;
    if (quality === 'qualified' && cfg.qualified) conversionNameToSend = cfg.qualified;
    if (quality === 'closed' && cfg.closed) conversionNameToSend = cfg.closed;

    // Build payload
    const payload = { token: token, quality: quality };
    if (sendValue !== null) payload.value = sendValue;
    if (conversionNameToSend) payload.conversion_name = conversionNameToSend;

    // Set loading feedback
    try {
      messageCell.setValue('loading...');
      messageCell.setFontColor('black');
      SpreadsheetApp.flush();
    } catch (_) {}

    // Fire the backend call
    const secret = getExportSecretFromProps();
    const options = {
      method: 'post',
      contentType: 'application/json',
      payload: JSON.stringify(payload),
      muteHttpExceptions: true,
      headers: { 'x-export-secret': secret }
    };

    const resp = UrlFetchApp.fetch(SALES_ENDPOINT, options);
    const code = resp.getResponseCode();
    const text = resp.getContentText();

        if (code === 200) {
          const displayQuality =
            (row[COL_QUALITY - 1] !== undefined &&
            row[COL_QUALITY - 1] !== null)
              ? String(row[COL_QUALITY - 1])
              : String(quality);

          const displayValue =
            (sendValue === null || sendValue === undefined)
              ? ''
              : String(sendValue);

          messageCell.setValue(
            'Lead Published: ' +
            displayQuality +
            ' : $' +
            displayValue
          );

          try {
            messageCell.setFontColor('green');
          } catch (_) {}

        } else {
      // try to extract friendly error
      let errText = text || ('HTTP ' + code);
      try {
        const j = JSON.parse(text);
        if (j && j.error) errText = j.error;
      } catch (__) {}
      messageCell.setValue('ERROR. ' + (String(errText).slice(0, 200)));
      try { messageCell.setFontColor('red'); } catch (_) {}
      // clear both quality & value on error as requested
      restorePreviousQualitySafe();
    }
  } catch (err) {
    try {
      const rowNum = (e && e.range && e.range.getRow()) ? e.range.getRow() : 1;
      const sh = e && e.range ? e.range.getSheet() : SpreadsheetApp.getActiveSheet();
      sh.getRange(rowNum, COL_MESSAGE).setValue('ERROR. ' + (err.message || err.toString()).slice(0, 200));
      sh.getRange(rowNum, COL_MESSAGE).setFontColor('red');
      // also clear both quality & value on exception
        try {
          if (
            e &&
            typeof e.oldValue !== 'undefined' &&
            e.oldValue !== null
          ) {
            sh.getRange(rowNum, COL_QUALITY).setValue(e.oldValue);
          }
        } catch (_) {}
    } catch (__) {}
    console.error('onEditTrigger error', err);
  }
}

// Helper to create an installable trigger for onEditTrigger (run once from editor)
function installOnEditTrigger() {
  // deletes existing installable triggers that point to onEditTrigger, then creates one
  const projectTriggers = ScriptApp.getProjectTriggers();
  projectTriggers.forEach(t => {
    if (t.getHandlerFunction() === 'onEditTrigger') ScriptApp.deleteTrigger(t);
  });
  ScriptApp.newTrigger('onEditTrigger').forSpreadsheet(SpreadsheetApp.getActive()).onEdit().create();
  SpreadsheetApp.getActive() && SpreadsheetApp.getUi() && SpreadsheetApp.getUi().alert('Installed onEdit trigger for onEditTrigger');
}



