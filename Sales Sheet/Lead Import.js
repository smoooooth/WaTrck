// Add this file alongside Conversions.js in the EXISTING conversions project.
// It deliberately defines no doGet/doPost or onOpen entry points.
// Set SALES_SHEET_FILE_ID to the intended Sales workbook. Preview first.
// Set SALES_IMPORT_ENABLED=true only after verification; existing job runs it.
const WATRCK_SALES_IMPORT = {
  endpoint: 'https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/exportsApi/sales/leads'
};

function previewSalesLeadImport() {
  return watrckRunConfiguredImport_(false, watrckSalesWorkbook_());
}

function importSalesLeadsOnce() {
  return watrckRunConfiguredImport_(true, watrckSalesWorkbook_());
}

// Config D (original Contact conversion name) routes to Config A (tab).
// Native Table metadata prevents allocation into the adjacent FILTER table.
function watrckRunConfiguredImport_(write, workbook, lockAlreadyHeld) {
  const lock = LockService.getScriptLock();
  if (!lockAlreadyHeld) lock.waitLock(30000);
  try {
    const config = workbook.getSheetByName('Config');
    if (!config || config.getLastRow() < 2) throw new Error('Config has no project mappings.');
    const rows = config.getRange(1, 1, config.getLastRow(), 4).getDisplayValues();
    if (rows[0][0] !== 'TabName' || rows[0][3] !== 'Contact_Conversion_Name') {
      throw new Error('Expected Config A=TabName and D=Contact_Conversion_Name.');
    }
    const routes = new Map();
    const tabs = new Set();
    rows.slice(1).forEach((row, index) => {
      if (row.every(value => value === '')) return;
      const tab = row[0], contact = row[3];
      if (!tab || !contact || tab.trim() !== tab || contact.trim() !== contact) {
        throw new Error('Missing or padded Config mapping at row ' + (index + 2));
      }
      if (routes.has(contact) || tabs.has(tab)) throw new Error('Duplicate Config Contact name or tab at row ' + (index + 2));
      tabs.add(tab);
      routes.set(contact, { tab, items: [] });
    });
    if (typeof Sheets === 'undefined') throw new Error('Enable Google Sheets API under Services in this Apps Script project.');
    const metadata = Sheets.Spreadsheets.get(workbook.getId(), {
      fields: 'sheets(properties(title),tables(range))'
    });
    const items = watrckFetchSalesLeads_();
    let unmappedLeads = 0;
    items.forEach(item => {
      if (!Object.prototype.hasOwnProperty.call(item, 'contact_conversion_name')) {
        throw new Error('Deploy the updated exportsApi before using Config routing.');
      }
      const route = routes.get(item.contact_conversion_name);
      if (route) route.items.push(item);
      else unmappedLeads++;
    });
    const ready = [], skippedTabs = [];
    const headers = ['Name', 'Phone number', 'Interest', 'Notes', 'Token', 'Date',
      'Quality', 'USD Value', 'SERVER FEEDBACK', 'Campaign', 'Ad group / Asset group', 'Ad', 'Campaign type'];
    routes.forEach(route => {
      const sheet = workbook.getSheetByName(route.tab);
      const info = (metadata.sheets || []).find(entry => entry.properties.title === route.tab);
      const tables = ((info && info.tables) || []).filter(table => {
        const r = table.range;
        return r && (r.startRowIndex || 0) === 0 && (r.startColumnIndex || 0) === 0 &&
          r.endColumnIndex === 13 && Number.isInteger(r.endRowIndex) && r.endRowIndex > 1;
      });
      const actual = sheet && tables.length === 1 ? sheet.getRange(1, 1, 1, 13).getDisplayValues()[0] : [];
      if (!sheet || tables.length !== 1 || !headers.every((h, i) => String(actual[i] || '').trim() === h)) {
        skippedTabs.push({ tab: route.tab, confirmedLeads: route.items.length, reason: 'Prepare the native A:M leads Table and headers.' });
        return;
      }
      const projects = new Set(route.items.map(item => item.project_id));
      if (projects.size > 1 || (projects.size === 1 && typeof route.items[0].project_id !== 'string')) {
        throw new Error('Contact name matches multiple or invalid project IDs for ' + route.tab);
      }
      ready.push({ tab: route.tab, project: route.items.length ? route.items[0].project_id : null,
        lastRow: tables[0].range.endRowIndex, items: route.items });
    });
    // Validate every ready tab before writing any tab.
    let results = ready.map(cfg => watrckImportTab_(false, workbook, cfg, cfg.items));
    if (write) results = ready.map(cfg => watrckImportTab_(true, workbook, cfg, cfg.items));
    const summary = { mode: write ? 'import' : 'preview', routing: 'Config Contact_Conversion_Name',
      confirmedLeads: items.length, existingRows: results.reduce((n, r) => n + r.existingRows, 0),
      newRows: results.reduce((n, r) => n + r.newRows, 0),
      sheetWrites: results.some(r => r.sheetWrites), unmappedLeads, skippedTabs, tabs: results };
    console.log(JSON.stringify(summary));
    return summary;
  } finally {
    if (!lockAlreadyHeld) lock.releaseLock();
  }
}

function watrckSalesWorkbook_() {
  const props = PropertiesService.getScriptProperties();
  const id = props.getProperty('SALES_SHEET_FILE_ID');
  if (!id) throw new Error('Set SALES_SHEET_FILE_ID to the intended Sales workbook first.');
  if (id === props.getProperty('SHEET_FILE_ID')) throw new Error('Sales and Conversions workbook IDs must differ.');
  return SpreadsheetApp.openById(id);
}

function runSalesImportForExistingJob_() {
  if (!PropertiesService.getScriptProperties().getProperty('SALES_SHEET_FILE_ID')) {
    throw new Error('SALES_SHEET_FILE_ID is missing.');
  }
  // Called only by runOnceFetch, which already owns the shared script lock.
  return watrckRunConfiguredImport_(true, watrckSalesWorkbook_(), true);
}

// Pure planning: existing rows retain all sales-entered values.
function watrckPlanLeadImport_(values, formulas, items, project) {
  const tokens = new Map();
  const available = [];
  values.forEach((row, index) => {
    const token = String(row[4] || '').trim().toUpperCase();
    if (token) {
      if (tokens.has(token)) throw new Error('Duplicate token in leads table at row ' + (index + 2));
      tokens.set(token, index + 2);
    } else if (row.every(value => value === '' || value === null) &&
               formulas[index].every(value => value === '')) {
      available.push(index + 2);
    }
  });
  const seen = new Set();
  const plan = [];
  items.forEach(item => {
    if (item.project_id !== project || !/^[A-Z0-9]{4,12}$/.test(item.token)) {
      throw new Error('Unexpected project or token in lead feed.');
    }
    if (seen.has(item.token)) throw new Error('Duplicate token in lead feed.');
    seen.add(item.token);
    const existing = tokens.has(item.token);
    const row = existing ? tokens.get(item.token) : available.shift();
    if (!row) throw new Error('No empty rows remain inside the configured leads Table. Extend its range first.');
    plan.push({ row, existing, item });
  });
  return plan;
}

function watrckImportTab_(write, workbook, cfg, items) {
  // The configured runner holds the script lock across all tabs.
  const sheet = workbook.getSheetByName(cfg.tab);
  if (!sheet) throw new Error('Sales tab not found: ' + cfg.tab);
  const headers = ['Name', 'Phone number', 'Interest', 'Notes', 'Token', 'Date',
    'Quality', 'USD Value', 'SERVER FEEDBACK', 'Campaign',
    'Ad group / Asset group', 'Ad', 'Campaign type'];
  const actual = sheet.getRange(1, 1, 1, 13).getDisplayValues()[0];
  if (!headers.every((value, index) => actual[index].trim() === value)) {
    throw new Error('Expected the prepared A:M leads headers.');
  }
  // Read A:M only. Adjacent FILTER output is never part of row allocation.
  const source = sheet.getRange(2, 1, cfg.lastRow - 1, 13);
  const plan = watrckPlanLeadImport_(source.getValues(), source.getFormulas(), items, cfg.project);
  const summary = {
    mode: write ? 'import' : 'preview', tab: cfg.tab, project: cfg.project,
    confirmedLeads: items.length,
    existingRows: plan.filter(entry => entry.existing).length,
    newRows: plan.filter(entry => !entry.existing).length,
    sheetWrites: false,
    attribution: 'Names pending Google Ads lookup; IDs will be retained in cell notes.'
  };
  if (write) {
    const safeText = value => {
      const text = value === null || value === undefined ? '' : String(value);
      return text.startsWith('=') ? "'" + text : text;
    };
    for (const entry of plan) {
      const item = entry.item;
      const range = sheet.getRange(entry.row, 1, 1, 13);
      const current = range.getValues()[0];
      const formulas = range.getFormulas()[0];
      const token = String(current[4] || '').trim().toUpperCase();
      if ((entry.existing && token !== item.token) ||
          (!entry.existing && (current.some(value => value !== '' && value !== null) || formulas.some(Boolean)))) {
        throw new Error('Row changed during import. Rerun preview.');
      }
      const date = item.date ? new Date(item.date) : '';
      if (date && !Number.isFinite(date.getTime())) throw new Error('Invalid lead date.');
      const phone = item.phone == null ? '' : String(item.phone).trim();
      const contact = phone || (item.username == null ? '' : String(item.username).trim());
      const attribution = [item.campaign_id ? 'Name pending' : 'Unavailable',
        item.adgroup_id || item.assetgroup_id ? 'Name pending' : 'Unavailable',
        item.ad_id ? 'Name pending' : 'Unavailable', 'Pending lookup'];
      if (!entry.existing) {
        // Write A:F together so retry recognizes the token. Never write G:I,
        // even for new leads with existing backend quality/value.
        sheet.getRange(entry.row, 2).setNumberFormat('@');
        sheet.getRange(entry.row, 3, 1, 2).setBackground('#fff2cc').setFontColor('#000000');
        sheet.getRange(entry.row, 1, 1, 6).setValues([[safeText(item.name), safeText(contact),
          'Fresh Lead — Review', 'Fresh Lead — Review', item.token, date]]);
        sheet.getRange(entry.row, 10, 1, 4).setValues([attribution]);
      } else {
        // Fill blank identity/date fields only. Never modify G:I or C:D.
        [[1, safeText(item.name)], [2, safeText(contact)], [6, date]].forEach(pair => {
          const index = pair[0] - 1;
          if (current[index] === '' && !formulas[index] && pair[1] !== '') {
            const cell = sheet.getRange(entry.row, pair[0]);
            if (pair[0] === 2) cell.setNumberFormat('@');
            cell.setValue(pair[1]);
          }
        });
        attribution.forEach((value, index) => {
          if (current[9 + index] === '' && !formulas[9 + index]) {
            sheet.getRange(entry.row, 10 + index).setValue(value);
          }
        });
      }
      const ids = [item.campaign_id, item.assetgroup_id || item.adgroup_id, item.ad_id];
      ids.forEach((id, index) => {
        const cell = sheet.getRange(entry.row, 10 + index);
        if (id && !cell.getNote()) cell.setNote('Google Ads ID: ' + String(id));
      });
    }
    SpreadsheetApp.flush();
    summary.sheetWrites = plan.length > 0;
  }
  console.log(JSON.stringify(summary));
  return summary;
}

function watrckFetchSalesLeads_() {
    const includeTestUsername = PropertiesService.getScriptProperties()
      .getProperty('SALES_INCLUDE_TEST_USERNAME') === 'true';
    const secret = PropertiesService.getScriptProperties().getProperty('EXPORT_SECRET');
    if (!secret) throw new Error('EXPORT_SECRET Script Property is missing.');
    const items = [];
    let after = null;
    const cursors = new Set();
    // Restart every run: catches older clicks confirmed after a prior run.
    for (let page = 0; ; page++) {
      if (page >= 100) throw new Error('Scan exceeded 100 pages. No sheet writes performed.');
      const url = WATRCK_SALES_IMPORT.endpoint + '?limit=200' +
        (includeTestUsername ? '&include_test_username=1' : '') +
        (after === null ? '' : '&after=' + encodeURIComponent(after));
      const response = UrlFetchApp.fetch(url, {
        headers: { 'x-export-secret': secret }, muteHttpExceptions: true
      });
      if (response.getResponseCode() !== 200) {
        throw new Error('Lead feed HTTP ' + response.getResponseCode());
      }
      const data = JSON.parse(response.getContentText());
      if (!Array.isArray(data.items) || data.testing !== false ||
          (includeTestUsername && data.include_test_username !== true) ||
          !(data.next_cursor === null || typeof data.next_cursor === 'string')) {
        throw new Error('Unexpected lead feed response.');
      }
      items.push(...data.items);
      after = data.next_cursor;
      if (after === null) break;
      if (cursors.has(after)) throw new Error('Repeated pagination cursor.');
      cursors.add(after);
    }
    return items;
}
