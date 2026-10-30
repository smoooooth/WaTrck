// Install beside Lead Import.js in the CONVERSIONS Apps Script project.
// Manual migration only. Uses SALES_SHEET_FILE_ID; imports must be disabled.
function previewAttributionLayouts() { return watrckMigrateLayouts_(false); }
function prepareAidaAttributionLayout() { return watrckMigrateLayouts_(true, 'AIDA_Oman'); }
// Run after checking the one-tab live test.
function prepareRemainingAttributionLayouts() { return watrckMigrateLayouts_(true); }

const WATRCK_LAYOUT_HEADERS = ['Name', 'Phone number', 'Interest', 'Notes', 'Token',
  'Date', 'Quality', 'USD Value', 'SERVER FEEDBACK'];
const WATRCK_LAYOUT_EXTRA = ['Campaign', 'Ad group / Asset group', 'Ad', 'Campaign type'];
const WATRCK_LAYOUT_WIDTH = 120;

function watrckLayoutHeaderFormat_(info) {
  const grid = (info.data || []).find(data => data.startColumn === 8 && (data.startRow || 0) === 0);
  if (!grid) throw new Error('I1 format snapshot is missing; no layout changes allowed.');
  return JSON.parse(JSON.stringify(grid.rowData?.[0]?.values?.[0]?.userEnteredFormat || {}));
}

function watrckLayoutStyleRequests_(plan, sheetId) {
  const requests = [];
  // Style only the four attribution columns in each Table, including headers.
  [[0, plan.source.range.endRowIndex, 9], [6, plan.filtered.range.endRowIndex, 22]]
    .forEach(([startRowIndex, endRowIndex, startColumnIndex]) => {
      requests.push({ updateDimensionProperties: {
        range: { sheetId, dimension: 'COLUMNS', startIndex: startColumnIndex, endIndex: startColumnIndex + 4 },
        properties: { pixelSize: WATRCK_LAYOUT_WIDTH }, fields: 'pixelSize'
      } }, { repeatCell: {
        range: { sheetId, startRowIndex, endRowIndex, startColumnIndex, endColumnIndex: startColumnIndex + 4 },
        cell: { userEnteredFormat: {
          backgroundColorStyle: { rgbColor: { red: 67 / 255, green: 67 / 255, blue: 67 / 255 } },
          textFormat: { foregroundColorStyle: { rgbColor: { red: 1, green: 1, blue: 1 } },
            fontSize: 10, bold: false, italic: false }
        } },
        fields: 'userEnteredFormat.backgroundColorStyle,userEnteredFormat.textFormat.foregroundColorStyle,' +
          'userEnteredFormat.textFormat.fontSize,userEnteredFormat.textFormat.bold,userEnteredFormat.textFormat.italic'
      } });
    });
  // Table expansion can inherit header styling. Restore I1's exact original
  // explicit format, without writing its value, note, validation or rich text.
  requests.push({ repeatCell: {
    range: { sheetId, startRowIndex: 0, endRowIndex: 1, startColumnIndex: 8, endColumnIndex: 9 },
    cell: { userEnteredFormat: plan.headerFormat }, fields: 'userEnteredFormat'
  } });
  return requests;
}

function watrckLayoutMatches_(actual, expected) {
  return expected.every((value, i) => String(actual[i] || '').trim() === value);
}

function watrckLayoutFormula_(formula, tableName, shifted) {
  const column = shifted ? 'O' : 'K';
  // Sheets displays spaces in Table names but uses underscores in formulas.
  const formulaTableName = tableName.replace(/ /g, '_');
  // Preserve the user's FILTER expression and selection logic.
  const unquoted = formula.replace(/"(?:[^"]|"")*"/g, '""');
  const refs = unquoted.match(/\b\$?[A-Z]+\$?\d+\b/gi) || [];
  if (!/^=(?:FILTER|IFERROR)\s*\(/i.test(formula) || !formula.includes(formulaTableName) ||
      ![2, 3, 4].every(row => refs.some(ref => ref.replace(/\$/g, '').toUpperCase() === column + row)) ||
      refs.some(ref => ![column + '2', column + '3', column + '4'].includes(ref.replace(/\$/g, '').toUpperCase()))) {
    throw new Error('Unexpected FILTER formula or control references; review this tab manually.');
  }
  return formula.split(/("(?:[^"]|"")*")/g).map((part, i) => i % 2 ? part :
    part.replace(/\bK(\$?[234])\b/gi, 'O$1')).join('');
}

function watrckLayoutRead_(sheet, info) {
  const tables = info.tables || [];
  const start = (table, row, column) => (table.range.startRowIndex || 0) === row &&
    (table.range.startColumnIndex || 0) === column;
  const source = tables.find(table => start(table, 0, 0));
  if (tables.length !== 2 || !source) throw new Error('Expected exactly two native Tables, with leads starting at A1.');
  const filtered = tables.find(table => table.tableId !== source.tableId);
  const prepared = source.range.endColumnIndex === 13 && start(filtered, 6, 13) && filtered.range.endColumnIndex === 26;
  const original = source.range.endColumnIndex === 9 && start(filtered, 6, 9) && filtered.range.endColumnIndex === 18;
  if (!prepared && !original) throw new Error('Partial or unexpected Table layout; no automatic insertion.');
  for (const table of tables) {
    if (!Number.isInteger(table.range.endRowIndex) || table.range.endRowIndex <= (table.range.startRowIndex || 0) + 1 ||
        (table.rowsProperties && table.rowsProperties.footerColorStyle)) {
      throw new Error('Unbounded, empty, or footer Table needs manual review.');
    }
  }
  const width = prepared ? 13 : 9;
  const expected = WATRCK_LAYOUT_HEADERS.concat(prepared ? WATRCK_LAYOUT_EXTRA : []);
  if (!watrckLayoutMatches_(sheet.getRange(1, 1, 1, width).getDisplayValues()[0], expected) ||
      !watrckLayoutMatches_(sheet.getRange(7, prepared ? 14 : 10, 1, width).getDisplayValues()[0], expected)) {
    throw new Error('Table headers do not match the expected Sales layout.');
  }
  const formula = sheet.getRange(8, prepared ? 14 : 10).getFormula();
  const expectedFormula = watrckLayoutFormula_(formula, source.name, prepared);
  const headerFormat = watrckLayoutHeaderFormat_(info);
  if (prepared) return { status: 'alreadyPrepared', source, filtered, headerFormat };
  // S:V becomes W:Z, the new filtered columns. Never consume existing content.
  const available = Math.min(4, sheet.getMaxColumns() - 18);
  if (available > 0) {
    const extension = sheet.getRange(7, 19, filtered.range.endRowIndex - 6, available);
    if (extension.getValues().some(row => row.some(v => v !== '' && v !== null)) ||
        extension.getFormulas().some(row => row.some(Boolean)) || extension.getNotes().some(row => row.some(Boolean))) {
      throw new Error('S:V contains data, formulas or notes needed by the filtered-table extension.');
    }
  }
  if ((info.merges || []).some(r => (r.startColumnIndex || 0) < 22 &&
      r.endColumnIndex > 9 && (r.startRowIndex || 0) < Math.max(source.range.endRowIndex, filtered.range.endRowIndex))) {
    throw new Error('Merged cells in the migration area need manual review.');
  }
  const sourceRange = sheet.getRange(1, 1, source.range.endRowIndex, 9);
  return { status: 'ready', source, filtered, expectedFormula, headerFormat,
    controls: sheet.getRange(2, 10, 3, 2).getValues(),
    sourceValues: sourceRange.getValues(), sourceFormulas: sourceRange.getFormulas(), sourceNotes: sourceRange.getNotes() };
}

function watrckLayoutRequests_(plan, sheetId, columnCount) {
  if (plan.status === 'alreadyPrepared') return watrckLayoutStyleRequests_(plan, sheetId);
  if (plan.status !== 'ready') return [];
  const extra = WATRCK_LAYOUT_EXTRA.map((columnName, i) => ({ columnIndex: 9 + i, columnName, columnType: 'TEXT' }));
  const columns = table => WATRCK_LAYOUT_HEADERS.map((header, i) => {
    const existing = (table.columnProperties || []).find(c => (c.columnIndex || 0) === i);
    if (!existing || String(existing.columnName).trim() !== header) throw new Error('Incomplete Table column metadata.');
    return JSON.parse(JSON.stringify(existing));
  });
  const update = (table, startColumnIndex, endColumnIndex, columnProperties) => ({ updateTable: {
    table: { tableId: table.tableId, range: { ...table.range, sheetId, startColumnIndex, endColumnIndex }, columnProperties },
    fields: 'range,columnProperties'
  } });
  const requests = [{ insertDimension: { range: { sheetId, dimension: 'COLUMNS', startIndex: 9, endIndex: 13 }, inheritFromBefore: true } }];
  if (columnCount + 4 < 26) requests.push({ appendDimension: { sheetId, dimension: 'COLUMNS', length: 26 - columnCount - 4 } });
  // Correct the auto-expanded filtered Table BEFORE extending the source Table.
  requests.push(update(plan.filtered, 13, 22, columns(plan.filtered)),
    update(plan.source, 0, 13, columns(plan.source).concat(extra)),
    update(plan.filtered, 13, 26, columns(plan.filtered).concat(extra)));
  [[0, 9], [6, 22]].forEach(([rowIndex, columnIndex]) => requests.push({ updateCells: {
    start: { sheetId, rowIndex, columnIndex },
    rows: [{ values: WATRCK_LAYOUT_EXTRA.map(stringValue => ({ userEnteredValue: { stringValue } })) }],
    fields: 'userEnteredValue'
  } }));
  return requests.concat(watrckLayoutStyleRequests_(plan, sheetId));
}

function watrckMigrateLayouts_(write, onlyTab) {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    const props = PropertiesService.getScriptProperties();
    if (props.getProperty('SALES_IMPORT_ENABLED') !== 'false') throw new Error('Set SALES_IMPORT_ENABLED=false during layout preparation.');
    if (!props.getProperty('SALES_SHEET_FILE_ID')) throw new Error('Set SALES_SHEET_FILE_ID to the intended Sales workbook.');
    if (typeof Sheets === 'undefined') throw new Error('Enable the Google Sheets API service.');
    const workbook = watrckSalesWorkbook_();
    const config = workbook.getSheetByName('Config');
    if (!config || config.getLastRow() < 2) throw new Error('Config has no tabs.');
    const names = config.getRange(2, 1, config.getLastRow() - 1, 1).getDisplayValues().flat().filter(Boolean);
    if (new Set(names).size !== names.length) throw new Error('Duplicate tabs in Config.');
    if (onlyTab && !names.includes(onlyTab)) throw new Error('Requested tab is not in Config.');
    const metadataOptions = {
      ranges: names.filter(name => !onlyTab || name === onlyTab).map(name => "'" + name.replace(/'/g, "''") + "'!I1"),
      fields: 'sheets(properties,tables,merges,data(startRow,startColumn,rowData(values(userEnteredFormat))))'
    };
    const metadata = Sheets.Spreadsheets.get(workbook.getId(), metadataOptions);
    const plans = [], results = [];
    for (const name of names.filter(name => !onlyTab || name === onlyTab)) {
      try {
        const sheet = workbook.getSheetByName(name);
        const info = metadata.sheets.find(s => s.properties.title === name);
        if (!sheet || !info) throw new Error('Configured tab is missing.');
        const plan = watrckLayoutRead_(sheet, info);
        const requests = watrckLayoutRequests_(plan, info.properties.sheetId, info.properties.gridProperties.columnCount);
        plans.push({ name, sheet, plan, requests });
        results.push({ tab: name, status: plan.status, leadsRange: 'A1:M' + plan.source.range.endRowIndex,
          filteredRange: 'N7:Z' + plan.filtered.range.endRowIndex, formulaCell: 'N8', controls: 'O2:O4',
          formatting: 'J:M and W:Z: 120px; dark grey 4 background; white 10pt text; no bold/italic; I1 preserved' });
      } catch (error) {
        results.push({ tab: name, status: 'blocked', reason: error.message });
      }
    }
    const summary = { mode: write ? 'prepareLayout' : 'previewLayout', sheetWrites: false, tabs: results };
    if (write) {
      if (results.some(result => result.status === 'blocked')) {
        console.log(JSON.stringify(summary));
        throw new Error('Layout preflight blocked. No changes made. Review previewLayout results.');
      }
      const requests = plans.flatMap(entry => entry.requests);
      if (requests.length) {
        // Atomic batch: an invalid request cannot leave a half-inserted layout.
        Sheets.Spreadsheets.batchUpdate({ requests }, workbook.getId());
        SpreadsheetApp.flush();
        const after = Sheets.Spreadsheets.get(workbook.getId(), metadataOptions);
        for (const entry of plans) {
          const info = after.sheets.find(s => s.properties.title === entry.name);
          if (JSON.stringify(watrckLayoutHeaderFormat_(info)) !== JSON.stringify(entry.plan.headerFormat)) {
            throw new Error('Layout applied, but I1 format verification failed for ' + entry.name + '. Keep imports disabled.');
          }
        }
        for (const entry of plans.filter(p => p.plan.status === 'ready')) {
          const info = after.sheets.find(s => s.properties.title === entry.name);
          if (watrckLayoutRead_(entry.sheet, info).status !== 'alreadyPrepared' ||
              entry.sheet.getRange(8, 14).getFormula() !== entry.plan.expectedFormula ||
              JSON.stringify(entry.sheet.getRange(2, 14, 3, 2).getValues()) !== JSON.stringify(entry.plan.controls)) {
            throw new Error('Layout applied, but formula/control verification failed for ' + entry.name + '. Keep imports disabled; do not insert again.');
          }
          const current = entry.sheet.getRange(1, 1, entry.plan.source.range.endRowIndex, 9);
          const values = current.getValues();
          if (JSON.stringify(current.getNotes()) !== JSON.stringify(entry.plan.sourceNotes) ||
              entry.plan.sourceValues.some((row, r) => row.some((value, c) =>
                !entry.plan.sourceFormulas[r][c] && JSON.stringify(values[r][c]) !== JSON.stringify(value)))) {
            throw new Error('Layout applied, but original lead-cell verification failed for ' + entry.name + '. Keep imports disabled.');
          }
        }
        summary.sheetWrites = true;
        results.forEach(result => { result.status = result.status === 'ready' ? 'prepared' : 'formatted'; });
      }
    }
    console.log(JSON.stringify(summary));
    return summary;
  } finally {
    lock.releaseLock();
  }
}
