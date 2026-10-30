// Offline contract/model test. Does not claim to emulate Google's Table engine.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const clone = x => JSON.parse(JSON.stringify(x));
const headers = ['Name', 'Phone number', 'Interest', 'Notes', 'Token', 'Date', 'Quality', 'USD Value', 'SERVER FEEDBACK'];
const extras = ['Campaign', 'Ad group / Asset group', 'Ad', 'Campaign type'];
const formula = '=FILTER(Project_Leads_2,IF(K2="",TEXT(Project_Leads_2[Date],"MMM")<>"",TEXT(Project_Leads_2[Date],"MMM")=K2),YEAR(Project_Leads_2[Date])=K3,IF(K4="",Project_Leads_2[Quality]=K4<>"",Project_Leads_2[Quality]=K4))';
const make = (name, id, prepared) => {
  const values = Array.from({ length: 24 }, () => Array(26).fill(''));
  const formulas = clone(values), notes = clone(values);
  const formats = values.map(row => row.map(() => ({ textFormat: { fontSize: 12, bold: true, italic: true } })));
  formats[0][8] = { backgroundColorStyle: { rgbColor: { red: 0.5, green: 0.4, blue: 0 } },
    textFormat: { fontSize: 11, bold: true, italic: true, fontFamily: 'Arial' }, horizontalAlignment: 'CENTER' };
  notes[0][8] = 'Preserve header note';
  values[0].splice(0, prepared ? 13 : 9, ...headers, ...(prepared ? extras : []));
  values[6].splice(prepared ? 13 : 9, prepared ? 13 : 9, ...headers, ...(prepared ? extras : []));
  values[1].splice(0, 9, 'Lead', '001234', 'Interest', 'Keep notes', 'TOK1234', '', 'Qualified', 500, 'Published');
  notes[1][4] = 'Keep token note';
  values[1][prepared ? 13 : 9] = 'Month'; values[2][prepared ? 14 : 10] = 2026;
  formulas[7][prepared ? 13 : 9] = prepared ? formula.replace(/K([234])/g, 'O$1') : formula;
  const cols = () => headers.map((columnName, columnIndex) => ({ columnIndex, columnName,
    ...(columnIndex === 6 ? { columnType: 'DROPDOWN', dataValidationRule: { condition: {
      type: 'ONE_OF_LIST', values: [{ userEnteredValue: 'Qualified' }]
    } } } : {}) })).concat(prepared ? extras.map((columnName, i) => ({ columnName, columnIndex: i + 9, columnType: 'TEXT' })) : []);
  return { values, formulas, notes, formats, widths: Array(26).fill(200), properties: { title: name, sheetId: id, gridProperties: { columnCount: 26 } }, tables: [
    { tableId: id + '-source', name: 'Project Leads_2', range: { sheetId: id, endRowIndex: 24, endColumnIndex: prepared ? 13 : 9 }, columnProperties: cols() },
    { tableId: id + '-filtered', name: 'Filtered_Leads', range: { sheetId: id, startRowIndex: 6, endRowIndex: 15,
      startColumnIndex: prepared ? 13 : 9, endColumnIndex: prepared ? 26 : 18 }, columnProperties: cols() }
  ] };
};
let state = [make('AIDA_Oman', 1, false), make('Vision_Iconic_Tower', 2, true)];
let calls = 0, failBatch = false, locked = false;
const properties = new Map([['SALES_IMPORT_ENABLED', 'false'], ['SALES_SHEET_FILE_ID', 'copy']]);
const sheet = name => ({
  getMaxColumns: () => state.find(s => s.properties.title === name).properties.gridProperties.columnCount,
  getRange(row, col, height = 1, width = 1) {
    const read = key => state.find(s => s.properties.title === name)[key].slice(row - 1, row - 1 + height)
      .map(r => r.slice(col - 1, col - 1 + width));
    return { getValues: () => read('values'), getDisplayValues: () => read('values').map(r => r.map(String)),
      getFormulas: () => read('formulas'), getNotes: () => read('notes'), getFormula: () => read('formulas')[0][0] };
  }
});
const config = { getLastRow: () => state.length + 1, getRange: () => ({ getDisplayValues: () => state.map(s => [s.properties.title]) }) };
const workbook = { getId: () => 'copy', getSheetByName: name => name === 'Config' ? config : sheet(name) };
const context = vm.createContext({ console: { log() {} },
  watrckSalesWorkbook_: () => workbook,
  PropertiesService: { getScriptProperties: () => ({ getProperty: key => properties.get(key) }) },
  LockService: { getScriptLock: () => ({ waitLock() { assert.equal(locked, false); locked = true; }, releaseLock() { locked = false; } }) },
  SpreadsheetApp: { flush() {} },
  Sheets: { Spreadsheets: {
    get(id, options) {
      assert.equal(id, 'copy');
      assert.ok(options.ranges.every(range => range.endsWith("'!I1")));
      return { sheets: clone(state).map(({ values, formulas, notes, formats, widths, ...info }) => ({ ...info,
        data: [{ startColumn: 8, rowData: [{ values: [{ userEnteredFormat: formats[0][8] }] }] }]
      })) };
    },
    batchUpdate({ requests }, id) {
      assert.equal(id, 'copy'); calls++;
      if (failBatch) throw new Error('Simulated atomic rejection');
      const staged = clone(state);
      for (const request of requests) {
        if (request.insertDimension) {
          const { range } = request.insertDimension;
          assert.equal(range.startIndex, 9); assert.equal(range.endIndex, 13);
          const s = staged.find(s => s.properties.sheetId === range.sheetId);
          for (const key of ['values', 'formulas', 'notes']) s[key].forEach(r => r.splice(9, 0, '', '', '', ''));
          s.formats.forEach(r => r.splice(9, 0, {}, {}, {}, {}));
          s.widths.splice(9, 0, 200, 200, 200, 200);
          s.formulas = s.formulas.map(r => r.map(f => f.replace(/K([234])/g, 'O$1')));
          // Reproduce the user's observed bug: filtered table expands leftward.
          s.tables[1].range.endColumnIndex += 4;
          s.properties.gridProperties.columnCount += 4;
        } else if (request.updateTable) {
          const { table, fields } = request.updateTable;
          assert.equal(fields, 'range,columnProperties');
          const s = staged.find(s => s.properties.sheetId === table.range.sheetId);
          const index = s.tables.findIndex(t => t.tableId === table.tableId);
          assert.ok(index >= 0);
          assert.equal(table.columnProperties.length, table.range.endColumnIndex - table.range.startColumnIndex);
          s.tables[index] = { ...s.tables[index], ...clone(table) };
          s.formats[0][8] = { textFormat: { bold: false } }; // Model unwanted Table header-style inheritance.
          const [a, b] = s.tables.map(t => t.range);
          assert.ok(a.endColumnIndex <= b.startColumnIndex, 'table overlap during migration');
        } else if (request.updateCells) {
          const { start, rows, fields } = request.updateCells;
          assert.equal(fields, 'userEnteredValue');
          assert.ok((start.rowIndex === 0 && start.columnIndex === 9) || (start.rowIndex === 6 && start.columnIndex === 22));
          const s = staged.find(s => s.properties.sheetId === start.sheetId);
          s.values[start.rowIndex].splice(start.columnIndex, 4, ...rows[0].values.map(c => c.userEnteredValue.stringValue));
        } else if (request.updateDimensionProperties) {
          const { range, properties, fields } = request.updateDimensionProperties;
          assert.equal(fields, 'pixelSize'); assert.equal(properties.pixelSize, 120);
          assert.ok(range.startIndex === 9 || range.startIndex === 22);
          assert.equal(range.endIndex, range.startIndex + 4);
          staged.find(s => s.properties.sheetId === range.sheetId).widths.fill(120, range.startIndex, range.endIndex);
        } else if (request.repeatCell) {
          const { range, cell, fields } = request.repeatCell;
          const s = staged.find(s => s.properties.sheetId === range.sheetId);
          if (range.startColumnIndex === 8) {
            assert.deepEqual([range.startRowIndex, range.endRowIndex, range.endColumnIndex], [0, 1, 9]);
            assert.equal(fields, 'userEnteredFormat');
            s.formats[0][8] = clone(cell.userEnteredFormat);
          } else {
            assert.ok(range.startColumnIndex === 9 || range.startColumnIndex === 22);
            assert.equal(range.endColumnIndex, range.startColumnIndex + 4);
            assert.ok(!fields.includes('userEnteredValue'));
            for (let r = range.startRowIndex; r < range.endRowIndex; r++) {
              for (let c = range.startColumnIndex; c < range.endColumnIndex; c++) {
                s.formats[r][c] = clone(cell.userEnteredFormat);
              }
            }
          }
        } else throw new Error('Unexpected mutation');
      }
      state = staged;
    }
  } }
});
vm.runInContext(fs.readFileSync(path.join(__dirname, '../Sales Sheet/Prepare Attribution Columns.js'), 'utf8'), context);
assert.equal(context.watrckLayoutFormula_(formula, 'Project Leads_2', false), formula.replace(/K([234])/g, 'O$1'));
assert.equal(context.watrckLayoutFormula_(formula, 'Project_Leads_2', false), formula.replace(/K([234])/g, 'O$1'));
assert.throws(() => context.watrckLayoutFormula_(formula, 'Unrelated Table', false), /Unexpected FILTER/);
assert.throws(() => context.watrckLayoutFormula_(formula.replace(/K3/g, 'K5'), 'Project Leads_2', false), /Unexpected FILTER/);
const before = JSON.stringify(state);
let result = context.previewAttributionLayouts();
assert.deepEqual(Array.from(result.tabs, t => t.status), ['ready', 'alreadyPrepared']);
assert.equal(JSON.stringify(state), before); assert.equal(calls, 0);
const plan = context.watrckLayoutRead_(sheet('AIDA_Oman'), context.Sheets.Spreadsheets.get('copy', { ranges: ["'AIDA_Oman'!I1"] }).sheets[0]);
const requests = context.watrckLayoutRequests_(plan, 1, 18);
assert.equal(requests[1].appendDimension.length, 4, 'ensure space through Z');
assert.deepEqual(JSON.parse(JSON.stringify(requests[3].updateTable.table.columnProperties[6])), state[0].tables[0].columnProperties[6]);
failBatch = true;
assert.throws(() => context.prepareAidaAttributionLayout(), /atomic rejection/);
assert.equal(JSON.stringify(state), before); failBatch = false;
result = context.prepareAidaAttributionLayout();
assert.equal(result.sheetWrites, true); assert.equal(result.tabs[0].status, 'prepared');
assert.equal(JSON.stringify(state[1]), JSON.stringify(JSON.parse(before)[1]), 'Vision untouched');
assert.deepEqual(state[0].values[1].slice(0, 9), JSON.parse(before)[0].values[1].slice(0, 9));
assert.equal(state[0].tables[1].range.startColumnIndex, 13);
assert.equal(state[0].formulas[7][13], formula.replace(/K([234])/g, 'O$1'));
assert.deepEqual(state[0].formats[0][8], JSON.parse(before)[0].formats[0][8], 'I1 full explicit format restored');
assert.equal(state[0].notes[0][8], 'Preserve header note');
result = context.prepareRemainingAttributionLayouts();
assert.equal(result.sheetWrites, true);
assert.ok(result.tabs.every(tab => tab.status === 'formatted'), 'existing layouts receive formatting only');
for (const s of state) {
  for (const [startRow, endRow, startCol] of [[0, 24, 9], [6, 15, 22]]) {
    for (let r = startRow; r < endRow; r++) for (let c = startCol; c < startCol + 4; c++) {
      const format = s.formats[r][c];
      assert.deepEqual(format.backgroundColorStyle.rgbColor, { red: 67 / 255, green: 67 / 255, blue: 67 / 255 });
      assert.equal(format.textFormat.fontSize, 10);
      assert.equal(format.textFormat.bold, false); assert.equal(format.textFormat.italic, false);
      assert.equal(s.widths[c], 120);
    }
  }
  assert.equal(s.values[0][8], 'SERVER FEEDBACK');
  assert.deepEqual(s.formats[0][8], JSON.parse(before)[0].formats[0][8]);
  assert.equal(s.widths[8], 200); assert.equal(s.widths[13], 200);
}
const after = JSON.stringify(state);
context.prepareRemainingAttributionLayouts();
assert.equal(JSON.stringify(state), after, 'reapplying styles never inserts extra columns');
const count = calls;
state = JSON.parse(before); state[0].values[8][18] = 'Do not overwrite';
assert.equal(context.previewAttributionLayouts().tabs[0].status, 'blocked');
assert.throws(() => context.prepareRemainingAttributionLayouts(), /preflight blocked/);
assert.equal(calls, count);
state = JSON.parse(before); state[0].tables[1].range.endColumnIndex = 22;
assert.equal(context.previewAttributionLayouts().tabs[0].status, 'blocked');
state = JSON.parse(before); properties.set('SALES_IMPORT_ENABLED', 'true');
assert.throws(() => context.prepareAidaAttributionLayout(), /SALES_IMPORT_ENABLED=false/);
assert.equal(locked, false);
console.log('PASS: read-only preview; native-table repair; preserved I1/value/note/style and lead/dropdown data; moved formula/controls; scoped 120px/dark-grey/10pt/plain styling; prepared-tab formatting; safe repeat; blocked layouts; disabled-import guard. Live styling still needs visual verification.');
