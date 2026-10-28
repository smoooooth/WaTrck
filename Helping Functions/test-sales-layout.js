// Local controlled test: node "Helping Functions/test-sales-layout.js"
// Tests the layout helper's guards and cell preservation with a Sheets stub.
// Does not simulate Google Sheets formula rewriting or Google Table expansion.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const cells = Array.from({ length: 8 }, () => Array(18).fill(''));
cells[0].splice(0, 9, 'Name', 'Phone number', 'Interest', 'Notes', 'Token',
  'Date', 'Quality', 'USD Value', 'SERVER FEEDBACK');
cells[1][4] = 'TEST123';
cells[1][6] = 'qualified';
cells[1][7] = 500;
cells[1][8] = 'Existing feedback';
cells[1][9] = 'Select Month';
cells[2][10] = 2025;
cells[6].splice(9, 9, ...cells[0].slice(0, 9));
cells[7][9] = '=FILTER(Project_Leads_2,...)';
const originalSales = cells.map(row => row.slice(0, 9));
const originalFilter = cells.map(row => row.slice(9));
let name = 'Vision_Iconic_Tower';
let insertions = 0;
let flushes = 0;
let releases = 0;
const sheet = {
  getName: () => name,
  getMaxColumns: () => cells[0].length,
  getRange(row, column, height = 1, width = 1) {
    return {
      getDisplayValues: () => cells.slice(row - 1, row - 1 + height)
        .map(values => values.slice(column - 1, column - 1 + width).map(String)),
      getDisplayValue: () => String(cells[row - 1][column - 1]),
      setValues(values) {
        values.forEach((valuesRow, offset) => {
          cells[row - 1 + offset].splice(column - 1, width, ...valuesRow);
        });
      }
    };
  },
  insertColumnsBefore(column, count) {
    assert.equal(column, 10);
    assert.equal(count, 4);
    cells.forEach(row => row.splice(column - 1, 0, ...Array(count).fill('')));
    insertions++;
  }
};
const context = vm.createContext({
  console: { log() {} },
  LockService: { getDocumentLock: () => ({ waitLock() {}, releaseLock() { releases++; } }) },
  SpreadsheetApp: {
    getActive: () => ({
      getActiveSheet: () => sheet,
      getSheetByName: () => ({
        getDataRange: () => ({ getDisplayValues: () => [['TabName'], ['Vision_Iconic_Tower']] })
      })
    }),
    flush() { flushes++; }
  }
});
const source = fs.readFileSync(path.join(__dirname, '../Sales Sheet/Prepare Attribution Columns.js'), 'utf8');
vm.runInContext(source, context);

name = 'Config';
assert.throws(() => context.prepareAttributionColumnsForActiveTab(), /Select a project tab/);
assert.equal(insertions, 0);
name = 'Vision_Iconic_Tower';
context.prepareAttributionColumnsForActiveTab();
assert.deepEqual(cells.map(row => row.slice(0, 9)), originalSales);
assert.deepEqual(cells.map(row => row.slice(13)), originalFilter);
assert.deepEqual(cells[0].slice(9, 13), ['Campaign', 'Ad group / Asset group', 'Ad', 'Campaign type']);
context.prepareAttributionColumnsForActiveTab();
assert.equal(insertions, 1);
assert.equal(flushes, 1);
// A failed header write after an insertion must not lead to a second insertion.
cells[0].fill('', 9, 13);
assert.throws(() => context.prepareAttributionColumnsForActiveTab(), /partial layout preparation/);
assert.equal(insertions, 1);
assert.equal(releases, 4);
console.log('PASS: sales cells preserved; filter cells moved four columns; retry does not reinsert; partial layout and unconfigured tab blocked.');
