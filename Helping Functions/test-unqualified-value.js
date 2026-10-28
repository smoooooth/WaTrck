'use strict';

// Local-only contract test: real Sales handler and backend route, stubbed I/O.
// No credentials, network calls, Google Sheets, or Firestore writes.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const root = path.resolve(__dirname, '..');
const salesSource = fs.readFileSync(path.join(root, 'Sales Sheet/Sales Sheet App Script.js'), 'utf8');
const backendSource = fs.readFileSync(path.join(root, 'functions/index.js'), 'utf8');
new vm.Script(backendSource); // Syntax-check the full backend without loading services.
const routeStart = backendSource.indexOf("app.post('/sales/quality-update',");
const routeEnd = backendSource.indexOf('\n});', routeStart);
assert.ok(routeStart >= 0 && routeEnd > routeStart, 'Sales route must be found');
const routeSource = backendSource.slice(routeStart, routeEnd + 4);

function runSales(quality, value, oldValue = '', column = 7) {
  const row = ['', '', '', '', 'TEST001', '', quality, value, ''];
  const calls = [];
  const sheet = {
    getName: () => 'TestProject',
    getLastColumn: () => 9,
    getRange: (_r, c) => ({
      getValues: () => [row],
      setValue: v => { row[c - 1] = v; },
      setFontColor: () => {}
    })
  };
  const context = vm.createContext({
    console,
    SpreadsheetApp: {
      getActive: () => ({ getSheetByName: () => ({
        getDataRange: () => ({ getValues: () => [['TestProject', 'Test Qualified', 'Test Closed']] })
      }) }),
      flush: () => {}
    },
    PropertiesService: { getScriptProperties: () => ({ getProperty: () => 'local-test-only' }) },
    UrlFetchApp: { fetch: (_url, options) => {
      calls.push(JSON.parse(options.payload));
      return { getResponseCode: () => 200, getContentText: () => '{"ok":true}' };
    } }
  });
  vm.runInContext(salesSource, context);
  context.onEditTrigger({ oldValue, range: {
    getSheet: () => sheet, getColumn: () => column, getRow: () => 2
  } });
  return { calls, row };
}

async function runBackend(body, existing = {}) {
  let handler;
  const writes = [];
  let reads = 0;
  const context = vm.createContext({
    console,
    app: { post: (_path, ...handlers) => { handler = handlers.at(-1); } },
    requireSecret: () => {},
    express: { json: () => () => {} },
    admin: { firestore: { FieldValue: { serverTimestamp: () => 'test-timestamp' } } },
    db: { doc: docPath => {
      reads++;
      if (docPath === 'tokenIndex/TEST001') {
        return { get: async () => ({ exists: true, data: () => ({ projectId: 'TestProject' }) }) };
      }
      assert.equal(docPath, 'projects/TestProject/clicks/TEST001');
      return {
        get: async () => ({ exists: true, data: () => ({ upload_version: 3, ...existing }) }),
        update: async update => { writes.push(update); }
      };
    } }
  });
  vm.runInContext(routeSource, context);
  const response = { code: 200, status(code) { this.code = code; return this; },
    json(data) { this.data = data; return this; } };
  await handler({ body }, response);
  return { response, writes, reads };
}

async function main() {
  for (const value of [10, 0, 0.25, '12.50', '0', ' 7 ']) {
    const sales = runSales('unqualified', value);
    assert.equal(sales.calls.length, 1);
    assert.equal(sales.calls[0].value, Number(value));
    assert.equal(sales.calls[0].conversion_name, undefined, 'Unqualified targets original Contact');
    assert.match(sales.row[8], /^Lead Published:/);
    const backend = await runBackend(sales.calls[0]);
    assert.equal(backend.response.code, 200);
    assert.equal(backend.writes.length, 1);
    assert.equal(backend.writes[0].conversion_value_final, Number(value));
    assert.equal(backend.writes[0].sales_sheet_updated_quality, 0);
    assert.equal(backend.writes[0].upload_version, 4);
    assert.equal(backend.writes[0].adjustment_pending, true);
    assert.equal(backend.writes[0].conversion_name_sales, undefined);
    assert.equal(backend.response.data.value, Number(value));
  }
  for (const value of [undefined, null, '', '   ', -1, '-1', 'abc', '10abc', NaN, Infinity, 'Infinity', true, false, [], [10], {}]) {
    const sales = runSales('unqualified', value);
    assert.equal(sales.calls.length, 0, `Invalid Sales value: ${String(value)}`);
    assert.match(sales.row[8], /^ERROR\./);
    assert.equal(sales.row[6], '', 'Quality restored on rejection');
    const backend = await runBackend({ token: 'TEST001', quality: 'unqualified', value });
    assert.equal(backend.response.code, 400, `Invalid backend value: ${String(value)}`);
    assert.equal(backend.reads, 0, 'Reject before accessing Firestore');
    assert.equal(backend.writes.length, 0);
  }
  for (const [quality, value, name, code] of [
    ['qualified', 500, 'Test Qualified', 1], ['closed', 10000, 'Test Closed', 2]
  ]) {
    const sales = runSales(quality, value);
    assert.equal(sales.calls[0].conversion_name, name);
    const backend = await runBackend(sales.calls[0], {
      sales_sheet_updated_quality: 1, conversion_name_sales: 'Test Qualified'
    });
    assert.equal(backend.response.code, 200);
    assert.equal(backend.writes[0].conversion_value_final, value);
    assert.equal(backend.writes[0].sales_sheet_updated_quality, code);
    assert.equal(backend.writes[0].conversion_name_sales, name);
    if (quality === 'closed') assert.equal(backend.writes[0].conversion_name_sales_qualified, 'Test Qualified');
    assert.equal(runSales(quality, '').calls.length, 0);
    assert.equal((await runBackend({ token: 'TEST001', quality })).response.code, 400);
  }
  for (const [previous, next] of [['qualified', 'unqualified'], ['closed', 'qualified'], ['closed', 'unqualified']]) {
    const sales = runSales(next, 10, previous);
    assert.equal(sales.calls.length, 0);
    assert.equal(sales.row[6], previous);
  }
  assert.equal(runSales('unqualified', 10, '', 8).calls.length, 0, 'Value-only edit must not submit');
  assert.equal(runSales(0, 10).calls[0].value, 10, 'Numeric quality code remains supported');
  console.log('PASS: Unqualified values survive Sales → backend; invalid values are rejected; Qualified/Closed, transition guards and trigger scope preserved. Local stubs only.');
}

main().catch(error => { console.error(error); process.exitCode = 1; });
