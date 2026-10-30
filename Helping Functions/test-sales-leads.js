// Offline contract test. No Firebase credentials, reads, writes, or deployment.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../functions/index.js'), 'utf8');
const start = source.indexOf("app.get('/sales/leads',");
const end = source.indexOf('\n/**', start);
assert.ok(start >= 0 && end > start);
const fixtures = [
  { id: 'A', used: false },
  { id: 'B', used: true, conversion_value_uploaded: true, google_campaign_id: '1234567890123456789', google_adgroup_id: '222', google_ad_id: '333', whatsapp_phone: '00123456789', quality_status: 'unqualified', conversion_value_final: 0, ts: '2026-10-29T10:00:00Z' },
  { id: 'C', used: true, gclid: '_testproduction_synthetic' },
  { id: 'D', used: true, ts: 'invalid-date', whatsapp_profile_name: 'Adham', whatsapp_username: 'adhamthabet010' },
  { id: 'F', used: true, whatsapp_username: 'adhamthabet01', whatsapp_phone: '001234', gclid: 'real_click_id' },
  { id: 'G', used: true, whatsapp_username: 'adhamthabet01', gclid: '_testproduction_synthetic' }
];
let handler;
let auth;
fixtures[1].conversion_name = 'Vision Contact';
fixtures[1].conversion_name_sales = 'Vision Closed';
fixtures[1].whatsapp_username = 'fallback_name';
function makeQuery(global) {
  let max, after = '';
  const records = fixtures.map(row => ({ ...row, path: 'projects/Vision Iconic Tower/clicks/' + row.id }));
  if (global) records.push({ id: 'E', path: 'projects/Other/clicks/E', used: true, conversion_name: 'Other Contact' },
    { id: 'X', path: 'unrelated/root/clicks/X', used: true });
  const key = row => global ? row.path : row.id;
  records.sort((a, b) => key(a) < key(b) ? -1 : 1);
  const query = {
    orderBy(field) { assert.equal(field, '__name__'); return query; },
    limit(value) { max = value; return query; },
    startAfter(value) { after = global ? value.path : value; return query; },
    async get() {
      const docs = records.filter(row => key(row) > after).slice(0, max)
        .map(row => ({ id: row.id, ref: { path: row.path }, data: () => row }));
      return { docs, size: docs.length };
    }
  };
  return query;
}
const requireSecret = () => {};
const context = vm.createContext({
  console,
  requireSecret,
  app: { get(route, middleware, callback) { assert.equal(route, '/sales/leads'); auth = middleware; handler = callback; } },
  admin: { firestore: { FieldPath: { documentId: () => '__name__' } } },
  tsToIso: value => new Date(value).toISOString(),
  db: {
    doc: path => ({ path }),
    collectionGroup(name) { assert.equal(name, 'clicks'); return makeQuery(true); },
    collection(name) {
    assert.equal(name, 'projects');
    return { doc(project) {
      assert.equal(project, 'Vision Iconic Tower');
      return { collection(name) {
        assert.equal(name, 'clicks');
        return makeQuery(false);
      } };
    } };
  } }
});
vm.runInContext(source.slice(start, end), context);
async function request(params) {
  const response = { code: 200, set(name, value) { assert.equal(name, 'Cache-Control'); assert.equal(value, 'no-store'); }, status(code) { this.code = code; return this; }, json(body) { this.body = JSON.parse(JSON.stringify(body)); return this; } };
  await handler({ query: { project: 'Vision Iconic Tower', ...params } }, response);
  return response;
}
(async () => {
  assert.equal(auth, requireSecret);
  const before = JSON.stringify(fixtures);
  let result = await request({ limit: '1' });
  assert.equal(result.body.items.length, 0);
  assert.equal(result.body.next_cursor, 'A');
  result = await request({ limit: '1', after: 'A' });
  assert.equal(result.body.items[0].token, 'B');
  assert.equal(result.body.items[0].campaign_id, '1234567890123456789');
  assert.equal(result.body.items[0].phone, '00123456789');
  assert.equal(result.body.items[0].username, 'fallback_name');
  assert.equal(result.body.items[0].value, 0);
  assert.equal(result.body.next_cursor, 'B');
  result = await request({});
  assert.deepEqual(result.body.items.map(row => row.token), ['B', 'D']);
  assert.equal(result.body.items[1].date, null);
  assert.equal(result.body.next_cursor, null);
  result = await request({ testing: '1' });
  assert.deepEqual(result.body.items.map(row => row.token), ['C']);
  result = await request({ include_test_username: '1' });
  assert.deepEqual(result.body.items.map(row => row.token), ['B', 'D', 'F']);
  assert.equal(result.body.include_test_username, true);
  result = await request({ testing: '1', include_test_username: '1' });
  assert.deepEqual(result.body.items.map(row => row.token), ['C', 'G']);
  assert.equal((await request({ include_test_username: 'true' })).code, 400);
  result = await request({ limit: '1', after: 'D' });
  assert.equal(result.body.items.length, 0, 'Operator username excluded even with phone and real GCLID');
  assert.equal(result.body.next_cursor, 'F', 'Excluded records must not stop pagination');
  for (const params of [{ project: '../bad' }, { project: ['bad'] }, { after: 'a/b' }, { limit: '201' }, { limit: '0' }, { limit: '1.5' }, { testing: 'true' }]) {
    assert.equal((await request(params)).code, 400);
  }
  result = await request({ project: undefined });
  assert.deepEqual(result.body.items.map(row => row.project_id), ['Other', 'Vision Iconic Tower', 'Vision Iconic Tower']);
  assert.equal(result.body.items[1].contact_conversion_name, 'Vision Contact');
  const tokens = [];
  let cursor;
  do {
    result = await request({ project: undefined, limit: '1', ...(cursor ? { after: cursor } : {}) });
    assert.equal(result.code, 200);
    tokens.push(...result.body.items.map(row => row.token));
    cursor = result.body.next_cursor;
  } while (cursor);
  assert.deepEqual(tokens, ['E', 'B', 'D']);
  assert.equal((await request({ project: undefined, after: 'bad' })).code, 400);
  assert.equal(JSON.stringify(fixtures), before);
  console.log('PASS: read-only pagination, exported leads retained, unused/test isolation, ID precision, missing dates, explicit zero, and request validation.');
})().catch(error => { console.error(error); process.exitCode = 1; });
