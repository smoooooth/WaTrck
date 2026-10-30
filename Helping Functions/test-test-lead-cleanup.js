// Offline Firestore/HTTP contract tests; no credentials or production calls.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const { testLeadReason, processTestLead, runTestLeadCleanup } = require('../functions/test-lead-cleanup');
const copy = value => JSON.parse(JSON.stringify(value));
let records, deleted, race;
const ref = path => ({ path, id: path.split('/').pop(), get: async () => snapshot(path) });
const snapshot = path => ({ ref: ref(path), id: path.split('/').pop(), exists: records.has(path), data: () => copy(records.get(path)) });
const db = {
  doc: ref,
  collectionGroup(name) {
    assert.equal(name, 'clicks');
    let limit = Infinity, after = '', unused = false;
    const query = {
      orderBy: () => query,
      limit(n) { limit = n; return query; },
      startAfter(doc) { after = doc.path; return query; },
      where(field, op, value) { assert.equal(field, 'used'); assert.equal(value, false); unused = true; return query; },
      async get() {
        const docs = [...records.keys()].sort().filter(p => p.includes('/clicks/') && p > after &&
          (!unused || records.get(p).used === false)).slice(0, limit).map(snapshot);
        return { docs, size: docs.length, forEach: fn => docs.forEach(fn) };
      }
    };
    return query;
  },
  async runTransaction(fn) {
    const pending = [];
    if (race) { const run = race; race = null; run(); }
    const result = await fn({ get: async r => snapshot(r.path), delete: r => pending.push(r.path) });
    pending.forEach(p => { records.delete(p); deleted.push(p); });
    return result;
  }
};
function reset() {
  records = new Map(); deleted = []; race = null;
  const lead = (token, data) => {
    const clickPath = 'projects/P/clicks/' + token;
    records.set(clickPath, data); records.set('tokenIndex/' + token, { projectId: 'P', clickPath });
  };
  lead('USER123', { used: true, whatsapp_username: 'adhamthabet01' });
  lead('TEST123', { used: true, gclid: 'prefix_testproduction_suffix' });
  lead('NAME123', { used: true, whatsapp_profile_name: 'Adham', whatsapp_username: 'someone_else' });
  lead('REAL123', { used: true, gclid: 'real_click', whatsapp_username: 'adhamthabet010' });
  lead('OLD1234', { used: false, ts: '2020-01-01T00:00:00Z' });
  records.set('other/P/clicks/FAKE123', { used: true, whatsapp_username: 'adhamthabet01' });
  records.set('projects/P/whatsapp_conversations/sender', { whatsapp_username: 'adhamthabet01' });
  records.set('whatsappSenderIndex/sender', { projectId: 'P' });
}
(async () => {
  reset();
  assert.equal(testLeadReason({ whatsapp_profile_name: 'Adham' }), null);
  const before = JSON.stringify([...records]);
  let result = await runTestLeadCleanup(db, '__name__');
  assert.equal(result.wouldDelete, 2); assert.equal(result.deleted, 0);
  assert.equal(JSON.stringify([...records]), before);
  result = await runTestLeadCleanup(db, '__name__', { dryRun: false });
  assert.equal(result.deleted, 2); assert.equal(deleted.length, 4);
  assert.ok(records.has('projects/P/clicks/NAME123'));
  assert.ok(records.has('projects/P/clicks/REAL123'));
  assert.ok(records.has('projects/P/clicks/OLD1234'));
  assert.ok(records.has('projects/P/whatsapp_conversations/sender'));
  assert.ok(records.has('whatsappSenderIndex/sender'));
  assert.ok(records.has('other/P/clicks/FAKE123'));
  assert.equal((await runTestLeadCleanup(db, '__name__', { dryRun: false })).deleted, 0);
  reset();
  race = () => records.set('projects/P/clicks/USER123', { used: true, whatsapp_username: 'real_client' });
  assert.equal((await processTestLead(db, ref('projects/P/clicks/USER123'), false)).status, 'no_longer_test');
  assert.equal(deleted.length, 0);
  reset();
  records.set('tokenIndex/USER123', { projectId: 'Other', clickPath: 'projects/Other/clicks/USER123' });
  assert.equal((await processTestLead(db, ref('projects/P/clicks/USER123'), false)).status, 'index_conflict');
  assert.equal(deleted.length, 0);
  records.delete('tokenIndex/USER123');
  assert.equal((await processTestLead(db, ref('projects/P/clicks/USER123'), false)).status, 'deleted');
  assert.equal(deleted.length, 1);
  reset(); records.clear();
  for (let i = 0; i < 2001; i++) records.set('projects/P/clicks/T' + String(i).padStart(5, '0'), { used: true });
  records.set('projects/P/clicks/ZZTEST', { used: true, gclid: '_testproduction_' });
  result = await runTestLeadCleanup(db, '__name__');
  assert.equal(result.morePages, true); assert.equal(result.scanned, 2000);
  result = await runTestLeadCleanup(db, '__name__', { after: result.nextCursor });
  assert.equal(result.morePages, false); assert.equal(result.wouldDelete, 1);
  await assert.rejects(runTestLeadCleanup(db, '__name__', { after: '../bad' }), /Invalid/);

  // Execute the actual existing cleanup endpoint, including the new scope gate.
  let handler;
  const source = fs.readFileSync(path.join(__dirname, '../functions/index.js'), 'utf8');
  const start = source.indexOf('// ---------- Production Firestore Cleanup ----------');
  const end = source.indexOf('// ---------- end production cleanup ----------', start);
  const context = vm.createContext({ db, Date, console: { log() {}, error() {} }, exports: {},
    functions: { https: { onRequest(fn) { handler = fn; return fn; } } },
    admin: { firestore: { FieldPath: { documentId: () => '__name__' } } },
    require: name => { assert.equal(name, './test-lead-cleanup'); return { runTestLeadCleanup }; },
    requireSecret: async (req, res, next) => req.headers['x-export-secret'] === 'offline' ? next() : res.status(401).json({ error: 'Unauthorized' })
  });
  vm.runInContext(source.slice(start, end), context);
  const request = async (query, authorized = false) => {
    const response = { code: 200, status(code) { this.code = code; return this; }, json(body) { this.body = body; return this; } };
    await handler({ query, body: {}, headers: authorized ? { 'x-export-secret': 'offline' } : {} }, response);
    return response;
  };
  reset();
  assert.equal((await request({ scope: 'tests', dryRun: 'false' })).code, 401);
  assert.equal(deleted.length, 0);
  let response = await request({ scope: 'tests' }, true);
  assert.equal(response.body.result.tests.dryRun, true);
  assert.equal(response.body.result.tests.wouldDelete, 2); assert.equal(deleted.length, 0);
  response = await request({ dryRun: 'true' });
  assert.equal(response.body.result.ttlHours, 168);
  assert.equal(response.body.result.wouldDelete, 1);
  response = await request({ scope: 'all', dryRun: 'false' }, true);
  assert.equal(response.body.result.tests.deleted, 2);
  assert.equal(response.body.result.unused.deleted, 1);
  assert.equal((await request({ scope: 'invalid' })).code, 400);
  console.log('PASS: exact test selectors, used tests removed, real names preserved, transaction recheck/index ownership, dry run, pagination, protected HTTP scope, unchanged seven-day default.');
})().catch(error => { console.error(error); process.exitCode = 1; });
