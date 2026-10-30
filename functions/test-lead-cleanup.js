'use strict';

// Operator-authorized test identities only. Never match profile/display names.
function testLeadReason(data) {
  if (data.whatsapp_username === 'adhamthabet01') return 'test_username';
  if (typeof data.gclid === 'string' && data.gclid.includes('_testproduction_')) return 'test_gclid';
  return null;
}

function isClickPath(path) {
  return /^projects\/[^/]+\/clicks\/[^/]+$/.test(path);
}

async function processTestLead(db, ref, dryRun) {
  const check = async (get, remove) => {
    const snap = await get(ref);
    if (!snap.exists) return { status: 'missing' };
    const reason = testLeadReason(snap.data());
    if (!reason || !isClickPath(ref.path)) return { status: 'no_longer_test' };
    const indexRef = db.doc('tokenIndex/' + ref.id);
    const index = await get(indexRef);
    if (index.exists) {
      const data = index.data();
      const matches = data.clickPath ? data.clickPath === ref.path : data.projectId === ref.path.split('/')[1];
      if (!matches) return { status: 'index_conflict', path: ref.path, reason };
    }
    if (!dryRun) {
      remove(ref);
      if (index.exists) remove(indexRef);
    }
    return { status: dryRun ? 'would_delete' : 'deleted', path: ref.path, reason, tokenIndex: index.exists };
  };
  if (dryRun) return check(ref => ref.get(), () => { throw new Error('Dry run cannot delete'); });
  return db.runTransaction(tx => check(ref => tx.get(ref), ref => tx.delete(ref)));
}

async function runTestLeadCleanup(db, documentId, options = {}) {
  const dryRun = options.dryRun !== false;
  let after = options.after;
  if (after !== undefined && (typeof after !== 'string' || after.split('/').length % 2 !== 0 ||
      after.split('/').slice(-2)[0] !== 'clicks' || after.split('/').some(s => !s || s === '.' || s === '..'))) {
    throw new Error('Invalid test cleanup cursor');
  }
  const summary = { dryRun, scanned: 0, matched: 0, deleted: 0, wouldDelete: 0,
    noLongerTest: 0, missing: 0, indexConflicts: 0, errors: 0, samples: [], nextCursor: null, morePages: false };
  // Bound each invocation. Continue with nextCursor until morePages=false.
  for (let page = 0; page < 10; page++) {
    let query = db.collectionGroup('clicks').orderBy(documentId).limit(200);
    if (after !== undefined) query = query.startAfter(db.doc(after));
    const snapshot = await query.get();
    summary.scanned += snapshot.size;
    const candidates = snapshot.docs.filter(doc => isClickPath(doc.ref.path) && testLeadReason(doc.data()));
    summary.matched += candidates.length;
    for (let offset = 0; offset < candidates.length; offset += 10) {
      const results = await Promise.all(candidates.slice(offset, offset + 10).map(async doc => {
        try { return await processTestLead(db, doc.ref, dryRun); }
        catch (_) { return { status: 'error', path: doc.ref.path }; }
      }));
      for (const result of results) {
        const key = { deleted: 'deleted', would_delete: 'wouldDelete', no_longer_test: 'noLongerTest',
          missing: 'missing', index_conflict: 'indexConflicts', error: 'errors' }[result.status];
        summary[key]++;
        if (summary.samples.length < 100 && result.path) summary.samples.push(result);
      }
    }
    if (snapshot.size < 200) return summary;
    after = snapshot.docs[snapshot.docs.length - 1].ref.path;
  }
  summary.nextCursor = after;
  summary.morePages = true;
  return summary;
}

module.exports = { testLeadReason, processTestLead, runTestLeadCleanup };
