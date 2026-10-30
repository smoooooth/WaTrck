// Offline test: no Google Ads or Google Sheets connections.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');
let source = fs.readFileSync(path.join(__dirname, '../Sales Sheet/Google Ads Name Lookup.js'), 'utf8');
const account = '1234567890';
const campaign = '1234567890123456789';
const group = '2234567890123456789';
const ad = '3234567890123456789';
let output;
let writes = 0;
let cache;
const sheet = {
  getLastRow: () => cache ? cache.length : 0,
  getMaxRows: () => 100,
  getRange(row, column, height, width) {
    return {
      getValues: () => cache,
      setNumberFormat(format) { assert.equal(format, '@'); },
      setValues(rows) { output = JSON.parse(JSON.stringify(rows)); writes++; },
      setFontWeight() {}
    };
  },
  setFrozenRows() {}
};
source = source.replace('PASTE_COPIED_SALES_WORKBOOK_URL_HERE', 'https://docs.google.com/spreadsheets/d/offline-copy/edit');
const context = vm.createContext({
  console: { log() {} },
  AdsApp: {
    currentAccount: () => ({ getCustomerId: () => '123-456-7890' }),
    search(query) {
      const rows = query.includes('FROM ad_group_ad')
        ? [{ campaign: { resourceName: `customers/${account}/campaigns/${campaign}` }, adGroupAd: {
          resourceName: `customers/${account}/adGroupAds/${group}~${ad}`,
          ad: { name: '', responsiveSearchAd: { headlines: [{ text: 'Headline A' }, { text: 'Headline B' }] } }
        } }]
        : query.includes('FROM ad_group ')
          ? [{ campaign: { resourceName: `customers/${account}/campaigns/${campaign}` }, adGroup: {
            resourceName: `customers/${account}/adGroups/${group}`, name: 'Group name'
          } }]
          : [{ campaign: { resourceName: `customers/${account}/campaigns/${campaign}`, name: '=Literal campaign name' } }];
      let index = 0;
      return { hasNext: () => index < rows.length, next: () => rows[index++] };
    }
  },
  SpreadsheetApp: {
    openByUrl: () => ({ getSheetByName: () => sheet }),
    flush() {}
  }
});
vm.runInContext(source, context);
context.main();
assert.equal(output.length, 4);
assert.equal(output[1][2], campaign);
assert.equal(output[1][5], "'=Literal campaign name");
assert.equal(output[2][3], group);
assert.equal(output[3][4], ad);
assert.equal(output[3][5], `Headline A | Headline B [ID: ${ad}]`);
cache = output.map(row => row.slice());
cache.push([account, 'CAMPAIGN', '999', '', '', 'Historical removed campaign', 'Search', '']);
context.main();
assert.equal(output.length, 5);
assert.equal(output.filter(row => row[2] === campaign && row[1] === 'CAMPAIGN').length, 1);
assert.equal(output[4][5], 'Historical removed campaign');
cache = [['Wrong header']];
assert.throws(() => context.main(), /different headers/);
assert.equal(writes, 2);
console.log('PASS: exact string IDs, headline labels, repeat-run dedupe, historical cache preservation, literal names, and incompatible-sheet guard.');
