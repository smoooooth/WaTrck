// Run in Google Ads > Tools > Bulk actions > Scripts (NOT Sheets Apps Script).
// Replace the URL below with the COPIED Sales workbook URL before running.
// Reads Ads data only. Writes only the Ads_Lookup worksheet in that workbook.
const WATRCK_LOOKUP_WORKBOOK_URL = 'PASTE_COPIED_SALES_WORKBOOK_URL_HERE';

function main() {
  if (!/^https:\/\/docs\.google\.com\/spreadsheets\/d\/[A-Za-z0-9_-]+/.test(WATRCK_LOOKUP_WORKBOOK_URL)) {
    throw new Error('Set WATRCK_LOOKUP_WORKBOOK_URL to the copied Sales workbook URL.');
  }
  const account = String(AdsApp.currentAccount().getCustomerId()).replace(/-/g, '');
  const headers = ['Account ID', 'Entity', 'Campaign ID', 'Ad group ID',
    'Ad ID', 'Name', 'Campaign type', 'Updated At'];
  const fresh = [];
  const counts = { campaigns: 0, adGroups: 0, ads: 0 };
  const now = new Date();
  // Resource-name components are strings. Never convert Google IDs to Numbers.
  function parse(resource, kind) {
    const match = String(resource || '').match(/^customers\/(\d+)\/(campaigns|adGroups|adGroupAds)\/(\d+)(?:~(\d+))?$/);
    if (!match || match[1] !== account || match[2] !== kind ||
        (kind === 'adGroupAds' && !match[4])) {
      throw new Error('Unexpected Ads resource name. No lookup write performed.');
    }
    return match;
  }
  function each(query, callback) {
    const rows = AdsApp.search(query);
    while (rows.hasNext()) callback(rows.next());
  }
  each("SELECT campaign.resource_name, campaign.name FROM campaign " +
    "WHERE campaign.advertising_channel_type = 'SEARCH'", row => {
    const campaign = parse(row.campaign.resourceName, 'campaigns')[3];
    fresh.push([account, 'CAMPAIGN', campaign, '', '', row.campaign.name, 'Search', now]);
    counts.campaigns++;
  });
  each("SELECT campaign.resource_name, ad_group.resource_name, ad_group.name FROM ad_group " +
    "WHERE campaign.advertising_channel_type = 'SEARCH'", row => {
    const campaign = parse(row.campaign.resourceName, 'campaigns')[3];
    const group = parse(row.adGroup.resourceName, 'adGroups')[3];
    fresh.push([account, 'AD_GROUP', campaign, group, '', row.adGroup.name, 'Search', now]);
    counts.adGroups++;
  });
  each("SELECT campaign.resource_name, ad_group_ad.resource_name, ad_group_ad.ad.name, " +
    "ad_group_ad.ad.responsive_search_ad.headlines FROM ad_group_ad " +
    "WHERE campaign.advertising_channel_type = 'SEARCH'", row => {
    const campaign = parse(row.campaign.resourceName, 'campaigns')[3];
    const resource = parse(row.adGroupAd.resourceName, 'adGroupAds');
    const group = resource[3];
    const ad = resource[4];
    const creative = row.adGroupAd.ad;
    const rsa = creative.responsiveSearchAd;
    const headlines = rsa && rsa.headlines
      ? rsa.headlines.map(asset => asset.text).filter(Boolean).slice(0, 3).join(' | ') : '';
    // This is a readable label for the ad, not a claim about the exact
    // headline combination shown to the individual lead.
    const label = String(creative.name || headlines || 'Search ad') + ' [ID: ' + ad + ']';
    fresh.push([account, 'AD', campaign, group, ad, label, 'Search', now]);
    counts.ads++;
  });

  // Finish all Ads reads before touching the lookup. Keep older cached names
  // if Google stops returning a removed entity; refresh renamed entities.
  const workbook = SpreadsheetApp.openByUrl(WATRCK_LOOKUP_WORKBOOK_URL);
  let sheet = workbook.getSheetByName('Ads_Lookup');
  const cached = new Map();
  const key = row => row.slice(0, 5).map(String).join('|');
  if (sheet && sheet.getLastRow() > 0) {
    const existing = sheet.getRange(1, 1, sheet.getLastRow(), 8).getValues();
    if (!headers.every((value, index) => existing[0][index] === value)) {
      throw new Error('Ads_Lookup already exists with different headers. No lookup write performed.');
    }
    existing.slice(1).forEach(row => {
      if (row.every(value => value === '')) return;
      if (!row.slice(0, 7).every(value => typeof value === 'string') || !row[0]) {
        throw new Error('Existing lookup IDs must be text. No lookup write performed.');
      }
      if (cached.has(key(row))) throw new Error('Duplicate lookup identity.');
      cached.set(key(row), row);
    });
  }
  fresh.forEach(row => cached.set(key(row), row));
  const output = [headers, ...cached.values()].map(row => row.map(value =>
    typeof value === 'string' && value.startsWith('=') ? "'" + value : value));
  if (!sheet) sheet = workbook.insertSheet('Ads_Lookup');
  if (sheet.getMaxRows() < output.length) {
    sheet.insertRowsAfter(sheet.getMaxRows(), output.length - sheet.getMaxRows());
  }
  sheet.getRange(1, 1, output.length, 7).setNumberFormat('@');
  sheet.getRange(1, 1, output.length, 8).setValues(output);
  sheet.getRange(1, 1, 1, 8).setFontWeight('bold');
  sheet.setFrozenRows(1);
  SpreadsheetApp.flush();
  console.log(JSON.stringify({ account, ...counts, cachedEntities: cached.size,
    lookupTab: 'Ads_Lookup', scope: 'Search only' }));
}
