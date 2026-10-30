# Sales Sheet Integration

Source:

```text
Sales Sheet/Sales Sheet App Script.js
```

Backend:

```text
POST https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/exportsApi/sales/quality-update
```

## Purpose

Sales Sheet turns human lead assessment into structured WaTrck funnel state and supplies the exact Qualified/Closed conversion action identity.

## Current columns

1-based indexes:

```text
E / 5 = Token
F / 6 = Date
G / 7 = Quality
H / 8 = Value
I / 9 = Feedback / Message
```

The script reacts only to edits in Quality column `G`.

## Config tab

Quality script's existing first three Config columns:

```text
TabName | QualifiedConv | ClosedConv
```

Code reads:

```text
A = tab name
B = Qualified conversion action
C = Closed conversion action
```

The quality-edit script uses A:C. The separate Lead Import.js now uses D,
headed Contact_Conversion_Name, to route each lead's original Contact name to
the tab in A. This is verified on the copy; moving to the original workbook
requires the cutover guide below. Preserve existing B/C headers and names.

## Installable trigger

Handler:

```text
onEditTrigger
```

It must be an **installable** onEdit trigger.

Helper:

```text
installOnEditTrigger
```

removes existing triggers for the handler and creates a new one.

Do not replace this with a simple trigger without reviewing Apps Script authorization requirements.

## Accepted quality values

Unqualified:

```text
0
unqualified
```

Qualified:

```text
1
qualified
```

Closed:

```text
2
closed
closed sale
```

## Transition rules

Blocked:

```text
Qualified → Unqualified
Closed → Qualified
Closed → Unqualified
```

Allowed:

```text
New → Unqualified
New → Qualified
New → Closed
Qualified → Closed
```

Do not remove the UI block alone if future product logic needs backward movement. The conversion/adjustment semantics would also need a design change.

## Value validation

Unqualified requires an explicit finite, non-negative value in column H.
Enter Value first, then edit Quality in column G to submit. Blank, negative,
and malformed values are rejected in both the Apps Script and backend.
An explicit `0` remains allowed; positive values restate the original Contact
to that value. This source change requires deploying `exportsApi` and saving
the updated Sales Sheet Apps Script before it is live.

Qualified and Closed require a numeric value.

Validation exists both in the Apps Script and backend.

Local controlled test (stubbed Sheets/Firestore, no production writes):

```bat
node "Helping Functions/test-unqualified-value.js"
```

## Conversion-name selection

Qualified uses:

```text
cfg.qualified
```

Closed uses:

```text
cfg.closed
```

The chosen value is sent as payload `conversion_name` and backend stores it as `conversion_name_sales`.

## Typical payload

```json
{
  "token": "ABC1234",
  "quality": "qualified",
  "value": 500,
  "conversion_name": "exact Qualified Google Ads action name"
}
```

Auth:

```text
x-export-secret: EXPORT_SECRET
```

## Backend behavior

Quality mapping:

```text
unqualified → 0
qualified   → 1
closed      → 2
```

Backend updates quality/final value, normal conversion pending state, adjustment pending state, and increments `upload_version`.

## Qualified → Closed preservation

When a previously Qualified lead becomes Closed, backend attempts to preserve the prior Qualified conversion name in:

```text
conversion_name_sales_qualified
```

This keeps the prior stage identity available instead of overwriting all history with Closed.

## UI feedback

During request:

```text
loading...
```

Success:

```text
Lead Published: <quality> : $<value>
```

Error:

```text
ERROR. ...
```

The script attempts to restore the previous quality on failure, reducing sheet/backend divergence.

## Security

Required Script Property:

```text
EXPORT_SECRET
```

Never hard-code or place it in sheet cells/documentation.

## Exact-name requirement

Every configured Sales tab must contain exact Qualified/Closed action strings. A typo can produce a row Google Ads filters out or cause a later adjustment to target the wrong action.

## Automatic lead import — current design (2026-10-30)

This section replaces the former Vision-only timer and separate Sales Web App
instructions. Those approaches are retired. For the verified live state, read
[CURRENT_STATE.md](CURRENT_STATE.md); for moving from the tested copy to the
original workbook, follow [SALES_PRODUCTION_CUTOVER.md](SALES_PRODUCTION_CUTOVER.md).

### Ownership and execution

The existing Cloud Scheduler job `Export_Conversions_To_Sheet` calls the
Conversions Apps Script Web App. Its runner attempts conversion export, then
Sales import, under one ScriptLock. Sales runs even if no conversions are pending.
Ordinary errors are collected and rethrown after both tasks have been attempted;
a forced timeout can still terminate the whole invocation.

| Source file | Installed location | Lifetime |
| --- | --- | --- |
| `Conversions/Conversions.js` | Conversions project's existing Code.gs (editor filename may differ) | Permanent |
| `Sales Sheet/Lead Import.js` | Lead Import.gs in that SAME Conversions project | Permanent |
| `Sales Sheet/Prepare Attribution Columns.js` | Separate file in that same project | Manual preparation only; remove after cutover |
| `Sales Sheet/Sales Sheet App Script.js` | Sales workbook's own bound script | Permanent qualification handler |
| `Sales Sheet/Google Ads Name Lookup.js` | Google Ads Scripts, when resumed | Deferred; do not install in Apps Script |

No additional import timer, Sales Web App or Cloud Scheduler job is needed.
The importer does not define doGet, doPost, or onEdit. Remove any former
`syncVisionLeadsScheduled` timer through the original project's Triggers page;
its setup/cleanup functions and Vision-only wrappers have been removed from source.
The conversions runner's `inspectLastWebExport` is retained as production
diagnostics, not a test-data generator.

### Configuration

In the Conversions project:

| Script Property | Meaning |
| --- | --- |
| `SHEET_FILE_ID` | Existing Conversions workbook; preserve |
| `EXPORT_SECRET` | Existing backend authentication secret; preserve |
| `SALES_SHEET_FILE_ID` | Explicit Sales destination workbook ID; required, must differ from Conversions |
| `SALES_IMPORT_ENABLED` | Exact `true` enables Sales on shared scheduled runs; `false` disables only Sales |
| `SALES_INCLUDE_TEST_USERNAME` | Exact `true` temporarily includes the operator's username; production `false` |

Enable the advanced Google Sheets API service with identifier `Sheets`.
A custom Cloud project may additionally need its Sheets API enabled.

Config A = `TabName`; D = `Contact_Conversion_Name`. B/C retain existing
Qualified/Closed action names. Matching is exact against the click's original
`conversion_name`, not the current Qualified/Closed name. There is no Project ID
column. Duplicate Config contacts/tabs and multiple project IDs sharing one
Contact name cause failure. Missing or unprepared tabs are skipped and reported;
unmapped leads are counted. A successful execution with skipped/unmapped leads
does not prove complete coverage.

A new project needs a unique Config row, a matching backend Contact name and a
prepared native leads Table; it needs no project-specific importer code.

### Read-only backend feed

`GET /sales/leads?limit=200` on exportsApi uses `x-export-secret`.
An optional `project` scopes it; without one it scans all click collections.
Only canonical `projects/<project>/clicks/<token>` documents with `used === true`
are returned. Already-exported conversions remain eligible. The feed does not
change export flags, acknowledge conversions, or write Firestore.

Items contain project_id, contact_conversion_name, token, date, name, phone,
username, quality/value and attribution IDs. All external IDs stay strings.
Missing dates stay null. Quality/value are exposed by the API but deliberately
ignored by the Sales importer.

Default filtering excludes both:
- exact stored `whatsapp_username === "adhamthabet01"`;
- GCLIDs containing `_testproduction_`.

The include-username property requests `include_test_username=1`. The backend
must echo `include_test_username: true`, otherwise the importer rejects the
response. This option does not include synthetic-GCLID records. The API's
separate `testing=1` mode is not used by the Sales importer.

Pagination returns `items`, `scanned`, `next_cursor`, `testing` and
`include_test_username`. Continue even through an empty filtered page.
Global cursors are document paths; project cursors are document IDs. Each
import restarts the scan so older clicks that became confirmed are discovered.
All pages are fetched before writing; a 100-page cap fails before writes.
This is a full scan, not an incremental cursor or a consistent database snapshot.

### Matching and field ownership

`previewSalesLeadImport` performs no Sheet writes.
`importSalesLeadsOnce` performs a manual import regardless of the scheduled
enable flag. Both use the explicitly configured Sales workbook.

The importer indexes Token (E) within each mapped project tab. Duplicate existing
tokens block import, even if the rows look identical. It does not deduplicate by
phone, name or row number. New rows use entirely empty A:M rows inside the native
Table, never the adjacent FILTER output or the sheet-wide last row. Full Tables
must be extended deliberately.

All ready tabs are preflighted before writes. Rows are checked again before
writing, but the ScriptLock does not lock out human editors. Earlier writes may
remain after a later failure; retry uses the token to avoid duplicating them.

| Columns | New row | Existing row |
| --- | --- | --- |
| A Name | Feed name | Fill only when blank, without overwriting formulas |
| B Phone number | Phone first; username only if phone absent/blank | Fill only when blank |
| C Interest / D Notes | Both `Fresh Lead — Review`, pale yellow #fff2cc, black text | Preserve content and formatting |
| E Token | Stable lead token | Preserve |
| F Date | Feed date when present | Fill only when blank |
| G Quality / H USD Value / I SERVER FEEDBACK | Never write | Never write |
| J:M Sources | Pending/unavailable labels and exact ID notes | Fill blank labels/notes only |

Phones retain leading zeros; formula-like incoming text is written literally.
A:F is written together including the token, so a failed attribution write can
be retried against the same row. This importer never deletes Sheet rows. Turning
off test inclusion or deleting tests in Firestore leaves previously imported
Sheet rows alone, as requested by the operator.

The summary's `existingRows` counts matching feed leads, not every populated row
in the workbook. `sheetWrites:true` means an import processed a nonempty plan;
it is not an exact count of changed cells. A preview line for each tab followed
by an import line is the normal preflight/write sequence.

### Native Table layout and formatting

Original layout:
- Leads Table A1:I; filtered Table J7:R; FILTER formula J8; controls K2:K4.

Prepared layout:
- Leads Table A1:M; filtered Table N7:Z; FILTER formula N8; controls O2:O4.
- J = Campaign; K = Ad group / Asset group; L = Ad; M = Campaign type.
- The same four fields appear at W:Z in the filtered view.
- Actual Table row limits are preserved. The tested copy used rows 1117 and 419.

Preparation is run in the Conversions project with `SALES_IMPORT_ENABLED=false`.
Use `previewAttributionLayouts`, then `prepareAidaAttributionLayout` for one
controlled tab, then `prepareRemainingAttributionLayouts` after inspection.

The helper requires exactly the two expected native Tables and headers. It
rejects partial layouts, unexpected FILTER/control references, Table footers,
affected merges and occupied S:V extension space. Table display names with
spaces are matched to underscore names in structured formulas.

A single atomic Sheets batch inserts J:M, corrects the auto-expanded filtered
Table to N:V first, extends the leads Table to A:M, then extends the filtered
Table to N:Z. It preserves Table IDs/names, original column metadata and FILTER
logic; Sheets shifts control references K to O. It verifies ranges, formula,
controls, original non-formula lead values and notes afterward.

The 2026-10-30 formatting update:
- Sets J:M and W:Z to 120 pixels wide.
- Within each Table, including headers, applies dark grey 4 background
  (#434343), white 10-point text, bold=false and italic=false.
- Snapshots and restores I1's explicit cell format; never writes I1's value,
  note, validation or rich text. Other A:I cell formats are outside the style requests.
- Reapplies formatting to already-prepared tabs without inserting columns.
  Preview still labels them `alreadyPrepared`; apply reports `formatted`.
- Does not change row heights or the filter controls' formatting.

The earlier layout migration was visually verified on all six copied tabs.
This newer formatting change has offline coverage but still needs a visual
check on the copy. Confirm I1 and filter results in Google Sheets before
production. Conditional formatting or native rendering cannot be proved by
the local model. A no-match FILTER #N/A is not by itself a migration failure.
If post-apply verification fails, keep imports disabled and inspect; do not
blindly insert columns again.

References: [Sheets native Tables](https://developers.google.com/workspace/sheets/api/guides/tables),
[batch requests](https://developers.google.com/workspace/sheets/api/reference/rest/v4/spreadsheets/request),
[cell formatting](https://developers.google.com/workspace/sheets/api/reference/rest/v4/spreadsheets/cells).

### Attribution lookup and reports: deferred

Exact IDs are already retained in cell notes. Visible values remain
`Name pending` / `Unavailable`, with `Pending lookup` for campaign type.
The importer does not yet read Ads_Lookup. Adding columns alone does not resolve names.

The separate Google Ads script exports Search campaign/ad-group/ad labels into
Ads_Lookup. Account/parent IDs are part of cache keys, and missing entities retain
their last known labels. Responsive Search Ads may use headline text as a label;
it is not the exact combination shown to a lead. Authorization failed during
the trial, so live query acceptance and name-consumption wiring remain unfinished.

PMax tracking is not complete: current landing pages do not capture an asset-group
ID, and the Search lookup does not export PMax entities. The reserved column name
does not imply PMax support is live. Spend, CPL and CPQL reporting is also deferred.

### Tests and growth limits

Local regression tests live under Helping Functions and must not be pasted into
Apps Script: test-sales-leads.js, test-sales-import.js, test-attribution-layout.js,
test-test-lead-cleanup.js, test-ads-name-lookup.js and test-unqualified-value.js.
The obsolete test-sales-layout.js for the retired insertion helper was removed.

The shared job took roughly 10–25 seconds for six tabs and about 43 eligible leads
in the supplied logs. That is a baseline, not a capacity guarantee. Full scans,
Sheet per-row reads and total Table size increase cost/runtime as data grows.
A two-minute schedule was discussed, but no live schedule change was confirmed.
See the cleanup pagination limitation in DEPLOYMENT.md; it is separate from Sales
import pagination.
