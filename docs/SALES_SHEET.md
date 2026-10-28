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

Current expected layout:

```text
TabName | QualifiedConv | ClosedConv
```

Code reads:

```text
A = tab name
B = Qualified conversion action
C = Closed conversion action
```

There is no current active code path for a fourth Contact conversion column. If one exists in the sheet, it is informational unless code is deliberately extended.

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

## Attribution layout preparation (2026-10-28)

The operator confirmed that a second, filtered table starts at J. Do not
overwrite J:M with attribution values before moving that table.

`Sales Sheet/Prepare Attribution Columns.js` is an additive, manually run
helper. It is not the importer and does not install triggers or call Firebase.
It checks the active tab against Config and the supplied A1:I1 headers, then
inserts four columns before J:

```text
J = Campaign
K = Ad group / Asset group
L = Ad
M = Campaign type
N onward = existing filter table
```

First controlled live check, using a disposable workbook copy:

1. In the copy, open Extensions → Apps Script → + → Script. Add the helper
   as a separate script file; preserve the existing Sales script.
2. Select one configured project tab in the workbook, then run
   `prepareAttributionColumnsForActiveTab` from the editor.
3. Confirm A:I is unchanged and the filter controls previously at K2:K4 are
   now at O2:O4. In the supplied sample, output J8 becomes N8.
4. Inspect the moved FILTER formula. Its three control references must now
   use O2, O3 and O4; structured references to `Project_Leads_2` must remain.
   Google Sheets formula rewriting and Table range behavior require live
   verification; the local stub test cannot prove them.
5. Select filter values matching a dated source row and verify the output.
   The supplied CSV has year 2025 selected and a lead with an empty Date;
   `#N/A` alone does not establish a formula defect.
6. Run the helper again: it must report that columns are already prepared
   and must not shift the filter table again.

The helper does not extend the named Google Table or rewrite the FILTER
logic. Verify those separately before enabling attribution import or
expecting attribution fields in the filtered output. The filtered results
remain a view; qualification edits belong in the original Sales table.

Local controlled check:

```bat
cd /d "D:\Real Estate Projects\Advanced Tracking System\New\WaTrck"
node "Helping Functions/test-sales-layout.js"
```

This checks preservation, column movement, repeat-run behavior and guards
with synthetic cells. It neither edits a real workbook nor tests Google's
spreadsheet engine. Automatic import, name lookup and PMax asset-group capture
are not implemented by this preparation step.
