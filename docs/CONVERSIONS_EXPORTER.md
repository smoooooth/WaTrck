# Normal Conversions Exporter

Source:

```text
Conversions/Conversions.js
```

## Role

Global normal conversion exporter for all WaTrck projects. It reads pending Firebase state and writes rows into the workbook that feeds Google Ads.

## Architecture declared in current code

Current file explicitly documents:

```text
one Apps Script / one Google Sheet for all projects
Cloud Scheduler invokes Apps Script Web App
Web App doGet/doPost → runOnceFetch → /exports/pending
no Apps Script time-based trigger for this exporter
```

Preserve that model unless intentionally changed.

## Backend

```text
base: https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/exportsApi
pending: /exports/pending
ack: /exports/mark-exported
```

Authentication:

```text
x-export-secret: EXPORT_SECRET
```

## Tabs

```text
Conversions_Sheet
NoGCLID_Review
TESTING_Conversions_Sheet
TESTING_NoGCLID_Review
```

## Current production headers

```text
Project
GCLID
Conversion Name
Conversion Time
Value
Currency
Order ID
Status
Uploaded At
```

Do not silently rename these to older conversational labels.

## No-GCLID review headers

```text
Project ID
Token
Conv. Time
Conv. Value
Source
Reason
Upload Version
Uploaded At
Note
```

## Test separation

Marker:

```text
_testproduction_
```

Real and test conversions are separated into different tabs.

## Dedupe

Production key:

```text
GCLID | Conversion Name
```

Reason: one lead may legitimately have the same GCLID with Contact, Qualified, and Closed.

Testing key:

```text
GCLID | Conversion Name | Order ID
```

This supports repeated synthetic scenarios without suppressing them globally.

## Row building

### Base Contact

Uses `conversion_name` and preserves the initial Contact value.

The normal exporter does not overwrite Contact with later Sales value. Value changes belong to the adjustment pipeline.

### Qualified/Closed

If quality code is `1` or `2` and `conversion_name_sales` exists, the exporter creates a new sales-stage conversion row.

### Unqualified

No new Unqualified conversion is created. Adjustments RESTATE the Contact to
the explicit non-negative Sales value; zero is permitted but not automatic.

## Malformed-data blocking

Exporter intentionally blocks and does not ACK states such as:

```text
missing project
missing Order ID
missing GCLID
backend requests base export but conversion_name missing
Qualified/Closed update missing conversion_name_sales
no valid conversion action
```

Do not make this “more tolerant” by acknowledging malformed data. That would create silent conversion loss.

## Write-before-ACK

Per batch:

```text
build/confirm rows
→ write sheet
→ SpreadsheetApp.flush()
→ ACK backend
```

If write fails, backend stays pending.

If ACK fails after a successful write, dedupe finds the existing row on retry and ACK can be retried without duplicating the conversion.

## ACK grouping

Rows are grouped by project because backend ACK route requires:

```text
?project=<projectId>
```

Body:

```json
{
  "order_ids": ["TOKEN1", "TOKEN2"]
}
```

## ACK retries

Current script retries:

- HTTP 429,
- HTTP 5xx,
- UrlFetch/network exceptions,
- HTTP 200 JSON with `success=false`.

Current constant:

```text
MAX_MARK_RETRIES = 2
```

After retry exhaustion it throws so scheduler execution visibly fails instead of pretending success.

## Script lock

`runOnceFetch()` obtains a script lock with a 30-second acquisition attempt. This prevents overlapping manual/scheduler runs.

Do not remove without redesigning concurrency behavior.

## Fetch limit

Current Apps Script requests:

```text
limit=500
```

Current backend query directly filters used + not-uploaded click documents, avoiding the older starvation pattern of scanning unrelated docs first.

If volume/starvation is revisited, analyze both backend query semantics and this fetch limit together.

## Currency

Current exporter constant:

```text
USD
```

Mixed-currency support would require coordinated review of action configuration, landing values, Sales values, and adjustment rows.

## Cloud Scheduler/Web App

`doGet` and `doPost` call `runOnceFetch()` and return JSON on success. Exceptions propagate, which is desirable for visible failed executions.

## Sales import through the existing job — verified on the copy

Install `Sales Sheet/Lead Import.js` alongside Conversions.js in the SAME
Conversions Apps Script project. It defines no Web App or onEdit entry points.
The existing `Export_Conversions_To_Sheet` Cloud Scheduler job calls this
project's Web App; no additional Sales job or Apps Script timer is needed.

Properties:
- `SHEET_FILE_ID`: Conversions workbook, unchanged.
- `EXPORT_SECRET`: existing backend secret, unchanged.
- `SALES_SHEET_FILE_ID`: explicit, separate Sales destination.
- `SALES_IMPORT_ENABLED=true`: enable the Sales step; false/missing disables it.
- `SALES_INCLUDE_TEST_USERNAME=false`: normal production exclusion; exact true
  temporarily includes adhamthabet01 for an operator simulation.

The advanced Sheets service (`Sheets`) is needed for native Table metadata.
Config D Contact_Conversion_Name maps the original backend Contact name to
Config A TabName. Source columns A:M must be prepared. The importer never writes
Sales G:I, never deletes Sheet rows, and does not acknowledge conversion exports.

`runOnceFetch` holds its existing ScriptLock while attempting processBatch and
then Sales import. Each exception is caught separately so both tasks are attempted
before any collected error is rethrown. Sales runs even when no conversions are
pending. Conversion row building, dedupe, flush-before-ACK and ACK retries are
unchanged. Both tasks share the runtime budget; this does not isolate forced
termination. The Sales step acquires no nested lock; manual Sales runs acquire
the same lock independently.

The operator confirmed all six copied project tabs importing through this job.
The original Sales workbook has not yet been cut over. Follow
[SALES_PRODUCTION_CUTOVER.md](SALES_PRODUCTION_CUTOVER.md) for the exact IDs, file
placement, Config checks, preparation and rollback. Current importer behavior,
pagination limits and deferred lookup work are in [SALES_SHEET.md](SALES_SHEET.md).

## Web App execution diagnostic

doGet/doPost call `runRecordedWebExport_` around `runOnceFetch`. The wrapper
preserves the success JSON and exception propagation. It saves
`LAST_WEB_EXPORT_DIAGNOSTIC` in Script Properties, containing a build marker,
Script ID, method, start/finish timestamps, status, Sales destination, raw Sales
toggle values, conversion fetched count and aggregate Sales counts. It stores no
secret values or lead payloads. Recording failures are caught separately.

Run `inspectLastWebExport` in the Conversions editor to see that record. It is
a last-call diagnostic, not a complete audit trail. A killed invocation may remain
started, and a failure before the wrapper starts leaves no new record. Manual
runOnceFetch does not write it, but manually running doGet/doPost DOES. During a
Scheduler test, do not invoke those manually; match the record's timestamp to
the forced job before concluding the job ran successfully.

This diagnostic was live-verified on 2026-10-30. The supplied 13:01 Cairo result
identified the correct Conversions Script ID and copied Sales destination,
completed in about 16 seconds, and reported 43 existing eligible leads with no
unmapped/skipped tabs. The operator then confirmed a fresh lead arrived
automatically. See CURRENT_STATE.md for the exact recorded IDs/timestamps.

The deployment mismatch lesson: saved editor code and a versioned Web App may
differ. If editor imports work but the job does not, inspect the deployed version,
its /exec URL, job target, project Script ID, properties and diagnostic timestamp.
Do not assume every HTTP 500 has that cause; the earlier isolated failure was
not diagnosed. A successful Scheduler transport response alone does not prove
the expected workbook was written.

Keep these diagnostic functions in permanent production code. Remove temporary
layout/cleanup-preview files after preparation, not this error visibility.
Offline integration check: `node "Helping Functions/test-sales-import.js"`.
