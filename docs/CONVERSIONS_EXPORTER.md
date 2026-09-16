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

No new Unqualified conversion is created. RESTATE-to-zero is handled by Adjustments.

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
