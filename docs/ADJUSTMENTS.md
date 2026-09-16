# Conversion Adjustments / RESTATE Pipeline

Source:

```text
Adjustments/Adjustments.js
```

Backend routes:

```text
GET  /exports/adjustments-pending
POST /exports/mark-adjustments-exported
```

## Purpose

Modify the value of an already-created conversion using Google Ads adjustment rows.

Current adjustment type:

```text
RESTATE
```

Primary use cases:

- Contact becomes Unqualified → RESTATE Contact to 0.
- Qualified value changes → RESTATE Qualified.
- Closed value changes → RESTATE Closed.

## Internal audit tab

```text
Adjusted_Conversions_Sheet
```

Headers:

```text
Project ID
Google Click ID
Conversion Name
Conversion Time
Adjustment Time
Adjustment Type
Adjustment Value
Adjusted Value Currency
Upload Version
```

This is the WaTrck audit record.

## Google-facing tab

```text
Google_Ads_Adjustments_Upload
```

Headers:

```text
Order ID
Conversion Name
Adjustment Time
Adjustment Type
Adjusted Value
Adjusted Value Currency
```

This is the clean sheet Google Ads scheduled uploads read.

## Datetime format

Current formatter returns:

```text
YYYY-MM-DD HH:MM:SS+0000
```

Current code:

```js
return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}+0000`;
```

Do not revert to another offset syntax without verifying current Google import requirements.

## Global fetch

Current Apps Script calls `/exports/adjustments-pending` with no project filter. Backend returns project identity for each item, then Apps Script groups ACKs by project.

## Eligibility

Apps Script requires a GCLID plus an actual adjustment case: final differs from initial or Sales explicitly has a pending numeric final value.

Backend separately filters for used + `adjustment_pending=true` and only newer upload versions.

## Target conversion selection

### Quality `0` — Unqualified

Target:

```text
base Contact conversion_name
```

### Quality `1` — Qualified

Target:

```text
conversion_name_sales
```

### Quality `2` — Closed

Target:

```text
conversion_name_sales
```

### No sales quality / legacy manual adjustment

Fallback:

```text
base conversion_name
```

## Internal dedupe key

```text
GCLID | Upload Version | Conversion Name
```

This lets retries find the exact original logical adjustment.

## Stable Adjustment Time invariant

If an internal row already exists for the same dedupe key, retry must reuse its exact stored `Adjustment Time`.

Do not generate a fresh timestamp on retry.

A changed timestamp can turn a retry into a distinct Google adjustment identity.

## Google-facing dedupe key

```text
Order ID | Conversion Name | Adjustment Time
```

## Missing Order ID

Order ID is required for safe Google-facing identity and backend ACK.

Current behavior:

- internal audit row may still exist,
- Google-facing row is not safely produced,
- item is not ACKed.

This leaves the issue visible/pending instead of silently losing the adjustment.

## `_testproduction_` isolation

Current marker:

```text
_testproduction_
```

For test GCLIDs:

- internal audit state is created/confirmed,
- real Google-facing tab is skipped,
- synthetic item can still be ACKed as an intentional test path.

Current source contains a historical commented temporary bypass:

```js
//const isTestProduction = false; // TEMPORARY: allow stress-test rows into Google Ads sheet
```

Do not uncomment in production.

## Write order

Production adjustment:

```text
1. internal audit row exists
2. Google-facing row exists
3. SpreadsheetApp.flush()
4. item becomes ACK-eligible
5. backend ACK
```

If write/flush throws, ACK must not occur.

## Backend version gate

Backend exports only when:

```text
upload_version > last_adjustment_version_exported
```

## Backend ACK invariant

Current transaction logic:

```js
const newLastExported = Math.max(
  currentLastExported,
  exportedVersion
);

const stillPending =
  currentUploadVersion > newLastExported;

tx.update(docRef, {
  last_adjustment_version_exported: newLastExported,
  adjustment_pending: stillPending
});
```

This must not be simplified.

## Protected race

```text
v1 fetched
↓
Sales creates v2
↓
v1 sheet write succeeds
↓
v1 ACK arrives
```

Correct result:

```text
last_adjustment_version_exported = 1
upload_version = 2
adjustment_pending = true
```

Then v2 remains exportable.

## Sales update side effects

Current `/sales/quality-update` sets:

```text
sales_sheet_quality_uploaded = false
conversion_value_uploaded = false
adjustment_pending = true
upload_version = current + 1
```

A Sales update can therefore drive both a new normal funnel event and an adjustment. Do not interpret `conversion_value_uploaded=false` as “only Contact pending.”

## Manual queue route

Backend also exposes:

```text
POST /queueAdjustment
```

It transactionally sets final value/source, marks normal and adjustment state pending, and increments upload version.

Do not use it casually against real production leads.

## Google Ads schedule

The Apps Script only fills the sheet. Google Ads reads the Google-facing tab through an external Upload schedule. No Google Ads API call is required.

## Historical stress validation

Recorded run:

```text
20260914163036-VF5C
54 leads
81 expected adjustment rows
54 v1
27 v2
```

See `TESTING.md` for details.
