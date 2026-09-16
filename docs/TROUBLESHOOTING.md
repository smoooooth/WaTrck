# Troubleshooting Guide

Use this guide symptom-first. Diagnose before editing.

## 1. WhatsApp click exists but no WaTrck Contact conversion

Check in order:

```text
1. Did the landing page create the click doc?
2. Did tokenIndex/<TOKEN> get created?
3. Did the WhatsApp message include Ref: #TOKEN?
4. Did Meta POST to Firebase whatsappWebhook?
5. Did webhook parse a message event?
6. Did the token resolve?
7. Did the click become used=true?
8. Is conversion_value_uploaded=false?
9. Does /exports/pending return it?
10. Did Conversions Apps Script write it?
11. Did backend ACK succeed?
12. Did Google Ads source import run and accept it?
```

Do not start by changing Google Ads if `used=true` was never reached.

## 2. Message appears in Chatwoot but WaTrck did nothing

First architectural check:

```text
Is Meta's WhatsApp callback pointing directly to Chatwoot instead of Firebase?
```

Correct WaTrck route:

```text
Meta → Firebase whatsappWebhook → Chatwoot
```

Also check:

- Firebase function logs,
- Meta app/WABA `messages` webhook configuration,
- the message contains the tracking token,
- `tokenIndex/<TOKEN>` exists.

## 3. WaTrck works but Chatwoot misses messages

Check:

- current `CHATWOOT_WEBHOOK_URL`,
- current Chatwoot inbox/phone-number webhook path,
- Firebase logs for:

```text
Chatwoot Forward Error (Ignored)
```

- signature/header expectations,
- Chatwoot/WABA onboarding state.

Remember: Chatwoot forwarding is intentionally non-blocking, so WaTrck can succeed while Chatwoot forwarding fails.

## 4. Conversation is saved but no conversion is created

This can be correct.

A follow-up message without a token can be stored using:

```text
whatsappSenderIndex
```

but sender identity alone should not activate a new click conversion.

## 5. Token is in the message but click does not become used

Check:

```text
token normalization
exact tokenIndex document ID
indexData.projectId
click document path
```

The expected path is:

```text
projects/<projectId>/clicks/<TOKEN>
```

Look for per-message processing errors rather than only the outer webhook HTTP result; the webhook catches individual message errors so one message failure does not necessarily produce a fatal endpoint failure.

## 6. Pending conversion repeatedly returns

Check:

- sheet row was actually written,
- `SpreadsheetApp.flush()` completed,
- `/exports/mark-exported` response,
- correct project was used in ACK,
- `EXPORT_SECRET` matches,
- exporter deliberately blocked malformed data.

A repeated pending item can be a protection mechanism, not necessarily a loop bug.

## 7. Duplicate normal conversion row

Production dedupe must be:

```text
GCLID | Conversion Name
```

Do not switch to GCLID-only dedupe because one GCLID can legitimately have:

```text
Contact
Qualified
Closed
```

Check whether:

- action name changed between retries,
- rows were manually edited,
- duplicate source workbooks/schedulers exist,
- Apps Script is running twice from separate triggers.

## 8. Qualified or Closed row is missing

Check in order:

1. Sales Sheet Config has the exact action name.
2. Installable `onEditTrigger` exists.
3. Sales value is numeric.
4. Sales endpoint returned 200.
5. Firestore has `conversion_name_sales`.
6. `sales_sheet_quality_uploaded` is false before export.
7. `/exports/pending` returns the item.
8. Conversions exporter logs do not show a blocked item.
9. Existing dedupe row is not already satisfying the conversion.

## 9. Sales Sheet says ERROR and restores quality

This is intentional failure rollback.

Common reasons:

```text
No token
Invalid quality input
Missing value for Qualified/Closed
Backward quality transition
EXPORT_SECRET mismatch
Token index not found
Click document not found
Backend 4xx/5xx
```

Read column I feedback before changing code.

## 10. Adjustment stays pending forever

Check:

```text
gclid exists
conversion_value_final is numeric
adjustment_pending=true
upload_version > last_adjustment_version_exported
correct target conversion name exists
order_id exists
internal row writes
Google-facing row writes for real GCLID
SpreadsheetApp.flush succeeds
mark-adjustments-exported returns 200
```

## 11. Adjustment duplicates on retry

Inspect whether retry still reuses the stored:

```text
Adjustment Time
```

from internal key:

```text
GCLID | Upload Version | Conversion Name
```

If code generates a fresh time on every retry, stop before running production again.

## 12. Newer adjustment disappeared after older ACK

Inspect:

```text
upload_version
last_adjustment_version_exported
adjustment_pending
```

Correct logic must compute:

```text
newLastExported = max(currentLastExported, exportedVersion)
stillPending = currentUploadVersion > newLastExported
```

If pending was cleared unconditionally, restore the version-aware transaction.

## 13. Fake stress rows appear in Google-facing adjustment sheet

Check current code still uses:

```js
const isTestProduction =
  gclid.toLowerCase().indexOf(TEST_GCLID_MARKER) !== -1;
```

and that the historical temporary bypass remains commented out.

If fake rows exist:

1. confirm they are synthetic `_testproduction_` rows,
2. restore isolation first,
3. then remove only confirmed fake rows from the Google-facing tab,
4. preserve useful internal audit evidence unless there is a reason to delete it.

## 14. Google Ads conversion routes to the wrong action

Trace exact name:

```text
landing/Sales conversion name
→ Firestore
→ Conversions_Sheet
→ Data Manager filter
→ Google Ads action
```

A `Project ID` filter alone does not separate Contact/Qualified/Closed within one project.

## 15. Google Ads action seems to ingest all shared-sheet rows

Inspect that action's source filter.

Expected:

```text
Conversion Name
Equals
<exact action name>
```

Do not assume same-name auto-routing.

## 16. Google Ads adjustment is rejected

Verify:

- original conversion has already been imported,
- Order ID matches original conversion identity,
- Conversion Name matches correct existing action,
- Adjustment Time format is valid,
- `Adjustment Type` is `RESTATE`,
- adjusted value is valid,
- currency is correct,
- Google Ads upload diagnostics.

Synthetic GCLIDs cannot validate real Google acceptance.

## 17. No adjustment row even though Sales updated lead

Check both backend and exporter semantics.

Sales update should set:

```text
adjustment_pending=true
upload_version++
```

Then backend pending endpoint must consider the item newer than `last_adjustment_version_exported`.

Apps Script must then see a usable numeric final value and GCLID.

## 18. Cleanup deleted something unexpected

First compare deployed cleanup revision with `main`.

Current source is designed to delete only:

```text
used === false
AND
age >= 24h
```

and transactionally re-check immediately before delete.

Used conversions should not even be returned by the cleanup query.

## 19. Cleanup leaves old unused clicks

Check:

- `used` is explicitly boolean `false`, not missing/string,
- timestamp can be parsed,
- document is actually older than 24h,
- cleanup scheduler invokes the correct endpoint,
- production `dryRun` override is not true.

Current safety policy deliberately skips records whose timestamp cannot be established.

## 20. Landing page opens the wrong phone

Check both:

```text
data-wa-phone
DEFAULT_PHONE
```

Then check the actual published Webflow/site source, not only GitHub.

Current context snapshot has a deliberate known discrepancy:

```text
Firebase → Chatwoot bridge: +971586992542
landing-page templates:     +201034285454
```

Read `KNOWN_DISCREPANCIES.md` before changing either.

## 21. Landing page sends wrong Contact action name

Inspect:

```text
WHATSAPP_CONVERSION_NAME
```

for the exact project template and published page.

Then inspect Google Ads action/filter exact string.

Do not bulk-rewrite historical Firestore rows.

## 22. Firebase CORS error from landing page

Current `saveToken` allowed origins include:

```text
https://www.seaborn-properties.com
https://fe95b27ee703b3b6a78fb912df565.webflow.io
```

If production/staging domain changed, update CORS deliberately and test the intended origins.

Do not use `*` as a casual fix without a security decision.

## 23. 401 from exports API

Check:

```text
Secret Manager: conversions_exports_secrect
Apps Script Script Property: EXPORT_SECRET
Firebase/GCP project identity
```

Do not rename the secret during troubleshooting.

## 24. Exporters seem to run twice

Inspect both:

```text
Apps Script triggers
Cloud Scheduler jobs
```

Current normal conversions architecture expects Cloud Scheduler Web App invocation and no Apps Script time trigger.

Current adjustments source contains both a trigger installer and Web App entry point, so live configuration must be inspected.

## 25. Code and live behavior disagree

Remember GitHub source and deployed Apps Script/Firebase revisions can drift.

Before changing code to fix an apparent contradiction:

1. identify deployed revision,
2. compare with `main`,
3. inspect execution logs,
4. then decide whether source or deployment is stale.
