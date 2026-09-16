# WaTrck Codex Operating Instructions

This repository is the production source code for **WaTrck — Production Tracking System**.

Repository: `smoooooth/WaTrck`  
Permanent branch: `main`

## Mandatory startup procedure

Before changing any code:

1. Confirm the repository is `smoooooth/WaTrck`.
2. Confirm the current branch is `main`.
3. Fetch/pull the latest remote state before editing.
4. Read `docs/00_READ_ME_FIRST.md`, `docs/CURRENT_STATE.md`, `docs/ARCHITECTURE.md`, and `docs/OPERATING_RULES.md`.
5. Read the subsystem-specific document for the files you intend to touch.
6. Inspect the **current code** before proposing or applying any patch.
7. If current code conflicts with these docs, stop and report the conflict. Do not silently choose one.
8. Make the smallest safe change.
9. Run one controlled test.
10. Only continue after the test result is understood.

## Source-of-truth precedence

Use this order:

1. Current code on `main`
2. Current external production configuration explicitly confirmed by the operator
3. These context documents
4. Historical notes / old chat context

Do not overwrite current code merely because a historical note says something different.

## Non-negotiable invariants

### Legacy Google Ads conversions remain intact

Landing pages currently fire direct legacy Google Ads conversions through:

- `LEGACY_GOOGLE_ADS_SEND_TO`
- `LEGACY_LEAD_FORM_GOOGLE_ADS_SEND_TO`

Do **not** remove, rename, suppress, merge, replace, or repurpose those mechanisms unless the operator explicitly requests it.

WaTrck offline conversion tracking is an additional layer.

### Do not rename the intentionally misspelled backend secret

The backend secret name is exactly:

`conversions_exports_secrect`

The spelling is production-significant. Do not “correct” it to `conversions_exports_secret`.

### Preserve adjustment version safety

The adjustment ACK logic must preserve this invariant:

```js
const newLastExported = Math.max(
  currentLastExported,
  exportedVersion
);

const stillPending =
  currentUploadVersion > newLastExported;
```

Never simplify adjustment acknowledgement to `adjustment_pending = false` without comparing versions.

If version `v1` is exported while `v2` is created before `v1` ACKs, `v1` ACK must **not** clear `v2`.

### Write before ACK

For both normal conversions and adjustments:

**Google Sheet state must be safely written/confirmed before backend acknowledgement.**

Do not reverse that order.

### Test-production isolation

The marker `_testproduction_` isolates synthetic/stress-test data from the real Google Ads feed.

Do not remove this isolation casually.

### Project and advertising IDs are strings

Do not coerce GCLIDs, project IDs, campaign IDs, ad-group IDs, ad IDs, WABA IDs, phone IDs, or similar external identifiers to JavaScript numbers unless an API contract explicitly requires it.

### Preserve funnel direction

Allowed Sales transitions are intentionally forward-only:

- New → Unqualified
- New → Qualified
- New → Closed
- Qualified → Closed

Do not add backward transitions without an explicit product decision.

### Do not redesign while fixing

The operator prefers surgical production changes.

Do not refactor unrelated code, rename working fields, reorganize files, replace Apps Script with a different stack, alter Google Ads strategy, or change deployment ownership unless that is the task.

## Deployment ownership matters

### Firebase-managed functions

In `functions/index.js`:

- `saveToken`
- `exportsApi`
- `whatsappWebhook`
- `cleanupFirestoreHttp`

These are managed with Firebase deployment.

### Legacy gcloud-managed function

Also present in `functions/index.js`:

- `exportConversionsToSheet`

This has historically been managed with `gcloud functions deploy`.

Do not assume `firebase deploy` is the intended owner of this legacy function.

### Google Apps Script

These files are source copies of scripts deployed manually into Google Apps Script projects:

- `Conversions/Conversions.js`
- `Adjustments/Adjustments.js`
- `Sales Sheet/Sales Sheet App Script.js`

Editing the repository does not automatically deploy those scripts into Google.

## Required reading by subsystem

If touching `functions/index.js`, read:

- `docs/ARCHITECTURE.md`
- `docs/DATA_MODEL.md`
- `docs/WHATSAPP_WEBHOOK.md`
- `docs/ADJUSTMENTS.md`
- `docs/SECURITY_AND_SECRETS.md`
- `docs/DEPLOYMENT.md`

If touching `Conversions/Conversions.js`, read:

- `docs/CONVERSIONS_EXPORTER.md`
- `docs/GOOGLE_ADS.md`
- `docs/ADJUSTMENTS.md`

If touching `Adjustments/Adjustments.js`, read:

- `docs/ADJUSTMENTS.md`
- `docs/GOOGLE_ADS.md`
- `docs/TESTING.md`

If touching `Sales Sheet/Sales Sheet App Script.js`, read:

- `docs/SALES_SHEET.md`
- `docs/ADJUSTMENTS.md`
- `docs/DATA_MODEL.md`

If touching landing-page files, read:

- `docs/ARCHITECTURE.md`
- `docs/GOOGLE_ADS.md`
- `docs/WHATSAPP_WEBHOOK.md`
- `docs/CURRENT_STATE.md`

## Expected working style

For every requested change:

1. State the exact current behavior.
2. State the minimum proposed change.
3. Identify production mechanisms that remain untouched.
4. Patch only the required lines.
5. Show the diff.
6. Run or describe one controlled test.
7. Report the result.
8. Stop before unrelated follow-up work unless requested.

## Do not fabricate external configuration

Some production configuration is outside GitHub:

- Google Ads conversion actions and goal settings
- Google Ads Data Manager mappings/filters
- Google Ads Upload schedules
- Google Sheets contents
- Google Apps Script deployments and Script Properties
- Cloud Scheduler jobs
- Meta App / WhatsApp Manager configuration
- Chatwoot configuration
- Secret Manager values

If a task depends on external configuration, inspect the current value through an available connected tool or ask for that exact value instead of guessing.

## Important current-state warning

At the snapshot used to create these docs:

- Firebase `whatsappWebhook` forwards Meta webhook payloads to Chatwoot using a URL containing `+971586992542`.
- The checked landing-page templates still contain WhatsApp destination number `+201034285454`.

Do not silently make those numbers match. Read `docs/KNOWN_DISCREPANCIES.md` and confirm intended production state.

## Communication preference

The operator wants:

- concise progress updates,
- explicit Windows CMD commands when commands are required,
- exact paths,
- exact UI navigation,
- one controlled step at a time,
- no unnecessary redesign,
- high accuracy over speed.
