# Known Discrepancies and External State to Confirm

This file intentionally records things Codex must **not auto-fix**.

A discrepancy can represent a controlled migration, stale deployment, or configuration that lives outside GitHub.

## 1. WhatsApp destination number versus Chatwoot bridge

Verified current backend source contains:

```text
CHATWOOT_WEBHOOK_URL
→ https://wa-chat.seaborn-properties.com/webhooks/whatsapp/+971586992542
```

Verified inspected landing-page templates still contain:

```text
data-wa-phone / DEFAULT_PHONE
→ +201034285454
```

Possible reasons include:

- real-number migration is in progress,
- backend/Chatwoot was updated before landing pages,
- +20 remains a deliberate test destination,
- live Webflow content differs from GitHub snippets.

Required action before changing:

**Confirm which WhatsApp number should receive current production landing-page traffic.**

Do not silently make the two numbers match.

## 2. Qualified and Closed action names

The exact live Qualified/Closed conversion-action names live in the Sales Sheet `Config` tab.

GitHub does not contain their current values.

Do not derive them automatically from Contact names.

Required verification:

```text
Sales Sheet → Config
```

## 3. Google Ads Primary/Secondary and goal inclusion

Conversion actions were created operationally, but GitHub cannot prove current:

- Primary/Secondary status,
- account-default goal inclusion,
- campaign custom-goal inclusion,
- bidding usage.

Before changing campaign optimization, inspect the live Google Ads account.

## 4. Google Ads Data Manager mapping/filter state

Operationally the new actions were linked to the shared source and filtered.

GitHub cannot prove the live configuration remains unchanged.

When troubleshooting action routing, inspect:

```text
source
field mapping
Conversion Name filter
```

## 5. Google Ads adjustment upload schedule

The Google-facing adjustment tab was connected through Uploads/Schedules.

GitHub cannot prove current:

- workbook/source,
- schedule time,
- timezone,
- last run,
- diagnostics.

Inspect Google Ads before changing it.

## 6. Apps Script deployed versions

Repository files are source copies.

GitHub does not prove that deployed Apps Script projects exactly match `main`.

Before diagnosing behavior that contradicts source:

1. open Apps Script,
2. compare code,
3. inspect deployment version,
4. inspect execution logs.

## 7. Adjustment scheduling strategy

Current `Adjustments.js` contains both:

```text
5-minute Apps Script trigger installer
```

and:

```text
Web App doGet/doPost scheduler entry point
```

Historical architecture also uses Cloud Scheduler.

Actual live triggers/jobs must be checked to avoid duplicate execution.

## 8. Legacy `exportConversionsToSheet`

This function remains in `functions/index.js` with gcloud deployment notes.

The modern global Apps Script conversion exporter also exists.

Do not assume the legacy function is unused or safe to remove.

Verify:

- deployed Cloud Function,
- Cloud Scheduler,
- current workbook flow.

## 9. Conversion sheet header wording

Older notes sometimes referred to columns as:

```text
Project ID
Google Click ID
Conversion Value
Conversion Currency
```

Current `Conversions/Conversions.js` declares:

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

Use the current code and actual workbook when exact labels matter.

## 10. Stress-test per-project stage counts

Current committed `adjustments-stress-last-run.json` records per project:

```text
contact: 3
qualified: 12
closed: 12
total: 27
```

Use that artifact if an older conversation summary gives a different breakdown.

## 11. Timezone

Google/Apps Script work has involved Dubai timezone context; the operator's ChatGPT timezone can differ.

Scheduling timezone is external configuration.

Do not assume Cairo or Dubai when a scheduler time matters.

Inspect the actual job/account timezone.

## 12. `WATRCK_DEBUG`

Inspected landing-page templates currently have:

```js
WATRCK_DEBUG = true;
```

This may be intentional during staging/migration.

Do not turn it off as unrelated cleanup.

## 13. Branch protection

At the context snapshot, GitHub reported `main` as unprotected.

That is repository administration state, not application logic.

Do not change branch-protection rules without explicit approval.

## 14. Live website versus GitHub templates

The repository contains landing-page snippets/templates, but live Webflow/site publication is an external deployment step.

If current live behavior differs from GitHub:

- inspect live page source,
- identify which template/version was published,
- do not assume GitHub automatically deploys it.

## 15. Adjustment workbook historical fake rows

During controlled stress testing, there was a temporary period where test isolation was bypassed so fake rows could be validated in the Google-facing sheet.

Current code has isolation restored.

GitHub cannot prove whether old fake rows were manually removed from the live workbook.

Before treating the adjustment sheet as clean production input, verify its rows.
