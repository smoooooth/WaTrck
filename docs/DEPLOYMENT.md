# Deployment and Operations Runbook

This document describes how to deploy WaTrck components without accidentally deploying unrelated mechanisms.

Always identify **which component owns the deployment** before running commands.

For the 2026-10-30 Sales import migration from the tested copy to the original
workbook, follow [SALES_PRODUCTION_CUTOVER.md](SALES_PRODUCTION_CUTOVER.md).
That guide gives the exact destinations and minimum Apps Script file list.

---

# Part A — Git / GitHub Desktop

## 1. Permanent branch

Use:

```text
main
```

Do not recreate `New-payload-fixes` as the normal working branch.

## 2. Before editing

In GitHub Desktop:

```text
Repository: WaTrck
Current Branch: main
Fetch origin
```

Confirm:

- repository is `smoooooth/WaTrck`,
- branch is `main`,
- no unrelated uncommitted production-code changes exist.

## 3. Local repository path

Historical operator path:

```text
D:\Real Estate Projects\Advanced Tracking System\New\WaTrck
```

If GitHub Desktop shows a different path, use:

```text
Repository → Show in Explorer
```

and treat that actual folder as authoritative.

## 4. Commit style

Prefer one logical change per commit.

Examples:

```text
Fix adjustment ACK version handling
Update production WhatsApp bridge URL
Update AIDA WaTrck Contact action name
```

Avoid combining unrelated backend, landing-page, Apps Script, and documentation changes in one commit unless one coordinated migration truly requires them.

---

# Part B — Firebase-managed Functions

Current Firebase default project from `.firebaserc`:

```text
aida-muscat-wa-tracking
```

Current region used by endpoints:

```text
us-central1
```

Firebase-managed functions documented in `functions/index.js`:

```text
saveToken
exportsApi
whatsappWebhook
cleanupFirestoreHttp
```

## 1. Fresh Windows CMD

Go to repository root:

```bat
cd /d "D:\Real Estate Projects\Advanced Tracking System\New\WaTrck"
```

Check Firebase CLI project:

```bat
firebase use
```

Expected project:

```text
aida-muscat-wa-tracking
```

If Firebase CLI is not authenticated under the intended authorized Google account, fix authentication before deployment.

## 2. Dependencies

Functions folder:

```bat
cd /d "D:\Real Estate Projects\Advanced Tracking System\New\WaTrck\functions"
```

If dependencies need reinstalling:

```bat
npm install
```

Runtime declared by project:

```text
Node 22
```

## 3. Deploy

From repository root:

```bat
cd /d "D:\Real Estate Projects\Advanced Tracking System\New\WaTrck"
```

General deployment command:

```bat
firebase deploy --only functions
```

However, prefer the narrowest supported function-target deployment when only one Firebase-managed function changed.

Before a broad deployment, remember `functions/index.js` also contains the legacy `exportConversionsToSheet` function with different historical ownership. Verify deployment behavior before unintentionally changing its deployed state.

## 4. Post-deploy

Run one test specific to the changed function.

Examples:

- `saveToken`: one synthetic click POST.
- `whatsappWebhook`: verification GET or one controlled WhatsApp token message.
- `exportsApi`: one pending/ACK test with controlled data.
- `cleanupFirestoreHttp`: dry-run or controlled expired unused click test when available.

---

# Part C — Legacy `exportConversionsToSheet`

`functions/index.js` contains historical deployment notes for:

```text
exportConversionsToSheet
```

The source comment says it has been managed by `gcloud functions deploy`, not the normal Firebase path.

The historical code includes a Gen 2 deployment command using:

```text
region: us-central1
runtime: nodejs22
```

plus sheet environment variables and a service account.

Treat those details as historical operational documentation, not an instruction to redeploy every time `functions/index.js` changes.

Before touching this function:

1. Check whether a deployed Cloud Function still exists.
2. Check its current source revision.
3. Check Cloud Scheduler jobs targeting it.
4. Check its current env vars.
5. Check service account.
6. Confirm whether it remains needed alongside the modern global Apps Script exporter.

Do not delete it simply because newer conversion-export code exists.

---

# Part D — Normal Conversions Apps Script

Repository source:

```text
Conversions/Conversions.js
```

Current architecture in the file says:

```text
one global workbook across all projects
Cloud Scheduler → Apps Script Web App
doGet/doPost → runOnceFetch → /exports/pending
```

and explicitly says not to create Apps Script time-based triggers for this exporter.

## Update process

1. Open the **global conversions Google Sheet**.
2. Go to:

```text
Extensions → Apps Script
```

3. Confirm this is the Apps Script project attached to the conversions workbook.
4. Compare deployed code with `main` before replacing anything.
5. Copy the required changes from:

```text
Conversions/Conversions.js
```

6. Save.
7. If production uses its Web App deployment, update the **existing** Web App deployment:

```text
Deploy
→ Manage deployments
→ select existing deployment
→ Edit
→ Version: New version
→ Deploy
```

8. Preserve the existing `/exec` URL unless a deliberate migration requires changing it.
9. Verify Script Properties:

```text
EXPORT_SECRET
SHEET_FILE_ID
```

10. Do **not** create an Apps Script time trigger for the normal exporter unless architecture is intentionally changed.
11. Run one manual controlled export if safe.
12. Verify the external Cloud Scheduler still calls the same Web App URL.

---

# Part E — Adjustments Apps Script

Repository source:

```text
Adjustments/Adjustments.js
```

## Update process

1. Open the dedicated adjustments Google Sheet workbook.
2. Go to:

```text
Extensions → Apps Script
```

3. Confirm this is the adjustments script, not the normal conversions script.
4. Compare current deployed script with GitHub `main`.
5. Update only the required code.
6. Save.
7. If a Web App deployment is used, update the existing deployment:

```text
Deploy
→ Manage deployments
→ Edit
→ New version
→ Deploy
```

8. Keep the existing `/exec` URL unless infrastructure is intentionally migrated.
9. Verify Script Properties, especially:

```text
EXPORT_SECRET
SHEET_FILE_ID
```

and any runtime `PROJECT_ID` / `CONVERSION_NAME` properties in use.

## Trigger warning

Current source supports both:

```text
createFiveMinuteAdjustmentsTrigger_clean()
```

and:

```text
doGet/doPost Web App runner
```

The actual production trigger/scheduler configuration lives outside GitHub.

Before creating a new trigger, inspect existing Apps Script triggers and Cloud Scheduler jobs so you do not create duplicate executions.

## Critical production checks

Before allowing live adjustments:

- `_testproduction_` isolation must be active.
- Any fake rows from an old temporary stress-test Google-facing run must be absent from the production upload feed.
- Current Adjustment Time format must remain intentional.

---

# Part F — Sales Sheet Apps Script

Repository source:

```text
Sales Sheet/Sales Sheet App Script.js
```

## Update process

1. Open the Sales Sheet workbook.
2. Go to:

```text
Extensions → Apps Script
```

3. Compare current script to `main`.
4. Update required code.
5. Save.
6. Verify Script Property:

```text
EXPORT_SECRET
```

7. Verify the installable trigger exists for:

```text
onEditTrigger
```

If missing, the helper in current source is:

```text
installOnEditTrigger
```

Run it only under the correct authorized Google identity.

8. Verify Config tab exact conversion-action names.
9. Test one controlled Sales row.

---

# Part G — Landing Pages / Website

Repository contains source snippets/templates under:

```text
Landing_page/
```

Git push does not necessarily publish the live website.

Before changing a project:

1. Identify the exact live page/snippet that is actually deployed.
2. Inspect project-specific constants.
3. Preserve:

```text
LEGACY_GOOGLE_ADS_SEND_TO
LEGACY_LEAD_FORM_GOOGLE_ADS_SEND_TO
```

unless explicitly changing them.

4. Verify intended:

```text
PROJECT_ID
WHATSAPP_CONVERSION_NAME
LEAD_VALUE_ESTIMATE
DEFAULT_PHONE / data-wa-phone
SAVE_TOKEN_ENDPOINT
ATTRIBUTION_TTL_HOURS
```

5. If a `.min.html` version is used operationally, ensure it is regenerated/updated consistently rather than hand-editing one copy and forgetting the other.
6. Publish through the actual Webflow/site workflow.
7. Open the live page.
8. Test one controlled CTA interaction.
9. Verify saveToken request and WhatsApp message token.

---

# Part H — Meta WhatsApp Webhook

Expected WaTrck callback:

```text
https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/whatsappWebhook
```

Correct routing:

```text
Meta → Firebase whatsappWebhook → Chatwoot
```

If Chatwoot/WABA/phone onboarding changes Meta settings, re-check the callback afterward.

## Verification GET from fresh Windows CMD

```bat
set "CALLBACK_URL=https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/whatsappWebhook"
set "VERIFY_TOKEN=YOUR_VERIFY_TOKEN"

curl -i "%CALLBACK_URL%?hub.mode=subscribe&hub.verify_token=%VERIFY_TOKEN%&hub.challenge=123456"
```

Expected body:

```text
123456
```

For setting Meta app callback/subscriptions through Graph API, verify the **current** Meta Graph API version/documentation at execution time. Do not treat a historical API version from chat as permanent.

---

# Part I — Google Ads normal conversion source

Shared source tab:

```text
Conversions_Sheet
```

Operational rule for every WaTrck conversion action:

```text
Filter:
Conversion Name
Equals
<exact action name>
```

If a conversion action is renamed, audit the complete identity chain:

```text
landing page or Sales Sheet Config
→ Firestore
→ Conversions_Sheet
→ Data Manager filter
→ Google Ads conversion action
```

Do not manually rewrite historical conversion rows as a shortcut.

---

# Part J — Google Ads adjustment upload schedule

Google-facing source tab:

```text
Google_Ads_Adjustments_Upload
```

Google Ads UI path:

```text
Goals
→ Conversions
→ Uploads
→ Schedules
```

This schedule is the consumer of the sheet.

It is separate from the process that populates the sheet.

Ensure the Apps Script/scheduler fills the sheet **before** Google Ads' scheduled fetch window.

Timezone is external configuration. Verify it in the live UI rather than assuming Cairo or Dubai.

---

# Part K — Cloud Scheduler

GitHub does not contain authoritative current Cloud Scheduler job definitions.

Before modifying a job, record:

```text
job name
region
target URL
HTTP method
auth mode
cron expression
timezone
retry settings
```

Potential jobs may target:

- conversions Apps Script Web App,
- adjustments Apps Script Web App,
- cleanup HTTP function,
- legacy `exportConversionsToSheet`.

Never create a replacement job until you know whether the old job is still enabled.

---

# Part L — Rollback discipline

For a production change:

1. Identify the current known-good commit.
2. Make one logical change.
3. Deploy only the affected subsystem.
4. Run one controlled post-deploy test.
5. If it fails, revert that change and redeploy.
6. Re-test restoration.
7. Do not add a second unrelated “fix” while the first deployment remains uncertain.

## Protected test-lead cleanup extension (2026-10-30)

Source: functions/test-lead-cleanup.js, called by cleanupFirestoreHttp.
Operator confirmed preserving the existing 168-hour unused-click TTL.

Existing requests without scope retain the original unused-only behavior.
New scope=tests removes only tests; scope=all runs tests plus the existing unused
cleanup. Both new scopes require the existing x-export-secret header and default
to dryRun=true. Explicit dryRun=false enables deletion. The operator deployed
the extension and supplied a successful Scheduler request to the existing GET
target `cleanupFirestoreHttp?scope=all&dryRun=false`. It uses the authorized
x-export-secret header. Do not create a new cleanup job. Its name/frequency and
whether it was resumed after simulation still need live confirmation.

Selectors are exact stored whatsapp_username === adhamthabet01 OR a string GCLID
containing _testproduction_. Profile name Adham is never a selector. Missing
usernames are not inferred from display names, phone numbers or conversation
history. No age or used restriction applies to these explicitly identified tests.

Only canonical projects/<project>/clicks/<token> documents and their matching
tokenIndex are removed. Each transaction re-reads eligibility and index ownership;
a conflicting index blocks deletion. Conversations/messages, sender indexes,
Sales/Conversions/Adjustments Sheet rows and Google Ads records are unchanged.
Database deletion has no automatic undo; the dry run is not a backup.

Each call scans up to 2,000 click documents in document-path order. If morePages
is true, pass nextCursor as after (URL-encoded) and continue until false. This is
required for complete coverage; a job that always restarts without consuming
continuations may miss tests beyond the first 2,000 records. Counts/samples avoid
logging names and phones. Samples are capped at 100. HTTP 409 indicates per-record
errors or index conflicts; successful earlier deletions may already have committed.
Retry is safe for missing/mutated records. Authenticated callers must inspect these
results, not only transport success. For large datasets use a cursor-aware caller
before enabling recurring test cleanup.

Verified rollout: the operator's dry run found 18 username tests among 97
documents. After a successful scheduled cleanup, the follow-up dry run found
zero matches among 79 documents, zero errors and no remaining pages. The backend
handles deletion; the Sales feed skips the username by default without deleting
any Sheet rows. The temporary Apps Script Cleanup Preview helper can be removed.

Future backend changes, if needed, deploy only exportsApi (feed) and/or
cleanupFirestoreHttp (including its required module) with
FUNCTIONS_DISCOVERY_TIMEOUT=60. The present Apps Script styling/cutover does not
require redeploying Firebase. For a new cleanup verification, use
scope=tests&dryRun=true and the existing secret header; review the body, not just
HTTP status. Never put the secret in a URL, source, or chat.

Offline check: node "Helping Functions/test-test-lead-cleanup.js". Tests cover exact
selectors, name-only nonmatches, used test deletion, dry run, transaction races,
index ownership, pagination, protected HTTP entry and preserved seven-day default.
