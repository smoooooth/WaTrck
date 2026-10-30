# Current State — WaTrck

This document records the current system state known at the context snapshot.

It combines facts verified from `main` with operational decisions confirmed outside the repository. Anything marked external should be re-verified in the relevant service before a production change.

## Latest handover — 2026-10-30

Automatic Sales import now works through the existing Conversions Cloud Scheduler
job into the TESTED COPY. The operator confirmed automatic arrival of a newly
simulated lead. Moving the destination to the original Sales workbook is still
pending; use [SALES_PRODUCTION_CUTOVER.md](SALES_PRODUCTION_CUTOVER.md).

Confirmed during this session:

- Production Sales workbook: `1U4509WWjTjlddPorHusPMuxnCdFZqkSK94zwSqbySVw`.
- Tested copy / last verified import destination:
  `159PCMRa4HPBRFuIKRDMB0qdwFn8PylK-T2GceUhCeAk`.
- Conversions Apps Script ID:
  `16o8aB_BH0RHuI1VltuEpMjcARrNdEWJQuDY5c_dH6R8Y6fVRso5fBC8M`.
- Existing job: `Export_Conversions_To_Sheet`, `europe-west4`, project
  `aida-muscat-wa-tracking`. It invokes the Conversions Web App, which runs both
  conversion export and Sales import. No separate Sales job/timer is intended.
- All six copied project tabs were migrated and visually checked: native leads
  Tables A1:M1117, filtered Tables N7:Z419, formula N8, controls O2:O4.
- Config D `Contact_Conversion_Name` routes to Config A `TabName`. There were
  37 eligible non-test leads matching 37 existing rows across all six tabs,
  with zero skipped tabs/unmapped leads in the supplied 07:29 preview.
- Duplicate tokens found in the copy were identical accidentally repeated leads
  and were resolved by the operator. Do not assume the original workbook's
  duplicates were also fixed. The importer deliberately refuses duplicate tokens.
- The username toggle was enabled for later simulations. A fresh diagnostic
  started at `2026-10-30T11:01:00.031Z`, completed at `11:01:15.848Z`, reported
  the script/copy IDs above, both Sales flags `true`, 43 eligible/existing leads,
  zero new rows, zero skipped tabs/unmapped leads. The operator subsequently
  confirmed a new lead imported automatically; its final count was not supplied.

Operational lessons: editor runs use saved source while a versioned `/exec`
uses its deployed version. Manual imports worked while an older/different Web
App path returned a 37-lead result without the new diagnostic. Aligning the job
with the updated deployment resolved the reported automatic-import issue. The
final URL/version number was not recorded here: inspect the CURRENT deployment
and job target before changing them; do not reuse an earlier chat URL.

One earlier Scheduler HTTP 500 / zero-second Apps Script failure was followed by
successful scheduled executions. Its underlying cause was not established.
Typical observed successful runs took 10–25 seconds. The last supplied cron was
`*/5 * * * *`, timezone Africa/Cairo. Two-minute scheduling was discussed, not
confirmed applied. Retry settings were adjusted by the operator during testing;
inspect their current values before further changes.

Live verification still needed before production cutover:

- Confirm `SALES_INCLUDE_TEST_USERNAME=false` after simulation (last diagnostic
  showed true) and whether the cleanup job needs resuming after any testing pause.
- Visually test the NEW 120px/dark-grey-background/10pt/plain source-column style
  on the copy. Earlier migration was verified; this latest styling was tested
  locally only. I1's explicit format is restored and its content is never written.
- Deploy the trimmed importer source (retired timer/Vision-only helpers removed).
  This local cleanup does not automatically update Apps Script.
- Complete the original-workbook cutover and verify a fresh scheduled import.

Name lookup authorization and importer name-consumption wiring remain deferred.
Visible source labels are pending; IDs are preserved in cell notes. PMax
asset-group capture and spend/CPL/CPQL reports are not completed features.
See [SALES_SHEET.md](SALES_SHEET.md) for the current code contract.

## Repository

```text
repo: smoooooth/WaTrck
branch: main
snapshot commit: d41b5274f94d8bf9aa29308a468c8fb98872ae16
```

The previous working branch `New-payload-fixes` was merged into `main` and is no longer intended as the ongoing production branch.

## Firebase / GCP

`.firebaserc` currently points to:

```text
aida-muscat-wa-tracking
```

Primary production endpoint region:

```text
us-central1
```

Node runtime in `functions/package.json`:

```text
22
```

Firebase-managed HTTP functions documented in `functions/index.js`:

```text
saveToken
exportsApi
whatsappWebhook
cleanupFirestoreHttp
```

Legacy function present in the same file but historically managed separately:

```text
exportConversionsToSheet
```

## Production endpoints in current code

Save token:

```text
https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/saveToken
```

Exports API base:

```text
https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/exportsApi
```

Important routes:

```text
GET  /exports/pending
POST /exports/mark-exported
GET  /exports/adjustments-pending
POST /exports/mark-adjustments-exported
GET  /sales/leads
POST /sales/quality-update
POST /queueAdjustment
```

WhatsApp webhook:

```text
https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/whatsappWebhook
```

Cleanup:

```text
https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/cleanupFirestoreHttp
```

Current cleanup code defaults to:

```text
unused click TTL = 168 hours (7 days; operator confirmed 2026-10-30)
CLEANUP_DRY_RUN = false
```

It only queries explicit `used === false` click documents and transactionally re-checks before deleting the click and matching `tokenIndex` entry.

The default unused-only path is unchanged. Deployed source adds protected
`scope=tests` / `scope=all` modes to cleanupFirestoreHttp, defaulting to dry run.
These match exact whatsapp_username `adhamthabet01` or GCLID `_testproduction_`,
including used leads, and remove only click + matching tokenIndex documents.
Conversation backups and already-exported Sheet rows are not deleted. This
extension was deployed by the operator. The existing cleanup job's confirmed
GET target is `cleanupFirestoreHttp?scope=all&dryRun=false`, with the existing
export-secret authentication. A live dry run scanned 97 records and matched 18
username tests; the job returned HTTP 200, and the next dry run scanned 79 with
zero matches/errors and no next page. This supports removal of those 18 tests.
The job name/frequency and its current enabled state after simulations are not
recorded. Each call is bounded to 2,000 scanned records; the simple job does not
consume continuation cursors. See DEPLOYMENT.md before claiming cleanup covers
larger datasets.

## Current WhatsApp / Chatwoot code state

Current Firebase code contains:

```text
CHATWOOT_WEBHOOK_URL =
https://wa-chat.seaborn-properties.com/webhooks/whatsapp/+971586992542
```

Firebase receives Meta first, then forwards the raw request body to Chatwoot with the original `X-Hub-Signature-256` header. Forwarding is fire-and-forget so a Chatwoot failure does not block WaTrck processing.

### Current landing-page phone discrepancy

The inspected landing-page templates still contain:

```text
+201034285454
```

as the WhatsApp CTA/default destination.

Therefore, at the snapshot:

```text
Firebase → Chatwoot bridge number context: +971586992542
Landing-page WhatsApp destination:        +201034285454
```

Do not automatically make these match. Confirm intended production migration state first. See `KNOWN_DISCREPANCIES.md`.

## Current projects and verified Contact conversion names

Updated on 2026-10-28: the operator confirmed `_WaTrck_` naming. The
Contact names below match current landing-page source and the supplied Sales
Sheet Config export. This updates conversion naming only; the other snapshot
sections are not a new verification of live production configuration.

### AIDA

```text
PROJECT_ID: AIDA_Oman
Contact CA: AIDA Oman_WaTrck_Contact
Lead estimate: 70 USD
```

### Trump Plaza Jeddah

```text
PROJECT_ID: Trump Plaza Jeddah
Contact CA: Trump Plaza Jed_WaTrck_Contact
Lead estimate: 70 USD
```

### Trump Park Residences

```text
PROJECT_ID: Trump Park Residences
Contact CA: Trump Park Res_WaTrck_Contact
Lead estimate: 70 USD
```

### Padel Living Residences

```text
PROJECT_ID: Padel Living Res
Contact CA: Padel Living_WaTrck_Contact
Lead estimate: 70 USD
```

### Simaisma Trump Residences

```text
PROJECT_ID: Simaisma Trump Residences
Contact CA: Simaisma Trump Res_WaTrck_Contact
```

### Vision Iconic Tower

```text
PROJECT_ID: Vision Iconic Tower
Contact CA: Vision Iconic Tower_WaTrck_Contact
```

Exact Qualified/Closed names are configured externally in the Sales Sheet `Config` tab. The operator-supplied Config export on 2026-10-28 contains:

| Sales tab | Project ID (verified in landing-page source) | Qualified conversion | Closed conversion |
| --- | --- | --- | --- |
| AIDA_Oman | AIDA_Oman | AIDA Oman_WaTrck_Qualified | AIDA Oman_WaTrck_Closed |
| Tr_Plaza_Jeddah | Trump Plaza Jeddah | Trump Plaza Jed_WaTrck_Qualified | Trump Plaza Jed_WaTrck_Closed |
| Tr_Park_Residences | Trump Park Residences | Trump Park Res_WaTrck_Qualified | Trump Park Res_WaTrck_Closed |
| Padel_Living | Padel Living Res | Padel Living_WaTrck_Qualified | Padel Living_WaTrck_Closed |
| Simaisma_Trump_Res | Simaisma Trump Residences | Simaisma Trump Res_WaTrck_Qualified | Simaisma Trump Res_WaTrck_Closed |
| Vision_Iconic_Tower | Vision Iconic Tower | Vision Iconic Tower_WaTrck_Qualified | Vision Iconic Tower_WaTrck_Closed |

This is an operator-supplied configuration snapshot, not a live Google Ads
account inspection. Do not derive action names from naming conventions.

## Landing-page behavior

Current project templates:

1. Generate a 7-character uppercase alphanumeric token.
2. Capture `gclid`, `campaign_id`, `adgroup_id`, and `ad_id`.
3. Retain attribution in `localStorage` for a 24-hour TTL.
4. POST a fire-and-forget click record to `saveToken`.
5. Save project Contact action name into Firestore as `conversion_name`.
6. Append `Ref: #<TOKEN>` to the WhatsApp message.
7. Fire the existing legacy Google Ads direct conversion on WhatsApp click.
8. Open WhatsApp immediately.

Legacy click conversion and WaTrck offline Contact are deliberately separate.

## Google Ads conversion architecture

WaTrck funnel:

```text
Contact
Qualified
Closed
```

Typical business values:

```text
Contact   = 70 USD
Qualified = 500 USD typical
Closed    = actual closed value
```

Unqualified does not create a new action. It RESTATEs Contact to the explicit
non-negative Sales value through adjustments; `0` is allowed but no longer
automatic. Source updated 2026-10-28; deploying `exportsApi` and saving the
Sales Sheet Apps Script are required to activate this behavior. Live deployment
has not been verified.

Existing legacy Lead Form/Contact conversion mechanisms remain in place during beta unless explicitly changed.

Operationally, the new WaTrck actions were created, linked to the shared conversion source, and filtered by exact `Conversion Name`. That live Google Ads state is external to GitHub and should be verified before future changes.

## Conversions workbook

Current `Conversions/Conversions.js` uses a global workbook for all projects.

Production tab:

```text
Conversions_Sheet
```

Current code headers:

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

Additional tabs:

```text
NoGCLID_Review
TESTING_Conversions_Sheet
TESTING_NoGCLID_Review
```

Production dedupe key:

```text
GCLID | Conversion Name
```

Testing dedupe key:

```text
GCLID | Conversion Name | Order ID
```

## Adjustments workbook

Internal audit sheet:

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

Google-facing tab:

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

Current adjustment type:

```text
RESTATE
```

Current adjustment datetime formatter:

```text
YYYY-MM-DD HH:MM:SS+0000
```

## Google Ads scheduled uploads

Architecture:

```text
Conversions_Sheet
→ Google Ads Data Manager conversion imports

Google_Ads_Adjustments_Upload
→ Google Ads Goals/Conversions/Uploads/Schedules
→ scheduled adjustment import
```

The Google Ads schedule reads the sheet. That is separate from the Apps Script process that fills the sheet.

## Sales Sheet

Current code expects Config:

```text
TabName | Qualified conversion name | Closed conversion name | Contact_Conversion_Name
```

Columns:

```text
E = Token
F = Date
G = Quality
H = Value
I = Feedback / Message
J = Campaign
K = Ad group / Asset group
L = Ad
M = Campaign type
```

The quality script reads Config A:C; the importer uses A/D and checks the exact
A1/D1 headers. The scheduled importer never writes Quality, USD Value or Feedback
(G:I), including on new rows. New C:D both receive yellow `Fresh Lead — Review`.
Column B uses a phone first, otherwise the stored WhatsApp username. Existing
Sales-entered details are preserved. Token E is the per-project dedupe key.

Quality:

```text
0 = Unqualified
1 = Qualified
2 = Closed
```

Forward transitions:

```text
New → Unqualified
New → Qualified
New → Closed
Qualified → Closed
```

Blocked backward transitions:

```text
Qualified → Unqualified
Closed → Qualified
Closed → Unqualified
```

## Secrets

Backend Secret Manager names:

```text
whatsapp_verify_token
conversions_exports_secrect
```

The second name is intentionally misspelled.

Apps Script Script Property:

```text
EXPORT_SECRET
```

Never commit actual secret values.

## Stress test status

Repo includes:

```text
Helping Functions/stress-test-adjustments.js
Helping Functions/adjustments-stress-last-run.json
```

Verified recorded run:

```text
run_id: 20260914163036-VF5C
projects: 3
leads_created: 54
expected_adjustment_rows: 81

quality totals:
Unqualified: 9
Qualified: 36
Closed: 36

versions:
v1: 54
v2: 27
```

Per project, the committed JSON records 27 rows:

```text
contact: 3
qualified: 12
closed: 12
```

This test used `_testproduction_` markers.

## Remaining production proof

Synthetic testing proves internal WaTrck behavior, not Google's acceptance of fake click IDs.

Full acceptance still requires a real Google Ads click/conversion path and a real adjustment against an imported conversion.
