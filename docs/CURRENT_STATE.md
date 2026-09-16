# Current State — WaTrck

This document records the current system state known at the context snapshot.

It combines facts verified from `main` with operational decisions confirmed outside the repository. Anything marked external should be re-verified in the relevant service before a production change.

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
unused click TTL = 24 hours
CLEANUP_DRY_RUN = false
```

It only queries explicit `used === false` click documents and transactionally re-checks before deleting the click and matching `tokenIndex` entry.

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

### AIDA

```text
PROJECT_ID: AIDA_Oman
Contact CA: AIDA Oman | WaTrck | Contact
Lead estimate: 70 USD
```

### Trump Plaza Jeddah

```text
PROJECT_ID: Trump Plaza Jeddah
Contact CA: Trump Plaza Jed | WaTrck | Contact
Lead estimate: 70 USD
```

### Trump Park Residences

```text
PROJECT_ID: Trump Park Residences
Contact CA: Trump Park Res | WaTrck | Contact
Lead estimate: 70 USD
```

### Padel Living Residences

```text
PROJECT_ID: Padel Living Res
Contact CA: Padel Living | WaTrck | Contact
Lead estimate: 70 USD
```

Exact Qualified/Closed names are configured externally in the Sales Sheet `Config` tab and are not hard-coded in the repo. Do not guess them from Contact names.

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

Unqualified does not create a new action. It RESTATEs Contact to `0` through adjustments.

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
TabName | QualifiedConv | ClosedConv
```

Columns:

```text
E = Token
F = Date
G = Quality
H = Value
I = Feedback / Message
```

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
