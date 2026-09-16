# Files and Deployment Ownership

WaTrck files are not all deployed by the same system. Treat deployment ownership as part of the architecture.

## Root

### `.firebaserc`

Current default Firebase project:

```text
aida-muscat-wa-tracking
```

### `firebase.json`

Firebase Functions source:

```text
functions/
```

Current config disables legacy runtime config.

### `README.md`

At the verified snapshot this was effectively empty. Do not rely on it as architecture documentation unless updated later.

## `functions/`

### `functions/index.js`

Contains:

```text
saveToken
whatsappWebhook
exportsApi
exportConversionsToSheet
cleanupFirestoreHttp
```

The file itself distinguishes deployment ownership.

#### Firebase-managed

```text
saveToken
exportsApi
whatsappWebhook
cleanupFirestoreHttp
```

#### Historically gcloud-managed

```text
exportConversionsToSheet
```

Do not redeploy the legacy function under a different mechanism just because the file also contains Firebase functions.

### `functions/package.json`

Current runtime:

```text
Node 22
```

Main dependencies:

```text
firebase-admin
firebase-functions
@google-cloud/secret-manager
axios
express
cors
```

## `Conversions/Conversions.js`

Source copy for the global conversions Google Apps Script.

A Git commit does not deploy it to Google.

Current code explicitly describes production scheduling as:

```text
Cloud Scheduler
→ Apps Script Web App
→ doGet/doPost
→ runOnceFetch
```

and says not to create Apps Script time-based triggers for the normal conversion exporter.

## `Adjustments/Adjustments.js`

Source copy for the adjustments workbook Apps Script.

Responsibilities:

- global pending-adjustment fetch,
- internal audit sheet,
- Google-facing upload tab,
- `_testproduction_` isolation,
- project/version ACK.

Current source contains both a 5-minute Apps Script trigger installer and Web App `doGet`/`doPost` entry points. The live trigger arrangement is external and must be verified before changing schedules.

## `Sales Sheet/Sales Sheet App Script.js`

Source copy for Sales Sheet Apps Script.

Requires an installable `onEditTrigger` and Script Property `EXPORT_SECRET`.

Git does not deploy it automatically.

## `Landing_page/`

Contains project snippets/templates, including:

```text
Landing_page/DarGlobal/Aida/...
Landing_page/DarGlobal/Padel Living Residences.html
Landing_page/DarGlobal/Trump Park Residences.html
Landing_page/DarGlobal/Trump Plaza Jeddah.html
```

AIDA has multiple sub-development templates sharing the same project-level tracking identity.

Many non-minified files have `.min.html` counterparts.

Do not assume Webflow/live pages automatically track GitHub changes. Understand the actual publication path first.

## `Helping Functions/`

Current helpers include:

```text
adjustments-stress-last-run.json
copydocs.js
debug-collectiongroup.js
delete_docs_by_token_v2.js
seed-test-conversions.js
stress-test-adjustments.js
token-cleanup.js
```

These are operational/testing tools. Do not turn them into production scheduled jobs without explicit approval.

`stress-test-adjustments.js` is the important high-coverage adjustments stress test.

`token-cleanup.js` is a helper and is separate from production `cleanupFirestoreHttp`.

## `Guides/`

Operator PDFs include historical deployment and webhook guides.

When a guide conflicts with current code, current `main` wins.

## External production state not fully represented by GitHub

### GCP / Firebase

- deployed revisions,
- Secret Manager values,
- Firestore indexes,
- Cloud Scheduler jobs,
- service account/IAM state.

### Google

- live Sheets content,
- Apps Script deployment versions,
- Script Properties,
- Google Ads conversion actions,
- Data Manager mappings/filters,
- Upload schedules,
- campaign goal configuration.

### Meta / WhatsApp

- App ID,
- WABA ID,
- Phone Number ID,
- access tokens,
- webhook UI configuration,
- coexistence state.

### Chatwoot

- inbox/channel state,
- number/WABA relationship,
- webhook/inbox configuration.

## Rule before any integration change

Explicitly identify:

```text
Which file changes?
Which deployment owns it?
Which external service also changes?
Which current mechanism remains untouched?
```

A Git commit alone is not necessarily an operational deployment.
