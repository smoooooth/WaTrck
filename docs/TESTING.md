# Testing Strategy and Known Results

WaTrck should be tested in layers. Do not jump from a code edit straight to a real Google Ads test when a smaller test can prove the same mechanism first.

## 1. Testing philosophy

Preferred sequence:

```text
static/code inspection
→ isolated backend behavior
→ synthetic Firestore test
→ Apps Script output verification
→ ACK/state verification
→ integration test
→ real Google Ads acceptance
```

The operator prefers **one controlled test at a time**. Do not combine unrelated test variables unless the task specifically requires an end-to-end stress test.

## 2. Test marker

Synthetic production-shaped GCLIDs use:

```text
_testproduction_
```

Example:

```text
_testproduction_-adjustments-...
```

This marker protects the real Google Ads feed.

### Normal exporter

Test conversions are routed to:

```text
TESTING_Conversions_Sheet
TESTING_NoGCLID_Review
```

### Adjustments exporter

Test adjustments are retained in the internal audit sheet:

```text
Adjusted_Conversions_Sheet
```

but current production code deliberately excludes them from:

```text
Google_Ads_Adjustments_Upload
```

## 3. Adjustment stress test

Script:

```text
Helping Functions/stress-test-adjustments.js
```

Recorded output:

```text
Helping Functions/adjustments-stress-last-run.json
```

Verified committed run:

```text
run_id:
20260914163036-VF5C

generated_at:
2026-09-14T16:34:14.250Z

projects:
AIDA_Oman
Trump Plaza Jeddah
Trump Park Residences

sets_per_project:
3

leads_created:
54

expected_adjustment_rows:
81
```

### Totals recorded in the committed JSON

```text
Unqualified: 9
Qualified:   36
Closed:      36

v1: 54
v2: 27
```

Per project:

```text
total:     27
contact:    3
qualified: 12
closed:    12
```

If an older chat summary gives a different per-project Contact count, prefer the committed JSON artifact.

## 4. What the stress test demonstrated

The historical stress run exercised:

- fresh lead creation,
- Unqualified, Qualified, Closed, and multi-stage scenarios,
- multiple upload versions,
- adjustment row generation,
- internal dedupe,
- Google-facing dedupe,
- ACK progression,
- preservation of newer pending versions,
- test-data isolation.

The project history also recorded successful workbook validation with no missing/extra/duplicate expected stress rows during the controlled test configuration.

Current production code has `_testproduction_` Google-facing isolation restored.

## 5. Never disable test isolation casually

`Adjustments.js` contains a commented historical temporary line:

```js
//const isTestProduction = false; // TEMPORARY: allow stress-test rows into Google Ads sheet
```

Do **not** enable that in production.

Doing so can put fake stress-test rows into the real Google-facing adjustment tab.

## 6. Critical adjustment race test

This is the most important backend concurrency test.

Create/fetch v1:

```text
upload_version = 1
last_adjustment_version_exported = 0
adjustment_pending = true
```

Before v1 ACK returns, create v2:

```text
upload_version = 2
adjustment_pending = true
```

Then ACK v1.

Expected final state:

```text
last_adjustment_version_exported = 1
upload_version = 2
adjustment_pending = true
```

Then v2 must remain available through `/exports/adjustments-pending`.

Any result that clears `adjustment_pending` after the stale v1 ACK is a failure.

## 7. Normal conversion retry test

Controlled scenario:

1. A pending conversion is fetched.
2. The row is written to the destination sheet.
3. Backend ACK fails or is interrupted.
4. Run exporter again.

Expected:

```text
existing row detected by dedupe
no duplicate production conversion row
backend ACK is retried
```

Production dedupe is:

```text
GCLID | Conversion Name
```

Testing dedupe is:

```text
GCLID | Conversion Name | Order ID
```

## 8. Adjustment retry test

Controlled scenario:

1. Pending adjustment is fetched.
2. Internal audit row is written.
3. Google-facing row is written for a real/non-test GCLID.
4. Sheet flush succeeds.
5. Backend ACK fails.
6. Run exporter again.

Expected:

- same internal key is detected,
- exact original `Adjustment Time` is reused,
- Google-facing row is not duplicated,
- same upload version is ACKed again safely.

If a retry generates a new Adjustment Time, stop. That violates the adjustment identity design.

## 9. Webhook token test

Use a controlled token from a known click record.

Before sending WhatsApp, verify:

```text
tokenIndex/<TOKEN>
```

exists and points to the expected project/click path.

Send a WhatsApp message containing:

```text
Ref: #TOKEN
```

Verify in order:

1. Firebase `whatsappWebhook` receives a POST.
2. The message/contact payload parses.
3. The token resolves.
4. The correct project resolves.
5. `projects/<projectId>/clicks/<TOKEN>` becomes `used=true`.
6. `used_at` is created.
7. WhatsApp identity fields are stored.
8. Conversation/message backup is written.
9. Chatwoot receives the forwarded raw Meta webhook.

## 10. No-token follow-up test

After a sender/project mapping exists, send another message without a token.

Expected:

- project conversation can still be backed up through `whatsappSenderIndex`,
- no unrelated click is marked used,
- no new Contact conversion is created merely from sender identity.

## 11. Webhook verification GET test

Conceptual CMD test:

```bat
set "CALLBACK_URL=https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/whatsappWebhook"
set "VERIFY_TOKEN=YOUR_VERIFY_TOKEN"

curl -i "%CALLBACK_URL%?hub.mode=subscribe&hub.verify_token=%VERIFY_TOKEN%&hub.challenge=123456"
```

Expected body:

```text
123456
```

Never commit the real verify token.

## 12. Cleanup safety tests

Before changing cleanup behavior, test these separately.

### Expired unused click

```text
used = false
age >= 24 hours
```

Expected:

```text
click doc deleted
tokenIndex/<token> deleted atomically
```

### Used conversion

```text
used = true
```

Expected:

It should not even be returned by the cleanup query.

### Race protection

Start with `used=false` so it becomes a cleanup candidate, then mark it `used=true` before the delete transaction commits.

Expected:

```text
transaction re-read sees used=true
status = became_used
nothing deleted
```

## 13. Sales Sheet tests

### Qualified

Use a controlled lead/token with:

```text
Quality = 1 / qualified
Value = numeric
```

Expected:

- backend returns 200,
- `sales_sheet_updated_quality=1`,
- `conversion_name_sales` equals exact Qualified action name from Config,
- `conversion_value_final` set,
- `upload_version` increments,
- normal conversion pending,
- adjustment pending.

### Closed

From New or Qualified:

```text
Quality = 2 / closed
Value = numeric
```

Expected:

- Closed action name becomes current `conversion_name_sales`,
- previous Qualified name is preserved in `conversion_name_sales_qualified` when applicable,
- upload version increments.

### Backward transition

Try:

```text
Qualified → Unqualified
```

or:

```text
Closed → Qualified
```

Expected:

Sales Sheet blocks it and restores previous quality.

## 14. Normal conversion sheet tests

For a Contact, verify row fields:

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

For Qualified/Closed, verify the same GCLID can legitimately have separate rows because the Conversion Name differs.

Do not flag that as a duplicate.

## 15. Real Google Ads validation

Synthetic tests prove WaTrck internals only.

They do **not** prove Google recognizes a fake GCLID.

Final production validation requires a real Google Ads click / valid GCLID.

Suggested chain:

```text
real ad click
→ landing page
→ WhatsApp message
→ Firestore used=true
→ Conversions_Sheet
→ Google Ads import
→ accepted Contact conversion
```

Then:

```text
Sales → Qualified
→ new Qualified conversion row
→ Google Ads import/acceptance
```

Then:

```text
Sales → Closed
→ new Closed conversion row
→ Google Ads import/acceptance
```

Then a real adjustment:

```text
already imported real conversion
→ RESTATE row
→ Google Ads scheduled adjustment upload
→ accepted adjustment
```

## 16. Do not manufacture noisy paid-ad traffic

Avoid repeatedly clicking live paid ads merely to create test data.

Use a legitimate controlled production validation method that minimizes campaign contamination.

## 17. Regression checklist after backend edit

Minimum checks:

```text
saveToken still writes click
tokenIndex still writes
WhatsApp token match still marks used
/exports/pending still returns correct state
Sales update still increments upload_version
adjustment pending still appears
stale ACK cannot clear newer version
cleanup still excludes used conversions
```

## 18. Regression checklist after landing-page edit

Verify:

```text
GCLID capture
campaign_id capture
adgroup_id capture
ad_id capture
token generation
saveToken payload
PROJECT_ID
WHATSAPP_CONVERSION_NAME
Ref: #TOKEN text
legacy Google Ads send_to still fires
WhatsApp still opens immediately
```

## 19. Regression checklist after Apps Script edit

Verify:

- Script Properties preserved,
- correct workbook,
- correct tab names,
- exact headers,
- write-before-ACK order,
- dedupe unchanged,
- test marker behavior,
- Web App deployment updated if required,
- scheduler/trigger still targets the intended runner.
