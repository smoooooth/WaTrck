# History and Architectural Decisions

This file records important decisions so future Codex sessions do not accidentally undo deliberate architecture.

## 1. Branch consolidation

Development work had been carried on:

```text
New-payload-fixes
```

Before consolidation, GitHub comparison showed that branch ahead of `main` with no `main` divergence.

The work was merged into:

```text
main
```

and `main` became the permanent system branch.

Do not treat `New-payload-fixes` as the current source of truth.

## 2. WaTrck Contact versus legacy Contact

Existing Google Ads Contact conversion fired on WhatsApp button **click**.

That is an intent signal and can include users who never actually message.

WaTrck Contact intentionally waits until Firebase sees a WhatsApp message with the tracking token.

Decision:

```text
keep legacy conversions during beta
add WaTrck offline conversions in parallel
```

Do not remove the legacy event as “duplicate tracking” without an explicit migration plan.

## 3. Funnel design

Chosen semantics:

```text
Contact remains a historical milestone
Qualified is an additional conversion occurrence
Closed is an additional conversion occurrence
```

A lead can therefore legitimately have all three.

The system is not designed as one mutable Google conversion that changes category over time.

## 4. Unqualified design

There is no new “Unqualified” Google conversion.

Decision:

```text
RESTATE the original Contact conversion to value 0
```

This communicates negative value without inventing a positive conversion event for an unqualified lead.

## 5. Positive-stage value adjustment design

Qualified/Closed are real milestone conversions.

If a positive stage's value changes, the adjustment targets that stage's existing conversion rather than rewriting every prior stage.

Current adjustment target is the current sales-stage conversion for quality 1/2.

## 6. Google Ads API production route was abandoned

A Google Cloud project enabled the Google Ads API and attempted Explorer Access.

Outcome:

```text
Explorer Access Denied
Test Access remains active
Production Access restricted
```

Decision:

Do not make production WaTrck depend on Google Ads API access.

Use:

```text
Google Sheets
Apps Script
Data Manager
Google Ads scheduled Uploads
```

unless API access/product direction explicitly changes later.

## 7. Shared conversion source

Rather than separate workbook/source per conversion action, WaTrck uses a shared conversions sheet.

Because the sheet contains multiple projects and stages, exact action routing is performed with:

```text
Conversion Name equals exact action name
```

Decision:

```text
one shared source
many filtered conversion-action destinations
```

## 8. Adjustments use scheduled sheet upload

The project discovered that Google Ads:

```text
Goals → Conversions → Uploads → Schedules
```

can fetch a Google Sheet containing adjustment rows.

Decision:

Use this mechanism for `RESTATE` instead of waiting for Google Ads API production access.

## 9. Adjustment audit and Google-facing feed are separate

Two tabs are intentionally maintained:

```text
Adjusted_Conversions_Sheet
```

for internal audit/version detail, and:

```text
Google_Ads_Adjustments_Upload
```

for the clean Google Ads schema.

This separation prevents internal bookkeeping fields from becoming part of the external import contract.

## 10. Stable retry identity

Retry safety was treated as a first-class requirement.

Decision:

Once a logical adjustment gets an `Adjustment Time`, retries reuse that exact time.

That is why internal dedupe includes:

```text
GCLID | Upload Version | Conversion Name
```

## 11. Version-aware adjustment ACK

Explicit race considered:

```text
v1 fetched
v2 created
v1 ACK arrives late
```

Decision:

ACK advances only `last_adjustment_version_exported` and derives `adjustment_pending` from whether current upload version is newer.

Never clear pending blindly.

## 12. Write-before-ACK

Both normal conversions and adjustments use:

```text
persist/confirm Sheet state
→ flush
→ ACK backend
```

Decision:

Prefer duplicate-safe retry over the risk of ACKing data that was never written.

## 13. Test-data isolation

Synthetic production-shaped tests use:

```text
_testproduction_
```

Decision:

Keep test data visible enough to validate the pipeline while isolating it from real Google Ads feeds.

Temporary bypasses used during controlled stress testing must not remain active.

## 14. Webhook routing correction

A major webhook issue was traced to routing.

Correct architecture:

```text
Meta
→ Firebase whatsappWebhook
→ Chatwoot
```

not:

```text
Meta
→ Chatwoot directly
```

because direct Chatwoot callback bypasses WaTrck token processing.

## 15. Independent WhatsApp conversation backup

WaTrck stores organized WhatsApp conversation/message data in Firestore even though Chatwoot also receives messages.

Decision:

Keep this independent record for attribution/debug traceability.

Do not remove it merely because Chatwoot has history.

## 16. WhatsApp identity expansion

Webhook handling evolved to account for privacy/username-based identities.

Current code supports:

- phone number when available,
- BSUID/user ID,
- username,
- profile name.

Decision:

Prefer stable business-scoped user identity when available while retaining phone data where Meta exposes it.

## 17. Cleanup policy

Attribution clicks that never become real contacts are temporary.

Decision:

Delete only:

```text
used === false
AND
age >= 24 hours
```

and transactionally re-read immediately before delete.

Used conversions and WhatsApp history are not part of automatic cleanup.

## 18. Global normal conversion exporter

The normal exporter was consolidated around one global Apps Script/workbook across projects.

Decision:

Use a global pending fetch plus project-aware ACK.

The current source documents Cloud Scheduler → Apps Script Web App as the normal scheduling architecture.

## 19. Stress-test milestone

Recorded run:

```text
20260914163036-VF5C
```

created 54 synthetic leads and expected 81 adjustment rows across three projects and two upload-version phases.

This is a confidence milestone, not a reason to skip future regression testing.

## 20. Real WhatsApp number migration

The system began with a test WhatsApp number and is moving/has moved pieces toward a real production number.

Current `main` snapshot shows:

```text
Firebase Chatwoot bridge URL:
+971586992542
```

while inspected landing-page templates still target:

```text
+201034285454
```

This is treated as a migration-state discrepancy, not an automatic bug.

## 21. WhatsApp Business app coexistence discussion

The team considered connecting the real WhatsApp Business App number to Cloud API while preserving app usage/history where Meta eligibility allows coexistence.

This is external account configuration, not guaranteed by source code.

Decision for code work:

Do not delete/modify application history or assume a full API-only migration merely because backend webhook code exists.

## 22. Production context belongs in the repo

To support Codex and future maintenance, architecture knowledge is now being stored in:

```text
AGENTS.md
docs/
```

Decision:

Future architectural changes should update these documents so the project does not depend on one chat thread for institutional knowledge.
