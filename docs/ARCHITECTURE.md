# Architecture — WaTrck Production Tracking System

## Purpose

WaTrck connects Google Ads click attribution to an actual WhatsApp conversation and then to downstream lead-quality outcomes.

The central problem is:

```text
Google Ads knows the click.
WhatsApp receives the lead.
Sales later knows whether the lead was good.
```

WaTrck preserves a token across those systems so the original click can later be tied to Contact, Qualified, Closed, and value-restatement outcomes.

## End-to-end flow

```text
Google Ads click
    ↓
Landing page
    ↓
Capture GCLID + campaign/ad IDs
    ↓
Generate 7-char WaTrck token
    ↓
POST click record to Firebase saveToken
    ↓
Open WhatsApp with "Ref: #TOKEN"
    ↓
User sends WhatsApp message
    ↓
Meta WhatsApp webhook
    ↓
Firebase whatsappWebhook
    ├── forwards raw Meta payload to Chatwoot
    ├── extracts sender identity
    ├── extracts #TOKEN
    ├── resolves tokenIndex
    ├── resolves project
    ├── stores conversation/message backup
    └── marks click used=true
            ↓
Conversions exporter
            ↓
Conversions_Sheet
            ↓
Google Ads Data Manager
            ↓
WaTrck Contact conversion
            ↓
Sales Sheet
            ↓
sales/quality-update
    ├── Unqualified
    ├── Qualified
    └── Closed
            ↓
normal conversion exporter
    ├── creates Qualified conversion when appropriate
    └── creates Closed conversion when appropriate
            ↓
adjustment exporter
    └── writes RESTATE rows
            ↓
Google_Ads_Adjustments_Upload
            ↓
Google Ads scheduled upload
```

## Landing-page layer

Source: `Landing_page/...`

Each project file contains project-specific configuration plus common tracking logic.

Important project-specific constants include:

```js
DEFAULT_MESSAGE
PROJECT_ID
WHATSAPP_CONVERSION_NAME
LEAD_VALUE_ESTIMATE
LEGACY_GOOGLE_ADS_SEND_TO
LEGACY_LEAD_FORM_GOOGLE_ADS_SEND_TO
DEFAULT_PHONE
```

### Attribution capture

Current templates capture:

```text
gclid
campaign_id
adgroup_id
ad_id
```

They persist these in `localStorage` for `ATTRIBUTION_TTL_HOURS = 24`.

### Token generation

Browser token generation uses `crypto.getRandomValues`.

Default length:

```text
7
```

Alphabet:

```text
A-Z and 0-9
```

### Click payload

Typical shape:

```json
{
  "token": "ABC1234",
  "gclid": "...",
  "campaign_id": "...",
  "adgroup_id": "...",
  "ad_id": "...",
  "ts": "ISO_TIMESTAMP",
  "page": "canonical-host/path",
  "projectId": "AIDA_Oman",
  "ctaId": "floating_whatsapp_widget",
  "lead_value_estimate": 70,
  "conversion_name": "AIDA Oman | WaTrck | Contact",
  "used": false
}
```

The request is deliberately fire-and-forget so Firebase latency does not delay the WhatsApp popup.

### WhatsApp message bridge

The landing page appends:

```text
Ref: #ABC1234
```

The token is the bridge from browser attribution to the later WhatsApp webhook.

### Legacy conversion remains separate

The landing page also fires:

```js
gtag("event", "conversion", {
  send_to: LEGACY_GOOGLE_ADS_SEND_TO
});
```

This measures button-click intent. WaTrck Contact measures an actual token-bearing WhatsApp message. Do not collapse them during beta.

## `saveToken`

Function: `exports.saveToken` in `functions/index.js`.

Responsibilities:

1. CORS handling.
2. Parse click payload.
3. Normalize/generate token.
4. Write `projects/<projectId>/clicks/<TOKEN>`.
5. Write `tokenIndex/<TOKEN>`.
6. Initialize conversion/adjustment state.

Typical initial flags:

```text
used = false
conversion_value_uploaded = false
upload_version = 0
adjustment_pending = false
```

`conversion_name` is the project-specific WaTrck Contact action name from the landing page.

## WhatsApp webhook

Function: `exports.whatsappWebhook`.

### GET

Meta verification handshake using Secret Manager secret `whatsapp_verify_token`.

### POST

Order of work:

1. Forward raw webhook body to Chatwoot.
2. Parse message/contact identity.
3. Extract phone/BSUID/username/profile identity.
4. Extract token using `#...` regex.
5. Resolve `tokenIndex`.
6. Resolve project.
7. Save/update sender project index.
8. Store conversation/message backup when project is known.
9. If token resolves to a click, mark that click `used=true` and store WhatsApp identity.

A no-token message may be backed up to an already-known project, but it must not create a new conversion.

## Sender project memory

Path:

```text
whatsappSenderIndex/<conversationId>
```

This lets later tokenless messages be associated with the correct project for conversation backup.

It is not a substitute for token matching when creating a Contact conversion.

## Conversation backup

Conversation:

```text
projects/<projectId>/whatsapp_conversations/<conversationId>
```

Messages:

```text
projects/<projectId>/whatsapp_conversations/<conversationId>/messages/<msgId>
```

This is a WaTrck-owned backup independent of Chatwoot.

## Conversion eligibility

A click becomes a WaTrck Contact when:

```text
used === true
```

The actual message/token match creates this state.

## Normal conversion export

Firebase route:

```text
GET /exports/pending
```

Apps Script:

```text
Conversions/Conversions.js
```

The script writes real/test GCLID rows or no-GCLID review rows, flushes the sheet, and only then ACKs Firebase.

## Sales qualification

Sales edits trigger `onEditTrigger` in the Sales Sheet Apps Script, which POSTs to:

```text
/sales/quality-update
```

The backend stores quality/value, increments `upload_version`, marks normal conversion processing pending, and marks adjustment processing pending.

Qualified/Closed conversion names come from the Sales Sheet Config tab.

## Funnel semantics

### Contact

Created after actual WhatsApp token match. Typical value `70`.

### Qualified

Creates a new conversion occurrence. Typical business value is `500`, but actual Sales Sheet value is authoritative for that row.

### Closed

Creates a new Closed conversion occurrence with the actual closed value.

### Unqualified

Does not create a separate Unqualified conversion. Instead the original Contact is RESTATEd to `0`.

## Adjustment architecture

Sales/adjustment updates set:

```text
adjustment_pending = true
upload_version = previous + 1
```

Adjustment exporter reads `/exports/adjustments-pending`, writes internal audit + Google-facing rows, flushes, then ACKs by project/version.

The ACK is version-aware so an older acknowledgement cannot clear newer pending work.

## Google Ads ingestion

### Normal conversions

Shared source:

```text
Conversions_Sheet
```

Each conversion action uses an exact `Conversion Name` filter over the shared source.

### Adjustments

Google-facing tab:

```text
Google_Ads_Adjustments_Upload
```

is consumed through Google Ads `Goals → Conversions → Uploads → Schedules`.

## Cleanup

Current production cleanup targets only abandoned attribution clicks:

```text
used === false
AND
age >= 24 hours
```

It transactionally re-checks before deleting both the click doc and its `tokenIndex` entry.

It is not a conversation-history cleanup and must not delete used conversions.

## Failure-safety principles

1. Sheet write before backend ACK.
2. Dedupe before retry ACK.
3. Stable Adjustment Time across retries.
4. Version-aware adjustment ACK.
5. Transactional cleanup re-read.
6. Legacy Google Ads tracking remains independent during beta.
