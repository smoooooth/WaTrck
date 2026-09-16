# Firestore Data Model and State Machine

## Primary click document

Path:

```text
projects/<projectId>/clicks/<TOKEN>
```

The document ID is the WaTrck token.

Example:

```text
projects/AIDA_Oman/clicks/ABC1234
```

## Initial click fields

`saveToken` currently initializes fields including:

```text
token
gclid
page
ts
projectId
ctaId
used
lead_value_estimate
conversion_name
conversion_value_initial
conversion_value_final
conversion_value_uploaded
conversion_value_uploaded_at
conversion_value_source
upload_version
adjustment_pending
google_campaign_id
google_adgroup_id
google_ad_id
```

Typical initial state:

```json
{
  "token": "ABC1234",
  "used": false,
  "lead_value_estimate": 70,
  "conversion_name": "project-specific WaTrck Contact action",
  "conversion_value_initial": null,
  "conversion_value_final": null,
  "conversion_value_uploaded": false,
  "upload_version": 0,
  "adjustment_pending": false
}
```

## `tokenIndex`

Path:

```text
tokenIndex/<TOKEN>
```

Current fields:

```text
token
projectId
clickPath
created_at
```

Purpose: map a token arriving from WhatsApp to the correct project/click without scanning every project.

## `whatsappSenderIndex`

Path:

```text
whatsappSenderIndex/<conversationId>
```

Current fields include:

```text
projectId
whatsapp_sender_id
updated_at
```

Purpose: route later tokenless messages from a previously known sender to the correct project conversation backup.

It must not create a new conversion by itself.

## WhatsApp conversations

Conversation document:

```text
projects/<projectId>/whatsapp_conversations/<conversationId>
```

Current metadata includes:

```text
projectId
last_active
last_message_preview
whatsapp_sender_id
phone_number
whatsapp_bsuid
whatsapp_username
whatsapp_profile_name
```

Message subcollection:

```text
projects/<projectId>/whatsapp_conversations/<conversationId>/messages/<msgId>
```

Message fields include:

```text
projectId
from
whatsapp_sender_id
whatsapp_phone
whatsapp_bsuid
whatsapp_username
whatsapp_profile_name
msg_id
text_body
message_type
meta_timestamp
stored_at
raw_payload
contact_payload
```

## WhatsApp identity derivation

The webhook derives a phone when `msg.from` or `contact.wa_id` looks like a phone number.

It derives a stable/business-scoped user identity from candidates such as:

```text
msg.from_user_id
msg.user_id
contact.user_id
```

and non-phone fallbacks.

Current sender preference is effectively:

```text
whatsappBsuid
→ whatsappPhone
→ rawFrom
```

The resolved sender is stored as `whatsapp_from` on the click after token match.

## Contact state transition

Before WhatsApp:

```text
used = false
```

After a valid token-bearing message:

```text
used = true
used_at = server timestamp
conversion_value_source = "whatsapp_message"
```

plus WhatsApp identity fields.

`used=true` is the fundamental Contact eligibility signal.

## Export-related fields

Important state includes:

```text
conversion_value_uploaded
conversion_value_uploaded_at
google_sheet_exported
google_sheet_export_job
exported_once
sales_sheet_quality_uploaded
sales_sheet_last_uploaded_at
```

These fields have evolved. Never simplify them without reading `/exports/pending`, `/exports/mark-exported`, and `Conversions/Conversions.js` together.

## Sales quality fields

`/sales/quality-update` writes/uses:

```text
quality_status
sales_sheet_updated_quality
sales_sheet_quality_uploaded
updated_by_sales_at
conversion_value_uploaded
adjustment_pending
upload_version
conversion_name_sales
conversion_name_sales_qualified
conversion_value_final
```

Quality codes:

```text
0 = unqualified
1 = qualified
2 = closed
```

## `upload_version`

`upload_version` is the logical adjustment revision.

Initial:

```text
0
```

Each accepted Sales update increments it:

```text
current + 1
```

`queueAdjustment` also increments it.

This field is central to concurrency safety, not decorative metadata.

## `adjustment_pending`

A Sales/value update requiring an adjustment sets:

```text
adjustment_pending = true
```

Backend pending logic compares:

```text
upload_version
```

against:

```text
last_adjustment_version_exported
```

Only newer versions should be exportable.

## Critical version-race example

Initial:

```text
upload_version = 1
last_adjustment_version_exported = 0
adjustment_pending = true
```

Exporter fetches v1.

Before v1 ACK, Sales creates v2:

```text
upload_version = 2
adjustment_pending = true
```

Then v1 ACK arrives.

Correct:

```text
newLastExported = max(0, 1) = 1
stillPending = 2 > 1 = true
```

Final:

```text
last_adjustment_version_exported = 1
adjustment_pending = true
```

Incorrect unconditional `adjustment_pending=false` would lose v2.

## Conversion-name fields

### Contact

```text
conversion_name
```

Source: landing-page `WHATSAPP_CONVERSION_NAME`.

### Current positive sales stage

```text
conversion_name_sales
```

Source: Sales Sheet Config.

### Preserved Qualified identity after Closed transition

```text
conversion_name_sales_qualified
```

The backend preserves the prior Qualified action name when moving a previously Qualified lead to Closed.

## Values

### Contact/initial

Landing pages send `lead_value_estimate`, currently `70` for inspected projects.

Backend/export logic may fall back through `conversion_value_initial`, `lead_value_estimate`, and default lead value.

### Sales/final

Sales writes `conversion_value_final`.

Unqualified:

```text
0
```

Qualified/Closed:

```text
numeric Sales value
```

## Do not model the funnel as one mutable conversion

One lead may legitimately generate:

```text
Contact
Qualified
Closed
```

These are separate conversion occurrences. Adjustments change values where required; they do not erase the historical milestones.
