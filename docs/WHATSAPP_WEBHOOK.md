# WhatsApp / Meta / Firebase / Chatwoot Webhook

## Correct routing model

```text
Meta WhatsApp
    ↓
Firebase whatsappWebhook
    ├── WaTrck token processing
    ├── Firestore conversation backup
    └── raw payload forwarding
            ↓
         Chatwoot
```

Meta must not bypass Firebase if WaTrck is expected to process the message.

## Firebase callback

Current endpoint:

```text
https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/whatsappWebhook
```

The important historical webhook fix was that the callback itself had to point to Firebase, not directly to the Chatwoot phone-number webhook.

## Verification GET

Meta supplies:

```text
hub.verify_token
hub.challenge
```

Firebase compares the verify token to Secret Manager:

```text
whatsapp_verify_token
```

and returns the challenge on success.

Conceptual test:

```text
GET /whatsappWebhook
?hub.mode=subscribe
&hub.verify_token=<verify-token>
&hub.challenge=123456
```

Expected body:

```text
123456
```

Never commit the real verify-token value.

## Chatwoot forwarding

Current code reads original:

```text
x-hub-signature-256
```

and forwards:
- original raw request body,
- `Content-Type: application/json`,
- original `X-Hub-Signature-256`.

Current Chatwoot URL in the verified snapshot:

```text
https://wa-chat.seaborn-properties.com/webhooks/whatsapp/+971586992542
```

Forwarding is intentionally fire-and-forget. A Chatwoot error is logged but should not block WaTrck processing.

## Message parsing

Webhook iterates:

```text
body.entry[]
→ entry.changes[]
→ change.value.messages[]
```

and reads `change.value.contacts[]` for profile/contact identity.

## Token extraction

Regex:

```js
/#([A-Z0-9]{4,12})/i
```

Landing pages currently generate seven characters. The webhook accepts a wider token length range.

Tokens are normalized to uppercase alphanumeric.

## Project resolution

Primary method:

```text
tokenIndex/<TOKEN>
```

Fallback for later conversation backup:

```text
whatsappSenderIndex/<conversationId>
```

A sender-index-only message may be saved, but it must not create a new conversion.

## Contact activation

After a valid token maps to a click, the click is updated with:

```text
used = true
used_at = server timestamp
whatsapp_from
whatsapp_phone
whatsapp_bsuid
whatsapp_username
whatsapp_profile_name
whatsapp_msg_id
conversion_value_source = "whatsapp_message"
```

This is the actual-contact signal.

## Conversation backup

Once project identity is known, WaTrck stores project-organized conversation history independently of Chatwoot.

Do not remove that backup solely because Chatwoot also stores chats.

## Manual alert test trigger

Current webhook intentionally contains:

```text
ForceTestError123
```

If the incoming text exactly matches it, code throws an `Alert System Test` error.

Do not delete this during unrelated cleanup without explicit approval.

## Production phone migration caution

Verified snapshot:

```text
Chatwoot bridge URL context: +971586992542
landing page destination:    +201034285454
```

Before changing landing pages to a real production number:

1. Confirm number onboarding/coexistence state.
2. Confirm Meta webhook still points to Firebase.
3. Confirm Firebase receives real inbound POST.
4. Confirm Firebase forwards to the intended Chatwoot inbox.
5. Confirm token-bearing message marks the Firestore click used.
6. Only then cut landing-page traffic over.

Avoid changing every layer simultaneously.

## WhatsApp Business app coexistence

Coexistence eligibility/configuration is external to GitHub. The code does not prove whether a number is app-only, Cloud-API-only, or coexistence.

Do not make code changes based on an unverified assumption about that state.

## Chatwoot reconnect risk

If Chatwoot onboarding changes Meta so the callback points directly to Chatwoot, WaTrck can stop receiving events.

After any WABA/Chatwoot reconnect:

1. re-check Meta callback,
2. send a controlled message,
3. verify Firebase logs,
4. verify Firestore click state,
5. verify Chatwoot receipt.

## Troubleshooting order

If Chatwoot receives a message but WaTrck does not:

1. Verify Meta callback is Firebase.
2. Verify Meta sends `messages` field events.
3. Verify Firebase function receives POST.
4. Verify token is present.
5. Verify `tokenIndex/<TOKEN>` exists.
6. Verify click path exists.
7. Inspect per-message processing logs.

If WaTrck works but Chatwoot does not:

1. Verify `CHATWOOT_WEBHOOK_URL`.
2. Verify current Chatwoot phone/inbox webhook path.
3. Look for `Chatwoot Forward Error (Ignored)` in Firebase logs.
4. Verify signature/header expectations.
