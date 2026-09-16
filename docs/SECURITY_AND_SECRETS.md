# Security, Secrets, and Identity Rules

## 1. Never store secret values in GitHub

This context package intentionally contains secret **names**, not their values.

Do not commit:

- Meta access tokens,
- Meta app secret,
- WhatsApp verify-token value,
- Google OAuth refresh/access tokens,
- Firebase service-account JSON,
- private keys,
- Apps Script export-secret value.

## 2. Backend Secret Manager names

Current backend reads:

```text
whatsapp_verify_token
```

and:

```text
conversions_exports_secrect
```

The second spelling is production-significant and intentionally preserved.

Do not rename it to:

```text
conversions_exports_secret
```

merely to correct spelling.

## 3. Apps Script secret

Normal conversions, adjustments, and Sales Sheet scripts use Script Property:

```text
EXPORT_SECRET
```

The value must match the backend export secret.

Do not put it in:

- source code,
- sheet cells,
- GitHub,
- documentation,
- screenshots shared outside the authorized team.

## 4. Backend API authorization

`exportsApi` checks the provided secret from:

```text
req.query.secret
```

or:

```text
x-export-secret
```

Production scripts use the header form.

Prefer headers over query strings because URLs are more likely to be exposed in logs/history.

## 5. WhatsApp verify token is a distinct credential

Webhook verification compares Meta's:

```text
hub.verify_token
```

to Secret Manager:

```text
whatsapp_verify_token
```

This is not the same as:

- Meta app secret,
- Meta access token,
- WABA ID,
- Phone Number ID,
- Chatwoot token.

Do not substitute one for another.

## 6. Chatwoot forwarding signature

Firebase forwards Meta's original:

```text
x-hub-signature-256
```

to Chatwoot along with the raw request body.

Do not synthesize a different signature unless the entire validation design is intentionally changed.

## 7. IDs should remain strings

Keep identifiers as strings unless an external API explicitly requires otherwise.

This includes:

```text
GCLID
campaign ID
ad group ID
ad ID
project ID
token
WABA ID
phone-number ID
Google IDs
```

JavaScript numeric coercion can create precision and formatting errors.

## 8. Account identity / authorization

Past setup work involved multiple Google identities and a denied Google Ads API production access application.

Do not build mechanisms intended to conceal account relationships or bypass Google restrictions.

Infrastructure should use legitimate authorized business/client ownership and access.

## 9. Google Ads API status

A previous Explorer Access application was denied.

Therefore:

- do not assume production Google Ads API access,
- do not introduce a production dependency on it without first verifying access has changed.

Current design avoids needing it.

## 10. Public repository awareness

Treat the repository as potentially visible to people beyond the operator.

Existing source contains some operational URLs/IDs/phone numbers. Their presence is not permission to add new secrets.

Before committing new operational metadata, ask whether it truly belongs in source control.

## 11. Secret rotation

If an export secret is rotated, all of these must stay synchronized:

```text
Secret Manager backend secret
Conversions Apps Script EXPORT_SECRET
Adjustments Apps Script EXPORT_SECRET
Sales Sheet Apps Script EXPORT_SECRET
```

Rotate as one controlled operation and test each client separately.

## 12. Do not log sensitive payloads unnecessarily

WhatsApp message/contact payloads can contain personal information.

Current code stores raw message/contact payloads for the organized Firestore backup.

Before expanding logging or exports, evaluate privacy/data-retention consequences.

Do not add full payload console logging casually in production.
