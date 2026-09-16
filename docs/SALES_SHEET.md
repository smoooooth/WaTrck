# Sales Sheet Integration

Source:

```text
Sales Sheet/Sales Sheet App Script.js
```

Backend:

```text
POST https://us-central1-aida-muscat-wa-tracking.cloudfunctions.net/exportsApi/sales/quality-update
```

## Purpose

Sales Sheet turns human lead assessment into structured WaTrck funnel state and supplies the exact Qualified/Closed conversion action identity.

## Current columns

1-based indexes:

```text
E / 5 = Token
F / 6 = Date
G / 7 = Quality
H / 8 = Value
I / 9 = Feedback / Message
```

The script reacts only to edits in Quality column `G`.

## Config tab

Current expected layout:

```text
TabName | QualifiedConv | ClosedConv
```

Code reads:

```text
A = tab name
B = Qualified conversion action
C = Closed conversion action
```

There is no current active code path for a fourth Contact conversion column. If one exists in the sheet, it is informational unless code is deliberately extended.

## Installable trigger

Handler:

```text
onEditTrigger
```

It must be an **installable** onEdit trigger.

Helper:

```text
installOnEditTrigger
```

removes existing triggers for the handler and creates a new one.

Do not replace this with a simple trigger without reviewing Apps Script authorization requirements.

## Accepted quality values

Unqualified:

```text
0
unqualified
```

Qualified:

```text
1
qualified
```

Closed:

```text
2
closed
closed sale
```

## Transition rules

Blocked:

```text
Qualified → Unqualified
Closed → Qualified
Closed → Unqualified
```

Allowed:

```text
New → Unqualified
New → Qualified
New → Closed
Qualified → Closed
```

Do not remove the UI block alone if future product logic needs backward movement. The conversion/adjustment semantics would also need a design change.

## Value validation

Unqualified automatically sends `0`.

Qualified and Closed require a numeric value.

Validation exists both in the Apps Script and backend.

## Conversion-name selection

Qualified uses:

```text
cfg.qualified
```

Closed uses:

```text
cfg.closed
```

The chosen value is sent as payload `conversion_name` and backend stores it as `conversion_name_sales`.

## Typical payload

```json
{
  "token": "ABC1234",
  "quality": "qualified",
  "value": 500,
  "conversion_name": "exact Qualified Google Ads action name"
}
```

Auth:

```text
x-export-secret: EXPORT_SECRET
```

## Backend behavior

Quality mapping:

```text
unqualified → 0
qualified   → 1
closed      → 2
```

Backend updates quality/final value, normal conversion pending state, adjustment pending state, and increments `upload_version`.

## Qualified → Closed preservation

When a previously Qualified lead becomes Closed, backend attempts to preserve the prior Qualified conversion name in:

```text
conversion_name_sales_qualified
```

This keeps the prior stage identity available instead of overwriting all history with Closed.

## UI feedback

During request:

```text
loading...
```

Success:

```text
Lead Published: <quality> : $<value>
```

Error:

```text
ERROR. ...
```

The script attempts to restore the previous quality on failure, reducing sheet/backend divergence.

## Security

Required Script Property:

```text
EXPORT_SECRET
```

Never hard-code or place it in sheet cells/documentation.

## Exact-name requirement

Every configured Sales tab must contain exact Qualified/Closed action strings. A typo can produce a row Google Ads filters out or cause a later adjustment to target the wrong action.
