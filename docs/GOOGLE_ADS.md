# Google Ads Integration

## Principle

WaTrck uses offline conversion ingestion to represent actual WhatsApp outcomes while preserving pre-existing direct Google Ads conversions.

Do not treat WaTrck as an automatic replacement for the legacy conversion setup.

## Funnel events

```text
Contact
Qualified
Closed
```

Unqualified is an adjustment outcome, not a new conversion action.

## Values

Current business design:

```text
Contact:   70 USD
Qualified: typically 500 USD, with Sales-provided value authoritative for the row
Closed:    actual closed value
```

## Legacy conversion protection

Landing pages contain:

```text
LEGACY_GOOGLE_ADS_SEND_TO
LEGACY_LEAD_FORM_GOOGLE_ADS_SEND_TO
```

Legacy Contact currently measures WhatsApp button click intent.

WaTrck Contact waits for actual WhatsApp token match.

Therefore:

```text
Legacy Contact = click intent
WaTrck Contact = actual message/contact
```

Keep legacy actions during beta unless explicitly migrating them.

## New WaTrck actions

Operational design is four projects × three stages = 12 WaTrck conversion actions.

Current Contact names verified in code:

```text
AIDA Oman | WaTrck | Contact
Trump Plaza Jed | WaTrck | Contact
Trump Park Res | WaTrck | Contact
Padel Living | WaTrck | Contact
```

Qualified/Closed names are external in Sales Sheet Config and must be read there exactly.

## Shared Data Manager source

Normal actions share `Conversions_Sheet`.

Each action should use an exact action-specific filter:

```text
Conversion Name
Equals
<exact conversion action name>
```

Do not assume Google Ads auto-routes rows only because the row text matches the action's display name.

The filter is the explicit routing guard over the shared source.

## Normal field mapping

Conceptually:

```text
GCLID / Google Click ID → Google Click ID
Conversion Time         → Conversion date/time
Value                   → Conversion value
Currency                → Currency
Order ID                → Transaction ID / Order ID
```

UI labels change over time, so inspect the current Google Ads mapping screen before editing.

Operational/audit columns should not be mapped just because they exist.

## Why Project ID is not enough as the action filter

A project has multiple stages.

Example:

```text
AIDA_Oman
```

can produce Contact, Qualified, and Closed rows.

A project-only filter would not isolate one action. `Conversion Name` does.

## Conversion-name source chain

### Contact

```text
landing WHATSAPP_CONVERSION_NAME
→ Firestore conversion_name
→ Conversions exporter
→ sheet Conversion Name
→ Google Ads filter/action
```

### Qualified/Closed

```text
Sales Sheet Config
→ Sales Script payload conversion_name
→ backend conversion_name_sales
→ Conversions exporter
→ sheet Conversion Name
→ Google Ads filter/action
```

Exact-string matching matters. Punctuation, spaces, and casing should be treated like identifier data.

## Goal/bidding safety during beta

Historical operating decision:

- leave old actions/goals in place,
- keep new WaTrck actions out of active bidding until intentionally promoted,
- do not casually add WaTrck actions to campaign custom goals.

Google Ads may force the first action in a goal/category to Primary when no alternative exists. If that happens, bidding impact still depends on whether that goal is selected/account-default/custom-goal-active.

Do not create fake/dummy actions solely to manipulate Primary/Secondary. Control goal inclusion and verify live campaign goal settings.

This state is external to GitHub.

## Count/value design

Historical intended WaTrck action behavior:

```text
Count: One
Value: different values
Currency: USD
```

Long-cycle Qualified/Closed actions were intended to use suitable click-through windows and data-driven attribution where available.

Do not blindly rewrite live settings from this document; inspect the current account first.

## Order ID

WaTrck uses the click document/token as Order ID.

Do not replace it with a random export-time identifier because adjustments and retry identity depend on stable linkage.

## Adjustments

Google-facing tab:

```text
Google_Ads_Adjustments_Upload
```

Google Ads path:

```text
Goals
→ Conversions
→ Uploads
→ Schedules
```

Rows use adjustment-specific fields and:

```text
Adjustment Type = RESTATE
```

## Adjustment semantics

Unqualified:

```text
original Contact → RESTATE to 0
```

Qualified value update:

```text
Qualified action → RESTATE current value
```

Closed value update:

```text
Closed action → RESTATE current value
```

## Google Ads API decision

A prior Explorer Access application was denied; test access remained, production access was restricted.

Production architecture deliberately avoids requiring Google Ads API and instead uses:

```text
Google Sheets
Apps Script
Data Manager
Google Ads Upload schedules
```

Do not redesign around Google Ads API unless access and product direction explicitly change.

## Synthetic-test limitation

A `_testproduction_` GCLID can test internal WaTrck behavior. It cannot prove Google recognizes the click or accepts a real conversion/RESTATE.

Final proof requires real Google Ads identifiers and an imported original conversion.
