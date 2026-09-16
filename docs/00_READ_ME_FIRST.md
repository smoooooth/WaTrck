# Read Me First — WaTrck Context Guide

This directory gives Codex durable project context without relying on old conversation history.

## Repository identity

Repository: `https://github.com/smoooooth/WaTrck`  
Permanent working branch: `main`

## Verified snapshot

These documents were prepared from current `main` observed on **2026-09-16**.

Code snapshot used during preparation:

```text
main commit:
d41b5274f94d8bf9aa29308a468c8fb98872ae16
```

Important file SHAs at that snapshot:

```text
functions/index.js
5774e56801cddf64281abb18f6badd21b916b58d

Conversions/Conversions.js
c39a323f6f34285fa07e7bfde56abaf1c22abe76

Adjustments/Adjustments.js
9a4f3a9b227958c7be76e8a35df26a2920a9321b

Sales Sheet/Sales Sheet App Script.js
990154f1291fd9d9d42d8c0bebe6f3e0eb294a57
```

Adding these documentation files will create a newer `main` commit. These SHAs are the code version the context was verified against, not permanent required SHAs.

## How Codex should use these documents

Always read:

- `AGENTS.md`
- `docs/CURRENT_STATE.md`
- `docs/ARCHITECTURE.md`
- `docs/OPERATING_RULES.md`

Then read by task.

Webhook / WhatsApp:

- `WHATSAPP_WEBHOOK.md`
- `DATA_MODEL.md`
- `SECURITY_AND_SECRETS.md`

Google Ads / conversions:

- `GOOGLE_ADS.md`
- `CONVERSIONS_EXPORTER.md`
- `ADJUSTMENTS.md`

Sales Sheet:

- `SALES_SHEET.md`
- `ADJUSTMENTS.md`
- `DATA_MODEL.md`

Deployments:

- `DEPLOYMENT.md`
- `FILES_AND_OWNERSHIP.md`

Testing:

- `TESTING.md`
- `TROUBLESHOOTING.md`

Historical rationale:

- `HISTORY_AND_DECISIONS.md`

Potential stale/conflicting state:

- `KNOWN_DISCREPANCIES.md`

## Precedence rules

If information conflicts, use:

1. Current `main` code
2. Explicitly confirmed live production configuration
3. `CURRENT_STATE.md`
4. Other context documents
5. Historical decisions
6. Old conversation assumptions

Do not change current code to conform to outdated documentation. Report the mismatch and determine which state is intended.

## Working philosophy

```text
Understand existing mechanism
→ isolate the smallest required change
→ preserve unrelated mechanisms
→ test one thing
→ verify persistence/ACK behavior
→ deploy only the relevant component
```

Avoid clean rewrites of working production code unless specifically requested.
