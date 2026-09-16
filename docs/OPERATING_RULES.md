# Operating Rules for Codex

These rules capture the operator's preferred way of working and are part of the project's safety model.

## 1. GitHub `main` is source of truth

Whenever exact code matters:

1. inspect `main`,
2. fetch current state,
3. read the exact file,
4. do not answer from historical memory alone.

If docs disagree with current code, report the mismatch rather than silently rewriting code to match stale documentation.

## 2. Surgical changes

Preferred workflow:

```text
one small change
→ one controlled test
→ inspect result
→ only then continue
```

Avoid:

- broad refactors,
- formatting unrelated files,
- architecture redesign during bug fixes,
- mass renames,
- dependency upgrades unrelated to the task.

## 3. Explain integrations concretely

Use “who communicates with whom and doing what.”

Prefer:

```text
Meta
→ Firebase whatsappWebhook
→ token matching + Firestore
→ forwards same raw webhook to Chatwoot
```

instead of vague phrases such as “the integration layer handles it.”

## 4. Windows CMD guidance

The operator uses Windows CMD.

When commands are required:

- state which folder to open,
- provide `cd /d` command,
- use CMD-compatible syntax,
- do not silently give PowerShell-only commands.

Historical repo root:

```bat
cd /d "D:\Real Estate Projects\Advanced Tracking System\New\WaTrck"
```

## 5. GitHub Desktop guidance

When Git operations are needed, prefer explicit GitHub Desktop UI instructions where practical.

Example:

```text
Current Branch → main
Fetch origin
Changes → review
Summary → commit message
Commit to main
Push origin
```

## 6. Explicit UI navigation

For Meta, Google Ads, Firebase/GCP, Apps Script, etc., say exactly where to go.

Example:

```text
Google Ads
→ Goals
→ Conversions
→ Uploads
→ Schedules
```

Do not simply say “update the schedule.”

## 7. Do not ask unnecessary questions

If current code/context is sufficient for a safe answer, proceed.

If an external value is truly unknown, identify exactly what is unknown and where it can be found.

Do not ask the operator to repeat information already available in the repo/context.

## 8. Preserve legacy behavior unless explicitly requested

Especially preserve:

```text
LEGACY_GOOGLE_ADS_SEND_TO
LEGACY_LEAD_FORM_GOOGLE_ADS_SEND_TO
```

and the associated existing conversion mechanisms.

WaTrck is currently layered alongside them.

## 9. Protect historical data

Do not casually:

- rewrite old Conversions Sheet rows,
- bulk-rename historical Firestore `conversion_name`,
- mutate already-imported Google Ads conversions,
- delete WhatsApp conversation history,
- delete audit rows.

New configuration should normally govern new events only.

## 10. External state must be verified externally

The repository cannot prove current live state of:

- Google Ads goal/bidding configuration,
- Data Manager filters,
- upload schedules,
- Apps Script deployed versions,
- Script Properties,
- Cloud Scheduler jobs,
- Meta WABA/phone configuration,
- Chatwoot inbox state.

Use language such as:

```text
current code expects...
```

when GitHub is the only evidence.

## 11. State blast radius before editing

Preferred format:

```text
Touching:
Adjustments/Adjustments.js

Not touching:
functions/index.js
Conversions exporter
Sales Sheet
landing pages
legacy Google Ads events
```

This prevents accidental scope creep.

## 12. Every code change gets a test

After a patch, specify exactly one first controlled test.

Do not call a task complete merely because code compiles or looks correct.

## 13. Exact strings are often identifiers

Do not casually normalize:

- project IDs,
- conversion action names,
- sheet names,
- secret names,
- endpoint routes,
- Firestore paths.

Case, punctuation, spaces, and spelling can be meaningful.

## 14. Complete outputs

If not directly editing the repository and providing a replacement file, give the full file or an unambiguous patch.

Do not provide ambiguous fragments that are easy to paste into the wrong block.

## 15. Production-first mindset

When choosing between:

```text
cleaner rewrite
```

and:

```text
boring proven mechanism
```

prefer the proven mechanism unless the task explicitly asks for modernization.

## 16. Do not deploy without authorization

Code editing and deployment are separate decisions.

If asked to edit only, do not automatically deploy.

If asked to deploy, identify which subsystem and test plan first.

## 17. Before destructive actions

Before:

- deleting branches,
- deleting Firestore docs,
- deleting Sheet rows,
- removing Google Ads actions,
- deleting WhatsApp configuration,
- replacing scheduler jobs,

state what is being destroyed and what backup/rollback exists.
