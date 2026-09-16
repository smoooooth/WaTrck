# Context Package Manifest

## Purpose

This package turns WaTrck architecture, invariants, operating decisions, and deployment knowledge into durable repository documentation for Codex.

It is **documentation only**. It does not contain replacement production code.

## Verification snapshot

Prepared against:

```text
repository: smoooooth/WaTrck
branch: main
code snapshot commit: d41b5274f94d8bf9aa29308a468c8fb98872ae16
date: 2026-09-16
```

Adding this documentation to `main` will naturally create a newer commit. The snapshot records the production-code state against which the docs were verified.

## Verified code fingerprints

```text
functions/index.js
SHA: 5774e56801cddf64281abb18f6badd21b916b58d

Conversions/Conversions.js
SHA: c39a323f6f34285fa07e7bfde56abaf1c22abe76

Adjustments/Adjustments.js
SHA: 9a4f3a9b227958c7be76e8a35df26a2920a9321b

Sales Sheet/Sales Sheet App Script.js
SHA: 990154f1291fd9d9d42d8c0bebe6f3e0eb294a57

Landing_page/DarGlobal/Aida/Aida Muscat.html
SHA: bd05d39df76b64c889c44855b70db876acb88a24

Landing_page/DarGlobal/Trump Plaza Jeddah.html
SHA: 9cd595338761deee6825e54b696065ee60a1c158

Landing_page/DarGlobal/Trump Park Residences.html
SHA: e4a44e0ad28c9a4ec5ab9bc8760e62b774f8af26

Landing_page/DarGlobal/Padel Living Residences.html
SHA: 4de292b7349b25404cc621a825ed921a9a95aacb
```

## Files in this package

### Root package files

#### `AGENTS.md`

The repository-wide Codex instruction layer.

Contains:
- startup procedure,
- source-of-truth precedence,
- hard production invariants,
- deployment ownership warnings,
- required reading by subsystem,
- preferred working style.

This **must go in the WaTrck repository root**.

#### `INSTALL_IN_REPO.md`

Human installation instructions for copying the package into the repo and committing it through GitHub Desktop.

This is a delivery helper. It does not have to be committed into WaTrck.

#### `PACKAGE_README.md`

Short explanation of the download package.

Delivery helper; optional in repo.

#### `PACKAGE_SHA256.json`

Generated SHA-256 hashes of the delivered package files so the package contents can be checked after download/extraction.

Delivery helper; optional in repo.

### `docs/` repository files

#### `00_READ_ME_FIRST.md`

Reading order, precedence, and verification snapshot.

#### `CURRENT_STATE.md`

Current system facts, endpoints, project names, current code configuration, and externally confirmed architecture.

#### `ARCHITECTURE.md`

Full end-to-end WaTrck flow from Google Ads click through WhatsApp and Sales to Google Ads offline conversions/adjustments.

#### `FILES_AND_OWNERSHIP.md`

Maps files to their deployment mechanism and warns where Git push does not equal deployment.

#### `DATA_MODEL.md`

Firestore paths, field meanings, conversion state, sender index, conversation backup, upload versions, and adjustment state machine.

#### `WHATSAPP_WEBHOOK.md`

Meta webhook verification, payload processing, token matching, sender identity, Firestore backup, and Firebase → Chatwoot bridge.

#### `GOOGLE_ADS.md`

WaTrck funnel actions, legacy conversion protection, shared Data Manager source, exact Conversion Name filters, goal/bidding safety, and scheduled adjustments.

#### `CONVERSIONS_EXPORTER.md`

Normal Apps Script exporter schemas, dedupe, write-before-ACK behavior, retries, locks, and test separation.

#### `ADJUSTMENTS.md`

RESTATE pipeline, both sheet schemas, target conversion logic, stable Adjustment Time, dedupe, and version-aware ACK invariant.

#### `SALES_SHEET.md`

Sales Sheet columns, Config layout, installable trigger, quality transitions, values, and backend payload behavior.

#### `TESTING.md`

Synthetic marker rules, stress-test results, race tests, webhook tests, regression checklists, and real-Google acceptance boundary.

#### `DEPLOYMENT.md`

GitHub Desktop, Firebase, gcloud legacy function, Apps Script, Sales Sheet, landing page, Meta webhook, Google Ads, and Cloud Scheduler deployment guidance.

#### `SECURITY_AND_SECRETS.md`

Secret names, credential hygiene, intentionally misspelled secret protection, signature handling, ID types, and account authorization notes.

#### `OPERATING_RULES.md`

Operator preferences: surgical changes, exact paths/commands, one controlled test, no silent redesign, and external-state verification.

#### `TROUBLESHOOTING.md`

Symptom-driven diagnostics across WhatsApp, Firestore, Apps Script, Sales, conversions, adjustments, Google Ads, cleanup, and CORS.

#### `HISTORY_AND_DECISIONS.md`

Architectural decisions and rationale so future work does not accidentally revisit/reverse deliberate choices.

#### `KNOWN_DISCREPANCIES.md`

Current mismatches or externally unverifiable states that Codex must confirm instead of auto-fixing.

#### `CODEX_START_PROMPT.md`

Ready-to-copy first-session prompt and templates for code changes, investigations, deployments, external configuration tasks, and patch review.

#### `CONTEXT_MANIFEST.md`

This file. Records package purpose, snapshot, fingerprints, and file inventory.

## Update rule

After any architecture-changing production update:

1. Update the actual code/system.
2. Test and verify it.
3. Update the relevant subsystem document.
4. Update `CURRENT_STATE.md` if production state changed.
5. Add architectural rationale to `HISTORY_AND_DECISIONS.md` when appropriate.
6. Add/remove items from `KNOWN_DISCREPANCIES.md` as migrations resolve or new ambiguities appear.
7. Refresh this manifest's code snapshot only during a deliberate context-package refresh.

Stale documentation should be treated as a production risk.
