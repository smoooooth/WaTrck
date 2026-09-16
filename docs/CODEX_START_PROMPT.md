# First Prompt to Give Codex

Use this **before** asking Codex to make its first WaTrck change.

Copy/paste the block below into the first Codex session for this repository.

```text
You are working on the production repository smoooooth/WaTrck.

The permanent source-of-truth branch is main.

Before doing anything else:

1. Confirm the repository is smoooooth/WaTrck and the current branch is main.
2. Fetch/inspect the latest main state.
3. Read the root AGENTS.md completely.
4. Read these files completely:
   - docs/00_READ_ME_FIRST.md
   - docs/CURRENT_STATE.md
   - docs/ARCHITECTURE.md
   - docs/OPERATING_RULES.md
   - docs/KNOWN_DISCREPANCIES.md
5. Then inspect the current production source files instead of relying only on the docs:
   - functions/index.js
   - Conversions/Conversions.js
   - Adjustments/Adjustments.js
   - Sales Sheet/Sales Sheet App Script.js
   - relevant Landing_page files
6. Summarize your understanding of:
   - the end-to-end WaTrck flow,
   - landing-page token/GCLID capture,
   - Firebase saveToken,
   - Firestore paths and state,
   - Meta → Firebase → Chatwoot webhook routing,
   - normal conversion exporter,
   - Sales Sheet Qualified/Closed flow,
   - RESTATE adjustment pipeline,
   - upload_version / last_adjustment_version_exported ACK invariant,
   - _testproduction_ isolation,
   - legacy Google Ads mechanisms that must remain untouched,
   - deployment ownership of every major file,
   - current known discrepancies and external state that cannot be proven from GitHub.
7. Explicitly identify any place where the latest main code disagrees with these docs.
8. Do not edit, commit, push, deploy, delete, or migrate anything yet.

Working rules for this project:
- GitHub main is the source of truth for code.
- Make surgical changes only.
- One controlled test at a time.
- Do not refactor unrelated code.
- Do not change LEGACY_GOOGLE_ADS_SEND_TO or LEGACY_LEAD_FORM_GOOGLE_ADS_SEND_TO unless I explicitly request it.
- Do not rename the intentionally misspelled secret conversions_exports_secrect.
- Do not weaken the adjustment version-aware ACK logic.
- Do not remove _testproduction_ isolation.
- Do not assume external Google Ads, Meta, Chatwoot, Apps Script, or Cloud Scheduler configuration from GitHub alone.
- If exact external configuration is required, tell me precisely what screen/value must be checked.

For now, only inspect and summarize. No changes.
```

---

# Prompt Template for a Code Change

After Codex has correctly summarized the system, use:

```text
Task:
<describe exactly one change>

Before editing:
1. Inspect the exact current file(s) on main.
2. Read the subsystem-specific docs referenced by AGENTS.md.
3. State the exact current behavior.
4. State the smallest proposed change.
5. List which files/mechanisms you will touch.
6. List which production mechanisms you will NOT touch.
7. State the first single controlled test you will run after the change.
8. Flag any external configuration that must be verified separately.

Then make only the required code change.

After editing:
- show the diff,
- run the controlled test if the environment supports it,
- report the result,
- do not deploy unless I explicitly ask.

Do not refactor unrelated code.
Do not alter legacy Google Ads conversions.
Do not rename conversions_exports_secrect.
Do not weaken upload_version adjustment ACK safety.
Do not disable _testproduction_ isolation.
```

---

# Prompt Template for Investigation Only

```text
Investigate this issue without editing anything:

<issue>

Read AGENTS.md and the relevant WaTrck docs first, then inspect current main.

Give me:
1. the observed current behavior from code,
2. the most likely failure point,
3. evidence from the exact current source,
4. which external configuration, if any, cannot be proven from GitHub,
5. the single best next diagnostic step,
6. no speculative redesign.

Do not edit or deploy anything.
```

---

# Prompt Template for a Deployment

```text
The code change has already been reviewed and I want deployment guidance.

Read:
- AGENTS.md
- docs/DEPLOYMENT.md
- docs/FILES_AND_OWNERSHIP.md
- the subsystem-specific docs

Then tell me exactly:
1. which component owns this deployment,
2. which directory/UI I must use,
3. exact Windows CMD commands or click-by-click UI steps,
4. which secrets/config must remain unchanged,
5. which other components must NOT be redeployed,
6. the single post-deploy validation test,
7. rollback procedure if that test fails.

Do not deploy unrelated components.
```

---

# Prompt Template for a Google Ads / Meta / Apps Script External Configuration Task

```text
This task concerns external configuration, not just repository code:

<task>

Use the repository docs to understand the intended architecture, but do not assume GitHub proves the current live external state.

First tell me exactly what I should open in the external UI and what values you need me to verify.
Then compare those live values to the architecture.
Do not recommend code changes until we know whether the problem is actually external configuration.
```

---

# Prompt Template for Reviewing a Proposed Codex Patch

```text
Review this proposed change against WaTrck's production invariants.

Check specifically:
- legacy Google Ads conversion preservation,
- exact project/conversion/sheet/secret names,
- write-before-ACK ordering,
- normal conversion dedupe,
- stable Adjustment Time retry behavior,
- upload_version version-aware ACK,
- _testproduction_ isolation,
- Firestore path compatibility,
- deployment ownership,
- whether the change touches anything outside the stated task.

Do not edit yet. Report risks and the minimum correction required.
```
