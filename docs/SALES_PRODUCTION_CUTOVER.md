# Move automatic lead import to the production Sales workbook

Prepared 2026-10-30. Automatic delivery has been confirmed on the copied Sales
workbook. This runbook has NOT yet been executed against the original workbook.
The new column styling needs one visual check on the copy first.

## Destinations and files

| Item | Confirmed identifier |
| --- | --- |
| Production Sales workbook | `1U4509WWjTjlddPorHusPMuxnCdFZqkSK94zwSqbySVw` |
| Tested Sales copy / destination rollback | `159PCMRa4HPBRFuIKRDMB0qdwFn8PylK-T2GceUhCeAk` |
| Conversions Apps Script project | `16o8aB_BH0RHuI1VltuEpMjcARrNdEWJQuDY5c_dH6R8Y6fVRso5fBC8M` |
| Existing Scheduler job | `Export_Conversions_To_Sheet`, region `europe-west4`, project `aida-muscat-wa-tracking` |

Open the [production Sales workbook](https://docs.google.com/spreadsheets/d/1U4509WWjTjlddPorHusPMuxnCdFZqkSK94zwSqbySVw/edit)
or the [tested copy](https://docs.google.com/spreadsheets/d/159PCMRa4HPBRFuIKRDMB0qdwFn8PylK-T2GceUhCeAk/edit)
as specified in each step. Do not move tabs out of the copy to replace production
tabs: production may contain newer Sales edits.

Repository root: `D:\Real Estate Projects\Advanced Tracking System\New\WaTrck`.

| File relative to repository root | Where it belongs | Keep deployed? |
| --- | --- | --- |
| `Conversions/Conversions.js` | Existing Conversions project's main Code.gs file | Yes |
| `Sales Sheet/Lead Import.js` | Lead Import.gs in the SAME Conversions project | Yes |
| `Sales Sheet/Prepare Attribution Columns.js` | Separate temporary file in that SAME project | Only during preparation |
| `Sales Sheet/Sales Sheet App Script.js` | Original Sales workbook's own bound project | Keep its existing quality handler; do not replace just for this cutover |
| `Sales Sheet/Google Ads Name Lookup.js` | Google Ads Scripts, later | No; deferred |
| `Helping Functions/*` | Local tooling only | Never paste into Apps Script |

`functions/test-lead-cleanup.js` is required backend production code despite its
name; do not delete it. No Firebase deployment is required for the formatting
and importer-helper cleanup in this handover. Backend feed and cleanup changes
were already deployed and exercised by the operator.

## 1. Protect the current workbook and confirm the runner

1. In the original Sales workbook, use **File → Make a copy** for a current backup.
   Keep the original workbook ID as the eventual destination.
2. Open the **Conversions workbook → Extensions → Apps Script**. In **Project
   Settings**, verify the Script ID against the table above. Do not perform these
   importer steps in the Sales workbook's bound project.
3. Record the current deployed version and Web App URL from **Deploy → Manage
   deployments**. Preserve existing `SHEET_FILE_ID` and `EXPORT_SECRET`; do not
   copy secrets into documentation/chat.
4. In **Project Settings → Script properties**, set `SALES_IMPORT_ENABLED=false`.
   Leave `SALES_SHEET_FILE_ID` pointing at the tested copy for now. Set
   `SALES_INCLUDE_TEST_USERNAME=false` for normal operation.
5. Let any already-running exporter finish before layout changes. The helper's
   ScriptLock also coordinates with the shared runner. Conversion export remains
   enabled; this property only disables its Sales step.

## 2. Check the new styling on the copy

1. Replace **Lead Import.gs in the Conversions project** with the current contents
   of `Sales Sheet/Lead Import.js`. Save. This removes retired trial functions;
   the scheduled entry point stays `runSalesImportForExistingJob_`.
2. Add/replace a separate **Prepare Attribution Columns.gs** with the current
   preparation file. Confirm **Services → Google Sheets API** is present with
   identifier `Sheets`; use **Services → + → Google Sheets API → Add** if absent.
3. Select `previewAttributionLayouts` in the editor's function dropdown and
   click **Run**. Expect the already-prepared copy tabs; investigate any `blocked`.
4. Run **`prepareAidaAttributionLayout`** once. Open the copy's AIDA_Oman tab.
   Confirm I1 looks exactly as before, J:M are 120px wide with dark grey 4 fill,
   white 10pt text and no bold/italic. Confirm the same styling at W:Z. Existing
   A:I values, quality dropdowns, USD values and feedback must remain intact.
5. Check a known matching month/year/quality selection: controls O2:O4, formula
   N8, filtered Table N7:Z. Also inspect a no-match selection. Do not interpret
   a legitimate FILTER `#N/A` as missing data or overwrite the formula.
6. Only after this check passes, run **`prepareRemainingAttributionLayouts`**.
   Existing prepared tabs report `formatted`; they must not shift again.

If any check fails, stop here with Sales imports disabled. These are manual
editor runs; they do not require updating the Web App deployment yet.

## 3. Select production and verify Config

1. In the **Conversions** project's Script properties change only
   `SALES_SHEET_FILE_ID` to:

   ```text
   1U4509WWjTjlddPorHusPMuxnCdFZqkSK94zwSqbySVw
   ```

2. Keep `SALES_IMPORT_ENABLED=false` and `SALES_INCLUDE_TEST_USERNAME=false`.
   Do not change `SHEET_FILE_ID` or `EXPORT_SECRET`.
3. Open the original Sales workbook's Config tab. A1 must be `TabName`, D1 must
   be `Contact_Conversion_Name`. Preserve B/C Qualified/Closed action names.
   Compare A/D to the operator-supplied mappings:

   | TabName | Contact_Conversion_Name |
   | --- | --- |
   | AIDA_Oman | AIDA Oman_WaTrck_Contact |
   | Tr_Plaza_Jeddah | Trump Plaza Jed_WaTrck_Contact |
   | Tr_Park_Residences | Trump Park Res_WaTrck_Contact |
   | Padel_Living | Padel Living_WaTrck_Contact |
   | Simaisma_Trump_Res | Simaisma Trump Res_WaTrck_Contact |
   | Vision_Iconic_Tower | Vision Iconic Tower_WaTrck_Contact |

   Retain any legitimate later projects with their exact configured Contact
   names. Do not infer names from tab labels or add a Project ID column.

## 4. Prepare the original Tables

1. Run **`previewAttributionLayouts`** from the Conversions editor. Confirm the
   destination property first. Original tabs should report `ready`; previously
   prepared tabs may report `alreadyPrepared`. Any `blocked` needs inspection.
2. Run **`prepareAidaAttributionLayout`**. Check original AIDA_Oman's I1, data,
   styling, dropdowns and filter as in step 2. Original Table row counts are
   preserved; they need not equal the copy's 1117/419.
3. Run **`prepareRemainingAttributionLayouts`** only after that check succeeds.
4. Inspect all affected tabs. The leads Table must include A:M and the filtered
   Table N:Z, with N8/O2:O4 for formula/controls. All original Sales details
   must remain. Do not insert columns manually after a verification error.

## 5. Preview, then import once

1. Run **`previewSalesLeadImport`**. It reads production but performs no writes.
   Require `unmappedLeads:0`, `skippedTabs:[]`, and no errors. Counts depend on
   current data; do not expect the historical 37 or 43 exactly.
2. If it reports duplicate tokens, stop. Identical duplicates were removed from
   the tested copy, which does not prove the original is clean. Compare the
   matching ORIGINAL A:M lead rows, including Sales notes, quality and value.
   Resolve only a verified accidental duplicate, preserving the intended row
   and any Sales details. Do not delete an entire worksheet row, which would
   also move the adjacent filter table. The importer does not delete duplicates.
3. Run **`importSalesLeadsOnce`**. This manual function writes even while the
   scheduled Sales flag is false. Verify the intended project tabs and one newly
   added row: correct token/phone (or username fallback), yellow review labels,
   attribution notes, and untouched G:H/I.
4. Run **`previewSalesLeadImport`** again. Expect zero new rows for the same feed;
   genuinely new leads arriving between runs can legitimately increase it.

Do not clear production Sales data to force a test. Existing unmatched rows and
previously imported test rows remain; backend cleanup never deletes Sheet rows.

## 6. Keep only required code in Google

1. After successful preparation, remove the temporary **Prepare Attribution
   Columns.gs** file from the Conversions Apps Script project. Keep its source
   in the repository for preparing future tabs.
2. Remove any temporary **Cleanup Preview** or ad-hoc test-only file created
   during this rollout, after checking that it contains only that helper. Keep
   the permanent main Code.gs and Lead Import.gs. Do not delete unrelated code
   or split the diagnostic functions out of the main source.
3. In the copied Sales workbook's old Apps Script project, open **Triggers**
   (clock icon). If `syncVisionLeadsScheduled` exists, delete only that trigger.
   Do not delete production Sales `onEditTrigger` or unrelated triggers. Old
   import-only files in the copy can then be removed; no import code is needed
   inside either Sales workbook's bound project. Other owners must remove any
   triggers they installed themselves.
4. In the original Sales workbook's own Apps Script project, confirm its existing
   installable `onEditTrigger` and `EXPORT_SECRET` are present. Keep the existing
   quality workflow. Do not install a duplicate trigger or paste Conversions code
   into it. Qualification edits belong in the original leads Table, not FILTER
   output. This cutover does not require changing that script.
5. Retain the optional `SALES_INCLUDE_TEST_USERNAME` property with value `false`.
   The last-execution diagnostic is useful production observability and stays.

## 7. Deploy the lean Conversions project and enable the job's Sales step

1. In the Conversions project use **Deploy → Manage deployments → select the
   currently working Web App → Edit (pencil) → Version: New version → Deploy**.
   Keep its current access/execution settings. Saving the editor alone does not
   update a versioned `/exec` deployment.
2. In Google Cloud Console, select project **aida-muscat-wa-tracking → Cloud
   Scheduler → Export_Conversions_To_Sheet** (`europe-west4`). Compare its target
   URL with that deployment's `/exec` URL. They must match. Do not use a stale
   URL copied from earlier chat. If intentionally creating a different deployment,
   update this SAME job's URL to the new one; do not create another job.
3. Set `SALES_IMPORT_ENABLED=true` in the Conversions Script properties.
4. In Scheduler, choose **Force run** on the existing job. Wait for its Apps
   Script Web App execution to finish; do not manually run doGet/runOnceFetch
   during this check, because that would obscure whether Scheduler worked.
5. In the Conversions editor run **`inspectLastWebExport`**. Require a fresh
   timestamp matching the forced job, `status:completed`, the Script ID above,
   the PRODUCTION Sales workbook ID, `salesImportEnabled:"true"`,
   `includeTestUsername:"false"`, zero unmapped leads and zero skipped tabs.
6. Confirm the Sales workbook contents and check both task results. A Scheduler
   HTTP 200 alone is insufficient. Observe the next scheduled execution and a
   genuinely new eligible lead appearing once in the intended production tab.

The existing five-minute frequency was verified earlier. A two-minute frequency
was discussed but not confirmed. Keep the current frequency during cutover.
Changing it later is a separate controlled Scheduler edit, not an Apps Script
timer installation.

## 8. Finish and record the live state

- If the backend cleanup job was paused for simulation, resume that existing job.
  Its confirmed target is `cleanupFirestoreHttp?scope=all&dryRun=false`, method
  GET, using the existing secret header. Its job name/schedule were not supplied;
  identify it by target rather than guessing. Do not expose the secret.
- Keep the username test toggle false. It does not control backend cleanup.
- Record the final deployed version, verified destination, actual cron and
  successful scheduled execution in CURRENT_STATE.md. Do not mark this cutover
  complete until those checks have passed.
- Keep the backup and tested copy until production is confirmed. Nothing in this
  runbook requires deleting either workbook or historical Sheet rows.

## Stop / rollback

Set `SALES_IMPORT_ENABLED=false` in the Conversions project to stop future Sales
imports while conversions continue. An already-running invocation may finish.
Wait for it before changing the destination. To return to the tested copy,
restore its `SALES_SHEET_FILE_ID`, confirm a preview, then re-enable deliberately.

Neither disabling import nor switching destinations undoes cells already
written. For a layout failure, keep Sales imports disabled and use the pre-change
backup/version history to recover only the affected changes after checking for
new Sales edits. Do not blindly restore a whole old workbook over newer work.
If the Apps Script code itself fails, the previously recorded Web App version
provides a code rollback through Manage deployments.
