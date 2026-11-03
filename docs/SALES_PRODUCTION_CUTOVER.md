# Sales transfer — short procedure

Production cutover succeeded on 2026-10-30 at 14:23 Cairo: the existing Cloud
Scheduler job imported 7 missing leads, with no unmapped leads or skipped tabs.
Production ID: `1U4509WWjTjlddPorHusPMuxnCdFZqkSK94zwSqbySVw`.
Username exclusion is enabled. See CURRENT_STATE.md for evidence.

## Repeatable transfer steps

All importer work below happens in the **Conversions workbook → Extensions →
Apps Script**, using its existing Code.gs and Lead Import.gs.

1. Make a fresh copy of the original Sales workbook.
2. Set Script properties: `SALES_IMPORT_ENABLED=false`,
   `SALES_SHEET_FILE_ID=<fresh copy ID>`, `SALES_INCLUDE_TEST_USERNAME=false`.
3. Add the current Prepare Attribution Columns.js as a separate .gs file.
   Run `previewAttributionLayouts`, then `prepareAidaAttributionLayout`.
   Check AIDA styling/I1, then run `prepareRemainingAttributionLayouts`.
4. Run `previewSalesLeadImport`. Resolve verified duplicate lead rows if flagged;
   clear only the extra original-table cells, not whole worksheet rows.
5. Set `SALES_IMPORT_ENABLED=true`. If source changed, update the EXISTING Web App
   deployment to a new version. Force the existing Export_Conversions_To_Sheet
   job; run `inspectLastWebExport` afterward. Check the copy's destination/counts.
6. After the copy passes, set `SALES_IMPORT_ENABLED=false` and change
   `SALES_SHEET_FILE_ID` to the production ID above.
7. Run `previewAttributionLayouts`, `prepareRemainingAttributionLayouts`,
   then `previewSalesLeadImport`. Require no errors, skipped tabs or unmapped leads.
8. Set `SALES_IMPORT_ENABLED=true`, force the SAME job, then inspect the diagnostic
   for the production ID, completed status and expected import counts.
   Property changes alone need no new deployment.

## Remove temporary Google-side files

After preparation, remove the temporary **Prepare Attribution Columns.gs** and
**Cleanup Preview** (if present) from the Conversions Apps Script project.
Keep **Code.gs**, **Lead Import.gs** and their production diagnostics.
Deploy the existing Web App to a new version after removing temporary files.

Keep the production Sales workbook's separate quality/onEdit script and trigger.
Keep source files and regression tests in the repository. Do not install the
deferred Google Ads name lookup or create another import timer/job.

To stop Sales import, set `SALES_IMPORT_ENABLED=false`; conversion export continues.
