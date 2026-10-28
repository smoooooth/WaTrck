// Add as a separate file in the existing Sales Sheet Apps Script project.
// Manual, one-tab layout preparation only. No triggers or backend requests.
// First run against ONE project tab in a disposable copy of the workbook.
function prepareAttributionColumnsForActiveTab() {
  const lock = LockService.getDocumentLock();
  lock.waitLock(30000);
  try {
    const workbook = SpreadsheetApp.getActive();
    const sheet = workbook.getActiveSheet();
    const config = workbook.getSheetByName('Config');
    if (!config) throw new Error('Config tab is missing.');
    const configured = config.getDataRange().getDisplayValues().slice(1)
      .some(row => String(row[0]).trim() === sheet.getName());
    if (!configured) throw new Error('Select a project tab listed in Config.');

    const expected = [
      'Name', 'Phone number', 'Interest', 'Notes', 'Token', 'Date',
      'Quality', 'USD Value', 'SERVER FEEDBACK'
    ];
    const actual = sheet.getRange(1, 1, 1, 9).getDisplayValues()[0];
    if (!expected.every((header, index) => actual[index].trim() === header)) {
      throw new Error('A1:I1 does not match the confirmed Sales Sheet layout. No columns inserted.');
    }

    const headers = ['Campaign', 'Ad group / Asset group', 'Ad', 'Campaign type'];
    const existing = sheet.getRange(1, 10, 1, 4).getDisplayValues()[0];
    if (headers.every((header, index) => existing[index] === header)) {
      console.log(sheet.getName() + ': attribution columns already prepared.');
      return;
    }
    // Also protects a retry after insertion succeeded but writing headers failed.
    // Inspect a partial layout manually instead of inserting a second block.
    const shiftedFeedback = sheet.getMaxColumns() >= 22
      ? sheet.getRange(7, 22).getDisplayValue()
      : '';
    if (headers.some(header => existing.includes(header)) || shiftedFeedback === 'SERVER FEEDBACK') {
      throw new Error('Possible partial layout preparation. Inspect J:M and the filter table before retrying.');
    }

    // Google Sheets moves the existing cells, formulas, and validation together.
    // Original filter controls K2:K4 become O2:O4; filter output J8 becomes N8.
    sheet.insertColumnsBefore(10, 4);
    sheet.getRange(1, 10, 1, 4).setValues([headers]);
    SpreadsheetApp.flush();
    console.log(sheet.getName() + ': added J:M; filter table moved to N onward. Verify its formula and Google Table range.');
  } finally {
    lock.releaseLock();
  }
}
