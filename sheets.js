const { google } = require("googleapis");
const cfg = require("./config");

let sheets = null;

if (cfg.SHEET.id && cfg.SHEET.email && cfg.SHEET.key) {
  const auth = new google.auth.JWT(cfg.SHEET.email, null, cfg.SHEET.key, [
    "https://www.googleapis.com/auth/spreadsheets",
  ]);
  sheets = google.sheets({ version: "v4", auth });
}

/**
 * Qator tuzilishi (10 ustun, A:J):
 *   Time Local | Employee id | Employee Name | Action | Shift Time |
 *   Shift Date | Late Minutes | Status | Notes | Didn't Come
 * Tarmoq/DNS uzilishlarida qator YO'QOLMAYDI — navbatga tushib har 30
 * soniyada qayta uriniladi (10 martagacha). Xato botni to'xtatmaydi.
 */
const pending = []; // { row, sheetName, attempts, onRow }

// Parses the Sheets API append response to get the actual row number written,
// e.g. updatedRange "Drenix!A12:J12" -> 12. Needed so /notes can later find
// and update the exact Notes cell for a specific flagged event.
function rowNumberFromResponse(res) {
  const range = res && res.data && res.data.updates && res.data.updates.updatedRange;
  const m = range && range.match(/![A-Za-z]+(\d+)/);
  return m ? Number(m[1]) : null;
}

async function tryAppend(row, sheetName) {
  const res = await sheets.spreadsheets.values.append({
    spreadsheetId: cfg.SHEET.id,
    range: `${sheetName || cfg.SHEET.name}!A:J`,
    valueInputOption: "USER_ENTERED",
    requestBody: { values: [row] },
  });
  return rowNumberFromResponse(res);
}

/**
 * Appends a row. Returns the row number on success (or null if Sheets isn't
 * configured / the append is queued for retry). `onRow(rowNum)` — if given —
 * is called once the row number becomes known, even if that happens later
 * via the retry queue (so /notes can still find the right row after an
 * outage).
 */
async function appendRow(row, sheetName, onRow) {
  if (!sheets) return null;
  try {
    const rowNum = await tryAppend(row, sheetName);
    if (onRow && rowNum) onRow(rowNum);
    return rowNum;
  } catch (e) {
    console.error("Google Sheets xatosi (navbatga qo'yildi, qayta urinadi):", e.message);
    pending.push({ row, sheetName, attempts: 0, onRow });
    if (pending.length > 500) pending.shift();
    return null;
  }
}

// Overwrites ONLY the Notes cell (column I) of a specific row with the given
// text. Called with the FULL already-merged notes string (old + new), so
// nothing is ever lost — the cell simply ends up containing everything.
async function updateNoteCell(sheetName, row, text) {
  if (!sheets || !row) return;
  try {
    await sheets.spreadsheets.values.update({
      spreadsheetId: cfg.SHEET.id,
      range: `${sheetName || cfg.SHEET.name}!I${row}:I${row}`,
      valueInputOption: "USER_ENTERED",
      requestBody: { values: [[text]] },
    });
  } catch (e) {
    console.error("Google Sheets Notes yangilashda xato:", e.message);
  }
}

async function flushPending() {
  if (!sheets || pending.length === 0) return;
  const batch = pending.splice(0, pending.length);
  for (const item of batch) {
    try {
      const rowNum = await tryAppend(item.row, item.sheetName);
      if (item.onRow && rowNum) item.onRow(rowNum);
    } catch (e) {
      item.attempts += 1;
      if (item.attempts < 10) pending.push(item);
      else console.error("Google Sheets: qator 10 urinishdan keyin tashlandi:", JSON.stringify(item.row));
    }
  }
}

if (!process.env.DRENIX_TEST) {
  setInterval(() => flushPending().catch(() => {}), 30 * 1000);
}

module.exports = { appendRow, updateNoteCell };
