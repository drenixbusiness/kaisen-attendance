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
 * Qator qo'shadi: [Sana, ID, Ism, Hodisa, Vaqt, Izoh]
 * Tarmoq/DNS uzilishlarida qator YO'QOLMAYDI — navbatga tushib har 30
 * soniyada qayta uriniladi (10 martagacha). Xato botni to'xtatmaydi.
 */
const pending = []; // { row, sheetName, attempts }

async function tryAppend(row, sheetName) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: cfg.SHEET.id,
    range: `${sheetName || cfg.SHEET.name}!A:F`,
    valueInputOption: "USER_ENTERED",
    requestBody: { values: [row] },
  });
}

async function appendRow(row, sheetName) {
  if (!sheets) return;
  try {
    await tryAppend(row, sheetName);
  } catch (e) {
    console.error("Google Sheets xatosi (navbatga qo'yildi, qayta urinadi):", e.message);
    pending.push({ row, sheetName, attempts: 0 });
    if (pending.length > 500) pending.shift();
  }
}

async function flushPending() {
  if (!sheets || pending.length === 0) return;
  const batch = pending.splice(0, pending.length);
  for (const item of batch) {
    try {
      await tryAppend(item.row, item.sheetName);
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

module.exports = { appendRow };
