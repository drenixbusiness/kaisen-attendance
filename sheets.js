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
 * Xato bo'lsa botni to'xtatmaydi, faqat konsolga yozadi.
 */
async function appendRow(row, sheetName) {
  if (!sheets) return;
  try {
    await sheets.spreadsheets.values.append({
      spreadsheetId: cfg.SHEET.id,
      range: `${sheetName || cfg.SHEET.name}!A:F`,
      valueInputOption: "USER_ENTERED",
      requestBody: { values: [row] },
    });
  } catch (e) {
    console.error("Google Sheets xatosi:", e.message);
  }
}

module.exports = { appendRow };
