// Node ichki SQLite moduli (Node 22.5+) — native kompilyatsiya kerak emas
const { DatabaseSync } = require("node:sqlite");
const fs = require("fs");
const path = require("path");
const cfg = require("./config");

function openDb(p) {
  fs.mkdirSync(path.dirname(path.resolve(p)), { recursive: true });
  const d = new DatabaseSync(p);
  d.exec("PRAGMA journal_mode = WAL;");
  return d;
}

const db = openDb(cfg.DB_PATH);
// Subscriptions live in a SHARED database when SHARED_DB_PATH is set, so a
// /start registration done on the MASTER instance is visible to every worker
// instance (they read chat ids from here to send DMs).
const subsDb = cfg.SHARED_DB_PATH ? openDb(cfg.SHARED_DB_PATH) : db;

subsDb.exec(`
CREATE TABLE IF NOT EXISTS subscriptions (
  chat_id TEXT PRIMARY KEY,
  emp_id  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pending (
  chat_id TEXT PRIMARY KEY
);
`);

db.exec(`
CREATE TABLE IF NOT EXISTS attendance (
  emp_id    TEXT NOT NULL,
  work_date TEXT NOT NULL,
  arrival   INTEGER,
  departure INTEGER,
  PRIMARY KEY (emp_id, work_date)
);
CREATE TABLE IF NOT EXISTS noshow (
  emp_id    TEXT NOT NULL,
  work_date TEXT NOT NULL,
  PRIMARY KEY (emp_id, work_date)
);
CREATE TABLE IF NOT EXISTS raw_events (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  emp_id     TEXT,
  name       TEXT,
  event_type TEXT NOT NULL,
  device_ip  TEXT,
  ts         INTEGER NOT NULL,
  source     TEXT
);
CREATE INDEX IF NOT EXISTS idx_raw_events_emp ON raw_events(emp_id, ts);
CREATE TABLE IF NOT EXISTS flagged_events (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  emp_id     TEXT NOT NULL,
  work_date  TEXT NOT NULL,
  event_type TEXT NOT NULL,   -- 'late_checkin' | 'no_show' | 'break_warning'
  chat_id    TEXT NOT NULL,
  message_id INTEGER NOT NULL,
  sheet_name TEXT,
  sheet_row  INTEGER,
  notes      TEXT NOT NULL DEFAULT '',
  created_ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_flag_reply ON flagged_events(chat_id, message_id);
CREATE TABLE IF NOT EXISTS breaks (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  emp_id    TEXT NOT NULL,
  work_date TEXT NOT NULL,
  out_ts    INTEGER NOT NULL,
  in_ts     INTEGER,
  warned    INTEGER DEFAULT 0
);
`);

module.exports = {
  // --- obunalar ---
  setPending: (chatId) => subsDb.prepare("INSERT OR REPLACE INTO pending (chat_id) VALUES (?)").run(String(chatId)),
  isPending: (chatId) => !!subsDb.prepare("SELECT 1 AS x FROM pending WHERE chat_id=?").get(String(chatId)),
  clearPending: (chatId) => subsDb.prepare("DELETE FROM pending WHERE chat_id=?").run(String(chatId)),

  subscribe: (chatId, empId) =>
    subsDb.prepare("INSERT OR REPLACE INTO subscriptions (chat_id, emp_id) VALUES (?, ?)").run(String(chatId), String(empId)),
  unsubscribe: (chatId) => subsDb.prepare("DELETE FROM subscriptions WHERE chat_id=?").run(String(chatId)),
  subscriptionFor: (chatId) => {
    const r = subsDb.prepare("SELECT emp_id FROM subscriptions WHERE chat_id=?").get(String(chatId));
    return r ? r.emp_id : null;
  },
  chatsForEmployee: (empId) =>
    subsDb.prepare("SELECT chat_id FROM subscriptions WHERE emp_id=?").all(String(empId)).map(r => r.chat_id),

  // --- davomat ---
  getAttendance: (empId, workDate) =>
    db.prepare("SELECT * FROM attendance WHERE emp_id=? AND work_date=?").get(String(empId), workDate),
  setArrival: (empId, workDate, ts) =>
    db.prepare("INSERT OR REPLACE INTO attendance (emp_id, work_date, arrival, departure) VALUES (?, ?, ?, NULL)")
      .run(String(empId), workDate, ts),
  setDeparture: (empId, workDate, ts) =>
    db.prepare("UPDATE attendance SET departure=? WHERE emp_id=? AND work_date=?").run(ts, String(empId), workDate),
  activeSession: (empId) =>
    db.prepare("SELECT * FROM attendance WHERE emp_id=? AND arrival IS NOT NULL AND departure IS NULL ORDER BY work_date DESC LIMIT 1")
      .get(String(empId)),

  // --- tanaffuslar ---
  openBreak: (empId) =>
    db.prepare("SELECT * FROM breaks WHERE emp_id=? AND in_ts IS NULL ORDER BY out_ts DESC LIMIT 1").get(String(empId)),
  lastClosedBreak: (empId) =>
    db.prepare("SELECT * FROM breaks WHERE emp_id=? AND in_ts IS NOT NULL ORDER BY in_ts DESC LIMIT 1").get(String(empId)),
  startBreak: (empId, workDate, ts) =>
    db.prepare("INSERT INTO breaks (emp_id, work_date, out_ts) VALUES (?, ?, ?)").run(String(empId), workDate, ts),
  endBreak: (id, ts) => db.prepare("UPDATE breaks SET in_ts=? WHERE id=?").run(ts, Number(id)),
  markWarned: (id) => db.prepare("UPDATE breaks SET warned=1 WHERE id=?").run(Number(id)),
  noShowSent: (empId, workDate) =>
    !!db.prepare("SELECT 1 AS x FROM noshow WHERE emp_id=? AND work_date=?").get(String(empId), workDate),
  markNoShow: (empId, workDate) =>
    db.prepare("INSERT OR IGNORE INTO noshow (emp_id, work_date) VALUES (?, ?)").run(String(empId), workDate),

  allOpenBreaks: () =>
    db.prepare("SELECT * FROM breaks WHERE in_ts IS NULL").all(),
  voidBreak: (id) =>
    db.prepare("UPDATE breaks SET in_ts = out_ts, warned = 1 WHERE id=?").run(Number(id)),
  logRawEvent: (empId, name, eventType, deviceIp, ts, source) =>
    db.prepare("INSERT INTO raw_events (emp_id, name, event_type, device_ip, ts, source) VALUES (?, ?, ?, ?, ?, ?)")
      .run(empId != null ? String(empId) : null, name || null, eventType, deviceIp || null, ts, source || null),
  rawEventsFor: (empId, sinceTs = 0) =>
    db.prepare("SELECT * FROM raw_events WHERE emp_id=? AND ts>=? ORDER BY ts ASC").all(String(empId), sinceTs),

  // --- /notes: reply-to-message flagged events (late checkin / no-show / break warning) ---
  createFlag: (empId, workDate, eventType, chatId, messageId, sheetName) => {
    const r = db.prepare(
      "INSERT INTO flagged_events (emp_id, work_date, event_type, chat_id, message_id, sheet_name, notes, created_ts) VALUES (?, ?, ?, ?, ?, ?, '', ?)"
    ).run(String(empId), workDate, eventType, String(chatId), Number(messageId), sheetName || null, Date.now());
    return Number(r.lastInsertRowid);
  },
  // Records the Sheets row number for a flag; returns its CURRENT notes text
  // (non-empty only if the employee already replied before the row was known
  // — e.g. Sheets was briefly unreachable — so the caller can push it now).
  setFlagSheetRow: (flagId, row) => {
    db.prepare("UPDATE flagged_events SET sheet_row=? WHERE id=?").run(Number(row), Number(flagId));
    const r = db.prepare("SELECT notes FROM flagged_events WHERE id=?").get(Number(flagId));
    return r && r.notes ? r.notes : null;
  },
  findFlagByReply: (chatId, messageId) =>
    db.prepare("SELECT * FROM flagged_events WHERE chat_id=? AND message_id=? ORDER BY id DESC LIMIT 1")
      .get(String(chatId), Number(messageId)),
  // Appends (never overwrites) a new note onto the flag's notes text.
  appendFlagNote: (flagId, noteText, tag) => {
    const cur = db.prepare("SELECT notes FROM flagged_events WHERE id=?").get(Number(flagId));
    const prefixed = tag ? `${tag} — ${noteText}` : noteText;
    const merged = (cur && cur.notes) ? `${cur.notes} | ${prefixed}` : prefixed;
    db.prepare("UPDATE flagged_events SET notes=? WHERE id=?").run(merged, Number(flagId));
    return merged;
  },

  lastBreakOfSession: (empId, workDate) =>
    db.prepare("SELECT * FROM breaks WHERE emp_id=? AND work_date=? ORDER BY out_ts DESC LIMIT 1")
      .get(String(empId), workDate),
  staleSessions: () =>
    db.prepare("SELECT * FROM attendance WHERE arrival IS NOT NULL AND departure IS NULL").all(),

  overdueBreaks: (limitMs, now) =>
    db.prepare("SELECT * FROM breaks WHERE in_ts IS NULL AND warned=0 AND ? - out_ts > ?").all(now, limitMs),
};
