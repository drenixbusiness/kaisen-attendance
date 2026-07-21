/**
 * Drenix Attendance Bot
 * Hikvision (DS-K1T342) -> Telegram + Google Sheets
 *
 * Event sources (both active simultaneously):
 *   1. ISAPI alertStream — the bot connects DIRECTLY to each device and pulls
 *      events in real time (primary, most reliable; needs device credentials).
 *   2. HTTP listener on :8090 — receives events the devices push via their
 *      "HTTP Listening" config (backup path).
 *   A global 10-second dedupe prevents the same scan from being processed twice.
 *
 * Fingerprint (per employee's own shift windows):
 *   - Inside check-in window: FIRST scan = CHECKED IN; every further fingerprint
 *     on either device is IGNORED.
 *   - Inside check-out window (next day): FIRST scan = CHECKED OUT; the rest IGNORED.
 *   - Fingerprints outside both windows are IGNORED.
 * Face ID (breaks):
 *   - Alternates: out -> back. One single warning if a break exceeds 30 minutes.
 */

const express = require("express");
const multer = require("multer");
const fs = require("fs");
const path = require("path");
const { Telegraf, Markup } = require("telegraf");

const cfg = require("./config");
const SHIFT_RULES = require("./shifts");
const store = require("./db");
const { appendRow, updateNoteCell } = require("./sheets");
const { subscribeDevice } = require("./alertstream");
const { startPolling } = require("./poller");

const TEST_MODE = process.env.DRENIX_TEST === "1"; // unit tests only — never set in production

const EMPLOYEES = JSON.parse(fs.readFileSync(path.join(__dirname, "employees.json"), "utf8"));

// Multi-company support: each employee may have a "company" key that maps to
// an entry in companies.json (its own Telegram group and Sheets tab).
// Employees without a company fall back to the defaults from .env.
const COMPANIES_FILE = path.join(__dirname, "companies.json");
const COMPANIES = fs.existsSync(COMPANIES_FILE)
  ? JSON.parse(fs.readFileSync(COMPANIES_FILE, "utf8"))
  : {};

function companyFor(emp) {
  const c = (emp && emp.company && COMPANIES[emp.company]) || {};
  return {
    label: c.label || cfg.COMPANY_NAME,
    groupChatId: c.groupChatId || cfg.GROUP_CHAT_ID,
    sheetName: c.sheetName || undefined, // undefined -> sheets.js default
  };
}
const EVENTS_LOG = path.join(__dirname, "events.log");

// ============================= HELPERS =============================

// Employee lookup.
// exact=true (DEVICE EVENTS): the device always reports IDs verbatim with
//   leading zeros ("001", "036"), and device person "1" (e.g. ADMIN) is a
//   DIFFERENT person from "001" — so events must match keys exactly.
//   (Zero-stripping here previously mapped ADMIN's scans onto employee "001",
//   which made a second back-to-back employee get "already checked in".)
// exact=false (/start registration): typing convenience — "1" finds "001".
function findEmployee(rawId, exact = false) {
  if (!rawId) return null;
  const id = String(rawId).trim();
  if (EMPLOYEES[id]) return { id, ...EMPLOYEES[id] };
  if (exact) return null;
  const noZeros = id.replace(/^0+/, "") || "0";
  for (const key of Object.keys(EMPLOYEES)) {
    if ((key.replace(/^0+/, "") || "0") === noZeros) return { id: key, ...EMPLOYEES[key] };
  }
  return null;
}

function fmtTime(ts) {
  return new Intl.DateTimeFormat("en-GB", {
    timeZone: cfg.TZ, hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
  }).format(new Date(ts));
}

function fmtDate(ts) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: cfg.TZ, year: "numeric", month: "2-digit", day: "2-digit",
  }).format(new Date(ts)); // YYYY-MM-DD
}

function fmtDateTime(ts) {
  return `${fmtDate(ts)} ${fmtTime(ts)}`;
}

// Builds one Google Sheets row in the fixed 10-column order:
// Time Local | Employee id | Employee Name | Action | Shift Time |
// Shift Date | Late Minutes | Status | Notes | Didn't Come
function buildSheetRow({ ts, emp, rule, workDate, action, lateMin, status, notes, didntCome }) {
  return [
    fmtDateTime(ts),
    emp.id,
    emp.name,
    action,
    rule ? `${rule.workStart} - ${rule.workEnd}` : "",
    workDate,
    (lateMin === undefined || lateMin === null || lateMin === "") ? "" : lateMin,
    status || "",
    notes || "",
    didntCome ? "Yes" : "",
  ];
}

function localMinutes(ts) {
  const p = new Intl.DateTimeFormat("en-GB", {
    timeZone: cfg.TZ, hour: "2-digit", minute: "2-digit", hour12: false,
  }).formatToParts(new Date(ts));
  return +p.find(x => x.type === "hour").value * 60 + +p.find(x => x.type === "minute").value;
}

const toMin = (s) => { const [h, m] = s.split(":").map(Number); return h * 60 + m; };

// Day of week in the configured timezone: 0=Sunday ... 6=Saturday
function localDayOfWeek(ts) {
  const name = new Intl.DateTimeFormat("en-US", { timeZone: cfg.TZ, weekday: "short" }).format(new Date(ts));
  return ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].indexOf(name);
}

function addDays(dateStr, days) {
  const d = new Date(dateStr + "T00:00:00Z");
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

function deviceLabel(ip) {
  if (cfg.INSIDE_IPS.includes(ip)) return "Inside device";
  if (cfg.OUTSIDE_IPS.includes(ip)) return "Outside device";
  return ip;
}

const minWord = (n) => `${n} min`;

function logEvent(line) {
  fs.appendFileSync(EVENTS_LOG, `${new Date().toISOString()} | ${line}\n`);
}

// ============================= TELEGRAM =============================

const bot = new Telegraf(cfg.BOT_TOKEN || "test-token");
if (TEST_MODE) {
  global.__msgSeq = global.__msgSeq || 1000;
  bot.telegram.sendMessage = async (chat, text, extra = {}) => {
    const message_id = ++global.__msgSeq;
    (global.__SENT = global.__SENT || []).push({ chat: String(chat), text, message_id, reply_markup: extra.reply_markup });
    return { message_id, chat: { id: chat } };
  };
}

// Send with retry: transient network/DNS failures (EAI_AGAIN, timeouts,
// resets) must never lose a check-in/checkout message. Permanent errors
// (chat not found, bot blocked) are reported once without retrying.
async function tgSend(chatId, text, tag, extra = {}) {
  const MAX = 6;
  for (let a = 1; a <= MAX; a++) {
    try {
      return await bot.telegram.sendMessage(chatId, text, { parse_mode: "HTML", ...extra });
    } catch (e) {
      const msg = String(e.message || "");
      const permanent = /chat not found|bot was blocked|user is deactivated|bot was kicked|not enough rights|400: Bad Request/i.test(msg);
      if (permanent || a === MAX) {
        console.error(`${tag} ${chatId}: ${msg}`);
        if (/chat not found/i.test(msg)) {
          console.error("  -> Check: is the bot added to the group? If the group was upgraded to a supergroup, its ID changed (starts with -100...) — update TELEGRAM_CHAT_ID in .env.");
        }
        return null;
      }
      // transient (DNS EAI_AGAIN, ETIMEDOUT, ECONNRESET, 5xx, 429...) — wait & retry
      await new Promise((r) => setTimeout(r, Math.min(10000 * a, 45000)));
    }
  }
  return null;
}

async function sendToGroup(text, chatId = cfg.GROUP_CHAT_ID) {
  await tgSend(chatId, text, "Failed to send to group");
}

// DM targets = DB subscriptions (/start flow) UNION static "chatId" values
// from employees.json ("chatId": "123456789" or ["id1","id2"]). The static
// form needs no polling at all, so independent instances never conflict.
function chatIdsFor(empId) {
  const ids = new Set(store.chatsForEmployee(empId).map(String));
  const info = EMPLOYEES[empId];
  if (info && info.chatId) {
    const c = info.chatId;
    (Array.isArray(c) ? c : [c]).forEach((x) => x && ids.add(String(x)));
  }
  return [...ids];
}

async function sendToEmployee(empId, text) {
  await Promise.all(chatIdsFor(empId).map((chatId) => tgSend(chatId, text, "Failed to DM")));
}

// Check-in / check-out / no-show -> employee DM + the employee's COMPANY group
async function notifyBoth(emp, text) {
  const co = companyFor(emp);
  console.log(`[notify DM+group ${co.label}] empId=${emp.id}: ${text.split("\n")[0].replace(/<[^>]+>/g, "")}`);
  await Promise.all([sendToEmployee(emp.id, text), sendToGroup(text, co.groupChatId)]);
}

// Like notifyBoth, but for the THREE "flaggable" event types the employee can
// explain (late check-in, No Show, break warning). Each DM gets its own
// inline "📝 Add reason" button so the employee never has to type /notes —
// tapping it opens Telegram's reply box already focused (force_reply), they
// just type the reason and hit send. The old /notes-as-a-reply command still
// works too (kept as a fallback), which is why the flag's message_id is
// still recorded.
async function notifyBothFlagged(emp, text, eventType, workDate) {
  const co = companyFor(emp);
  console.log(`[notify DM+group ${co.label}] empId=${emp.id}: ${text.split("\n")[0].replace(/<[^>]+>/g, "")}`);
  const chatIds = chatIdsFor(emp.id);
  const flagIds = chatIds.map((chatId) => store.createFlag(emp.id, workDate, eventType, chatId, 0, co.sheetName));
  await Promise.all([
    Promise.all(chatIds.map(async (chatId, i) => {
      const kb = Markup.inlineKeyboard([[Markup.button.callback("📝 Add reason", `note:${flagIds[i]}`)]]);
      const msg = await tgSend(chatId, text, "Failed to DM", kb);
      if (msg && msg.message_id) store.setFlagMessageId(flagIds[i], msg.message_id);
    })),
    sendToGroup(text, co.groupChatId),
  ]);
  return flagIds;
}

// In-memory: chatId -> flagId, set the moment the employee taps "📝 Add
// reason". The NEXT plain-text message from that chat is captured as the
// note (no /notes needed) and the state is cleared either way.
const noteState = new Map();

async function saveNoteForFlag(flagId, noteText) {
  const tag = fmtTime(Date.now());
  const merged = store.appendFlagNote(flagId, noteText, tag);
  const flag = store.getFlag(flagId);
  if (flag && flag.sheet_row) {
    await updateNoteCell(flag.sheet_name, flag.sheet_row, merged).catch(() => {});
  } // if the row isn't known yet, linkSheetRow() will push it once it is
  return merged;
}

// Handles a tap on the "📝 Add reason" inline button.
async function handleNoteButtonPress(ctx) {
  const flagId = Number(ctx.match[1]);
  await ctx.answerCbQuery();
  noteState.set(String(ctx.chat.id), flagId);
  return ctx.reply("✍️ Please type your reason:", Markup.forceReply().placeholder("Type your reason..."));
}
bot.action(/^note:(\d+)$/, handleNoteButtonPress);

// Called at the top of the text handler: if this chat is waiting for a note
// (button was tapped), the incoming text IS the reason — capture it and
// return true so the caller doesn't fall through to /start's registration
// flow. Returns false for every other message.
async function handleNoteTextReply(ctx) {
  const chatId = String(ctx.chat.id);
  if (!noteState.has(chatId)) return false;
  const flagId = noteState.get(chatId);
  noteState.delete(chatId);
  const noteText = (ctx.message.text || "").trim();
  if (!noteText) {
    await ctx.reply("ℹ️ Empty message — reason not saved. Tap \"📝 Add reason\" again if you'd like to add one.");
    return true;
  }
  await saveNoteForFlag(flagId, noteText);
  await ctx.reply("✅ Your note has been saved.");
  return true;
}

// Once a flagged event's Sheets row number becomes known (immediately, or
// later via the retry queue after an outage), push any notes already
// accumulated for it into the Notes cell — nothing typed early is ever lost.
function linkSheetRow(flagId, sheetName, rowNum) {
  if (!flagId || !rowNum) return;
  const notes = store.setFlagSheetRow(flagId, rowNum);
  if (notes) updateNoteCell(sheetName, rowNum, notes).catch(() => {});
}

// Breaks and break warnings -> employee DM only
async function notifyDM(empId, text) {
  console.log(`[notify DM] empId=${empId}: ${text.split("\n")[0].replace(/<[^>]+>/g, "")}`);
  await sendToEmployee(empId, text);
}

// Two-step registration state (in memory): chatId -> { stage, empId, attempts }
const regState = new Map();
const MAX_SECRET_ATTEMPTS = 3;

bot.start((ctx) => {
  // Already registered? Stay registered until /stop — just remind them.
  const existingEmpId = store.subscriptionFor(ctx.chat.id);
  if (existingEmpId) {
    const emp = findEmployee(existingEmpId, true) || { id: existingEmpId, name: `Employee #${existingEmpId}` };
    return ctx.reply(
      `✅ You are already registered as <b>${emp.name}</b> (${companyFor(emp).label}).\nYour check-in/check-out notifications will keep arriving here.\nSend /stop if you want to disconnect.`,
      { parse_mode: "HTML" }
    );
  }
  regState.set(ctx.chat.id, { stage: "id" });
  return ctx.reply(`Hello! This is the attendance bot.\nPlease enter your employee ID:`);
});

bot.command("health", async (ctx) => {
  const groups = Object.keys(COMPANIES).length
    ? Object.entries(COMPANIES).map(([k, c]) => [c.label || k, c.groupChatId])
    : [[cfg.COMPANY_NAME, cfg.GROUP_CHAT_ID]];
  const results = [];
  for (const [label, chatId] of groups) {
    try {
      await bot.telegram.sendMessage(chatId, `🩺 ${label} health check — please ignore.`);
      results.push(`${label} (${chatId}): ✅ OK`);
    } catch (e) {
      results.push(`${label} (${chatId}): ❌ ${e.message}`);
    }
  }
  const devLines = [...new Set([...cfg.INSIDE_IPS, ...cfg.OUTSIDE_IPS])].map((ip) => {
    const last = deviceLastEvent.get(ip);
    const stream = streamState.get(ip);
    const age = last ? `last event ${Math.round((Date.now() - last) / 1000)}s ago` : "waiting for first event";
    const mode = cfg.ALERTSTREAM_ENABLED
      ? (stream ? (stream.live ? "stream LIVE + poller" : "stream sync + poller") : "poller")
      : "poller only";
    return `${deviceLabel(ip)} (${ip}): ${mode}, ${age}`;
  }).join("\n");
  const hint = results.some((r) => r.includes("❌"))
    ? "\n⚠️ Fix: add THIS bot to the failing group; if the group became a supergroup its ID changed (starts with -100...) — update TELEGRAM_CHAT_ID in .env and restart."
    : "";
  return ctx.reply(
    `🩺 <b>Health</b>\nBot: ✅ running\nGroups:\n${results.join("\n")}${hint}\nDevices:\n${devLines}\nPolling every ${cfg.POLL_INTERVAL_MS / 1000}s`,
    { parse_mode: "HTML" }
  );
});

bot.command("stop", (ctx) => {
  store.unsubscribe(ctx.chat.id);
  regState.delete(ctx.chat.id);
  return ctx.reply("✅ Unsubscribed — all your data has been cleared. Send /start to connect again.");
});

// /notes — ONLY works as a REPLY (in the private DM) to one of the bot's own
// Late check-in / No Show / Break-warning messages. The explanation is
// APPENDED to that specific event's Notes cell in Google Sheets — an earlier
// note is never overwritten, the new one is simply added alongside it.
async function handleNotesCommand(ctx) {
  if (ctx.chat.type !== "private") return; // group usage is silently ignored
  const replyTo = ctx.message.reply_to_message;
  if (!replyTo) {
    return ctx.reply("ℹ️ Please REPLY to the bot's Late / No Show / Break-warning message with /notes <reason> to explain it.");
  }
  const flag = store.findFlagByReply(ctx.chat.id, replyTo.message_id);
  if (!flag) {
    return ctx.reply("❌ This message doesn't accept notes — only Late check-in, No Show, and Break-warning messages do.");
  }
  const noteText = ctx.message.text.replace(/^\/notes(@\w+)?\s*/i, "").trim();
  if (!noteText) {
    return ctx.reply("ℹ️ Please write your reason after the command, e.g.: /notes bus was late");
  }
  await saveNoteForFlag(flag.id, noteText);
  return ctx.reply("✅ Your note has been saved.");
}
bot.command("notes", handleNotesCommand);

bot.on("text", async (ctx) => {
  if (await handleNoteTextReply(ctx)) return; // "📝 Add reason" flow took this message
  const st = regState.get(ctx.chat.id);
  if (!st) return;
  const input = ctx.message.text.trim();

  // Step 1: employee ID
  if (st.stage === "id") {
    const emp = findEmployee(input);
    if (!emp) {
      return ctx.reply("❌ Employee ID not found. Please try again:");
    }
    regState.set(ctx.chat.id, { stage: "secret", empId: emp.id, attempts: 0 });
    return ctx.reply("🔐 Now enter your personal secret code:");
  }

  // Step 2: secret code
  if (st.stage === "secret") {
    const emp = findEmployee(st.empId);
    const expected = String(emp?.secret || "");
    if (!expected) {
      regState.delete(ctx.chat.id);
      return ctx.reply("❌ No secret is configured for this employee. Contact the administrator.");
    }
    if (input.toUpperCase() !== expected.toUpperCase()) {
      st.attempts += 1;
      if (st.attempts >= MAX_SECRET_ATTEMPTS) {
        regState.delete(ctx.chat.id);
        return ctx.reply("❌ Too many wrong attempts. Send /start to try again.");
      }
      return ctx.reply(`❌ Wrong secret code. ${MAX_SECRET_ATTEMPTS - st.attempts} attempt(s) left. Try again:`);
    }
    regState.delete(ctx.chat.id);
    store.subscribe(ctx.chat.id, emp.id);
    return ctx.reply(
      `✅ Connected: <b>${emp.name}</b> — ${companyFor(emp).label} (${SHIFT_RULES[emp.shiftKey]?.label || emp.shiftKey})\nYour check-in/check-out notifications will arrive here.\nSend /stop to disconnect.`,
      { parse_mode: "HTML" }
    );
  }
});

// ============================= CORE LOGIC =============================

function arrivalStatus(m, rule) {
  const diff = m - toMin(rule.workStart);
  // Up to +10 min (lateAllowableMin) counts as on time; earlier is on time too
  if (diff <= rule.lateAllowableMin) {
    if (diff < 0) return { text: `✅ On time (${minWord(-diff)} early)`, note: `On time (${-diff} min early)` };
    return { text: `✅ On time`, note: "On time" };
  }
  if (diff <= 120) return { text: `⚠️ <b>Late by ${minWord(diff)}</b>`, note: `Late (${diff} min)` };
  return { text: `🔴 <b>Very late by ${minWord(diff)} — marked as No Show</b>`, note: `Very late (${diff} min) — No Show` };
}

async function doCheckIn(emp, ts, devName, rule, today) {
  const m = localMinutes(ts);
  store.setArrival(emp.id, today, ts);
  const st = arrivalStatus(m, rule);
  const diff = m - toMin(rule.workStart);
  const isLate = diff > rule.lateAllowableMin; // covers both "Late" and "Very late — No Show"
  const lateMin = isLate ? diff : 0;
  let text = `🏢 <b>CHECKED IN</b>\n👤 Name: ${emp.name} (ID: ${emp.id})\n🏷 Shift: ${rule.label}\n📅 Shift Date: ${today}\n🕐 ${fmtTime(ts)}\n${st.text}\n📟 ${devName}`;
  const row = buildSheetRow({ ts, emp, rule, workDate: today, action: "Checked in", lateMin, status: st.note });
  const sheetName = companyFor(emp).sheetName;
  if (isLate) {
    // Flaggable: reply to THIS message, tap Menu, pick /notes, then type the reason.
    text += `\n\n💬 To explain: reply to this message, tap Menu, choose /notes, then type your reason.`;
    const flagIds = await notifyBothFlagged(emp, text, "late_checkin", today);
    const rowNum = await appendRow(row, sheetName);
    if (flagIds[0]) linkSheetRow(flagIds[0], sheetName, rowNum);
  } else {
    await notifyBoth(emp, text);
    appendRow(row, sheetName);
  }
}

// Direction-aware event handling.
// OUTSIDE device (10.1.1.248) is the ENTRY point: fingerprint check-in,
// face break-return. INSIDE device (10.1.1.211) is the EXIT point:
// fingerprint check-out, face break-out.
// This makes tailgating safe: an employee who walked out WITHOUT scanning
// (behind a colleague) and then scans on the OUTSIDE device on return simply
// gets IGNORED (no false "break out"), because entering can never open a break.
function deviceRole(ip) {
  if (cfg.INSIDE_IPS.includes(ip)) return "inside";
  if (cfg.OUTSIDE_IPS.includes(ip)) return "outside";
  return "unknown";
}

async function doCheckOut(emp, rule, ts, devName, workDate, rec) {
  store.setDeparture(emp.id, workDate, ts);
  const workedMin = Math.round((ts - rec.arrival) / 60000);
  const worked = `${Math.floor(workedMin / 60)}h ${workedMin % 60}m`;
  await notifyBoth(emp,
    `🚪 <b>CHECKED OUT</b>\n👤 Name: ${emp.name} (ID: ${emp.id})\n🏷 Shift: ${rule.label}\n📅 Shift Date: ${workDate}\n🕐 ${fmtTime(ts)}\n⏱ Worked: ${worked}\n📟 ${devName}`);
  appendRow(buildSheetRow({ ts, emp, rule, workDate, action: "Checked out", notes: `Worked: ${worked}` }), companyFor(emp).sheetName);
}

async function doBreakIn(emp, ts, devName, open) {
  store.endBreak(open.id, ts);
  const dur = Math.round((ts - open.out_ts) / 60000);
  const over = dur > cfg.BREAK_LIMIT_MIN;
  // No Telegram message for break returns — tracked in events.log and Sheets.
  console.log(`[break in] ${emp.name} @ ${fmtTime(ts)} — ${dur} min${over ? " (OVER LIMIT)" : ""}`);
  const rule = SHIFT_RULES[emp.shiftKey];
  appendRow(buildSheetRow({
    ts, emp, rule, workDate: open.work_date, action: "Back from break",
    status: over ? "Over limit" : "",
    notes: over ? `${dur} min (over limit)` : `${dur} min`,
  }), companyFor(emp).sheetName);
}

async function doBreakOut(emp, ts, devName, workDate, note = "") {
  store.startBreak(emp.id, workDate, ts);
  // No Telegram message for normal break-outs — only the 30-min warning is
  // sent. Everything is still tracked in events.log and Google Sheets.
  console.log(`[break out] ${emp.name} @ ${fmtTime(ts)} (${devName})${note ? " — " + note : ""}`);
  const rule = SHIFT_RULES[emp.shiftKey];
  appendRow(buildSheetRow({ ts, emp, rule, workDate, action: "Break started", notes: note }), companyFor(emp).sheetName);
}

async function handleAuthEvent(emp, ts, deviceIp, kind) {
  const rule = SHIFT_RULES[emp.shiftKey];
  if (!rule) return logEvent(`IGNORE: no shift rule for ${emp.id} (${emp.shiftKey})`);

  let role = deviceRole(deviceIp);
  if (role === "unknown") {
    logEvent(`WARN: event from unrecognized device ip=${deviceIp} — treating as OUTSIDE`);
    role = "outside";
  }
  const devName = deviceLabel(deviceIp);
  const m = localMinutes(ts);
  const today = fmtDate(ts);
  const todayRec = store.getAttendance(emp.id, today);
  const open = store.openBreak(emp.id);

  // ============ OUTSIDE device — entering the building ============
  if (role === "outside") {
    // 1) An open break exists -> this entry closes it (BACK FROM BREAK).
    if (open) {
      return doBreakIn(emp, ts, devName, open);
    }
    // 2) Not checked in yet TODAY -> CHECK IN, at ANY time of day. There is
    //    no earliest-allowed clock time anymore: an employee who arrives
    //    hours before their shift MUST be recognized immediately (previously
    //    a scan before validCheckInFrom was silently dropped as "entered, no
    //    break counted", so the employee looked absent until a later scan —
    //    which then got wrongly marked as their check-in time, often "Late").
    //    The only real guard needed is: don't open a second, overlapping
    //    check-in while a PREVIOUS shift's session is still unresolved
    //    (relevant for overnight shifts just after midnight) — that is
    //    exactly what store.activeSession() (any date) protects against.
    //    Works for fingerprint AND Face ID AND unknown auth codes, so a
    //    forgotten fingerprint or an unrecognized event code can never
    //    silently swallow a check-in again.
    if ((!todayRec || !todayRec.arrival) && !store.activeSession(emp.id)) {
      const label = kind === "face" ? `${devName} (via Face ID)` : devName;
      return doCheckIn(emp, ts, label, rule, today);
    }
    // 3) Checked in, no open break: the EXIT scan was missed (or the employee
    //    walked out behind a colleague). NEVER drop this silently — but first
    //    filter genuine duplicates (repeated door scans within 60s).
    const lastClosed = store.lastClosedBreak(emp.id);
    if ((todayRec && todayRec.arrival && ts - todayRec.arrival < 60 * 1000) ||
        (lastClosed && lastClosed.in_ts && ts - lastClosed.in_ts < 60 * 1000)) {
      return logEvent(`DEDUP outside ${kind}: ${emp.name} — repeated door scan`);
    }
    logEvent(`ENTER-only outside ${kind}: ${emp.name} — no matching exit scan before this entry`);
    console.log(`[entered] ${emp.name} @ ${fmtTime(ts)} — no prior exit scan, no break counted`);
    appendRow(buildSheetRow({ ts, emp, rule, workDate: today, action: "Entered (no exit scan)" }), companyFor(emp).sheetName);
    return;
  }

  // ============ INSIDE device — leaving the building ============
  // 1) Check-out window -> CHECK OUT, but ONLY with a fingerprint (or an
  //    unrecognized auth code — some firmwares report fingerprints with
  //    unknown codes). Face ID NEVER checks out: after the shift ends the
  //    employee may still go to the store with Face ID before finally leaving
  //    with a fingerprint — those Face ID exits/returns are breaks.
  if (kind !== "face" && m >= toMin(rule.validCheckOutFrom) && m <= toMin(rule.validCheckOutTo)) {
    const workDate = addDays(today, -rule.checkOutDayOffset);
    const rec = store.getAttendance(emp.id, workDate);
    if (rec && rec.arrival && !rec.departure) {
      if (open) {
        store.endBreak(open.id, ts);
        logEvent(`AUTO-CLOSE open break for ${emp.name} at checkout`);
      }
      return doCheckOut(emp, rule, ts, devName, workDate, rec);
    }
    if (rec && rec.departure) {
      if (rec.departure > 0 && ts - rec.departure < 120 * 1000) {
        return logEvent(`DEDUP inside ${kind}: ${emp.name} — door scan right after checkout`);
      }
      logEvent(`EXIT after checkout: ${emp.name} (${workDate})`);
      console.log(`[exited] ${emp.name} @ ${fmtTime(ts)} — already checked out`);
      appendRow(buildSheetRow({ ts, emp, rule, workDate, action: "Exited (after checkout)" }), companyFor(emp).sheetName);
      return;
    }
    // no check-in for that shift date — fall through to the session logic
  }

  const session = store.activeSession(emp.id);
  if (session) {
    // A scan right after check-in is a door double-scan, not a break
    if (ts - session.arrival < 60 * 1000) {
      return logEvent(`DEDUP inside ${kind}: ${emp.name} — scan right after check-in, not a break`);
    }
    if (open) {
      if (ts - open.out_ts < 60 * 1000) {
        return logEvent(`DEDUP inside ${kind}: ${emp.name} — repeated exit scan`);
      }
      // The RETURN scan of the previous break was never recorded. Do NOT let
      // one missed event poison the chain: close the stale break now and
      // start a fresh one with an explanatory note.
      store.endBreak(open.id, ts);
      logEvent(`AUTO-CLOSE stale break for ${emp.name} (return scan was missed); starting a new break`);
      return doBreakOut(emp, ts, devName, session.work_date, "Previous break's return scan was not recorded — it was closed automatically.");
    }
    // ANY scan type on the exit device mid-shift is a break-out (employees may
    // habitually use a fingerprint at the door) — events must never be lost.
    return doBreakOut(emp, ts, devName, session.work_date);
  }

  // No active session: a fingerprint here in the check-in zone means the
  // employee scanned the WRONG device to check in — still count it, at any
  // time (see the OUTSIDE-device comment above for why there is no longer a
  // clock-time lower bound).
  if (kind === "fp" && (!todayRec || !todayRec.arrival)) {
    return doCheckIn(emp, ts, `${devName} (wrong device)`, rule, today);
  }
  // Distinguish "exit after an already-completed checkout" (e.g. a Face ID
  // scan hours after checking out) from "exit with no check-in at all".
  const coDate = addDays(today, -rule.checkOutDayOffset);
  const coRec = store.getAttendance(emp.id, coDate);
  const doneRec = (coRec && coRec.departure) ? coRec : ((todayRec && todayRec.departure) ? todayRec : null);
  if (doneRec) {
    if (doneRec.departure > 0 && ts - doneRec.departure < 120 * 1000) {
      return logEvent(`DEDUP inside ${kind}: ${emp.name} — door scan right after checkout`);
    }
    logEvent(`EXIT after checkout: ${emp.name}`);
    console.log(`[exited] ${emp.name} @ ${fmtTime(ts)} — already checked out`);
    appendRow(buildSheetRow({ ts, emp, rule, workDate: today, action: "Exited (after checkout)" }), companyFor(emp).sheetName);
    return;
  }
  logEvent(`EXIT without check-in: ${emp.name}`);
  console.log(`[exited] ${emp.name} @ ${fmtTime(ts)} — no check-in today`);
  appendRow(buildSheetRow({ ts, emp, rule, workDate: today, action: "Exited (no check-in)" }), companyFor(emp).sheetName);
  return;
}

// Maintenance watchdog (every minute + at startup). Keeps state clean so
// yesterday's leftovers can never confuse a new shift:
//  A) Face ID exit in the CHECKOUT WINDOW with no return within
//     FACE_EXIT_CHECKOUT_MIN -> converted into a real CHECKOUT (forgot the
//     fingerprint, went home). If they return sooner it stays a break.
//  B) Abandoned open breaks (outside the checkout window) -> 30-min warning
//     once, then voided after STALE_BREAK_HOURS.
//  C) Sessions never checked out -> closed automatically after their checkout
//     window ends, so the next work day always starts with a clean state.
async function runMaintenance(now = Date.now()) {
  // --- A/B: open breaks ---
  for (const b of store.allOpenBreaks()) {
    const emp = findEmployee(b.emp_id, true) || { id: b.emp_id, name: `Employee #${b.emp_id}` };
    const rule = SHIFT_RULES[emp.shiftKey];
    const outM = localMinutes(b.out_ts);
    const inCheckoutWin = rule && outM >= toMin(rule.validCheckOutFrom) && outM <= toMin(rule.validCheckOutTo);

    if (inCheckoutWin) {
      // Likely a departure with Face ID — wait FACE_EXIT_CHECKOUT_MIN, then
      // convert to checkout. No 30-min break warning for these.
      const rec = store.getAttendance(b.emp_id, b.work_date);
      if (rec && rec.arrival && !rec.departure && now - b.out_ts > cfg.FACE_EXIT_CHECKOUT_MIN * 60000) {
        store.voidBreak(b.id);
        store.setDeparture(b.emp_id, b.work_date, b.out_ts);
        const workedMin = Math.round((b.out_ts - rec.arrival) / 60000);
        const worked = `${Math.floor(workedMin / 60)}h ${workedMin % 60}m`;
        logEvent(`FACE-EXIT->CHECKOUT: ${emp.name} left at ${fmtTime(b.out_ts)} with Face ID and did not return`);
        await notifyBoth(emp,
          `🚪 <b>CHECKED OUT</b>\n👤 Name: ${emp.name} (ID: ${emp.id})\n🏷 Shift: ${rule.label}\n📅 Shift Date: ${b.work_date}\n🕐 ${fmtTime(b.out_ts)}\n⏱ Worked: ${worked}\nℹ️ Exited with Face ID after the shift and did not return — counted as checkout.`);
        appendRow(buildSheetRow({
          ts: b.out_ts, emp, rule, workDate: b.work_date, action: "Checked out",
          notes: `Face ID exit, no return — auto checkout (worked ${worked})`,
        }), companyFor(emp).sheetName);
      }
      continue;
    }

    // Abandoned break: void after STALE_BREAK_HOURS so it can never swallow
    // tomorrow's first entry as a fake "back from break".
    if (now - b.out_ts > cfg.STALE_BREAK_HOURS * 3600 * 1000) {
      store.voidBreak(b.id);
      logEvent(`AUTO-VOID stale open break: ${emp.name} (out at ${fmtTime(b.out_ts)}, never returned)`);
      appendRow(buildSheetRow({
        ts: now, emp, rule, workDate: b.work_date, action: "Break voided",
        status: "Voided", notes: "Never returned — voided automatically",
      }), companyFor(emp).sheetName);
      continue;
    }

    if (!b.warned && now - b.out_ts > cfg.BREAK_LIMIT_MIN * 60000) {
      store.markWarned(b.id);
      const dur = Math.round((now - b.out_ts) / 60000);
      const warnText = `🔴 <b>WARNING!</b>\n👤 ${emp.name}\n☕ Has been on break for <b>${minWord(dur)}</b> — exceeded the ${minWord(cfg.BREAK_LIMIT_MIN)} limit and has not returned yet!\n🕐 Left at: ${fmtTime(b.out_ts)}\n\n💬 To explain: reply to this message, tap Menu, choose /notes, then type your reason.`;
      const sheetName = companyFor(emp).sheetName;
      const flagIds = await notifyBothFlagged(emp, warnText, "break_warning", b.work_date);
      const rowNum = await appendRow(buildSheetRow({
        ts: now, emp, rule, workDate: b.work_date, action: "WARNING",
        status: "Break over limit", notes: `Break ${dur} min — over limit`,
      }), sheetName);
      if (flagIds[0]) linkSheetRow(flagIds[0], sheetName, rowNum);
    }
  }

  // --- C: sessions that never checked out -> AUTO CHECKOUT ---
  // At the end of each shift's checkout window (i.e. before the NEW work day
  // begins) every still-open check-in is converted into a real CHECKED OUT:
  //   1) if the employee's LAST exit scan of that shift exists (walked out
  //      with Face ID and never returned) — that scan's time is the checkout;
  //   2) if there is no exit scan at all — the scheduled shift end is used.
  // This wipes yesterday's open state completely, so the new day always
  // starts fresh with a clean check-in/check-out cycle.
  for (const srow of store.staleSessions()) {
    const emp = findEmployee(srow.emp_id, true);
    const rule = emp && SHIFT_RULES[emp.shiftKey];
    if (!rule) continue;
    const deadline = Date.parse(`${addDays(srow.work_date, rule.checkOutDayOffset)}T${rule.validCheckOutTo}:00+05:00`);
    if (!Number.isFinite(deadline) || now <= deadline) continue;

    const lastBrk = store.lastBreakOfSession(srow.emp_id, srow.work_date);
    let outTs = lastBrk && lastBrk.out_ts > srow.arrival ? Number(lastBrk.out_ts) : null;
    let note;
    if (outTs) {
      note = "Auto checkout — left without a fingerprint checkout; time taken from the last exit scan.";
    } else {
      outTs = Date.parse(`${addDays(srow.work_date, rule.checkOutDayOffset)}T${rule.workEnd}:00+05:00`);
      note = "Auto checkout — no checkout scan was recorded; scheduled shift end used.";
    }
    if (!Number.isFinite(outTs) || outTs <= srow.arrival) outTs = Number(srow.arrival);

    store.setDeparture(srow.emp_id, srow.work_date, outTs);
    const workedMin = Math.round((outTs - srow.arrival) / 60000);
    const worked = `${Math.floor(workedMin / 60)}h ${workedMin % 60}m`;
    logEvent(`AUTO-CHECKOUT: ${emp.name} (${srow.work_date}) at ${fmtTime(outTs)} — ${note}`);
    await notifyBoth(emp,
      `🚪 <b>CHECKED OUT</b>\n👤 Name: ${emp.name} (ID: ${emp.id})\n🏷 Shift: ${rule.label}\n📅 Shift Date: ${srow.work_date}\n🕐 ${fmtTime(outTs)}\n⏱ Worked: ${worked}\nℹ️ ${note}`);
    appendRow(buildSheetRow({
      ts: outTs, emp, rule, workDate: srow.work_date, action: "Checked out",
      notes: note + ` (worked ${worked})`,
    }), companyFor(emp).sheetName);
  }
}

setInterval(() => runMaintenance().catch((e) => console.error("maintenance error:", e.message)), 60 * 1000);
if (!TEST_MODE) runMaintenance().catch(() => {}); // clean stale state at startup too

// No-Show watchdog — if no check-in within (120 + lateAllowableMin) minutes
// of shift start, alert the employee DM + the group ONCE per shift date.
setInterval(async () => {
  const now = Date.now();
  if (cfg.NO_SHOW_OFF_DAYS.includes(localDayOfWeek(now))) return; // day off (e.g. Sunday) — no No Show alerts
  const today = fmtDate(now);
  const m = localMinutes(now);
  for (const [id, info] of Object.entries(EMPLOYEES)) {
    if (cfg.MY_COMPANY && (info.company || "").toLowerCase() !== cfg.MY_COMPANY) continue;
    const rule = SHIFT_RULES[info.shiftKey];
    if (!rule) continue;
    const graceMin = 120 + rule.lateAllowableMin; // e.g. 130 minutes
    if (m < toMin(rule.workStart) + graceMin) continue; // deadline not reached yet
    const rec = store.getAttendance(id, today);
    if (rec && rec.arrival) continue;               // already checked in
    if (store.noShowSent(id, today)) continue;      // already alerted
    store.markNoShow(id, today);
    const empObj = { id, ...info };
    const nsText = `🚫 <b>No Show Alert</b>\n👤 Name: ${info.name}\n🏷 Shift: ${rule.label}\n📅 Shift Date: ${today}\n⏱️ No check-in received within ${graceMin} minutes of shift start\n\n💬 To explain: reply to this message, tap Menu, choose /notes, then type your reason.`;
    const sheetName = companyFor(empObj).sheetName;
    const flagIds = await notifyBothFlagged(empObj, nsText, "no_show", today);
    const rowNum = await appendRow(buildSheetRow({
      ts: now, emp: empObj, rule, workDate: today, action: "No Show",
      status: "No Show", notes: `No check-in within ${graceMin} min of shift start`, didntCome: true,
    }), sheetName);
    if (flagIds[0]) linkSheetRow(flagIds[0], sheetName, rowNum);
  }
}, 60 * 1000);

// ============================= EVENT PROCESSING =============================

// Last processed event per device (any source) — shown in /health
const deviceLastEvent = new Map();

// Per-device stream state: SYNC (history replay) -> LIVE
const streamState = new Map(); // deviceKey -> { live, timer }

function armLiveTimer(st, deviceKey) {
  clearTimeout(st.timer);
  st.timer = setTimeout(() => {
    if (!st.live) {
      st.live = true;
      console.log(`[${deviceKey}] history replay finished — LIVE mode, all events will be processed`);
    }
  }, 5000);
}

// Called the moment a device stream (re)connects: start in SYNC mode and arm
// the 5s timer immediately, so a device with NO replay backlog goes LIVE 5s
// after connecting — its very first live scan is then processed even if the
// device clock is wrong.
function markStreamConnected(deviceKey) {
  const st = { live: false, timer: null };
  streamState.set(deviceKey, st);
  armLiveTimer(st, deviceKey);
}

function noteEventAndCheckLive(deviceKey, evtTime) {
  let st = streamState.get(deviceKey);
  if (!st) { st = { live: false, timer: null }; streamState.set(deviceKey, st); }
  if (!st.live) armLiveTimer(st, deviceKey); // replay still flowing — keep waiting
  if (st.live) return true;
  // during replay, still let genuinely fresh events through
  return !Number.isNaN(evtTime) && Math.abs(Date.now() - evtTime) <= 10 * 60 * 1000;
}

// Global dedupe: the same physical scan may arrive via alertStream, the HTTP
// listener AND the poller — process it only once. Keys include the device IP
// and the event serialNo, so entries can safely be held for 10 minutes.
const recentEvents = new Map(); // key -> ts
function isDuplicate(key) {
  const now = Date.now();
  for (const [k, t] of recentEvents) if (now - t > 10 * 60 * 1000) recentEvents.delete(k);
  if (recentEvents.has(key)) return true;
  recentEvents.set(key, now);
  return false;
}

async function processEvent(evt, sourceIp, source) {
  try {
    const ace = evt.AccessControllerEvent;
    if (!ace) return; // heartbeat / videoloss / other noise

    const rawId = ace.employeeNoString || (ace.employeeNo != null ? String(ace.employeeNo) : null);
    if (!rawId) return; // event without a person

    // --- Live/backlog detection ---
    // On (re)connect the device replays its stored HISTORY through the stream.
    // SYNC mode (right after connect): only events whose own timestamp is
    //   within 10 min of now are processed; stale ones are skipped as backlog.
    // LIVE mode (after 5 quiet seconds = replay finished): EVERY event is
    //   processed, even if the device clock is wrong.
    const evtTime = Date.parse(evt.dateTime || "");
    const deviceKey = evt.ipAddress || sourceIp || "?";
    // The poller tracks serialNo itself and only ever emits NEW events, so it
    // bypasses the stream backlog gate entirely.
    if (source !== "poll" && !noteEventAndCheckLive(deviceKey, evtTime)) {
      return logEvent(`BACKLOG skip: eventTime=${evt.dateTime} empId=${rawId} name=${ace.name || "-"} sub=${ace.subEventType}`);
    }

    const sub = Number(ace.subEventType);
    let kind = null;
    if (cfg.FP_CODES.includes(sub)) kind = "fp";
    else if (cfg.FACE_CODES.includes(sub)) kind = "face";
    else {
      // Fallback: some firmwares use one shared subEventType for all auth
      // passes; distinguish by currentVerifyMode when it names a single method.
      const vm = String(ace.currentVerifyMode || "").toLowerCase();
      if (["fp", "fingerprint", "fingerprintorpw", "fporpw"].includes(vm)) kind = "fp";
      else if (["face", "faceorpw"].includes(vm)) kind = "face";
    }

    const deviceIp = evt.ipAddress || sourceIp || "?";
    logEvent(`src=${source} | ip=${deviceIp} | empId=${rawId} | name=${ace.name || "-"} | subEventType=${sub} | verifyMode=${ace.currentVerifyMode || "-"} | ${kind ? kind.toUpperCase() : "UNKNOWN"}`);
    if (!kind) kind = "other";

    // ===== RAW EVENT LOG — every Face/Fingerprint authentication event is
    // recorded in the local database (Employee ID + Event Type), exactly
    // like the device's own event history, REGARDLESS of whether the
    // employee is recognized or what the attendance logic decides to do
    // with it. This is a pure audit trail and can never be skipped. =====
    const dedupeKeyRaw = `${deviceIp}:${rawId}:${kind}:${ace.serialNo || Math.floor(Date.now() / 10000)}`;
    if (isDuplicate(dedupeKeyRaw)) {
      return logEvent(`DEDUP source: empId=${rawId} — same scan already processed`);
    }
    const eventTypeLabel = kind === "face" ? "Authenticated via Face"
      : kind === "fp" ? "Authenticated via Fingerprint"
      : "Authenticated via Other";
    try {
      store.logRawEvent(rawId, ace.name || null, eventTypeLabel, deviceIp, Date.now(), source);
    } catch (e) {
      console.error("raw_events yozishda xato:", e.message);
    }

    const emp = findEmployee(rawId, true); // exact — device IDs are verbatim
    if (!emp) {
      const nm = (evt.AccessControllerEvent && evt.AccessControllerEvent.name) || "?";
      const k = `unk:${rawId}`;
      if (!global.__unkWarned) global.__unkWarned = new Map();
      const lastW = global.__unkWarned.get(k) || 0;
      if (Date.now() - lastW > 24 * 3600 * 1000) {
        global.__unkWarned.set(k, Date.now());
        console.warn(`⚠️  UNKNOWN employee scanned: ID=${rawId} name=${nm} — if this is OUR employee, add them to employees.json and restart`);
      }
      return logEvent(`IGNORE: empId=${rawId} (${nm}) not found in employees.json`);
    }

    // Split deployment: each instance processes ONLY its own company's
    // employees, so two instances can never double-handle the same scan.
    if (cfg.MY_COMPANY && (emp.company || "").toLowerCase() !== cfg.MY_COMPANY) {
      return logEvent(`SKIP: ${emp.name} belongs to '${emp.company}' — handled by that company's instance`);
    }

    if (kind === "other") {
      // Unrecognized auth code: keep the raw dump for diagnostics, but do NOT
      // drop the event — the direction-based logic below can still use it
      // (e.g. an unknown fingerprint code on the outside device is a check-in).
      logEvent(`UNKNOWN RAW: ${JSON.stringify(evt)}`);
    }

    const ts = Date.now();
    deviceLastEvent.set(deviceIp, ts);
    console.log(`[event ${source}] ${emp.name} (${emp.id}) ${kind.toUpperCase()} @ ${deviceLabel(deviceIp)}`);
    await handleAuthEvent(emp, ts, deviceIp, kind);
  } catch (e) {
    console.error("Error while processing event:", e);
  }
}

// ============================= FORWARDING =============================

function forwardEvent(evt) {
  const urls = [...cfg.FORWARD_URLS];
  if (cfg.PERSONAL_BOT_URL) {
    const u = cfg.PERSONAL_BOT_URL.replace(/\/+$/, "");
    urls.push(u.includes("/hikvision") ? u : u + "/hikvision/event");
  }
  for (const url of urls) {
    fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(evt),
    }).catch(() => {}); // downstream bots may be offline — ignore silently
  }
}

// ============================= HTTP LISTENER (backup path) =============================

const app = express();
const upload = multer({ storage: multer.memoryStorage() });
app.use(express.json({ limit: "20mb", type: ["application/json", "text/plain"] }));

function extractEventJson(req) {
  if (req.body && typeof req.body === "object" && (req.body.AccessControllerEvent || req.body.eventType)) {
    return req.body;
  }
  if (req.body && typeof req.body.event_log === "string") {
    try { return JSON.parse(req.body.event_log); } catch (_) {}
  }
  if (req.files) {
    for (const f of req.files) {
      const s = f.buffer.toString("utf8").trim();
      if (s.startsWith("{")) { try { return JSON.parse(s); } catch (_) {} }
    }
  }
  if (req.body && typeof req.body === "object") {
    for (const v of Object.values(req.body)) {
      if (typeof v === "string" && v.trim().startsWith("{")) {
        try { return JSON.parse(v); } catch (_) {}
      }
    }
  }
  return null;
}

async function onHttpPush(req, res) {
  res.status(200).send("OK");
  const evt = extractEventJson(req);
  if (!evt) return;
  forwardEvent(evt);
  const ip = (req.ip || "").replace("::ffff:", "");
  await processEvent(evt, ip, "push");
}

app.post(cfg.EVENT_PATH, upload.any(), onHttpPush);
app.post("*", upload.any(), onHttpPush);
app.get("*", (req, res) => res.send(`${cfg.COMPANY_NAME} Attendance Bot is running ✅`));

// Devices sometimes abort a push mid-upload — that's harmless noise, not an
// error worth logging a stack trace for.
app.use((err, req, res, next) => {
  const msg = String((err && err.message) || "");
  if (msg.includes("Request aborted") || msg.includes("ECONNABORTED") || msg.includes("aborted")) {
    if (!res.headersSent) res.status(400).end();
    return;
  }
  console.error("HTTP listener error:", msg);
  if (!res.headersSent) res.status(500).end();
});

// ============================= ALERTSTREAM (primary path) =============================

const ALL_DEVICE_IPS = [...new Set([...cfg.INSIDE_IPS, ...cfg.OUTSIDE_IPS])];

if (TEST_MODE) {
  // no device connections in unit tests
} else if (!cfg.ALERTSTREAM_ENABLED) {
  console.log("alertStream disabled by config (ALERTSTREAM_ENABLED=false) — poller-only mode, this is fine");
  if (cfg.DEVICE_PASSWORD) {
    for (const ip of ALL_DEVICE_IPS) {
      startPolling(ip, cfg.DEVICE_HTTP_PORT, cfg.DEVICE_USERNAME, cfg.DEVICE_PASSWORD, (evt, deviceIp) => {
        processEvent(evt, deviceIp, "poll");
      }, console.log, cfg.POLL_INTERVAL_MS);
    }
  } else {
    console.warn("⚠️  DEVICE_PASSWORD is empty — no device connection possible.");
  }
} else if (!cfg.DEVICE_PASSWORD) {
  console.warn("⚠️  DEVICE_PASSWORD is empty in .env — alertStream (real-time pull) is DISABLED.");
  console.warn("    Set DEVICE_USERNAME / DEVICE_PASSWORD (the device web-login credentials) to enable it.");
} else {
  for (const ip of ALL_DEVICE_IPS) {
    // Guaranteed path: poll the device for new events every 4 seconds
    startPolling(ip, cfg.DEVICE_HTTP_PORT, cfg.DEVICE_USERNAME, cfg.DEVICE_PASSWORD, (evt, deviceIp) => {
      processEvent(evt, deviceIp, "poll");
    }, console.log, cfg.POLL_INTERVAL_MS);
    subscribeDevice(
      ip, cfg.DEVICE_HTTP_PORT, cfg.DEVICE_USERNAME, cfg.DEVICE_PASSWORD,
      (evt, deviceIp) => {
        forwardEvent(evt);
        processEvent(evt, deviceIp, "stream");
      },
      console.log,
      (deviceIp) => markStreamConnected(deviceIp)
    );
  }
}

// ============================= START =============================

if (!TEST_MODE) {
  app.listen(cfg.PORT, "0.0.0.0", () => {
    console.log(`HTTP listener: 0.0.0.0:${cfg.PORT}${cfg.EVENT_PATH}`);
  });

  // ===== STARTUP SELF-DIAGNOSTICS — pm2 logs will name the exact problem =====
  console.log(`── ${cfg.COMPANY_NAME} bot starting ──`);
  console.log(`Employees: ${Object.keys(EMPLOYEES).length} | Port: ${cfg.PORT} | Poll: ${cfg.POLL_INTERVAL_MS / 1000}s | alertStream: ${cfg.ALERTSTREAM_ENABLED ? "on" : "off"}`);
  try {
    const subs = store.chatsForEmployee ? "" : "";
  } catch (_) {}
  bot.telegram.getMe()
    .then((me) => console.log(`Telegram token OK ✅ — bot @${me.username}`))
    .catch((e) => console.error(`TELEGRAM TOKEN ERROR ❌ — ${e.message} (check TELEGRAM_BOT_TOKEN in .env)`));
  // Registers the bot's commands so Telegram shows the native "Menu" button
  // next to the message input (private chats): tapping it lists /start,
  // /stop, /notes, /health — tapping one inserts it, no typing needed. This
  // is what makes "reply, then pick /notes from the menu" possible.
  bot.telegram.setMyCommands([
    { command: "start", description: "Register to receive your attendance notifications" },
    { command: "notes", description: "Reply to a Late / No Show / Break-warning message to explain it" },
    { command: "stop", description: "Unsubscribe from notifications" },
    { command: "health", description: "Check the bot's status" },
  ]).then(() => console.log("Bot commands menu registered ✅"))
    .catch((e) => console.error("Failed to register commands menu:", e.message));
  // Verify the group WITHOUT sending a message
  bot.telegram.getChat(cfg.GROUP_CHAT_ID)
    .then((c) => console.log(`Group OK ✅ — "${c.title || c.id}" (${cfg.GROUP_CHAT_ID})`))
    .catch((e) => {
      console.error(`GROUP ERROR ❌ — ${cfg.GROUP_CHAT_ID}: ${e.message}`);
      console.error(`  FIX: 1) Add THIS bot to that group. 2) If the group became a supergroup, its ID changed (starts with -100...) — post a message in the group, open https://api.telegram.org/bot<TOKEN>/getUpdates, take chat.id, update TELEGRAM_CHAT_ID in .env, restart.`);
    });

  if (!cfg.TELEGRAM_POLLING || cfg.TELEGRAM_MODE === "worker") {
    // Workers only SEND messages. Polling stays off so the single shared bot
    // token never hits Telegram's 409 Conflict; /start /stop /health are
    // handled by the MASTER instance for all companies.
    console.log(cfg.TELEGRAM_POLLING
      ? "Worker mode: Telegram polling disabled — /start is handled by the master instance"
      : "Independent mode: Telegram polling disabled — DM chat ids come from employees.json (chatId)");
  } else {
    const launchPolling = (attempt = 1) => {
      bot.launch()
        .then(() => console.log("Telegram polling started ✅ — /start /stop /health are ACTIVE"))
        .catch((e) => {
          console.error(`TELEGRAM POLLING FAILED ❌ (attempt ${attempt}): ${e.message}`);
          if (/409/.test(e.message)) {
            console.error("  FIX: ANOTHER process is polling this SAME token (an old bot is still running).");
            console.error("  Run `pm2 list`, find the old process, `pm2 delete <old-name>` — this bot will recover automatically.");
          }
          setTimeout(() => launchPolling(attempt + 1), 15000);
        });
    };
    launchPolling();
    process.once("SIGINT", () => bot.stop("SIGINT"));
    process.once("SIGTERM", () => bot.stop("SIGTERM"));
  }
}

if (TEST_MODE) {
  module.exports = { handleAuthEvent, processEvent, store, runMaintenance, handleNotesCommand, handleNoteButtonPress, handleNoteTextReply };
}
