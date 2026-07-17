require("dotenv").config();

const splitList = (s) => (s || "").split(",").map(x => x.trim()).filter(Boolean);

module.exports = {
  BOT_TOKEN: process.env.TELEGRAM_BOT_TOKEN,
  GROUP_CHAT_ID: process.env.TELEGRAM_CHAT_ID,
  COMPANY_NAME: process.env.COMPANY_NAME || "Drenix",

  PORT: +(process.env.PORT || 8090),
  EVENT_PATH: "/hikvision/event",

  OUTSIDE_IPS: splitList(process.env.OUTSIDE_DEVICE_IPS),
  INSIDE_IPS: splitList(process.env.INSIDE_DEVICE_IPS),

  // Device admin credentials — required for the real-time alertStream connection
  DEVICE_USERNAME: process.env.DEVICE_USERNAME || "admin",
  DEVICE_HTTP_PORT: +(process.env.DEVICE_HTTP_PORT || 80),
  DEVICE_PASSWORD: process.env.DEVICE_PASSWORD || "",

  FORWARD_URLS: splitList(process.env.FORWARD_BOT_URLS),
  PERSONAL_BOT_URL: (process.env.PERSONAL_BOT_URL || "").trim(),

  DB_PATH: process.env.DB_PATH || "./attendance.db",

  // ===== SPLIT (per-company instances, ONE shared bot token) =====
  // master = handles Telegram polling (/start, /stop, /health) for everyone.
  // worker = sends messages only (no polling -> no 409 Conflict).
  TELEGRAM_MODE: (process.env.TELEGRAM_MODE || "master").toLowerCase(),
  // false = this instance NEVER polls Telegram (no /start handling) — DM chat
  // ids come from employees.json "chatId" fields instead. This lets ANY number
  // of fully independent instances share one bot token with zero conflicts.
  TELEGRAM_POLLING: (process.env.TELEGRAM_POLLING || "true").toLowerCase() !== "false",
  // Events are processed ONLY for employees of this company (empty = all).
  MY_COMPANY: (process.env.MY_COMPANY || "").trim().toLowerCase(),
  // Shared subscriptions DB so DMs work from every instance (empty = local).
  SHARED_DB_PATH: (process.env.SHARED_DB_PATH || "").trim(),
  // Stagger polling across instances to keep device load low.
  POLL_INTERVAL_MS: +(process.env.POLL_INTERVAL_MS || 4000),
  // Devices allow a limited number of concurrent alertStream connections.
  // With several instances, keep the stream on ONE bot only; the others rely
  // on their pollers (guaranteed delivery) — this prevents "too many
  // requests" / connection-reset churn on the devices.
  ALERTSTREAM_ENABLED: (process.env.ALERTSTREAM_ENABLED || "true").toLowerCase() !== "false",

  SHEET: {
    id: process.env.GOOGLE_SHEET_ID || "",
    name: process.env.GOOGLE_SHEET_NAME || "Sheet1",
    email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || "",
    key: (process.env.GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY || "").replace(/\\n/g, "\n"),
  },

  // Days when No Show alerts are NOT sent (0=Sunday, 1=Monday ... 6=Saturday)
  NO_SHOW_OFF_DAYS: [0],

  BREAK_LIMIT_MIN: 30,          // break limit (minutes)
  FACE_EXIT_CHECKOUT_MIN: 30,   // After the shift ends: a Face ID exit with no
                                // return within 30 minutes = CHECKOUT
  STALE_BREAK_HOURS: 3,         // abandoned open breaks are voided after this
  DEDUP_SECONDS: 180,           // window for ignoring repeated face scans

  // Hikvision subEventType codes — verify via events.log on first test
  FP_CODES: [38, 76],           // fingerprint authentication passed
  FACE_CODES: [75, 1, 21],      // face authentication passed

  TZ: "Asia/Tashkent",
};
