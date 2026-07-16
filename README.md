# Drenix Attendance Bot — Setup (Windows, 10.1.1.133)

## 1. REQUIRED: fill in device credentials
Open `.env` and set:
```
DEVICE_USERNAME=admin
DEVICE_PASSWORD=<the password you use to log in to the device web page>
DEVICE_HTTP_PORT=90
```
This enables the **alertStream** connection — the bot connects directly to both
devices (10.1.1.211 and 10.1.1.248, HTTP port 90) and pulls events in REAL TIME.
This is the primary, reliable path and does not depend on the devices' HTTP
Listening push settings.

## 2. Install & run
```
cd C:\drenix-bot
npm install
node index.js
```
Expected console output:
```
HTTP listener: 0.0.0.0:8090/hikvision/event
[alertStream 10.1.1.211:90] connected ✅ (real-time events active)
[alertStream 10.1.1.248:90] connected ✅ (real-time events active)
Telegram bot started
```
If you see `auth failed` — the DEVICE_USERNAME/DEVICE_PASSWORD in `.env` is wrong.

## 3. Windows Firewall (once)
```
netsh advfirewall firewall add rule name="Drenix Bot 8090" dir=in action=allow protocol=TCP localport=8090
```

## 4. Run as a service (auto-start)
```
npm install -g pm2 pm2-windows-startup
pm2 start index.js --name drenix-bot
pm2 save
pm2-startup install
```

## How it works

### Event sources
1. **Poller (guaranteed)** — every 4 seconds the bot queries each device for new events via ISAPI AcsEvent, tracked by serialNo. Catches every scan even if the stream is late or drops it; uses the device's own clock, so a wrong device clock cannot break it.
2. **alertStream (fast path)** — direct real-time stream from both devices with Digest auth. Auto-reconnects on drop; a 90s idle watchdog kills dead connections.
3. **HTTP listener on :8090 (backup)** — accepts events the devices push themselves.
A serialNo-based global dedupe ensures the same scan is never processed twice regardless of which source delivers it first.

### Fingerprint (per-employee shift windows)
Each employee's shift comes from `employees.json` (`shiftKey`) → `shifts.js`.
Example, shift 6-3 (work 18:00–03:00):
- **Check-in zone (from 14:00 until end of day)**: the FIRST fingerprint = CHECKED IN. Every further fingerprint — on either device — is IGNORED.
- **Check-out window 02:50–11:00 (next day)**: the FIRST fingerprint = CHECKED OUT (worked hours calculated). The rest are IGNORED.
- Arrival status: up to workStart + 10 min = **On time** (e.g. 18:00–18:10); 10 min – 2 hours = **Late by X min**; over 2 hours = **Very late — marked as No Show**.
- **No Show Alert**: if no check-in arrives within 130 minutes of shift start, the bot sends a No Show alert (once per shift date) to the employee DM and the group. **Sundays are skipped** (non-working day) — configurable via `NO_SHOW_OFF_DAYS` in `config.js` (0=Sunday ... 6=Saturday, e.g. `[0, 6]` for Sat+Sun).
- Check-in, check-out and No Show messages go to the employee DM **and** the group.

### Face ID (breaks) — direction-based
- **Inside device (10.1.1.211) = exit**: face here = BREAK STARTED (only during an active shift).
- **Outside device (10.1.1.248) = entry**: face here = BACK FROM BREAK if an open break exists. If there is NO open break (the employee walked out earlier without scanning, behind a colleague), the scan is **IGNORED** — entering can never open a break, so tailgating no longer creates false break-outs.
- Any scan (face, fingerprint, or even an unrecognized event code) on the OUTSIDE device with no check-in yet, inside the check-in zone, counts as CHECK-IN — a forgotten fingerprint or unknown code can never swallow a check-in.
- A fingerprint on the INSIDE device in the check-in zone with no check-in yet also counts as CHECK-IN (wrong-device mistake).
- If a break exceeds 30 minutes: **one single warning**. Duration is shown when they return.
- All break messages (out / back / warning) go to the employee DM **only** — the group receives just fingerprint check-in/check-out and No Show alerts.

### Telegram
- `/start` → employee enters their ID ("001" or "1" both work) → then their **personal secret code** (from `employees.json`, case-insensitive; 3 wrong attempts reset the flow) → personal notifications in DM. Secrets are handed to each employee individually by the administrator.
- `/stop` → unsubscribes and clears their data.
- The group (`-5259287256`) receives every event.

### Google Sheets
Every event is appended to the `Drenix` sheet: `Date | ID | Name | Event | Time | Note`.
The service account (`hikvision-sheets-bot@face-id-drenix.iam.gserviceaccount.com`) must be added to the sheet as **Editor**.

### Forwarding
Every raw Hikvision event is forwarded as JSON to `FORWARD_BOT_URLS` and `PERSONAL_BOT_URL` (fire-and-forget; offline targets are ignored).

## First test — verify event codes
Scan a fingerprint and a face, then open `events.log`. Each line shows
`subEventType=XX | FP / FACE / UNKNOWN`. If you see `UNKNOWN`, add that code to
`FP_CODES` or `FACE_CODES` in `config.js` and restart. Codes vary by firmware.

## Multiple companies in ONE bot
`companies.json` maps each company to its own Telegram group and Google Sheets tab:
```json
{
  "drenix": { "label": "Drenix", "groupChatId": "-5259287256", "sheetName": "Drenix" },
  "jm":     { "label": "JM",     "groupChatId": "-5206027752", "sheetName": "JM" }
}
```
Each employee in `employees.json` gets a `"company"` key. Check-in / check-out / No Show go to that employee's OWN company group; breaks stay DM-only. Sheets rows go to the company's tab. Employees without a company fall back to the `.env` defaults. The bot must be a member of every group listed. Run only ONE bot process — it handles all companies and polls the devices once (less device load). `/health` now tests every group.

## Adding a new employee
Add to `employees.json` (with a unique secret and company) and restart:
```json
"55": { "name": "New Employee", "shiftKey": "6-3", "secret": "AB12-CD34", "company": "jm" }
```
To change someone's secret, edit it in `employees.json` and restart the bot. Already-registered chats stay subscribed; the secret is only checked during /start registration.

## Files
- `.env` — token, group, device IPs/credentials, Sheets, forward URLs
- `employees.json` — employees (ID → name, shift)
- `shifts.js` — shift rules (5-2, 6-3, 7-4)
- `alertstream.js` — real-time device connection (Digest auth)
- `attendance.db` — SQLite database (built-in node:sqlite, no compilation needed)
- `events.log` — raw event log
