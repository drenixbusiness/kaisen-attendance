/**
 * Guaranteed-delivery event poller.
 * Every few seconds the bot itself asks the device for recent access events
 * via ISAPI (/ISAPI/AccessControl/AcsEvent). This catches every scan even when
 * the device's alertStream is late or silently drops events (the observed
 * problem on the outside device). The device's own clock is used for the time
 * window (synced via /ISAPI/System/time), so a wrong device clock does not
 * break the queries. New events are detected by serialNo, so nothing is ever
 * processed twice and history is never replayed.
 */

const { digestJsonRequest, digestTextRequest } = require("./alertstream");

function isoInTz(ms, tzSuffix) {
  const m = tzSuffix.match(/([+-])(\d{2}):(\d{2})/);
  const off = m ? (Number(m[2]) * 60 + Number(m[3])) * 60000 * (m[1] === "-" ? -1 : 1) : 0;
  return new Date(ms + off).toISOString().slice(0, 19) + tzSuffix;
}

function startPolling(ip, port, user, pass, onEvent, log = console.log, intervalMs = 4000) {
  let clockOffset = null;      // device clock - server clock (ms)
  let tzSuffix = "+05:00";
  let lastSerial = -1;
  let initialized = false;     // first cycle sets the baseline without notifying
  let inFlight = false;
  let lastClockSync = 0;
  let errStreak = 0;

  async function syncClock() {
    // Some firmwares ignore format=json on System endpoints and return XML —
    // accept both formats.
    const text = await digestTextRequest(ip, port, user, pass, "GET", "/ISAPI/System/time?format=json");
    let lt = null;
    try {
      const j = JSON.parse(text);
      lt = j && j.Time && j.Time.localTime;
    } catch (_) {
      const m = String(text).match(/<localTime>([^<]+)<\/localTime>/);
      if (m) lt = m[1];
    }
    if (!lt) throw new Error("no localTime in /ISAPI/System/time response: " + String(text).slice(0, 120));
    const m = String(lt).match(/([+-]\d{2}:\d{2})$/);
    if (m) tzSuffix = m[1];
    const t = Date.parse(lt);
    if (Number.isNaN(t)) throw new Error("unparsable device time: " + lt);
    clockOffset = t - Date.now();
    lastClockSync = Date.now();
  }

  async function fetchWindow(startIso, endIso) {
    const infos = [];
    let pos = 0;
    for (let page = 0; page < 10; page++) {
      const body = {
        AcsEventCond: {
          searchID: "drenix-attendance-bot",
          searchResultPosition: pos,
          maxResults: 30,
          major: 5, // access controller events
          minor: 0, // all sub types
          startTime: startIso,
          endTime: endIso,
        },
      };
      const j = await digestJsonRequest(ip, port, user, pass, "POST", "/ISAPI/AccessControl/AcsEvent?format=json", body);
      const a = j && j.AcsEvent;
      const list = (a && a.InfoList) || [];
      infos.push(...list);
      pos += list.length;
      if (!a || a.responseStatusStrg !== "MORE" || list.length === 0) break;
    }
    return infos;
  }

  async function tick() {
    if (inFlight) return;
    inFlight = true;
    try {
      if (clockOffset === null || Date.now() - lastClockSync > 2 * 60 * 1000) {
        await syncClock();
      }
      const devNow = Date.now() + clockOffset;
      const infos = await fetchWindow(isoInTz(devNow - 5 * 60 * 1000, tzSuffix), isoInTz(devNow + 2 * 60 * 1000, tzSuffix));

      let maxSerial = lastSerial;
      const fresh = [];
      for (const info of infos) {
        const sn = Number(info.serialNo);
        if (!Number.isFinite(sn)) continue;
        if (sn > maxSerial) maxSerial = sn;
        if (initialized && sn > lastSerial) fresh.push(info);
      }
      fresh.sort((a, b) => Number(a.serialNo) - Number(b.serialNo));
      lastSerial = maxSerial;

      if (!initialized) {
        initialized = true;
        log(`[poller ${ip}:${port}] active ✅ (baseline serialNo=${lastSerial}, device clock offset ${Math.round(clockOffset / 1000)}s, every ${intervalMs / 1000}s)`);
      }

      for (const info of fresh) {
        onEvent({
          ipAddress: ip,
          dateTime: info.time,
          AccessControllerEvent: {
            employeeNoString: info.employeeNoString,
            employeeNo: info.employeeNo,
            name: info.name,
            subEventType: info.minor,
            serialNo: info.serialNo,
            currentVerifyMode: info.currentVerifyMode,
          },
        }, ip);
      }
      errStreak = 0;
    } catch (e) {
      errStreak++;
      // Single transient failures (device reset a connection) are retried on
      // the next 4s tick — only log when the problem PERSISTS.
      if (errStreak === 3 || errStreak % 15 === 0) {
        log(`[poller ${ip}:${port}] still failing after ${errStreak} attempts: ${e.message}`);
      }
    } finally {
      inFlight = false;
    }
  }

  const timer = setInterval(tick, intervalMs);
  tick();
  return () => clearInterval(timer);
}

module.exports = { startPolling };
