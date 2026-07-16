/**
 * Hikvision ISAPI alertStream client.
 * Connects directly to each device (GET /ISAPI/Event/notification/alertStream)
 * with HTTP Digest authentication and receives events in real time as a
 * long-lived multipart stream. Does NOT depend on the device's
 * "HTTP Listening" push configuration. Auto-reconnects on any failure.
 */

const http = require("http");
const crypto = require("crypto");

const md5 = (s) => crypto.createHash("md5").update(s).digest("hex");

function buildDigestHeader(user, pass, method, uri, wwwAuth) {
  const pick = (k) => {
    const m = wwwAuth.match(new RegExp(`${k}="([^"]+)"`)) || wwwAuth.match(new RegExp(`${k}=([^,\\s]+)`));
    return m ? m[1] : null;
  };
  const realm = pick("realm") || "";
  const nonce = pick("nonce") || "";
  const opaque = pick("opaque");
  let qop = pick("qop");
  if (qop && qop.includes(",")) qop = qop.split(",")[0].trim();

  const cnonce = crypto.randomBytes(8).toString("hex");
  const nc = "00000001";
  const ha1 = md5(`${user}:${realm}:${pass}`);
  const ha2 = md5(`${method}:${uri}`);
  const response = qop
    ? md5(`${ha1}:${nonce}:${nc}:${cnonce}:${qop}:${ha2}`)
    : md5(`${ha1}:${nonce}:${ha2}`);

  let h = `Digest username="${user}", realm="${realm}", nonce="${nonce}", uri="${uri}", response="${response}"`;
  if (qop) h += `, qop=${qop}, nc=${nc}, cnonce="${cnonce}"`;
  if (opaque) h += `, opaque="${opaque}"`;
  return h;
}

/**
 * Scans a text buffer for complete top-level JSON objects (brace matching,
 * string-aware), invokes cb(parsedJson) for each, returns the unconsumed tail.
 */
function extractJsonObjects(buf, cb) {
  let i = buf.indexOf("{");
  while (i !== -1) {
    let depth = 0, inStr = false, esc = false, end = -1;
    for (let j = i; j < buf.length; j++) {
      const c = buf[j];
      if (inStr) {
        if (esc) esc = false;
        else if (c === "\\") esc = true;
        else if (c === '"') inStr = false;
      } else {
        if (c === '"') inStr = true;
        else if (c === "{") depth++;
        else if (c === "}") {
          depth--;
          if (depth === 0) { end = j; break; }
        }
      }
    }
    if (end === -1) return buf.slice(i); // incomplete object — wait for more data
    const segment = buf.slice(i, end + 1);
    try { cb(JSON.parse(segment)); } catch (_) { /* not valid JSON, skip */ }
    buf = buf.slice(end + 1);
    i = buf.indexOf("{");
  }
  return "";
}

/**
 * Opens (and keeps re-opening) the alertStream of one device.
 * onEvent(evtJson, deviceIp) is called for every JSON event received.
 */
function subscribeDevice(ip, port, user, pass, onEvent, log = console.log, onConnect = null) {
  const uri = "/ISAPI/Event/notification/alertStream";
  let alive = true;
  let retryTimer = null;

  const retry = (reason, delayMs = 5000) => {
    if (!alive) return;
    log(`[alertStream ${ip}:${port}] disconnected (${reason}), retrying in ${Math.round(delayMs / 1000)}s...`);
    clearTimeout(retryTimer);
    retryTimer = setTimeout(connect, delayMs);
  };

  // Some firmwares don't expose the alertStream endpoint at all (HTTP 404).
  // That's fine — the 4s poller still delivers every event. Log it once in a
  // friendly way and only re-check hourly instead of spamming every 5s.
  const notSupported = () => {
    log(`[alertStream ${ip}:${port}] not supported by this device firmware (HTTP 404) — no problem, the 4s poller handles all events. Re-checking hourly.`);
    clearTimeout(retryTimer);
    retryTimer = setTimeout(connect, 60 * 60 * 1000);
  };

  const consume = (res) => {
    log(`[alertStream ${ip}:${port}] connected ✅ (real-time events active)`);
    if (onConnect) { try { onConnect(ip); } catch (_) {} }

    let buf = "";
    let lastData = Date.now();

    // Idle watchdog: the device sends periodic heartbeats, so a silent
    // connection for 90s means it is dead (half-open TCP) — force reconnect.
    const idleTimer = setInterval(() => {
      if (Date.now() - lastData > 90_000) {
        log(`[alertStream ${ip}:${port}] no data for 90s — forcing reconnect`);
        res.destroy(new Error("idle timeout"));
      }
    }, 30_000);
    const cleanup = () => clearInterval(idleTimer);

    res.setEncoding("utf8");
    res.on("data", (chunk) => {
      lastData = Date.now();
      buf += chunk;
      buf = extractJsonObjects(buf, (evt) => onEvent(evt, ip));
      if (buf.length > 1_000_000) buf = buf.slice(-100_000); // safety valve
    });
    res.on("end", () => { cleanup(); retry("stream ended"); });
    res.on("error", (e) => { cleanup(); retry(e.message); });
    res.on("close", () => { cleanup(); });
  };

  const connect = () => {
    if (!alive) return;
    const req1 = http.request({ host: ip, port, path: uri, method: "GET", timeout: 15000 }, (res1) => {
      if (res1.statusCode === 200) return consume(res1);
      if (res1.statusCode === 401) {
        const www = res1.headers["www-authenticate"] || "";
        res1.resume();
        const auth = buildDigestHeader(user, pass, "GET", uri, www);
        const req2 = http.request(
          { host: ip, port, path: uri, method: "GET", headers: { Authorization: auth } },
          (res2) => {
            if (res2.statusCode === 200) return consume(res2);
            res2.resume();
            if (res2.statusCode === 404) return notSupported();
            if (res2.statusCode === 401 || res2.statusCode === 403) {
              return retry(`auth failed, HTTP ${res2.statusCode} — check DEVICE_USERNAME/DEVICE_PASSWORD in .env`, 30000);
            }
            retry(`HTTP ${res2.statusCode}`);
          }
        );
        req2.on("error", (e) => retry(e.message));
        req2.end();
        return;
      }
      res1.resume();
      if (res1.statusCode === 404) return notSupported();
      retry(`HTTP ${res1.statusCode}`);
    });
    req1.on("timeout", () => { req1.destroy(); retry("connect timeout"); });
    req1.on("error", (e) => retry(e.message));
    req1.end();
  };

  connect();
  return () => { alive = false; clearTimeout(retryTimer); };
}

// Keep-alive agents per device: reuse TCP connections instead of opening a
// new one for every poll — Hikvision terminals reset connections under the
// load of multiple rapidly-connecting clients.
const keepAliveAgents = new Map();
function agentFor(ip, port) {
  const k = `${ip}:${port}`;
  if (!keepAliveAgents.has(k)) {
    keepAliveAgents.set(k, new http.Agent({ keepAlive: true, keepAliveMsecs: 3000, maxSockets: 2 }));
  }
  return keepAliveAgents.get(k);
}

/**
 * One-shot HTTP request with Digest auth that returns parsed JSON.
 * Used by the poller for /ISAPI/System/time and /ISAPI/AccessControl/AcsEvent.
 */
function digestTextRequestOnce(ip, port, user, pass, method, path, bodyObj) {
  return new Promise((resolve, reject) => {
    const body = bodyObj ? JSON.stringify(bodyObj) : null;
    const doReq = (authHeader) => {
      const headers = { "Content-Type": "application/json" };
      if (body) headers["Content-Length"] = Buffer.byteLength(body);
      if (authHeader) headers["Authorization"] = authHeader;
      const req = http.request({ host: ip, port, path, method, headers, timeout: 10000, agent: agentFor(ip, port) }, (res) => {
        let data = "";
        res.setEncoding("utf8");
        res.on("data", (c) => (data += c));
        res.on("end", () => {
          if (res.statusCode === 401 && !authHeader) {
            const auth = buildDigestHeader(user, pass, method, path, res.headers["www-authenticate"] || "");
            return doReq(auth);
          }
          if (res.statusCode >= 200 && res.statusCode < 300) {
            resolve(data);
          } else {
            reject(new Error(`HTTP ${res.statusCode}: ${data.slice(0, 150)}`));
          }
        });
      });
      req.on("timeout", () => req.destroy(new Error("request timeout")));
      req.on("error", reject);
      if (body) req.write(body);
      req.end();
    };
    doReq(null);
  });
}

// Transient connection drops (device closed an idle keep-alive socket, brief
// overload from multiple polling clients) are retried automatically.
const TRANSIENT = ["ECONNRESET", "socket hang up", "EPIPE", "ETIMEDOUT", "request timeout"];
async function digestTextRequest(ip, port, user, pass, method, path, bodyObj) {
  try {
    return await digestTextRequestOnce(ip, port, user, pass, method, path, bodyObj);
  } catch (e) {
    const msg = String((e && e.message) || e);
    if (!TRANSIENT.some((t) => msg.includes(t))) throw e;
    await new Promise((r) => setTimeout(r, 300));
    return digestTextRequestOnce(ip, port, user, pass, method, path, bodyObj);
  }
}

async function digestJsonRequest(ip, port, user, pass, method, path, bodyObj) {
  const text = await digestTextRequest(ip, port, user, pass, method, path, bodyObj);
  try {
    return JSON.parse(text);
  } catch (_) {
    throw new Error(`non-JSON response from ${path}: ${String(text).slice(0, 120)}`);
  }
}

module.exports = { subscribeDevice, digestJsonRequest, digestTextRequest };
