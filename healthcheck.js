// Manual health check for independent (no-polling) instances.
// Usage: node healthcheck.js
// Sends a test message to this project's group and reports the result.
const cfg = require("./config");

(async () => {
  const api = `https://api.telegram.org/bot${cfg.BOT_TOKEN}`;
  try {
    const me = await (await fetch(`${api}/getMe`)).json();
    if (!me.ok) throw new Error(JSON.stringify(me));
    console.log(`Telegram token OK ✅ — @${me.result.username}`);
  } catch (e) {
    console.error("Token/network ERROR ❌:", e.message);
    process.exit(1);
  }
  try {
    const r = await (await fetch(`${api}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chat_id: cfg.GROUP_CHAT_ID, text: `🩺 ${cfg.COMPANY_NAME} health check — please ignore.` }),
    })).json();
    if (!r.ok) throw new Error(r.description || JSON.stringify(r));
    console.log(`Group send OK ✅ — ${cfg.GROUP_CHAT_ID}`);
  } catch (e) {
    console.error(`Group send ERROR ❌ (${cfg.GROUP_CHAT_ID}):`, e.message);
    console.error("  -> Is the bot added to this group? Is the ID correct?");
    process.exit(1);
  }
})();
