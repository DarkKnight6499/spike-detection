// Starts a one-shot spike poll every 5 minutes in market hours; GitHub's own cron is too slow.
const REPO = "DarkKnight6499/spike-detection";
const DISPATCH = `https://api.github.com/repos/${REPO}/actions/workflows/spikes.yml/dispatches`;
const OPEN_MIN = 9 * 60 + 30;    // ET window; the cron covers both UTC offsets, this gate keeps it DST-safe
const CLOSE_MIN = 16 * 60;

async function ntfy(env, title, message) {
  if (!env.NTFY_TOPIC) return;
  await fetch(`https://ntfy.sh/${env.NTFY_TOPIC}`, { method: "POST", body: message, headers: { Title: title } });
}

function etNow() {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York", hour: "numeric", minute: "numeric", hour12: false, weekday: "short",
  }).formatToParts(new Date());
  const get = (t) => parts.find((p) => p.type === t).value;
  return { minutes: (Number(get("hour")) % 24) * 60 + Number(get("minute")), weekday: get("weekday") };
}

async function dispatch(env) {
  const res = await fetch(DISPATCH, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.GH_TOKEN}`,
      Accept: "application/vnd.github+json",
      "User-Agent": "spike-trigger",
      "X-GitHub-Api-Version": "2022-11-28",
    },
    body: JSON.stringify({ ref: "main" }),
  });
  if (res.status !== 204) {
    const detail = `${res.status} ${(await res.text()).slice(0, 200)}`;
    await ntfy(env, "spike trigger failed", `workflow_dispatch returned ${detail} (token expired or revoked?)`);
    throw new Error(detail);
  }
}

export default {
  async scheduled(event, env, ctx) {
    const { minutes, weekday } = etNow();
    if (weekday === "Sat" || weekday === "Sun") return;
    if (minutes < OPEN_MIN || minutes > CLOSE_MIN) return;
    ctx.waitUntil(dispatch(env));
  },
};
