// Optional extra starter for the spike poller; GitHub's own schedule in spikes.yml is the main one.
// The poller is a long-running loop, so only the two segment starts need dispatching.
const REPO = "DarkKnight6499/spike-detection";
const DISPATCH = `https://api.github.com/repos/${REPO}/actions/workflows/spikes.yml/dispatches`;
const SLOTS = [9 * 60 + 25, 14 * 60 + 50];   // ET minutes of day; the cron covers both UTC offsets, this gate keeps it DST-safe
const SLACK_MINUTES = 8;

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
    if (SLOTS.some((slot) => Math.abs(minutes - slot) <= SLACK_MINUTES)) ctx.waitUntil(dispatch(env));
  },
};
