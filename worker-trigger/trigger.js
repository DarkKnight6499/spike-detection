// Starts the spike stream on time; GitHub's own cron is too slow. Two segments keep each job under the 6h limit.
const REPO = "DarkKnight6499/spike-detection";
const DISPATCH = `https://api.github.com/repos/${REPO}/actions/workflows/spikes.yml/dispatches`;
// [ET hour, ET minute, stop time passed to the script]; the second run queues behind the first via the workflow concurrency group.
const SEGMENTS = [
  [9, 25, "14:55"],
  [14, 50, "16:00"],
];
const SLACK_MINUTES = 8;   // cron fires at both UTC offsets; only the one matching ET (DST-safe) dispatches

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

async function dispatch(env, until) {
  const res = await fetch(DISPATCH, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.GH_TOKEN}`,
      Accept: "application/vnd.github+json",
      "User-Agent": "spike-trigger",
      "X-GitHub-Api-Version": "2022-11-28",
    },
    body: JSON.stringify({ ref: "main", inputs: { until } }),
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
    for (const [h, m, until] of SEGMENTS) {
      if (Math.abs(minutes - (h * 60 + m)) <= SLACK_MINUTES) ctx.waitUntil(dispatch(env, until));
    }
  },
};
