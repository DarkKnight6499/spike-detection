"""Real-time S&P 500 spike detector on Alpaca minute bars (free IEX feed) with ntfy push alerts.

Usage (PowerShell):
    py spike_detector.py poll                 # one-shot: alert on moves of 2%+ (up or down) over the last 15 minutes or 2 minutes
    py spike_detector.py poll --loop [--until 16:00] [--max-minutes 335]   # same check every minute in one process
    py spike_detector.py notify-test          # send one test push to NTFY_TOPIC, exit 1 if ntfy rejects it
    py spike_detector.py live [--until 14:55] # stream until the given ET time (default close), alert to ntfy
    py spike_detector.py replay 2026-09-30    # run the detector over one historical day, no ntfy
Env vars: ALPACA_KEY, ALPACA_SECRET, NTFY_TOPIC (live mode only).
"""
import asyncio
import csv
import io
import json
import os
import statistics
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

# ---------------- CONFIG ----------------
ALPACA_KEY_ENV = "ALPACA_KEY"
ALPACA_SECRET_ENV = "ALPACA_SECRET"
NTFY_TOPIC_ENV = "NTFY_TOPIC"
NTFY_BASE_URL = "https://ntfy.sh"

BASE_DIR = Path(__file__).parent
UNIVERSE_CACHE = BASE_DIR / "sp500_constituents.csv"
UNIVERSE_MAX_AGE_DAYS = 30
ALERT_LOG = BASE_DIR / "alerts_log.csv"
SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
HTTP_USER_AGENT = "Mozilla/5.0 (spike-detector; contact: you@example.com)"

WINDOW_BARS = 30          # rolling baseline length in 1-minute bars
MIN_BARS = 15             # bars needed before a ticker can alert
Z_THRESHOLD = 4.0         # 1-bar return z-score trigger
MIN_ABS_RETURN = 0.003    # z trigger also needs a move of at least 0.30%
ABS_RETURN_3BAR = 0.010   # or a 1.0% cumulative move over 3 bars
SIGMA_FLOOR = 0.0005      # avoids divide-by-near-zero on illiquid names
COOLDOWN_MINUTES = 10     # per-ticker repeat suppression
SKIP_FIRST_MINUTES = 3    # opening prints are noisy

BATCH_SECONDS = 5         # merge alerts landing close together into one push
MAX_PUSHES_PER_HOUR = 60
MAX_ALERTS_IN_PUSH = 8

SEED_MINUTES = 60         # history fetched at live start to warm the baselines
HIST_CHUNK = 100          # symbols per historical request

SNAPSHOT_URL = "https://data.alpaca.markets/v2/stocks/snapshots"
SNAPSHOT_CHUNK = 100
BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
WINDOW_MINUTES = 15       # poll mode: main window, equals the simulator delay so the move is the gain vs its stale price
FAST_SPIKE_PCT = 0.02    # poll mode: the fast window needs a bigger move
FAST_WINDOW_MINUTES = 2   # poll mode: second window that catches sharp moves right away
FAST_REF_LOOKBACK_MINUTES = 5
REF_LOOKBACK_MINUTES = 30 # the reference bar must be WINDOW_MINUTES to this many minutes old
SPIKE_PCT = 0.01          # poll mode: alert when the move over WINDOW_MINUTES reaches this, up or down
POLL_INTERVAL_SECONDS = 60
POLL_SETTLE_SECONDS = 5   # wait this long after each boundary so the latest bars are published
MAX_WAIT_FOR_OPEN_SECONDS = 3600  # loop mode: wait for the open only if it is this close, else exit
REALERT_STEP = 0.01       # poll mode: inside the cooldown, re-alert only if the gain grew by this much
POLL_COOLDOWN_MINUTES = 15
HEARTBEAT_EVERY_CYCLES = 60  # loop mode: low-priority "still running" push roughly hourly
MAX_CONSECUTIVE_FAILURES = 3  # loop mode: push an error alert after this many failed polls in a row
STALE_TRADE_MINUTES = 10  # ignore symbols whose last IEX trade is older than this
POLL_STATE = BASE_DIR / "alert_state.json"
POLL_LOG = BASE_DIR / "alerts" / "poll_alerts.json"   # tracked in git; the workflow commits it

ET = ZoneInfo("America/New_York")
MARKET_OPEN = dtime(9, 30)
MARKET_CLOSE = dtime(16, 0)

SYMBOL_COLUMN = "symbol"
TIME_COLUMN = "timestamp"
CLOSE_COLUMN = "close"
VOLUME_COLUMN = "volume"
LOG_COLUMNS = ["time_et", "symbol", "ret_1bar", "ret_3bar", "zscore", "volume_ratio", "close"]
# ----------------------------------------


@dataclass
class Alert:
    symbol: str
    ts: datetime
    ret_1bar: float
    ret_3bar: float
    zscore: float
    volume_ratio: float
    close: float


class SpikeDetector:
    def __init__(self):
        self.closes = {}
        self.rets = {}
        self.vols = {}
        self.last_alert = {}

    def update(self, symbol, ts, close, volume, can_alert=True):
        closes = self.closes.setdefault(symbol, deque(maxlen=4))
        rets = self.rets.setdefault(symbol, deque(maxlen=WINDOW_BARS))
        vols = self.vols.setdefault(symbol, deque(maxlen=WINDOW_BARS))
        alert = None
        if closes:
            ret = close / closes[-1] - 1.0
            ret3 = close / closes[0] - 1.0 if len(closes) == 3 else 0.0
            if can_alert and len(rets) >= MIN_BARS:
                alert = self._check(symbol, ts, close, volume, ret, ret3, rets, vols)
            rets.append(ret)
        closes.append(close)
        vols.append(volume)
        return alert

    def _check(self, symbol, ts, close, volume, ret, ret3, rets, vols):
        mu = statistics.fmean(rets)
        sigma = max(statistics.pstdev(rets), SIGMA_FLOOR)
        z = (ret - mu) / sigma
        z_trigger = abs(z) >= Z_THRESHOLD and abs(ret) >= MIN_ABS_RETURN
        if not (z_trigger or abs(ret3) >= ABS_RETURN_3BAR):
            return None
        last = self.last_alert.get(symbol)
        if last is not None and ts - last < timedelta(minutes=COOLDOWN_MINUTES):
            return None
        self.last_alert[symbol] = ts
        typical_vol = statistics.median(vols) if vols else 0
        vol_ratio = volume / typical_vol if typical_vol else float("nan")
        return Alert(symbol, ts, ret, ret3, z, vol_ratio, close)


def format_push(alerts):
    alerts = sorted(alerts, key=lambda a: abs(a.ret_1bar), reverse=True)
    lines = [f"{a.symbol} {a.ret_1bar:+.2%} (3m {a.ret_3bar:+.2%}, z {a.zscore:+.1f}, vol x{a.volume_ratio:.1f})"
             for a in alerts[:MAX_ALERTS_IN_PUSH]]
    if len(alerts) > MAX_ALERTS_IN_PUSH:
        lines.append(f"+{len(alerts) - MAX_ALERTS_IN_PUSH} more")
    title = f"{len(alerts)} spike{'s' if len(alerts) != 1 else ''}"
    return title, "\n".join(lines)


def log_alerts(alerts):
    new_file = not ALERT_LOG.exists()
    with open(ALERT_LOG, "a", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        if new_file:
            writer.writerow(LOG_COLUMNS)
        for a in alerts:
            writer.writerow([a.ts.astimezone(ET).isoformat(), a.symbol, f"{a.ret_1bar:.5f}",
                             f"{a.ret_3bar:.5f}", f"{a.zscore:.2f}", f"{a.volume_ratio:.2f}", f"{a.close:.4f}"])


class Notifier:
    def __init__(self, topic):
        self.url = f"{NTFY_BASE_URL}/{topic}"
        self.sent = deque()

    def push(self, title, body, high=False, status=False, tag=None):
        now = time.time()
        while self.sent and now - self.sent[0] > 3600:
            self.sent.popleft()
        if not status and len(self.sent) >= MAX_PUSHES_PER_HOUR:
            print("ntfy hourly cap reached, alert logged only")
            return
        try:
            resp = requests.post(self.url, data=body.encode("utf-8"), timeout=10,
                                 headers={"Title": title, "Priority": "high" if high else ("min" if status else "default"),
                                          "Tags": "heartbeat" if status else (tag or "chart_with_upwards_trend")})
            resp.raise_for_status()
            print(f"ntfy sent ({resp.status_code}): {title}")
            if not status:
                self.sent.append(now)
            return True
        except requests.RequestException as exc:
            print(f"ntfy FAILED: {exc}")
            return False


def load_universe():
    fresh = (UNIVERSE_CACHE.exists()
             and time.time() - UNIVERSE_CACHE.stat().st_mtime < UNIVERSE_MAX_AGE_DAYS * 86400)
    if fresh:
        return pd.read_csv(UNIVERSE_CACHE)[SYMBOL_COLUMN].tolist()
    resp = requests.get(SP500_URL, headers={"User-Agent": HTTP_USER_AGENT}, timeout=30)
    resp.raise_for_status()
    table = pd.read_html(io.StringIO(resp.text))[0]
    symbols = sorted(table["Symbol"].str.strip().unique())
    pd.DataFrame({SYMBOL_COLUMN: symbols}).to_csv(UNIVERSE_CACHE, index=False)
    return symbols


def fetch_bars(symbols, start, end):
    from alpaca.data.enums import DataFeed
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame
    client = StockHistoricalDataClient(os.environ[ALPACA_KEY_ENV], os.environ[ALPACA_SECRET_ENV])
    frames = []
    for i in range(0, len(symbols), HIST_CHUNK):
        req = StockBarsRequest(symbol_or_symbols=symbols[i:i + HIST_CHUNK], timeframe=TimeFrame.Minute,
                               start=start, end=end, feed=DataFeed.IEX)
        df = client.get_stock_bars(req).df
        if not df.empty:
            frames.append(df.reset_index())
    if not frames:
        return pd.DataFrame(columns=[SYMBOL_COLUMN, TIME_COLUMN, CLOSE_COLUMN, VOLUME_COLUMN])
    return pd.concat(frames).sort_values(TIME_COLUMN)


def in_market_hours(now=None):
    now = (now or datetime.now(ET)).astimezone(ET)
    return now.weekday() < 5 and MARKET_OPEN <= now.time() < MARKET_CLOSE


def run_replay(day):
    symbols = load_universe()
    start = datetime.combine(day, MARKET_OPEN, tzinfo=ET)
    end = datetime.combine(day, MARKET_CLOSE, tzinfo=ET)
    bars = fetch_bars(symbols, start, end)
    det = SpikeDetector()
    found = []
    for row in bars.itertuples(index=False):
        ts = getattr(row, TIME_COLUMN).to_pydatetime()
        minutes_in = (ts.astimezone(ET) - start).total_seconds() / 60
        alert = det.update(getattr(row, SYMBOL_COLUMN), ts, getattr(row, CLOSE_COLUMN),
                           getattr(row, VOLUME_COLUMN), can_alert=minutes_in >= SKIP_FIRST_MINUTES)
        if alert:
            found.append(alert)
    print(f"{day}: {len(bars)} bars, {len(symbols)} symbols, {len(found)} alerts")
    for a in found:
        print(f"{a.ts.astimezone(ET):%H:%M} {a.symbol:6} {a.ret_1bar:+.2%} z {a.zscore:+.1f} vol x{a.volume_ratio:.1f}")
    return found


def market_is_open():
    # Alpaca clock knows holidays; fall back to the weekday/hours check if it fails.
    try:
        from alpaca.trading.client import TradingClient
        return TradingClient(os.environ[ALPACA_KEY_ENV], os.environ[ALPACA_SECRET_ENV], paper=True).get_clock().is_open
    except Exception as exc:
        print(f"clock check failed ({exc}), using fixed hours")
        return in_market_hours()


def seconds_until(until_et):
    now = datetime.now(ET)
    return (datetime.combine(now.date(), until_et, tzinfo=ET) - now).total_seconds()


async def run_live(force=False, until_et=MARKET_CLOSE):
    from alpaca.data.enums import DataFeed
    from alpaca.data.live import StockDataStream
    if not force and not market_is_open():
        print("Market closed (use --force to stream anyway)")
        return
    deadline = None if force else seconds_until(until_et)
    if deadline is not None and deadline <= 0:
        print("Deadline already passed")
        return
    symbols = load_universe()
    det = SpikeDetector()
    notifier = Notifier(os.environ[NTFY_TOPIC_ENV])
    pending = []

    now = datetime.now(ET)
    seed = fetch_bars(symbols, now - timedelta(minutes=SEED_MINUTES), now)
    for row in seed.itertuples(index=False):
        det.update(getattr(row, SYMBOL_COLUMN), getattr(row, TIME_COLUMN).to_pydatetime(),
                   getattr(row, CLOSE_COLUMN), getattr(row, VOLUME_COLUMN), can_alert=False)
    print(f"Seeded {len(seed)} bars for {len(symbols)} symbols")

    session_start = datetime.combine(now.date(), MARKET_OPEN, tzinfo=ET)

    async def on_bar(bar):
        ts = bar.timestamp
        can_alert = (ts.astimezone(ET) - session_start).total_seconds() / 60 >= SKIP_FIRST_MINUTES
        alert = det.update(bar.symbol, ts, bar.close, bar.volume, can_alert=can_alert)
        if alert:
            pending.append(alert)

    async def flusher():
        while True:
            await asyncio.sleep(BATCH_SECONDS)
            if pending:
                batch = pending[:]
                pending.clear()
                log_alerts(batch)
                title, body = format_push(batch)
                print(f"{title}\n{body}")
                await asyncio.to_thread(notifier.push, title, body, max(abs(a.zscore) for a in batch) >= 6)

    stream = StockDataStream(os.environ[ALPACA_KEY_ENV], os.environ[ALPACA_SECRET_ENV], feed=DataFeed.IEX)
    stream.subscribe_bars(on_bar, *symbols)
    try:
        await asyncio.wait_for(asyncio.gather(stream._run_forever(), flusher()), timeout=deadline)
    except asyncio.TimeoutError:
        print(f"Reached {until_et} ET, stopping")
    finally:
        await stream.close()


@dataclass
class PriceAlert:
    symbol: str
    price: float
    ref_price: float
    pct: float
    trade_time: str
    ref_time: str
    window: int = WINDOW_MINUTES


def parse_ts(text):
    try:
        return datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None


def check_spikes(snapshots, refs, state, now):
    """Pure core of poll mode. refs maps symbol to (close, time) about WINDOW_MINUTES ago; state holds last alerts for today."""
    today = now.astimezone(ET).date().isoformat()
    last = dict(state.get("last", {})) if state.get("date") == today else {}
    alerts = []
    for symbol, snap in snapshots.items():
        trade = (snap or {}).get("latestTrade") or {}
        price, ref_set = trade.get("p"), refs.get(symbol)
        trade_ts = parse_ts(trade.get("t"))
        if not price or not ref_set or trade_ts is None:
            continue
        if now - trade_ts > timedelta(minutes=STALE_TRADE_MINUTES):
            continue
        if isinstance(ref_set, tuple):
            ref_set = {WINDOW_MINUTES: ref_set}
        # each window has its own threshold; report the window with the biggest qualifying move
        hits = [(w, r) for w, r in ref_set.items()
                if abs(price / r[0] - 1.0) >= (FAST_SPIKE_PCT if w == FAST_WINDOW_MINUTES else SPIKE_PCT)]
        if not hits:
            continue
        window, ref = max(hits, key=lambda item: abs(price / item[1][0] - 1.0))
        pct = price / ref[0] - 1.0
        prev = last.get(symbol)
        if prev is not None:
            prev_ts = parse_ts(prev["t"])
            recent = prev_ts is not None and now - prev_ts < timedelta(minutes=POLL_COOLDOWN_MINUTES)
            same_side = (prev["pct"] > 0) == (pct > 0)
            if recent and same_side and abs(pct) - abs(prev["pct"]) < REALERT_STEP:
                continue
        last[symbol] = {"pct": pct, "t": now.isoformat()}
        alerts.append(PriceAlert(symbol, price, ref[0], pct, trade.get("t", ""), ref[1], window))
    return alerts, {"date": today, "last": last}


def format_poll_push(alerts):
    alerts = sorted(alerts, key=lambda a: abs(a.pct), reverse=True)
    lines = [f"{a.symbol} {a.pct:+.1%} in {a.window}m to {a.price:.2f} (was {a.ref_price:.2f})"
             for a in alerts[:MAX_ALERTS_IN_PUSH]]
    if len(alerts) > MAX_ALERTS_IN_PUSH:
        lines.append(f"+{len(alerts) - MAX_ALERTS_IN_PUSH} more")
    title = f"{len(alerts)} stock{'s' if len(alerts) != 1 else ''} moved {SPIKE_PCT:.0%}+ (stale sim price = 'was')"
    return title, "\n".join(lines)


def alpaca_headers():
    return {"APCA-API-KEY-ID": os.environ[ALPACA_KEY_ENV], "APCA-API-SECRET-KEY": os.environ[ALPACA_SECRET_ENV]}


def fetch_snapshots(symbols):
    out = {}
    for i in range(0, len(symbols), SNAPSHOT_CHUNK):
        resp = requests.get(SNAPSHOT_URL, headers=alpaca_headers(), timeout=30,
                            params={"symbols": ",".join(symbols[i:i + SNAPSHOT_CHUNK]), "feed": "iex"})
        resp.raise_for_status()
        data = resp.json()
        out.update(data.get("snapshots", data))
    return out


def fetch_reference_prices(symbols, now):
    """Per symbol {window_minutes: (close, time)}: last 1-minute bar at or before now - window, within that window's lookback."""
    start = (now - timedelta(minutes=REF_LOOKBACK_MINUTES)).isoformat()
    windows = {WINDOW_MINUTES: REF_LOOKBACK_MINUTES, FAST_WINDOW_MINUTES: FAST_REF_LOOKBACK_MINUTES}
    refs = {}
    for i in range(0, len(symbols), SNAPSHOT_CHUNK):
        params = {"symbols": ",".join(symbols[i:i + SNAPSHOT_CHUNK]), "timeframe": "1Min", "start": start,
                  "end": (now - timedelta(minutes=FAST_WINDOW_MINUTES)).isoformat(), "feed": "iex", "limit": 10000,
                  "adjustment": "raw"}
        while True:
            resp = requests.get(BARS_URL, headers=alpaca_headers(), timeout=30, params=params)
            resp.raise_for_status()
            data = resp.json()
            for symbol, bars in (data.get("bars") or {}).items():
                for window, lookback in windows.items():
                    cutoff, oldest = now - timedelta(minutes=window), now - timedelta(minutes=lookback)
                    ok = [b for b in bars if oldest <= parse_ts(b["t"]) <= cutoff]
                    if ok:
                        refs.setdefault(symbol, {})[window] = (ok[-1]["c"], ok[-1]["t"])
            token = data.get("next_page_token")
            if not token:
                break
            params["page_token"] = token
    return refs


def append_poll_log(alerts, now):
    POLL_LOG.parent.mkdir(exist_ok=True)
    history = json.loads(POLL_LOG.read_text(encoding="utf-8")) if POLL_LOG.exists() else []
    history.extend({"time_et": now.astimezone(ET).isoformat(), "symbol": a.symbol, "pct_change": round(a.pct, 4),
                    "window_minutes": a.window, "price": a.price, "ref_price": a.ref_price,
                    "ref_time": a.ref_time, "trade_time": a.trade_time} for a in alerts)
    POLL_LOG.write_text(json.dumps(history, indent=2), encoding="utf-8")


def market_clock():
    """Returns (is_open, seconds_until_next_open); falls back to fixed hours if Alpaca's clock is unreachable."""
    try:
        from alpaca.trading.client import TradingClient
        clock = TradingClient(os.environ[ALPACA_KEY_ENV], os.environ[ALPACA_SECRET_ENV], paper=True).get_clock()
        return clock.is_open, (clock.next_open - clock.timestamp).total_seconds()
    except Exception as exc:
        print(f"clock check failed ({exc}), using fixed hours")
        return in_market_hours(), 0 if in_market_hours() else 86400


def run_poll_loop(until_et=MARKET_CLOSE, max_minutes=335):
    """Polls every minute inside one process; stops at until_et, after max_minutes, or when the market is shut."""
    started = time.time()
    notifier = Notifier(os.environ[NTFY_TOPIC_ENV])
    symbols = load_universe()
    print(f"Loop started: {len(symbols)} symbols, stop at {until_et} ET or after {max_minutes} min")
    notifier.push("Spike detector running", f"Watching {len(symbols)} symbols, alert at {SPIKE_PCT:.0%}+ in {WINDOW_MINUTES}m",
                  status=True)
    cycles = failures = total_failures = 0
    while time.time() - started < max_minutes * 60 and seconds_until(until_et) > 0:
        is_open, to_open = market_clock()
        if not is_open:
            if to_open > MAX_WAIT_FOR_OPEN_SECONDS:
                print("Market shut and next open is far away, exiting")
                return
            time.sleep(min(to_open + 1, 60))
            continue
        try:
            run_poll_once(symbols, notifier)
            cycles += 1
            failures = 0
            if cycles % HEARTBEAT_EVERY_CYCLES == 0:
                notifier.push("Spike detector OK", f"{cycles} polls done, {total_failures} failed", status=True)
        except Exception as exc:
            failures += 1
            total_failures += 1
            print(f"poll failed, will retry next cycle: {exc}")
            if failures == MAX_CONSECUTIVE_FAILURES:
                notifier.push("Spike detector FAILING", f"{failures} polls in a row failed: {exc}", high=True, status=True)
        sys.stdout.flush()
        time.sleep(POLL_INTERVAL_SECONDS - time.time() % POLL_INTERVAL_SECONDS + POLL_SETTLE_SECONDS)
    print(f"Loop finished: {cycles} polls, {total_failures} failed")
    notifier.push("Spike detector stopped", f"{cycles} polls done, {total_failures} failed", status=True)


def run_poll(force=False):
    if not force and not market_is_open():
        print("Market closed")
        return
    run_poll_once(load_universe(), Notifier(os.environ[NTFY_TOPIC_ENV]))


def run_poll_once(symbols, notifier):
    now = datetime.now(timezone.utc)
    snapshots = fetch_snapshots(symbols)
    refs = fetch_reference_prices(symbols, now)
    usable = sum(1 for sym, s in snapshots.items() if (s or {}).get("latestTrade") and sym in refs)
    print(f"{len(snapshots)} snapshots, {len(refs)} reference prices, {usable} usable of {len(symbols)} symbols")
    state = json.loads(POLL_STATE.read_text()) if POLL_STATE.exists() else {}
    alerts, new_state = check_spikes(snapshots, refs, state, now)
    POLL_STATE.write_text(json.dumps(new_state))
    if not alerts:
        print("No spikes")
        return
    append_poll_log(alerts, now)
    title, body = format_poll_push(alerts)
    print(f"{title}\n{body}")
    tag = "chart_with_downwards_trend" if all(a.pct < 0 for a in alerts) else "chart_with_upwards_trend"
    notifier.push(title, body, high=True, tag=tag)


def main():
    args = sys.argv[1:]
    if not args or args[0] not in ("live", "replay", "poll", "notify-test"):
        print(__doc__)
        return
    if args[0] == "notify-test":
        ok = Notifier(os.environ[NTFY_TOPIC_ENV]).push("Spike detector test", "If you see this, notifications work.", high=True)
        sys.exit(0 if ok else 1)
    if args[0] == "poll":
        if "--loop" in args:
            until = dtime.fromisoformat(args[args.index("--until") + 1]) if "--until" in args else MARKET_CLOSE
            minutes = int(args[args.index("--max-minutes") + 1]) if "--max-minutes" in args else 335
            run_poll_loop(until, minutes)
        else:
            run_poll(force="--force" in args)
        return
    if args[0] == "replay":
        day = date.fromisoformat(args[1]) if len(args) > 1 else date.today() - timedelta(days=1)
        run_replay(day)
    else:
        until = dtime.fromisoformat(args[args.index("--until") + 1]) if "--until" in args else MARKET_CLOSE
        asyncio.run(run_live(force="--force" in args, until_et=until))


if __name__ == "__main__":
    main()
