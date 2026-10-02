import json
from datetime import datetime, timedelta, timezone

import spike_detector as sd

T0 = datetime(2026, 9, 30, 14, 0, tzinfo=timezone.utc)


def feed(det, symbol, prices, start=0):
    out = []
    for i, p in enumerate(prices):
        out.append(det.update(symbol, T0 + timedelta(minutes=start + i), p, 1000))
    return out


def quiet_prices(n, base=100.0):
    return [base * (1 + 0.0002 * ((-1) ** i)) for i in range(n)]


def test_quiet_series_no_alert():
    assert not any(feed(sd.SpikeDetector(), "AAA", quiet_prices(60)))


def test_jump_alerts():
    det = sd.SpikeDetector()
    feed(det, "AAA", quiet_prices(30))
    alert = det.update("AAA", T0 + timedelta(minutes=30), 102.0, 5000)
    assert alert is not None and alert.ret_1bar > 0.015 and alert.volume_ratio > 4


def test_small_move_below_min_abs_return_ignored():
    det = sd.SpikeDetector()
    feed(det, "AAA", quiet_prices(30))
    assert det.update("AAA", T0 + timedelta(minutes=30), 100.2, 1000) is None


def test_warmup_blocks_alerts():
    det = sd.SpikeDetector()
    feed(det, "AAA", quiet_prices(5))
    assert det.update("AAA", T0 + timedelta(minutes=5), 105.0, 1000) is None


def test_cooldown_suppresses_repeat():
    det = sd.SpikeDetector()
    feed(det, "AAA", quiet_prices(30))
    assert det.update("AAA", T0 + timedelta(minutes=30), 102.0, 1000) is not None
    assert det.update("AAA", T0 + timedelta(minutes=31), 104.5, 1000) is None


def test_can_alert_false_suppresses():
    det = sd.SpikeDetector()
    feed(det, "AAA", quiet_prices(30))
    assert det.update("AAA", T0 + timedelta(minutes=30), 102.0, 1000, can_alert=False) is None


def test_format_push_caps_lines():
    alerts = [sd.Alert(f"S{i}", T0, 0.01 * (i + 1), 0.02, 5.0, 3.0, 100.0) for i in range(12)]
    title, body = sd.format_push(alerts)
    assert title == "12 spikes" and body.count("\n") == sd.MAX_ALERTS_IN_PUSH and body.endswith("+4 more")


NOW = datetime(2026, 9, 30, 19, 45, tzinfo=timezone.utc)
TRADE_T = "2026-09-30T19:44:30.123456789Z"


def snap(price, trade_t=TRADE_T):
    return {"latestTrade": {"p": price, "t": trade_t}}


def refs_for(**closes):
    return {sym: (close, "2026-09-30T19:30:00Z") for sym, close in closes.items()}


def test_poll_alerts_on_gains_over_window_only():
    snaps = {"UP": snap(106), "EDGE": snap(104), "DN": snap(80)}
    alerts, _ = sd.check_spikes(snaps, refs_for(UP=100, EDGE=100, DN=100), {}, NOW)
    assert [a.symbol for a in alerts] == ["UP"] and round(alerts[0].pct, 2) == 0.06


def test_poll_skips_missing_reference_stale_trade_and_bad_data():
    snaps = {"NOREF": snap(110), "STALE": snap(110, "2026-09-30T19:00:00Z"), "BAD": {"latestTrade": None}, "NONE": None}
    alerts, _ = sd.check_spikes(snaps, refs_for(STALE=100, BAD=100, NONE=100), {}, NOW)
    assert alerts == []


def test_poll_cooldown_suppresses_repeat_then_allows_after_expiry():
    refs = refs_for(UP=100)
    first, state = sd.check_spikes({"UP": snap(106)}, refs, {}, NOW)
    soon, state = sd.check_spikes({"UP": snap(107)}, refs, state, NOW + timedelta(minutes=5))
    later, state = sd.check_spikes({"UP": snap(107, "2026-09-30T20:14:30Z")}, refs, state, NOW + timedelta(minutes=31))
    assert len(first) == 1 and soon == [] and len(later) == 1


def test_poll_realerts_inside_cooldown_when_gain_grows():
    refs = refs_for(UP=100)
    _, state = sd.check_spikes({"UP": snap(106)}, refs, {}, NOW)
    grown, _ = sd.check_spikes({"UP": snap(112, "2026-09-30T19:49:30Z")}, refs, state, NOW + timedelta(minutes=5))
    assert len(grown) == 1


def test_poll_state_resets_next_day():
    _, state = sd.check_spikes({"UP": snap(106)}, refs_for(UP=100), {}, NOW)
    next_day = NOW + timedelta(days=1)
    alerts, _ = sd.check_spikes({"UP": snap(106, "2026-10-01T19:44:30Z")}, refs_for(UP=100), state, next_day)
    assert len(alerts) == 1


def test_poll_push_title_and_json_log(tmp_path, monkeypatch):
    alerts, _ = sd.check_spikes({"X": snap(140)}, refs_for(X=100), {}, NOW)
    title, body = sd.format_poll_push(alerts)
    assert title == "1 stock up 5%+ in 15m" and "X +40.0% in 15m to 140.00 (was 100.00)" in body
    monkeypatch.setattr(sd, "POLL_LOG", tmp_path / "alerts" / "poll_alerts.json")
    sd.append_poll_log(alerts, NOW)
    sd.append_poll_log(alerts, NOW)
    rows = json.loads(sd.POLL_LOG.read_text())
    assert len(rows) == 2 and rows[0]["symbol"] == "X" and rows[0]["window_minutes"] == 15 and rows[0]["pct_change"] == 0.4
