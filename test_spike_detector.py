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


def snap(price, prev):
    return {"latestTrade": {"p": price, "t": "2026-09-30T14:00:00Z"}, "prevDailyBar": {"c": prev}}


def test_poll_alerts_at_ten_percent_only():
    alerts, _ = sd.check_spikes({"UP": snap(111, 100), "DN": snap(89, 100), "OK": snap(109, 100)}, {}, "2026-09-30")
    assert sorted(a.symbol for a in alerts) == ["DN", "UP"]


def test_poll_dedupes_until_move_extends():
    first, state = sd.check_spikes({"UP": snap(111, 100)}, {}, "2026-09-30")
    again, state = sd.check_spikes({"UP": snap(112, 100)}, state, "2026-09-30")
    extended, state = sd.check_spikes({"UP": snap(117, 100)}, state, "2026-09-30")
    assert len(first) == 1 and again == [] and len(extended) == 1


def test_poll_state_resets_next_day_and_handles_missing_data():
    _, state = sd.check_spikes({"UP": snap(111, 100)}, {}, "2026-09-30")
    nxt, _ = sd.check_spikes({"UP": snap(111, 100), "BAD": {"latestTrade": None}, "NONE": None}, state, "2026-10-01")
    assert [a.symbol for a in nxt] == ["UP"]


def test_poll_reversal_realerts_and_split_tag():
    _, state = sd.check_spikes({"X": snap(111, 100)}, {}, "2026-09-30")
    down, _ = sd.check_spikes({"X": snap(50, 100)}, state, "2026-09-30")
    title, body = sd.format_poll_push(down)
    assert len(down) == 1 and "check split" in body and title.startswith("1 stock moved")
