#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb"]
# ///
"""Hugging Face 지표 히스토리 수집기.

  ./hfhist.py collect            targets.toml 대상의 현재값을 원장에 추가
  ./hfhist.py backfill           hub-stats 과거 스냅샷에서 빠진 날짜를 채움
      --step N                   N번째 스냅샷마다 (빠른 첫 실행용)
      --all                      이미 받은 스냅샷도 다시 조회 (지표를 추가한 뒤)
  ./hfhist.py chart              targets.toml의 [[charts]]대로 charts/*.svg를 만들고 README에 넣음
  ./hfhist.py selftest

원장(data/ledger.csv)은 추가 전용이다. 기존 줄은 고치거나 지우지 않는다.
비공개 저장소는 HF_TOKEN 환경변수를 쓴다.
"""
import argparse
import bisect
import csv
import html
import json
import math
import os
import re
import sys
import tomllib
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).parent
LEDGER = ROOT / "data" / "ledger.csv"
FIELDS = ["observed_at", "entity_id", "target", "metric", "value", "origin"]
HUB = "https://huggingface.co"
STATS = "cfahlgren1/hub-stats"  # 허브 전체 통계를 매일 올리는 공개 데이터셋


def get(url):
    headers = {"User-Agent": "hfhist"}
    if os.environ.get("HF_TOKEN"):
        headers["Authorization"] = f"Bearer {os.environ['HF_TOKEN']}"
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60) as r:
        return json.load(r)


def read(path=LEDGER):
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def key(row):
    return tuple(str(row[f]) for f in FIELDS)


def append(rows, path=LEDGER):
    """원장 끝에 덧붙인다. 모든 칸이 같은 행(같은 스냅샷을 다시 받은 경우)만 건너뛴다."""
    # ponytail: 호출마다 원장 전체를 다시 읽는다. 수십만 줄을 넘기면 seen을 호출자가 들고 다니게 바꾼다.
    seen = {key(r) for r in read(path)}
    new = []
    for r in rows:
        if key(r) not in seen:
            seen.add(key(r))
            new.append(r)
    path.parent.mkdir(exist_ok=True)
    fresh = not path.exists()
    with path.open("a", newline="") as f:
        w = csv.DictWriter(f, FIELDS)
        if fresh:
            w.writeheader()
        w.writerows(new)
    return len(new)


def observations(when, rec, target, metrics, origin):
    """레코드 하나를 지표별 원장 행으로 바꾼다. 숫자가 아닌 값(없음·NaN)은 기록하지 않는다."""
    rows = []
    for m in metrics:
        v = rec.get(m)
        if isinstance(v, (int, float)) and not isinstance(v, bool) and v == v:
            rows.append(dict(observed_at=when, entity_id=rec["_id"], target=target, metric=m, value=v, origin=origin))
    return rows


def collect():
    """현재값을 기록하고 {설정의 target: (entity_id, createdAt)}를 돌려준다. 실패한 대상은 빠진다."""
    cfg = tomllib.loads((ROOT / "targets.toml").read_text())
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows, found, failed = [], {}, []
    for t in cfg["targets"]:
        kind = t.split("/")[0]
        metrics = cfg["metrics"][kind]
        query = "&".join(f"expand[]={m}" for m in [*metrics, "createdAt"])
        try:
            rec = get(f"{HUB}/api/{t}?{query}")
        except (OSError, ValueError) as e:
            # 실패는 기록하지 않는다. 0이나 빈 값이 원장에 들어가면 되돌릴 수 없다.
            failed.append(f"{t}: {e}")
            continue
        actual = f"{kind}/{rec['id']}"
        if actual != t:
            print(f"이름 변경: {t} -> {actual}", file=sys.stderr)
        found[t] = (rec["_id"], rec["createdAt"])
        rows += observations(now, rec, actual, metrics, "api")
    print(f"collect: {append(rows)}행 추가, 대상 {len(found)}/{len(cfg['targets'])}")
    for f in failed:
        print(f"수집 실패 {f}", file=sys.stderr)
    return cfg, found, failed


def snapshot(kind, sha, where, metrics):
    import duckdb

    con = duckdb.connect()
    con.sql("LOAD httpfs")
    cols = "|".join(["_id", "id", *metrics])  # 옛 스냅샷에는 없는 컬럼이 있어 정규식으로 있는 것만 고른다
    url = f"{HUB}/datasets/{STATS}/resolve/{sha}/{kind}.parquet"
    cur = con.execute(f"SELECT COLUMNS('^({cols})$') FROM read_parquet('{url}') WHERE {where}")
    names = [d[0] for d in cur.description]
    return [dict(zip(names, row)) for row in cur.fetchall()]


def backfill(step=1, everything=False):
    """현재값을 먼저 기록하고 과거를 채운다. 과거를 못 채워도 실패가 아니다: 수집을 시작한 날부터 기록이 쌓인다."""
    cfg, found, failed = collect()
    try:
        fill(cfg, found, step, everything)
    except Exception as e:  # 스냅샷 저장소가 사라졌거나 닿지 않는 경우
        print(f"과거 기록을 채우지 못했다: {str(e)[:200]}\n수집을 시작한 날부터의 기록만 쌓인다.", file=sys.stderr)
    return failed


def fill(cfg, found, step, everything):
    import duckdb

    duckdb.connect().sql("INSTALL httpfs")
    ledger = sorted(read(), key=lambda r: r["observed_at"])
    by_name = {r["target"]: r["entity_id"] for r in ledger}  # 같은 이름이면 가장 최근 관측의 entity
    done = {(r["origin"], r["entity_id"]) for r in ledger}
    commits, p = [], 0
    # ponytail: 실행마다 커밋 목록 전체(약 50쪽)를 받는다. 느려지면 이미 본 sha에서 멈추게 한다.
    while page := get(f"{HUB}/api/datasets/{STATS}/commits/main?limit=100&p={p}"):
        commits += [(c["date"][:19] + "Z", c["id"], c["title"]) for c in page]
        p += 1
    added = errors = 0
    for kind in sorted({t.split("/")[0] for t in cfg["targets"]}):
        # API가 실패한 대상(삭제 등)은 원장에 남은 entity_id로 찾는다. 그것도 없으면 건너뛴다.
        ids = []
        for t in (t for t in cfg["targets"] if t.startswith(kind + "/")):
            if t in found or t in by_name:
                ids.append(found[t][0] if t in found else by_name[t])
            else:
                print(f"backfill 건너뜀 {t}: API 실패, 원장 기록 없음", file=sys.stderr)
        if not ids:
            continue
        created = {i: c for i, c in found.values()}
        where = "_id IN (" + ",".join("'" + i.replace("'", "''") + "'" for i in ids) + ")"
        snaps = [(date, sha) for date, sha, title in commits[::-1] if f"{kind}.parquet" in title][::step]
        # ponytail: 받은 스냅샷은 (출처, 대상)으로만 기억한다. 지표를 추가했으면 --all로 다시 받는다.
        # 대상이 스냅샷에 없던 날(비공개 기간 등)은 실행마다 다시 조회한다.
        todo = [
            (date, sha)
            for date, sha in snaps
            if everything or any((f"hub-stats@{sha}", i) not in done and created.get(i, "") <= date for i in ids)
        ]
        print(f"backfill {kind}: 스냅샷 {len(todo)}개 조회")
        with ThreadPoolExecutor(8) as ex:
            futures = {ex.submit(snapshot, kind, sha, where, cfg["metrics"][kind]): (date, sha) for date, sha in todo}
            for n, fut in enumerate(as_completed(futures), 1):
                date, sha = futures[fut]
                try:
                    recs = fut.result()
                except Exception as e:  # 다음 실행에서 다시 시도된다
                    errors += 1
                    print(f"스냅샷 실패 {date[:10]} {sha[:8]}: {str(e)[:120]}", file=sys.stderr)
                    continue
                origin = f"hub-stats@{sha}"
                added += append([o for r in recs for o in observations(date, r, f"{kind}/{r['id']}", cfg["metrics"][kind], origin)])
                if n % 50 == 0:
                    print(f"  {n}/{len(todo)}")
    print(f"backfill: {added}행 추가, 실패 {errors}건")


# 차트 ---------------------------------------------------------------------

TITLES = {"downloadsAllTime": "누적 다운로드", "downloads": "최근 30일 다운로드", "likes": "좋아요", "trendingScore": "트렌딩 점수"}
FORMS = ("line", "area", "bars", "card")
STYLES = ("auto", "light", "dark", "sketch")
GAUGES = {"downloads", "trendingScore"}  # 쌓이는 값이 아니라서 월별 증가를 낼 수 없는 지표
SERIES = [  # (라이트, 다크) 범주형 8색. 순서가 색각 안전성을 보장하므로 바꾸지 않는다.
    ("#2a78d6", "#3987e5"), ("#eb6834", "#d95926"), ("#1baf7a", "#199e70"), ("#eda100", "#c98500"),
    ("#e87ba4", "#d55181"), ("#008300", "#008300"), ("#6250d6", "#9085e9"), ("#e34948", "#e66767"),
]
INK = {  # 역할: (라이트, 다크)
    ".bg{fill:%s}": ("#fcfcfb", "#1a1a19"), ".t1{fill:%s}": ("#0b0b0b", "#f0efec"), ".t2{fill:%s}": ("#52514e", "#c3c2b7"),
    ".mu{fill:%s}": ("#898781", "#898781"), ".sm{stroke:%s}": ("#898781", "#898781"),
    ".grid{stroke:%s}": ("#e1e0d9", "#2c2c2a"), ".axis{stroke:%s}": ("#c3c2b7", "#383835"),
}
W = 800


def compact(v):
    v = float(f"{v:.3g}")  # 먼저 유효숫자 3자리로 반올림해야 999,500이 1M이 된다
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(v) >= div:
            return f"{v / div:g}{suffix}"
    return f"{v:g}"


def frame(w, h, style, marks, text):
    """표면·색·글꼴을 입힌다. auto는 보는 사람의 라이트/다크를 따르고, sketch는 auto 색에 손그림 질감을 더한다."""

    def css(i):
        rules = "".join(k % v[i] for k, v in INK.items())
        return rules + "".join(f".s{n}{{stroke:{c[i]}}}.f{n}{{fill:{c[i]}}}" for n, c in enumerate(SERIES))

    colors = css(1) if style == "dark" else css(0) + ("" if style == "light" else f"@media (prefers-color-scheme:dark){{{css(1)}}}")
    sketch = style == "sketch"
    font = "'Comic Sans MS','Chalkboard SE','Marker Felt',cursive" if sketch else "system-ui,-apple-system,'Segoe UI',sans-serif"
    wobble = (
        f'<filter id="sk" filterUnits="userSpaceOnUse" x="0" y="0" width="{w}" height="{h}">'
        '<feTurbulence type="fractalNoise" baseFrequency="0.03" numOctaves="2" seed="4"/>'
        '<feDisplacementMap in="SourceGraphic" scale="5"/></filter>'
    )
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" font-family="{font}" font-size="12">\n'
        f"<style>{colors}.ln{{fill:none;stroke-width:{3 if sketch else 2};stroke-linejoin:round;stroke-linecap:round}}"
        f".wash{{opacity:.1}}.tk{{font-variant-numeric:tabular-nums}}</style>{wobble if sketch else ''}\n"
        f'<rect class="bg" width="{w}" height="{h}" rx="8"/>\n'
        f'<g{" filter=\"url(#sk)\"" if sketch else ""}>\n' + "\n".join(marks) + "\n</g>\n" + "\n".join(text) + "\n</svg>\n"
    )


def scale(lo, hi):
    """0을 포함하는 깔끔한 y축 (최솟값, 최댓값, 눈금 간격)."""
    lo, hi = min(lo, 0), max(hi, 0)
    raw = max(hi - lo, 1) / 4
    mag = 10 ** math.floor(math.log10(raw))
    tick = next(s * mag for s in (1, 2, 5, 10) if s * mag >= raw)
    return math.floor(lo / tick) * tick, max(math.ceil(hi / tick) * tick, tick), tick


def month_starts(a, b):
    """a보다 뒤이고 b 이하인 달 첫날들."""
    y, m, out = a.year, a.month, []
    while True:
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        d = datetime(y, m, 1, tzinfo=timezone.utc)
        if d > b:
            return out
        out.append(d)


def monthly(pts):
    """[(시각, 값)] -> [(달 첫날, 그 달의 증가량)]. 관측이 처음부터 끝까지 덮는 달만 낸다.

    달 경계의 값은 앞뒤 관측을 선형 보간한다. 스냅샷이 월말에 빠져도 달 길이가 어긋나지 않는다.
    """
    times = [d for d, _ in pts]

    def at(b):
        i = bisect.bisect_right(times, b) - 1
        if i == len(pts) - 1:
            return pts[i][1]
        (t0, v0), (t1, v1) = pts[i], pts[i + 1]
        return v0 + (v1 - v0) * ((b - t0) / (t1 - t0))

    bounds = month_starts(pts[0][0], pts[-1][0]) if pts else []
    return [(b0, at(b1) - at(b0)) for b0, b1 in zip(bounds, bounds[1:])]


def column(x, w, y0, y1):
    """기준선(y0) 쪽은 각지게, 값(y1) 쪽 끝은 4px로 둥글게."""
    r = min(4, w / 2, abs(y1 - y0))
    s = r if y1 < y0 else -r
    return (
        f"M{x:.1f},{y0:.1f}V{y1 + s:.1f}Q{x:.1f},{y1:.1f} {x + r:.1f},{y1:.1f}"
        f"H{x + w - r:.1f}Q{x + w:.1f},{y1:.1f} {x + w:.1f},{y1 + s:.1f}V{y0:.1f}Z"
    )


def plot(title, series, form="line", style="auto"):
    """series: [(색 슬롯, 이름, [(datetime, 값), ...])]. form: line | area | bars(월별 증가)."""
    L, R, T, PH = 56, 64, 52, 300
    bars, k = form == "bars", len(series)
    H = T + PH + 36 + (18 * k + 8 if k > 1 else 0)
    times = sorted({d for _, _, pts in series for d, _ in pts})
    vals = [v for _, _, pts in series for _, v in pts]
    ymin, ymax, tick = scale(min(vals), max(vals))
    Y = lambda v: T + PH * (ymax - v) / (ymax - ymin)
    if bars:
        times = [times[0], *month_starts(times[0], times[-1])]  # 대상마다 기간이 달라도 달이 빠지지 않게
        band = (W - L - R) / len(times)
        bw = max(1, min(24, (band - 4) / k - 2))
        X = lambda d: L + band * (times.index(d) + 0.5)
        ticks = [(d, f"{d:%Y-%m}") for d in times[:: max(1, math.ceil(len(times) / 7))]]
    else:
        first, x0, x1 = times[0], times[0].timestamp(), times[-1].timestamp()
        X = lambda d: L + (W - L - R) * ((d.timestamp() - x0) / (x1 - x0) if x1 > x0 else 1)
        starts = month_starts(first, times[-1])
        ticks = [(d, f"{d:%Y-%m}") for d in starts[:: max(1, math.ceil(len(starts) / 7))]]
        # 관측이 한 달 안에만 있으면 달 눈금이 없으므로 첫날과 끝날을 쓴다
        ticks = ticks or [(d, f"{d:%m-%d}") for d in dict.fromkeys((first, times[-1]))]
    marks = []
    text = [f'<text class="t1" x="{L}" y="30" font-size="16" font-weight="600">{html.escape(title)}</text>']
    for i in range(round(ymin / tick), round(ymax / tick) + 1):
        y = Y(i * tick)
        marks.append(f'<line class="{"axis" if i == 0 else "grid"}" x1="{L}" x2="{W - R}" y1="{y:.1f}" y2="{y:.1f}"/>')
        text.append(f'<text class="mu tk" x="{L - 8}" y="{y + 4:.1f}" text-anchor="end">{compact(i * tick)}</text>')
    text += [f'<text class="mu tk" x="{X(d):.1f}" y="{T + PH + 18}" text-anchor="middle">{label}</text>' for d, label in ticks]
    placed = []
    for row, (n, name, pts) in enumerate(series):
        ex, ey, end = X(pts[-1][0]), Y(pts[-1][1]), compact(pts[-1][1])
        if bars:
            off = (row - (k - 1) / 2) * (bw + 2) - bw / 2  # 같은 달의 막대를 2px 간격으로 나란히
            marks += [f'<path class="f{n}" d="{column(X(d) + off, bw, Y(0), Y(v))}"/>' for d, v in pts]
            if k == 1:  # 마지막 막대에만 값 라벨
                text.append(f'<text class="t2 tk" x="{ex:.1f}" y="{ey + (-6 if pts[-1][1] >= 0 else 16):.1f}" text-anchor="middle">{end}</text>')
        else:
            line = " ".join(f"{X(d):.1f},{Y(v):.1f}" for d, v in pts)
            if form == "area":
                marks.append(f'<polygon class="f{n} wash" points="{line} {ex:.1f},{Y(0):.1f} {X(pts[0][0]):.1f},{Y(0):.1f}"/>')
            marks.append(f'<polyline class="ln s{n}" points="{line}"/><circle class="f{n}" cx="{ex:.1f}" cy="{ey:.1f}" r="4"/>')
            # 끝값 라벨은 4개 이하이고 서로 겹치지 않을 때만 붙인다. 나머지는 범례가 값을 싣는다.
            if k <= 4 and all(abs(ey - p) >= 14 for p in placed):
                placed.append(ey)
                text.append(f'<text class="t2 tk" x="{ex + 8:.1f}" y="{ey + 4:.1f}">{end}</text>')
        if k > 1:
            y = T + PH + 44 + 18 * row
            swatch = f'<rect class="f{n}" x="{L + 4}" y="{y - 10}" width="10" height="10" rx="2"/>' if bars else f'<line class="ln s{n}" x1="{L}" x2="{L + 18}" y1="{y - 4}" y2="{y - 4}"/>'
            marks.append(swatch)
            text.append(f'<text class="t2" x="{L + 26}" y="{y}">{html.escape(name)} · {end}</text>')
    return frame(W, H, style, marks, text)


def card(title, series, style="auto"):
    """대상마다 한 칸: 지표와 이름, 현재값, 30일 전 대비 변화(그 무렵 관측이 있을 때만), 최근 90일 추세선."""
    w, rh = 420, 100
    marks, text = [], []
    for row, (n, name, pts) in enumerate(series):
        y, (now, value) = rh * row, pts[-1]
        if row:
            marks.append(f'<line class="grid" x1="20" x2="{w - 20}" y1="{y}" y2="{y}"/>')
        text.append(f'<text class="t2" x="20" y="{y + 28}">{html.escape(title)} · {html.escape(name)}</text>')
        shown = compact(value) if abs(value) >= 1e4 or value != int(value) else f"{value:,.0f}"
        text.append(f'<text class="t1" x="20" y="{y + 62}" font-size="28" font-weight="600">{shown}</text>')
        ago = now - timedelta(days=30)
        then, old = min(pts, key=lambda p: abs(p[0] - ago))
        if abs(then - ago) <= timedelta(days=3):  # 30일 전 무렵의 관측이 없으면 다른 기간을 30일이라 부르지 않는다
            delta = value - old
            arrow = "▲" if delta > 0 else "▼" if delta < 0 else "–"
            text.append(f'<text class="t2 tk" x="20" y="{y + 84}">{arrow} {compact(abs(delta))} · 30일 전 대비</text>')
        recent = [(d, v) for d, v in pts if (now - d).days <= 90]
        lo, hi = min(v for _, v in recent), max(v for _, v in recent)
        t0, span = recent[0][0], max((now - recent[0][0]).total_seconds(), 1)
        sx = lambda d: w - 150 + 120 * (d - t0).total_seconds() / span
        sy = lambda v: y + 72 - 44 * ((v - lo) / (hi - lo) if hi > lo else 0.5)
        line = " ".join(f"{sx(d):.1f},{sy(v):.1f}" for d, v in recent)
        # 추세선은 흐리게, 현재값 점만 대상 색으로
        marks.append(f'<polyline class="ln sm" points="{line}"/><circle class="f{n}" cx="{sx(now):.1f}" cy="{sy(value):.1f}" r="4"/>')
    return frame(w, rh * len(series), style, marks, text)


def chart():
    cfg = tomllib.loads((ROOT / "targets.toml").read_text())
    rows = read()
    # 원장은 시간순이 아니다(백필이 과거를 뒤에 붙인다). 이름은 가장 최근 관측의 것을 쓴다.
    names = {r["entity_id"]: r["target"] for r in sorted(rows, key=lambda r: r["observed_at"])}
    order = list(dict.fromkeys(r["entity_id"] for r in rows))  # 원장 등장 순서. 추가 전용이라 대상별 색이 바뀌지 않는다.
    if len(order) > len(SERIES):
        sys.exit(f"차트는 대상 {len(SERIES)}개까지 그린다. 지금 {len(order)}개.")
    (ROOT / "charts").mkdir(exist_ok=True)
    embeds = []
    for c in cfg["charts"]:
        metric, form, style = c["metric"], c.get("form", "line"), c.get("style", "auto")
        if form not in FORMS or style not in STYLES:
            sys.exit(f"charts 설정 오류: form은 {FORMS}, style은 {STYLES} 중 하나. 받은 값: {form}, {style}")
        if form == "bars" and metric in GAUGES:
            sys.exit(f"charts 설정 오류: {metric}은 쌓이는 값이 아니라 bars(월별 증가)로 그릴 수 없다. line이나 area를 쓴다.")
        series = []
        for n, e in enumerate(order):  # 지표가 없는 대상이 있어도 색 슬롯은 대상에 고정
            pts = [(datetime.fromisoformat(r["observed_at"]), float(r["value"])) for r in rows if r["entity_id"] == e and r["metric"] == metric]
            pts.sort(key=lambda p: p[0])  # 같은 시각의 관측은 기록된 순서를 지킨다
            if form == "bars":
                pts = monthly(pts)
            if pts:
                series.append((n, names[e].split("/", 1)[1], pts))
        if not series:
            print(f"chart: {metric} {form} 건너뜀 (그릴 기록이 아직 부족함)", file=sys.stderr)
            continue
        title = TITLES.get(metric, metric) + (" 월별 증가" if form == "bars" else "")
        if form == "card":
            doc = card(title, series, style)
        else:
            doc = plot(title + (f" · {series[0][1]}" if len(series) == 1 else ""), series, form, style)
        name = f"{metric}-{form}-{style}.svg"
        (ROOT / "charts" / name).write_text(doc)
        embeds.append(f"![{title} ({form}, {style})](charts/{name})")
        print(f"chart: charts/{name}")
    # README의 <!-- charts --> 구간만 다시 쓴다. 표시가 없으면 README를 건드리지 않는다.
    readme = ROOT / "README.md"
    if readme.exists():
        block = "<!-- charts -->\n" + "\n\n".join(embeds) + "\n<!-- /charts -->"
        readme.write_text(re.sub(r"<!-- charts -->.*<!-- /charts -->", lambda _: block, readme.read_text(), flags=re.S))


def selftest():
    import tempfile

    p = Path(tempfile.mkdtemp()) / "data" / "ledger.csv"
    rec = {"_id": "abc", "id": "o/n", "downloads": 5, "likes": None, "trendingScore": float("nan")}
    rows = observations("2026-01-01T00:00:00Z", rec, "datasets/o/n", ["downloads", "likes", "trendingScore", "missing"], "api")
    assert [r["metric"] for r in rows] == ["downloads"], rows
    assert append(rows, p) == 1 and append(rows, p) == 0  # 같은 관측은 한 번만
    before = p.read_bytes()
    assert append([{**rows[0], "value": 0}], p) == 1  # 값이 달라지면(리셋) 새 줄로 남는다
    assert append([{**rows[0], "observed_at": "2026-01-01T09:00:00Z"}], p) == 1  # 같은 날 값이 돌아와도(A→B→A) 남는다
    assert p.read_bytes().startswith(before) and len(read(p)) == 3  # 기존 줄은 그대로
    assert compact(8388304) == "8.39M" and compact(2000) == "2K" and compact(0) == "0" and compact(999500) == "1M"
    t = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert plot("x", [(0, "a", [(t, 1.0)])]).count("<polyline") == 1
    day = lambda m, d: datetime(2026, m, d, tzinfo=timezone.utc)
    # 하루 10씩 쌓이는 값: 1/15~4/10 관측이면 온전히 덮인 2월(28일)과 3월(31일)만 나온다
    grow = [(day(1, 15), 0.0), (day(2, 20), 360.0), (day(4, 10), 850.0)]
    assert [(d.month, round(v)) for d, v in monthly(grow)] == [(2, 280), (3, 310)], monthly(grow)
    assert monthly([(day(1, 1), 50.0), (day(2, 1), 20.0), (day(3, 1), 20.0)])[0][1] == 0.0  # 2월: 20 -> 20
    assert scale(-20, 15)[:2] == (-20, 20) and scale(0, 0)[1] > 0
    assert "01-01" in plot("x", [(0, "a", [(t, 1.0)])])  # 한 달 안의 관측만 있어도 x축 눈금이 있다
    # 과거를 못 채워도 backfill은 실패가 아니다 (현재값 수집 결과만 돌려준다)
    g, saved = globals(), (collect, fill)
    g["collect"], g["fill"] = lambda: ({}, {}, []), lambda *a: 1 / 0
    try:
        assert backfill() == []
    finally:
        g["collect"], g["fill"] = saved
    print("selftest ok")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["collect", "backfill", "chart", "selftest"])
    ap.add_argument("--step", type=int, default=1)
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()
    if args.cmd == "collect":
        sys.exit(1 if collect()[2] else 0)
    elif args.cmd == "backfill":
        sys.exit(1 if backfill(args.step, args.all) else 0)
    elif args.cmd == "chart":
        chart()
    else:
        selftest()
