#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb"]
# ///
"""Keep a history of Hugging Face metrics that only exist as current values.

  ./hfhist.py collect            append the current values of the targets in targets.toml
  ./hfhist.py backfill           collect, then fill missing past days from hub-stats snapshots
      --step N                   only every Nth snapshot (for a quick first run)
      --all                      query snapshots again even if already fetched (after adding a metric)
  ./hfhist.py chart              draw charts/*.svg as listed under [[charts]] and update the README
  ./hfhist.py selftest

The ledger (data/ledger.csv) is append-only. Existing lines are never edited or removed.
Private repositories need the HF_TOKEN environment variable.
"""
import argparse
import bisect
import csv
from fnmatch import fnmatchcase
import html
import json
import math
import os
import re
import subprocess
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
STATS = "cfahlgren1/hub-stats"  # public dataset that uploads hub-wide stats every day


def get(url):
    """Fetch JSON. A paginated list is followed through its Link: rel="next" headers to the end."""
    headers = {"User-Agent": "hfhist"}
    if os.environ.get("HF_TOKEN"):
        headers["Authorization"] = f"Bearer {os.environ['HF_TOKEN']}"
    items = []
    while url:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60) as r:
            data = json.load(r)
            more = re.search(r'<([^>]+)>;\s*rel="next"', r.headers.get("Link") or "")
        if not isinstance(data, list):
            return data
        items += data
        url = more and more[1]
    return items


def read(path=LEDGER):
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def key(row):
    return tuple(str(row[f]) for f in FIELDS)


def append(rows, path=LEDGER):
    """Append to the ledger. Only rows identical in every field (a re-fetched snapshot) are skipped."""
    # ponytail: re-reads the whole ledger on every call. Past a few hundred thousand lines, let the caller carry `seen`.
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
    """Turn one record into a ledger row per metric. Non-numeric values (missing, NaN) are not recorded."""
    rows = []
    for m in metrics:
        v = rec.get(m)
        if isinstance(v, (int, float)) and not isinstance(v, bool) and v == v:
            rows.append(dict(observed_at=when, entity_id=rec["_id"], target=target, metric=m, value=v, origin=origin))
    return rows


def collect():
    """Record current values and return {live target: (entity_id, createdAt)}. Failed targets are left out."""
    cfg = tomllib.loads((ROOT / "targets.toml").read_text())
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows, found, failed = [], {}, []
    for t in cfg["targets"]:
        kind, _, rest = t.partition("/")
        metrics = cfg["metrics"][kind]
        query = "&".join(f"expand[]={m}" for m in [*metrics, "createdAt"])
        everything = rest.endswith("/*")
        try:
            # "<kind>/<owner>/*" means every repository of the owner. It is looked up on each run, so new ones join automatically.
            recs = get(f"{HUB}/api/{kind}?author={rest[:-2]}&limit=1000&{query}") if everything else [get(f"{HUB}/api/{t}?{query}")]
        except (OSError, ValueError) as e:
            # A failure is never recorded: a 0 or blank in the ledger could not be taken back.
            failed.append(f"{t}: {e}")
            continue
        if not recs:  # A misspelled owner or an expired token also yields an empty list; passing silently would drop everything.
            failed.append(f"{t}: no repositories found")
        for rec in recs:
            actual = f"{kind}/{rec['id']}"
            if not everything and actual != t:
                print(f"renamed: {t} -> {actual}. Update targets.toml to the new name.", file=sys.stderr)
            found[actual] = (rec["_id"], rec["createdAt"])
            rows += observations(now, rec, actual, metrics, "api")
    print(f"collect: {append(rows)} rows added, {len(found)} targets")
    for f in failed:
        print(f"collect failed {f}", file=sys.stderr)
    return cfg, found, failed


def snapshot(kind, sha, where, metrics):
    import duckdb

    con = duckdb.connect()
    con.sql("LOAD httpfs")
    cols = "|".join(["_id", "id", *metrics])  # older snapshots lack some columns, so select whichever exist by regex
    url = f"{HUB}/datasets/{STATS}/resolve/{sha}/{kind}.parquet"
    cur = con.execute(f"SELECT COLUMNS('^({cols})$') FROM read_parquet('{url}') WHERE {where}")
    names = [d[0] for d in cur.description]
    return [dict(zip(names, row)) for row in cur.fetchall()]


def backfill(step=1, everything=False):
    """Record current values, then fill the past. Failing to fill the past is not an error: records start from today."""
    cfg, found, failed = collect()
    try:
        fill(cfg, found, step, everything)
    except Exception as e:  # the snapshot repository is gone or unreachable
        print(f"could not fill past records: {str(e)[:200]}\nRecords will start from the day collection began.", file=sys.stderr)
    return failed


def fill(cfg, found, step, everything):
    import duckdb

    duckdb.connect().sql("INSTALL httpfs")
    done = {(r["origin"], r["entity_id"]) for r in read()}
    # ponytail: fetches the full commit list (about 50 pages) on every run. If that gets slow, stop at the first known sha.
    commits = [(c["date"][:19] + "Z", c["id"], c["title"]) for c in get(f"{HUB}/api/datasets/{STATS}/commits/main?limit=100")]
    added = errors = 0
    for kind in sorted({t.split("/")[0] for t in found}):
        # Only live targets are filled. A deleted target keeps the records fetched while it existed.
        ids = [i for t, (i, _) in found.items() if t.startswith(kind + "/")]
        created = {i: c for i, c in found.values()}
        where = "_id IN (" + ",".join("'" + i.replace("'", "''") + "'" for i in ids) + ")"
        snaps = [(date, sha) for date, sha, title in commits[::-1] if f"{kind}.parquet" in title][::step]
        # ponytail: fetched snapshots are remembered by (origin, target) only. After adding a metric, refetch with --all.
        # Days when a target was absent from the snapshot (e.g. while private) are queried again on every run.
        todo = [
            (date, sha)
            for date, sha in snaps
            if everything or any((f"hub-stats@{sha}", i) not in done and created.get(i, "") <= date for i in ids)
        ]
        print(f"backfill {kind}: querying {len(todo)} snapshots")
        with ThreadPoolExecutor(8) as ex:
            futures = {ex.submit(snapshot, kind, sha, where, cfg["metrics"][kind]): (date, sha) for date, sha in todo}
            for n, fut in enumerate(as_completed(futures), 1):
                date, sha = futures[fut]
                try:
                    recs = fut.result()
                except Exception as e:  # retried on the next run
                    errors += 1
                    print(f"snapshot failed {date[:10]} {sha[:8]}: {str(e)[:120]}", file=sys.stderr)
                    continue
                origin = f"hub-stats@{sha}"
                added += append([o for r in recs for o in observations(date, r, f"{kind}/{r['id']}", cfg["metrics"][kind], origin)])
                if n % 50 == 0:
                    print(f"  {n}/{len(todo)}")
    print(f"backfill: {added} rows added, {errors} snapshots failed")


# Charts --------------------------------------------------------------------

TITLES = {"downloadsAllTime": "Total downloads", "downloads": "Downloads, last 30 days", "likes": "Likes", "trendingScore": "Trending score"}
FORMS = ("summary", "line", "area", "bars")
STYLES = ("auto", "light", "dark", "sketch")
GAUGES = {"downloads", "trendingScore"}  # not cumulative, so totals and monthly increases are meaningless
SERIES = [  # (light, dark) categorical colors. The order is what keeps them colorblind-safe; do not reorder.
    ("#2a78d6", "#3987e5"), ("#eb6834", "#d95926"), ("#1baf7a", "#199e70"), ("#eda100", "#c98500"),
    ("#e87ba4", "#d55181"), ("#008300", "#008300"), ("#6250d6", "#9085e9"), ("#e34948", "#e66767"),
]
INK = {  # role: (light, dark)
    ".bg{fill:%s}": ("#fcfcfb", "#1a1a19"), ".t1{fill:%s}": ("#0b0b0b", "#f0efec"), ".t2{fill:%s}": ("#52514e", "#c3c2b7"),
    ".mu{fill:%s}": ("#898781", "#898781"), ".fo{fill:%s}": ("#898781", "#898781"), ".gap{stroke:%s}": ("#fcfcfb", "#1a1a19"),
    ".grid{stroke:%s}": ("#e1e0d9", "#2c2c2a"), ".axis{stroke:%s}": ("#c3c2b7", "#383835"),
}
W = 800


def compact(v):
    v = float(f"{v:.3g}")  # round to 3 significant digits first, so 999,500 becomes 1M
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(v) >= div:
            return f"{v / div:g}{suffix}"
    return f"{v:g}"


def exact(v):
    """Full digits with thousands separators for whole numbers, compact otherwise."""
    return f"{v:,.0f}" if v == int(v) else compact(v)


def frame(w, h, style, marks, text):
    """Wrap marks and text with the surface, colors and font. auto follows the viewer's light/dark setting; sketch adds a hand-drawn wobble to auto."""

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
        f".wash{{opacity:.1}}.gap{{fill:none;stroke-width:1;stroke-linejoin:round}}.tk{{font-variant-numeric:tabular-nums}}</style>{wobble if sketch else ''}\n"
        f'<rect class="bg" width="{w}" height="{h}" rx="8"/>\n'
        f'<g{" filter=\"url(#sk)\"" if sketch else ""}>\n' + "\n".join(marks) + "\n</g>\n" + "\n".join(text) + "\n</svg>\n"
    )


def scale(lo, hi):
    """A clean y axis that includes 0: (min, max, tick step)."""
    lo, hi = min(lo, 0), max(hi, 0)
    raw = max(hi - lo, 1) / 4
    mag = 10 ** math.floor(math.log10(raw))
    tick = next(s * mag for s in (1, 2, 5, 10) if s * mag >= raw)
    return math.floor(lo / tick) * tick, max(math.ceil(hi / tick) * tick, tick), tick


def month_starts(a, b):
    """First days of the months after a, up to and including b."""
    y, m, out = a.year, a.month, []
    while True:
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        d = datetime(y, m, 1, tzinfo=timezone.utc)
        if d > b:
            return out
        out.append(d)


def monthly(pts):
    """[(time, value)] -> [(month start, increase in that month)]. Only months fully covered by observations.

    The value at each month boundary is interpolated linearly between the surrounding observations,
    so a missing month-end snapshot does not stretch or shrink a month.
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
    """A column that is square at the baseline (y0) and rounded by 4px at the value end (y1)."""
    r = min(4, w / 2, abs(y1 - y0))
    s = r if y1 < y0 else -r
    return (
        f"M{x:.1f},{y0:.1f}V{y1 + s:.1f}Q{x:.1f},{y1:.1f} {x + r:.1f},{y1:.1f}"
        f"H{x + w - r:.1f}Q{x + w:.1f},{y1:.1f} {x + w:.1f},{y1 + s:.1f}V{y0:.1f}Z"
    )


def time_axis(times, left, right):
    """The x scale over the observed range and its month ticks."""
    first, x0, x1 = times[0], times[0].timestamp(), times[-1].timestamp()
    X = lambda d: left + (right - left) * ((d.timestamp() - x0) / (x1 - x0) if x1 > x0 else 1)
    starts = month_starts(first, times[-1])
    ticks = [(d, f"{d:%Y-%m}") for d in starts[:: max(1, math.ceil(len(starts) / 7))]]
    # with observations inside a single month there are no month ticks, so use the first and last day
    return X, ticks or [(d, f"{d:%m-%d}") for d in dict.fromkeys((first, times[-1]))]


def plot(title, series, form="line", style="auto"):
    """series: [(color slot, name, [(datetime, value), ...])]. form: line | area | bars (monthly increase)."""
    L, R, T, PH = 56, 64, 52, 300
    bars, k = form == "bars", len(series)
    H = T + PH + 36 + (18 * k + 8 if k > 1 else 0)
    times = sorted({d for _, _, pts in series for d, _ in pts})
    vals = [v for _, _, pts in series for _, v in pts]
    ymin, ymax, tick = scale(min(vals), max(vals))
    Y = lambda v: T + PH * (ymax - v) / (ymax - ymin)
    if bars:
        times = [times[0], *month_starts(times[0], times[-1])]  # keep every month even when targets cover different ranges
        band = (W - L - R) / len(times)
        bw = max(1, min(24, (band - 4) / k - 2))
        X = lambda d: L + band * (times.index(d) + 0.5)
        ticks = [(d, f"{d:%Y-%m}") for d in times[:: max(1, math.ceil(len(times) / 7))]]
    else:
        X, ticks = time_axis(times, L, W - R)
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
            off = (row - (k - 1) / 2) * (bw + 2) - bw / 2  # columns of the same month side by side with a 2px gap
            marks += [f'<path class="f{n}" d="{column(X(d) + off, bw, Y(0), Y(v))}"/>' for d, v in pts]
            if k == 1:  # value label on the last column only
                text.append(f'<text class="t2 tk" x="{ex:.1f}" y="{ey + (-6 if pts[-1][1] >= 0 else 16):.1f}" text-anchor="middle">{end}</text>')
        else:
            line = " ".join(f"{X(d):.1f},{Y(v):.1f}" for d, v in pts)
            if form == "area":
                marks.append(f'<polygon class="f{n} wash" points="{line} {ex:.1f},{Y(0):.1f} {X(pts[0][0]):.1f},{Y(0):.1f}"/>')
            marks.append(f'<polyline class="ln s{n}" points="{line}"/><circle class="f{n}" cx="{ex:.1f}" cy="{ey:.1f}" r="4"/>')
            # End labels only for up to 4 series and only where they do not collide. The legend carries the rest.
            if k <= 4 and all(abs(ey - p) >= 14 for p in placed):
                placed.append(ey)
                text.append(f'<text class="t2 tk" x="{ex + 8:.1f}" y="{ey + 4:.1f}">{end}</text>')
        if k > 1:
            y = T + PH + 44 + 18 * row
            swatch = f'<rect class="f{n}" x="{L + 4}" y="{y - 10}" width="10" height="10" rx="2"/>' if bars else f'<line class="ln s{n}" x1="{L}" x2="{L + 18}" y1="{y - 4}" y2="{y - 4}"/>'
            marks.append(swatch)
            text.append(f'<text class="t2" x="{L + 26}" y="{y}">{html.escape(name)} · {end}</text>')
    return frame(W, H, style, marks, text)


def value_at(pts, t):
    """The last value observed at or before t, or 0 before the first observation."""
    i = bisect.bisect_right(pts, t, key=lambda p: p[0])
    return pts[i - 1][1] if i else 0


def grew(pts, ago):
    """Increase since `ago`, or None when it cannot be told.

    Uses the observation nearest to `ago` when one lies within 3 days. A target first seen after `ago` counts
    only what it gained since then, so a repository tracked late does not report its lifetime total as recent growth.
    """
    then, old = min(pts, key=lambda p: abs(p[0] - ago))
    if abs(then - ago) <= timedelta(days=3):
        return pts[-1][1] - old
    return pts[-1][1] - pts[0][1] if pts[0][0] > ago else None


def summary(title, series, style="auto"):
    """One panel to read at a glance: the total, its history as a stacked area, and a table that is also the legend.

    series: [(color slot, name, [(datetime, value), ...])]. The top edge of the stack is the total in the header.
    The five largest targets get their own layer and row; the rest are folded into "Other".
    """
    PAD, L, T, PH, ROW = 24, 56, 92, 200, 24
    series = sorted(series, key=lambda s: -s[2][-1][1])
    grid = sorted({d for _, _, pts in series for d, _ in pts})
    now, ago = grid[-1], grid[-1] - timedelta(days=30)
    if len(grid) > 300:  # daily points over years add nothing at this width
        grid = sorted({*grid[:: math.ceil(len(grid) / 300)], now})
    # ponytail: a target first recorded long after it was created enters the stack as a step, not a ramp.
    rows = [(f"f{n}", name, [pts]) for n, name, pts in series[:5]]
    if series[5:]:
        rows.append(("fo", f"Other ({len(series) - 5})", [pts for _, _, pts in series[5:]]))
    at = lambda group, t: sum(value_at(pts, t) for pts in group)
    totals = [at(group, now) for _, _, group in rows]
    total = sum(totals)
    # 30-day change per row: the sum over its targets where it can be told, None where it cannot for any
    changes = [sum(known) if (known := [g for pts in group if (g := grew(pts, ago)) is not None]) else None for _, _, group in rows]
    # with under 30 days of history overall there is no 30-day change to show
    monthly_col = min(pts[0][0] for _, _, pts in series) <= ago and any(c is not None for c in changes)
    fmt = compact if total >= 1e6 else exact  # the two header numbers share one precision
    signed = lambda v, f=exact: ("+" if v > 0 else "−" if v < 0 else "") + f(abs(v))

    stack = [[0.0] * len(grid)]
    for _, _, group in rows:
        stack.append([low + at(group, t) for low, t in zip(stack[-1], grid)])
    ymin, ymax, tick = scale(0, max(stack[-1]))
    Y = lambda v: T + PH * (ymax - v) / (ymax - ymin)
    X, ticks = time_axis(grid, L, W - PAD)
    xs = [X(t) for t in grid]
    if len(grid) == 1:  # first day of collection: a flat band instead of an invisible sliver
        xs, stack = [L, W - PAD], [level * 2 for level in stack]
    path = lambda level: [f"{x:.1f},{Y(v):.1f}" for x, v in zip(xs, level)]

    marks = [f'<line class="grid" x1="{L}" x2="{W - PAD}" y1="{Y(i * tick):.1f}" y2="{Y(i * tick):.1f}"/>' for i in range(1, round(ymax / tick) + 1)]
    marks += [f'<polygon class="{cls}" points="{" ".join(path(high) + path(low)[::-1])}"/>' for (cls, _, _), low, high in zip(rows, stack, stack[1:])]
    # a hairline in the surface color separates the layers; the axis is drawn last so nothing covers it
    marks += [f'<polyline class="gap" points="{" ".join(path(level))}"/>' for level in stack[1:-1]]
    marks.append(f'<line class="axis" x1="{L}" x2="{W - PAD}" y1="{Y(0):.1f}" y2="{Y(0):.1f}"/>')

    head = f'<tspan class="t1" font-size="30" font-weight="600">{fmt(total)}</tspan>'
    if monthly_col:
        head += f'<tspan class="t2 tk" dx="12" font-size="13">{signed(sum(c for c in changes if c is not None), fmt)} in the last 30 days</tspan>'
    text = [
        f'<text class="t2" x="{PAD}" y="30" font-size="13">{html.escape(title)}</text>',
        f'<text class="mu tk" x="{W - PAD}" y="30" text-anchor="end">Updated {now:%Y-%m-%d}</text>',
        f'<text x="{PAD}" y="68">{head}</text>',
    ]
    text += [f'<text class="mu tk" x="{L - 8}" y="{Y(i * tick) + 4:.1f}" text-anchor="end">{compact(i * tick)}</text>' for i in range(round(ymax / tick) + 1)]
    text += [f'<text class="mu tk" x="{X(d):.1f}" y="{T + PH + 18}" text-anchor="middle">{label}</text>' for d, label in ticks]

    c_total, c_change, y = W - PAD - (110 if monthly_col else 0), W - PAD, T + PH + 52
    room = int((c_total - 80 - PAD - 20) / 7.5)  # rough character budget for a name before the Total column
    text.append(f'<text class="mu" x="{c_total}" y="{y}" text-anchor="end">Total</text>')
    if monthly_col:
        text.append(f'<text class="mu" x="{c_change}" y="{y}" text-anchor="end">Last 30 days</text>')
    marks.append(f'<line class="grid" x1="{PAD}" x2="{W - PAD}" y1="{y + 9}" y2="{y + 9}"/>')
    for (cls, name, _), value, change in zip(rows, totals, changes):
        y += ROW
        name = name if len(name) <= room else name[: room - 1] + "…"
        marks.append(f'<circle class="{cls}" cx="{PAD + 5}" cy="{y - 4}" r="5"/>')
        text.append(f'<text class="t1" x="{PAD + 20}" y="{y}" font-size="13">{html.escape(name)}</text>')
        text.append(f'<text class="t1 tk" x="{c_total}" y="{y}" font-size="13" text-anchor="end">{exact(value)}</text>')
        if monthly_col:
            text.append(f'<text class="t2 tk" x="{c_change}" y="{y}" font-size="13" text-anchor="end">{"—" if change is None else signed(change)}</text>')
    return frame(W, y + 20, style, marks, text)


def tracked(cfg, rows):
    """(entity_id, latest name) of the ledger entities that match targets.toml, in first-recorded order.

    Targets deleted on Hugging Face are still included while they have records. Renamed targets also match
    by their earlier names, and matching ignores case.
    """
    # The ledger is not in time order (backfill appends the past later). Use the name of the latest observation.
    names, known = {}, {}
    for r in sorted(rows, key=lambda r: r["observed_at"]):
        names[r["entity_id"]] = r["target"]
        known.setdefault(r["entity_id"], set()).add(r["target"].lower())
    wanted = [t.lower() for t in cfg["targets"]]
    order = dict.fromkeys(r["entity_id"] for r in rows)
    return [(e, names[e]) for e in order if any(fnmatchcase(n, t) for n in known[e] for t in wanted)]


def chart():
    cfg = tomllib.loads((ROOT / "targets.toml").read_text())
    rows = read()
    order = tracked(cfg, rows)
    (ROOT / "charts").mkdir(exist_ok=True)
    embeds = []
    for c in cfg["charts"]:
        metric, form, style = c["metric"], c.get("form", "line"), c.get("style", "auto")
        if form not in FORMS or style not in STYLES:
            sys.exit(f"charts config error: form must be one of {FORMS} and style one of {STYLES}. Got: {form}, {style}")
        if form in ("bars", "summary") and metric in GAUGES:
            sys.exit(f"charts config error: {metric} is not cumulative, so it cannot be drawn as {form}. Use line or area.")
        series = []
        # ponytail: line, area and bars draw only the first 8 targets recorded. The summary takes all of them and folds
        # the tail into "Other"; past 8 targets its colors repeat, so two of the top five could share one.
        shown = order if form == "summary" else order[: len(SERIES)]
        for n, (e, name) in enumerate(shown):  # the color slot stays with the target even when some targets lack the metric
            pts = [(datetime.fromisoformat(r["observed_at"]), float(r["value"])) for r in rows if r["entity_id"] == e and r["metric"] == metric]
            pts.sort(key=lambda p: p[0])  # observations at the same instant keep their recorded order
            if form == "bars":
                pts = monthly(pts)
            if pts:
                series.append((n % len(SERIES), name.split("/", 1)[1], pts))
        if not series:
            print(f"chart: skipped {metric} {form} (not enough records yet)", file=sys.stderr)
            continue
        title = TITLES.get(metric, metric) + (", monthly increase" if form == "bars" else "")
        # When every target has the same owner, say it once in the title instead of on every row.
        owners = {name.rpartition("/")[0] for _, name, _ in series}
        if len(series) == 1:
            title += f" · {series[0][1]}"
        elif len(owners) == 1 and owners != {""}:
            title += f" · {owners.pop()}"
            series = [(n, name.rpartition("/")[2], pts) for n, name, pts in series]
        doc = summary(title, series, style) if form == "summary" else plot(title, series, form, style)
        name = f"{metric}-{form}-{style}.svg"
        (ROOT / "charts" / name).write_text(doc)
        embeds.append((title, name))
        print(f"chart: charts/{name}")
    for old in (ROOT / "charts").glob("*.svg"):  # charts no longer in the config
        if old.name not in {name for _, name in embeds}:
            old.unlink()
    # Rewrite only the <!-- charts --> and <!-- embed --> blocks of the README. Without the markers it is left alone.
    base = raw_base()
    blocks = {
        "charts": "\n\n".join(f"![{alt}](charts/{name})" for alt, name in embeds),
        # absolute URLs for use outside the repository, such as a Hugging Face card
        "embed": "```markdown\n" + "\n\n".join(f"![{alt}]({base}/{name})" for alt, name in embeds) + "\n```" if base else "No GitHub remote, so no URLs can be generated.",
    }
    readme = ROOT / "README.md"
    if readme.exists():
        text = readme.read_text()
        for tag, body in blocks.items():
            text = re.sub(f"<!-- {tag} -->.*<!-- /{tag} -->", lambda _: f"<!-- {tag} -->\n{body}\n<!-- /{tag} -->", text, flags=re.S)
        readme.write_text(text)


def raw_base():
    """Base URL for loading charts/ from outside the repository, or None if origin is not on GitHub."""
    try:
        url = subprocess.run(["git", "-C", str(ROOT), "remote", "get-url", "origin"], capture_output=True, text=True).stdout
    except OSError:
        return None
    m = re.search(r"github\.com[:/](.+?)(?:\.git)?\s*$", url)
    return m and f"https://raw.githubusercontent.com/{m[1]}/HEAD/charts"


def selftest():
    import tempfile

    p = Path(tempfile.mkdtemp()) / "data" / "ledger.csv"
    rec = {"_id": "abc", "id": "o/n", "downloads": 5, "likes": None, "trendingScore": float("nan")}
    rows = observations("2026-01-01T00:00:00Z", rec, "datasets/o/n", ["downloads", "likes", "trendingScore", "missing"], "api")
    assert [r["metric"] for r in rows] == ["downloads"], rows
    assert append(rows, p) == 1 and append(rows, p) == 0  # the same observation only once
    before = p.read_bytes()
    assert append([{**rows[0], "value": 0}], p) == 1  # a changed value (a reset) becomes a new line
    assert append([{**rows[0], "observed_at": "2026-01-01T09:00:00Z"}], p) == 1  # kept even when the value returns the same day (A->B->A)
    assert p.read_bytes().startswith(before) and len(read(p)) == 3  # existing lines untouched
    assert compact(8388304) == "8.39M" and compact(2000) == "2K" and compact(0) == "0" and compact(999500) == "1M"
    t = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert plot("x", [(0, "a", [(t, 1.0)])]).count("<polyline") == 1
    day = lambda m, d: datetime(2026, m, d, tzinfo=timezone.utc)
    # growing by 10 a day: observed Jan 15 to Apr 10, only fully covered February (28 days) and March (31) come out
    grow = [(day(1, 15), 0.0), (day(2, 20), 360.0), (day(4, 10), 850.0)]
    assert [(d.month, round(v)) for d, v in monthly(grow)] == [(2, 280), (3, 310)], monthly(grow)
    assert monthly([(day(1, 1), 50.0), (day(2, 1), 20.0), (day(3, 1), 20.0)])[0][1] == 0.0  # February: 20 -> 20
    assert scale(-20, 15)[:2] == (-20, 20) and scale(0, 0)[1] > 0
    assert "01-01" in plot("x", [(0, "a", [(t, 1.0)])])  # x ticks exist even with observations inside one month
    # summary: the header total is the sum of the latest values, with one layer per target
    two = [(0, "a", [(day(1, 1), 100.0), (day(1, 30), 400.0), (day(3, 1), 1500.0)]), (1, "b", [(day(2, 1), 40.0), (day(3, 1), 500.0)])]
    doc = summary("x", two)  # as of Mar 1; 30 days ago is Jan 30: a grew 1500-400, b (nearest Feb 1) 500-40
    assert ">2,000<" in doc and doc.count("<polygon") == 2 and "+1,560 in the last 30 days" in doc, doc[:600]
    assert value_at(two[1][2], day(1, 15)) == 0 and value_at(two[1][2], day(2, 15)) == 40.0
    assert grew([(day(1, 1), 100.0), (day(3, 1), 900.0)], day(1, 30)) is None  # a gap around 30 days ago: unknown, not 800
    assert grew([(day(2, 20), 100000.0), (day(3, 1), 100050.0)], day(1, 30)) == 50.0  # tracked late: only growth since first seen
    assert "Last 30 days" not in summary("x", [(0, "a", [(day(3, 1), 5.0), (day(3, 9), 9.0)])])  # under 30 days of history
    assert "<polygon" in summary("x", [(0, "a", [(day(3, 1), 5.0)])])  # a single day still draws
    many = [(i % 8, f"t{i}", [(day(1, 1), 0.0), (day(3, 1), 10.0 + i)]) for i in range(9)]
    assert ">126<" in summary("x", many) and "Other (4)" in summary("x", many)  # all 9 targets count toward the total
    led = [
        dict(observed_at="2026-01-02", entity_id="b", target="datasets/o/new"),
        dict(observed_at="2026-01-01", entity_id="b", target="datasets/o/old"),
        dict(observed_at="2026-01-01", entity_id="c", target="datasets/x/y"),
    ]
    assert tracked({"targets": ["datasets/o/*"]}, led) == [("b", "datasets/o/new")]  # whole owner, latest name
    assert tracked({"targets": ["datasets/X/y"]}, led) == [("c", "datasets/x/y")]  # case-insensitive
    assert tracked({"targets": ["datasets/o/old"]}, led) == [("b", "datasets/o/new")]  # even when listed under the earlier name
    # backfill does not fail when the past cannot be filled (it returns only the collect result)
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
