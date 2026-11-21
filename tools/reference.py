#!/usr/bin/env python3
"""Slow, literal implementation of rules.md, used only to cross-check the native pipeline.
Usage: reference.py DATA_DIR OUT_DIR   (writes user_daily.csv and campaign_daily.csv)"""
import bisect
import calendar
import glob
import json
import os
import re
import sys
from collections import defaultdict

DAY = 86_400_000
WEEK = 7 * DAY
GAP = 1_800_000

F1 = re.compile(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})Z$")
F2 = re.compile(r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})([+-])([0-9]{2}):([0-9]{2})$")
F3 = re.compile(r"^[0-9]+$")


def ts_ms(s):
    if s is None:
        return None
    m = F3.match(s)
    if m and s.isascii():
        return int(s)
    m = F1.match(s) or F2.match(s)
    if not m or not s.isascii():
        return None
    y, mo, d, h, mi, se = (int(x) for x in m.groups()[:6])
    if not (1 <= mo <= 12) or h > 23 or mi > 59 or se > 59:
        return None
    if y == 0:
        leap = True
    else:
        leap = calendar.isleap(y)
    dim = [31, 29 if leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][mo - 1]
    if not (1 <= d <= dim):
        return None
    # days from civil
    yy = y - (1 if mo <= 2 else 0)
    era = (yy if yy >= 0 else yy - 399) // 400
    yoe = yy - era * 400
    doy = (153 * (mo + (-3 if mo > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    days = era * 146097 + doe - 719468
    ms = (days * 86400 + h * 3600 + mi * 60 + se) * 1000
    if len(m.groups()) > 6:
        sg, oh, om = m.group(7), int(m.group(8)), int(m.group(9))
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60000
        ms -= off if sg == "+" else -off
    return ms


def parse_line(line, n):
    fields = line.split(",")
    if len(fields) != n:
        return None
    out = []
    for f in fields:
        if '"' in f:
            if len(f) >= 2 and f[0] == '"' and f[-1] == '"' and '"' not in f[1:-1]:
                f = f[1:-1]
            else:
                return None
        out.append(f if f != "" else None)
    return out


def read_rows(pattern, n):
    for path in sorted(glob.glob(pattern)):
        with open(path, "rb") as fh:
            data = fh.read().decode("utf-8", errors="surrogateescape")
        lines = data.split("\n")
        if data.endswith("\n"):
            lines = lines[:-1]
        for line in lines[1:]:
            r = parse_line(line, n)
            if r is not None:
                yield r


def main():
    data, out = sys.argv[1], sys.argv[2]
    man = json.load(open(os.path.join(data, "manifest.json")))
    ws, we = ts_ms(man["window_start"]), ts_ms(man["window_end"])
    products = {r[0]: r[1] for r in read_rows(os.path.join(data, "products.csv"), 2)}
    campaigns = {r[0]: r[1] for r in read_rows(os.path.join(data, "campaigns.csv"), 2)}

    parent = {}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in read_rows(os.path.join(data, "links", "*.csv"), 2):
        if a is None or b is None:
            continue
        parent.setdefault(a, a)
        parent.setdefault(b, b)
        ra, rb = find(a), find(b)
        if ra != rb:
            if rb.encode("utf-8", "surrogateescape") < ra.encode("utf-8", "surrogateescape"):
                ra, rb = rb, ra
            parent[rb] = ra

    def person(u):
        return find(u) if u in parent else u

    best = {}
    for r in read_rows(os.path.join(data, "events", "*.csv"), 7):
        if r[0] is None:
            continue
        ing = ts_ms(r[3])
        cur = best.get(r[0])
        if cur is None or ing < cur[0]:
            best[r[0]] = (ing, r)
    evs = defaultdict(list)
    for ing, r in best.values():
        eid, user, ts, _, et, prod, price = r
        if user is None or prod is None or et not in ("view", "cart", "purchase"):
            continue
        t = ts_ms(ts)
        if t is None or not (ws <= t < we):
            continue
        p = 0
        if et == "purchase":
            if price is None or not re.match(r"^[0-9]+$", price) or not price.isascii():
                continue
            p = int(price)
            if not (1 <= p <= 10_000_000):
                continue
        if ing - t > DAY:
            continue
        evs[person(user)].append((t, p, products.get(prod, "unknown") if p else None))

    seen = set()
    clicks = defaultdict(list)
    for r in read_rows(os.path.join(data, "clicks", "*.csv"), 4):
        cid, user, camp, ts = r
        if cid is None or cid in seen:
            continue
        seen.add(cid)
        t = ts_ms(ts)
        if user is None or t is None or not (ws - WEEK <= t < we) or camp not in campaigns:
            continue
        clicks[person(user)].append((t, cid.encode("utf-8", "surrogateescape"), camp))

    user_daily = {}
    camp_daily = defaultdict(lambda: [0, 0, 0, 0])
    for pid, lst in evs.items():
        lst.sort(key=lambda x: x[0])
        cl = sorted(clicks.get(pid, []))
        cts = [c[0] for c in cl]
        sessions = []
        for t, p, c in lst:
            if sessions and t - sessions[-1]["last"] <= GAP:
                s = sessions[-1]
            else:
                s = {"start": t, "n": 0, "rev": 0, "cats": defaultdict(int)}
                sessions.append(s)
            s["last"] = t
            s["n"] += 1
            if p:
                s["rev"] += p
                s["cats"][c] += p
        for s in sessions:
            st = s["start"]
            day = st // DAY
            k = (pid, day)
            u = user_daily.setdefault(k, [0, 0, 0, defaultdict(int)])
            u[0] += 1
            u[1] += s["n"]
            u[2] += s["rev"]
            for c, v in s["cats"].items():
                u[3][c] += v
            lo = bisect.bisect_left(cts, st - WEEK)
            hi = bisect.bisect_right(cts, st)
            window = cl[lo:hi]
            if window:
                tmax = window[-1][0]
                cand = min(x for x in window if x[0] == tmax)
                credited = cand[2]
            else:
                credited = "organic"
            cd = camp_daily[(credited, day)]
            cd[0] += 1
            cd[1] += 1 if s["rev"] > 0 else 0
            cd[2] += s["rev"]
            for camp in set(x[2] for x in window):
                camp_daily[(camp, day)][3] += 1

    import datetime

    def dstr(d):
        return (datetime.date(1970, 1, 1) + datetime.timedelta(days=d)).isoformat()

    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "user_daily.csv"), "w", encoding="utf-8", errors="surrogateescape") as f:
        f.write("user_id,day,sessions,events,revenue_cents,top_category\n")
        for (pid, day), (n, e, rev, cats) in user_daily.items():
            top = ""
            if cats:
                top = sorted(cats.items(), key=lambda kv: (-kv[1], kv[0].encode()))[0][0]
            f.write(f"{pid},{dstr(day)},{n},{e},{rev},{top}\n")
    with open(os.path.join(out, "campaign_daily.csv"), "w") as f:
        f.write("campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n")
        for (camp, day), (n, ps, rev, a) in camp_daily.items():
            ch = "organic" if camp == "organic" else campaigns[camp]
            f.write(f"{camp},{ch},{dstr(day)},{n},{ps},{rev},{a}\n")


if __name__ == "__main__":
    main()
