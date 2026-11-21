#!/usr/bin/env python3
"""Random adversarial datasets (long lines, odd quoting, CR, unicode, no trailing newline,
many tiny files, header-only files, weird timestamps) to diff the native binary against
reference.py.  Usage: torture.py OUT_DIR SEED"""
import json
import os
import random
import sys

out, seed = sys.argv[1], int(sys.argv[2])
R = random.Random(seed)
WS = 1790812800000  # 2026-10-01
DAY = 86400000


def iso(ms, off_min=None):
    import datetime
    t = datetime.datetime.fromtimestamp(ms / 1000, datetime.UTC)
    if off_min is None:
        return t.strftime("%Y-%m-%dT%H:%M:%SZ")
    lt = t + datetime.timedelta(minutes=off_min)
    sg = "+" if off_min >= 0 else "-"
    a = abs(off_min)
    return lt.strftime("%Y-%m-%dT%H:%M:%S") + f"{sg}{a // 60:02d}:{a % 60:02d}"


def ts_str(ms):
    r = R.random()
    if r < 0.4 and ms % 1000 == 0:
        return iso(ms)
    if r < 0.55 and ms % 1000 == 0:
        return iso(ms, R.choice([-600, -330, 0, 60, 345, 720, 1439, -1439]))
    if r < 0.6:
        return "0" * R.randint(1, 5) + str(ms)
    if r < 0.66:
        return R.choice(["", "2026-10-03 10:00:00Z", "2026-02-29T10:00:00Z", "2028-02-29T10:00:00Z",
                         "2026-10-03T24:00:00Z", "2026-10-03T10:00:60Z", "2026-13-03T10:00:00Z",
                         "2026-10-03T10:00:00+24:00", "2026-10-03T10:00:00+0200", "1.79e12", "-5",
                         "2026-10-03T10:00:00.000Z", "2026-10-03t10:00:00Z", "2026-10-03T10:00:00z",
                         "１７９０８１２８０００００", "9" * 25, "86400000", "2026-10-03T10:00:00-00:00",
                         "２026-10-03T10:00:00Z", str(ms) + "\r"])
    return str(ms)


def rid(prefix, pool):
    r = R.random()
    if r < 0.85:
        return prefix + R.choice(pool)
    if r < 0.9:
        return prefix + "x" * R.randint(10, 300)  # long
    if r < 0.93:
        return " " + prefix + R.choice(pool)
    if r < 0.96:
        return (prefix + R.choice(pool)).upper()
    if r < 0.98:
        return prefix + "é" + R.choice(pool)
    return prefix + R.choice(pool) + "\t"


def q(v):
    r = R.random()
    if r < 0.2:
        return '"' + v + '"'
    if r < 0.205:
        return '"' + v
    if r < 0.21:
        return v + '"'
    if r < 0.212:
        return 'a"b' + v
    return v


def write_part(folder, header, lines):
    os.makedirs(folder, exist_ok=True)
    nfiles = R.randint(1, 6)
    buckets = [[] for _ in range(nfiles)]
    for ln in lines:
        buckets[R.randrange(nfiles)].append(ln)
    for i, b in enumerate(buckets):
        body = header + "\n" + "\n".join(b)
        if b and R.random() < 0.7:
            body += "\n"
        if R.random() < 0.1:
            body = header  # header only, no newline
        with open(os.path.join(folder, f"part-{i:05d}.csv"), "w", encoding="utf-8") as f:
            f.write(body)


users = [str(i) for i in range(40)]
events = []
used_ing = {}
for i in range(R.randint(200, 3000)):
    eid = rid("e", [str(R.randint(0, 400))])
    base = WS + R.randint(-DAY, 15 * DAY)
    if R.random() < 0.3:
        base = base // 1000 * 1000
    ts = ts_str(base)
    ingms = (base // 1000 + R.choice([60, 600, 86400, 86401, -5, 3 * 86400])) * 1000
    # copies with equal ingest_ts must be identical: give each copy of an id a distinct ingest
    while (eid, ingms) in used_ing:
        ingms += 1000
    used_ing[(eid, ingms)] = 1
    ing = iso(ingms)
    et = R.choice(["view"] * 6 + ["cart"] * 2 + ["purchase"] * 3 + ["Purchase", "view ", ""])
    prod = R.choice(["p1", "p2", "p9", " p1", "P1", "", "p" + "1" * 40])
    price = R.choice(["100", "0250", "10000000", "10000001", "0", "", "19.99", "-5", "٢٥٠", "1000\r", str(R.randint(1, 99999))])
    fields = [eid, rid("u", users), ts, ing, et, prod, price]
    if R.random() < 0.01:
        fields = fields[:-1]
    if R.random() < 0.01:
        fields.append("x")
    line = ",".join(q(f) for f in fields)
    if R.random() < 0.01:
        line = ""
    if R.random() < 0.02:
        line += "\r"
    events.append(line)
clicks = []
for i in range(R.randint(50, 1500)):
    cid = R.choice(["k", "K", " k", "k" + "z" * 200]) + str(i)  # unique per click
    base = WS + R.randint(-9 * DAY, 15 * DAY)
    if R.random() < 0.05:
        base = clicks_last_base if clicks else base  # same-millisecond clicks
    clicks_last_base = base
    fields = [cid, rid("u", users), R.choice(["c_1", "c_2", "c_3", "C_1", " c_1", "c_999", ""]), ts_str(base)]
    for _ in range(1 + (R.random() < 0.08) + (R.random() < 0.03)):
        clicks.append(",".join(q(f) for f in fields))  # identical copies, quoted differently
links = []
for i in range(R.randint(0, 80)):
    links.append(",".join(q(f) for f in [rid("u", users), rid("u", users)]))
write_part(os.path.join(out, "events"), "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents", events)
write_part(os.path.join(out, "clicks"), "click_id,user_id,campaign_id,ts", clicks)
write_part(os.path.join(out, "links"), "user_id_a,user_id_b", links)
with open(os.path.join(out, "products.csv"), "w") as f:
    f.write("product_id,category\np1,shoes\np2,books\n" + "p" + "1" * 40 + ",toys\n")
with open(os.path.join(out, "campaigns.csv"), "w") as f:
    f.write("campaign_id,channel\nc_1,search\nc_2,social\nc_3,email\n")
with open(os.path.join(out, "manifest.json"), "w") as f:
    json.dump({"window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-15T00:00:00Z"}, f)
