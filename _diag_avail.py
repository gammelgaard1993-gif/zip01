import sqlite3
import datetime as dt

import redis

DEVICE = "dev_0000"

c = sqlite3.connect("teton.db")
tot = c.execute(
    "select count(*) from events where device_id=?", (DEVICE,)
).fetchone()[0]
hb = c.execute(
    "select count(*), min(ts), max(ts) from events where device_id=? and type='heartbeat'",
    (DEVICE,),
).fetchone()

r = redis.from_url("redis://localhost:6379/0")
now = dt.datetime.now(dt.timezone.utc).timestamp()
key = f"device:{DEVICE}:heartbeats"
zc = r.zcard(key)
win = r.zcount(key, now - 300, now)
beats = r.zrange(key, 0, -1, withscores=True)

print("SQLite events total:", tot)
print("SQLite heartbeat count / min ts / max ts:", hb)
print("server now:", dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat())
print("Redis zcard (all beats in set):", zc)
print("Redis zcount [now-300, now]:", win)
if beats:
    oldest = dt.datetime.fromtimestamp(beats[0][1], dt.timezone.utc).isoformat()
    newest = dt.datetime.fromtimestamp(beats[-1][1], dt.timezone.utc).isoformat()
    print("oldest beat:", oldest)
    print("newest beat:", newest, "(age", round(now - beats[-1][1], 1), "s)")
