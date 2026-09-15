import sqlite3

db = sqlite3.connect('app.db')
cur = db.cursor()
cur.execute("SELECT id, device_id, room_id, ts, published_at FROM fall_warnings WHERE device_id = ? ORDER BY id DESC LIMIT 10", ('diag',))
for row in cur.fetchall():
    print(row)
db.close()
