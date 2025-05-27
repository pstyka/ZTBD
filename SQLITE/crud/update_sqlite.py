import sqlite3
import time
import random
from faker import Faker

fake = Faker()
conn = sqlite3.connect("sqlite.db")
cur = conn.cursor()

cur.execute("SELECT customerid FROM customers")
customer_ids = [row[0] for row in cur.fetchall()]
statuses = ['benchmark_pending', 'benchmark_shipped', 'benchmark_delivered']

sizes = [1000, 10000, 100000, 1000000]
results = {f"Update {i}": [] for i in range(1, 4)}

def generate_orders(n):
    return [(random.choice(customer_ids), random.choice(statuses), round(random.uniform(10, 1000), 2)) for _ in range(n)]

for size in sizes:
    data = generate_orders(size)

    cur.execute("DELETE FROM orders WHERE status LIKE 'benchmark%'")
    cur.executemany("INSERT INTO orders (customerid, status, totalamount) VALUES (?, ?, ?)", data)
    conn.commit()

    # 1. update all
    start = time.perf_counter()
    cur.execute("UPDATE orders SET status = 'benchmark_updated' WHERE status LIKE 'benchmark%'")
    conn.commit()
    results["Update 1"].append(f"{size}: {round(time.perf_counter() - start, 4)} sec")

    # 2. update where total > 500
    cur.execute("DELETE FROM orders WHERE status = 'benchmark_updated'")
    cur.executemany("INSERT INTO orders (customerid, status, totalamount) VALUES (?, ?, ?)", data)
    conn.commit()

    start = time.perf_counter()
    cur.execute("UPDATE orders SET status = 'benchmark_expensive' WHERE status LIKE 'benchmark%' AND totalamount > 500")
    conn.commit()
    results["Update 2"].append(f"{size}: {round(time.perf_counter() - start, 4)} sec")

    # 3. update last 10%
    cur.execute("DELETE FROM orders WHERE status = 'benchmark_expensive'")
    cur.executemany("INSERT INTO orders (customerid, status, totalamount) VALUES (?, ?, ?)", data)
    conn.commit()

    cur.execute("SELECT MAX(orderid) FROM orders WHERE status LIKE 'benchmark%'")
    max_id = cur.fetchone()[0]
    threshold = max_id - size // 10

    start = time.perf_counter()
    cur.execute("UPDATE orders SET status = 'benchmark_recent' WHERE status LIKE 'benchmark%' AND orderid > ?", (threshold,))
    conn.commit()
    results["Update 3"].append(f"{size}: {round(time.perf_counter() - start, 4)} sec")

cur.close()
conn.close()

with open("update_times_sqlite.txt", "w") as f:
    for k, v in results.items():
        f.write(f"{k}:\n")
        for line in v:
            f.write(line + "\n")
        f.write("\n")
