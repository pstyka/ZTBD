import psycopg2
import time
import random
from faker import Faker

fake = Faker()

conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres", host="localhost", port=5432)
cur = conn.cursor()

cur.execute("SELECT customerid FROM customers")
customer_ids = [row[0] for row in cur.fetchall()]
statuses = ['benchmark_pending', 'benchmark_shipped', 'benchmark_delivered']

sizes = [1000, 10000, 100000, 1000000]
results = {f"Update {i}": [] for i in range(1, 4)}

def generate_orders(n):
    return [(random.choice(customer_ids), random.choice(statuses), round(random.uniform(10, 1000), 2)) for _ in range(n)]

for size in sizes:
    print(f"\n=== {size} rows ===")
    data = generate_orders(size)

    # insert test data
    cur.execute("DELETE FROM orders WHERE status LIKE 'benchmark%'")
    cur.executemany("INSERT INTO orders (customerid, status, totalamount) VALUES (%s, %s, %s)", data)
    conn.commit()

    # 1. update all
    start = time.perf_counter()
    cur.execute("UPDATE orders SET status = 'benchmark_updated' WHERE status LIKE 'benchmark%'")
    conn.commit()
    duration = round(time.perf_counter() - start, 4)
    results["Update 1"].append(f"{size}: {duration} sec")
    print(f"Update 1: {duration} sec")

    # 2. update where total > 500
    cur.execute("DELETE FROM orders WHERE status = 'benchmark_updated'")
    cur.executemany("INSERT INTO orders (customerid, status, totalamount) VALUES (%s, %s, %s)", data)
    conn.commit()

    start = time.perf_counter()
    cur.execute("UPDATE orders SET status = 'benchmark_expensive' WHERE status LIKE 'benchmark%' AND totalamount > 500")
    conn.commit()
    duration = round(time.perf_counter() - start, 4)
    results["Update 2"].append(f"{size}: {duration} sec")
    print(f"Update 2: {duration} sec")

    # 3. update last 10%
    cur.execute("DELETE FROM orders WHERE status = 'benchmark_expensive'")
    cur.executemany("INSERT INTO orders (customerid, status, totalamount) VALUES (%s, %s, %s)", data)
    conn.commit()

    cur.execute("SELECT MAX(orderid) FROM orders WHERE status LIKE 'benchmark%'")
    result = cur.fetchone()[0]
    if result is None:
        print(f"⚠️  No benchmark orders found for size {size}, skipping Update 3.")
        results["Update 3"].append(f"{size}: skipped (no matching rows)")
        continue

    max_id = result
    threshold = max_id - size // 10

    start = time.perf_counter()
    cur.execute("UPDATE orders SET status = 'benchmark_recent' WHERE status LIKE 'benchmark%' AND orderid > %s", (threshold,))
    conn.commit()
    duration = round(time.perf_counter() - start, 4)
    results["Update 3"].append(f"{size}: {duration} sec")
    print(f"Update 3: {duration} sec")

cur.close()
conn.close()

with open("update_times_postgresql.txt", "w") as f:
    for k, v in results.items():
        f.write(f"{k}:\n")
        for line in v:
            f.write(line + "\n")
        f.write("\n")

print("\n✅ All update benchmarks completed.")