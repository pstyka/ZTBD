import psycopg2
import time
import random
from faker import Faker

fake = Faker()
conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres", host="localhost", port=5432)
cur = conn.cursor()

cur.execute("SELECT customerid FROM customers")
customer_ids = [row[0] for row in cur.fetchall()]
statuses = ['benchmark_del1', 'benchmark_del2', 'benchmark_del3']

sizes = [1000, 10000, 100000, 1000000]
results = {f"Delete {i}": [] for i in range(1, 4)}

def generate_orders(n):
    return [(random.choice(customer_ids), random.choice(statuses), round(random.uniform(10, 1000), 2)) for _ in range(n)]

for size in sizes:
    data = generate_orders(size)

    # 1. DELETE all
    cur.execute("TRUNCATE orders RESTART IDENTITY CASCADE")
    cur.executemany("INSERT INTO orders (customerid, status, totalamount) VALUES (%s, %s, %s)", data)
    conn.commit()
    start = time.perf_counter()
    cur.execute("DELETE FROM orders")
    conn.commit()
    results["Delete 1"].append(f"{size}: {round(time.perf_counter() - start, 4)} sec")

    # 2. DELETE WHERE total > 500
    cur.executemany("INSERT INTO orders (customerid, status, totalamount) VALUES (%s, %s, %s)", data)
    conn.commit()
    start = time.perf_counter()
    cur.execute("DELETE FROM orders WHERE totalamount > 500")
    conn.commit()
    results["Delete 2"].append(f"{size}: {round(time.perf_counter() - start, 4)} sec")

    # 3. DELETE ostatnie 10%
    cur.execute("TRUNCATE orders RESTART IDENTITY CASCADE")
    cur.executemany("INSERT INTO orders (customerid, status, totalamount) VALUES (%s, %s, %s)", data)
    conn.commit()
    cur.execute("SELECT MAX(orderid) FROM orders")
    max_id = cur.fetchone()[0]
    cutoff = max_id - size // 10
    start = time.perf_counter()
    cur.execute("DELETE FROM orders WHERE orderid > %s", (cutoff,))
    conn.commit()
    results["Delete 3"].append(f"{size}: {round(time.perf_counter() - start, 4)} sec")

cur.close()
conn.close()

with open("delete_times_postgresql.txt", "w") as f:
    for k, v in results.items():
        f.write(f"{k}:\n")
        for line in v:
            f.write(line + "\n")
        f.write("\n")
