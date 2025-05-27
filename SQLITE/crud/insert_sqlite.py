import sqlite3
import time
import random
from faker import Faker

fake = Faker()
conn = sqlite3.connect("sqlite.db")
cur = conn.cursor()

cur.execute("SELECT customerid FROM customers")
customer_ids = [row[0] for row in cur.fetchall()]
statuses = ['pending', 'shipped', 'delivered', 'cancelled']
sizes = [1000, 10000, 100000, 1000000]
results = {f"Insert {i}": [] for i in range(1, 4)}

def generate_data(n):
    return [(random.choice(customer_ids), random.choice(statuses), round(random.uniform(10, 1000), 2)) for _ in range(n)]

for size in sizes:
    data = generate_data(size)

    # 1. executemany
    cur.execute("DELETE FROM orders")
    conn.commit()
    start = time.perf_counter()
    cur.executemany("INSERT INTO orders (customerid, status, totalamount) VALUES (?, ?, ?)", data)
    conn.commit()
    results["Insert 1"].append(f"{size}: {round(time.perf_counter() - start, 4)} sec")

    # 2. pojedyncze INSERTY
    cur.execute("DELETE FROM orders")
    conn.commit()
    start = time.perf_counter()
    for row in data:
        cur.execute("INSERT INTO orders (customerid, status, totalamount) VALUES (?, ?, ?)", row)
    conn.commit()
    results["Insert 2"].append(f"{size}: {round(time.perf_counter() - start, 4)} sec")

    # 3. transakcja + executemany
    cur.execute("DELETE FROM orders")
    conn.commit()
    start = time.perf_counter()
    with conn:
        conn.executemany("INSERT INTO orders (customerid, status, totalamount) VALUES (?, ?, ?)", data)
    results["Insert 3"].append(f"{size}: {round(time.perf_counter() - start, 4)} sec")

cur.close()
conn.close()

with open("insert_times_sqlite.txt", "w") as f:
    for label, lines in results.items():
        f.write(f"{label}:\n")
        for line in lines:
            f.write(line + "\n")
        f.write("\n")
