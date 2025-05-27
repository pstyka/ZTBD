import psycopg2
import time
import random
from faker import Faker
import io

fake = Faker()

conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres", host="localhost", port=5432)
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
    cur.execute("TRUNCATE orders RESTART IDENTITY CASCADE")
    conn.commit()
    start = time.perf_counter()
    cur.executemany("INSERT INTO orders (customerid, status, totalamount) VALUES (%s, %s, %s)", data)
    conn.commit()
    results["Insert 1"].append(f"{size}: {round(time.perf_counter() - start, 4)} sec")

    # 2. INSERT ... VALUES
    cur.execute("TRUNCATE orders RESTART IDENTITY CASCADE")
    conn.commit()
    args = ",".join(cur.mogrify("(%s, %s, %s)", row).decode() for row in data)
    start = time.perf_counter()
    cur.execute("INSERT INTO orders (customerid, status, totalamount) VALUES " + args)
    conn.commit()
    results["Insert 2"].append(f"{size}: {round(time.perf_counter() - start, 4)} sec")

    # 3. COPY FROM
    cur.execute("TRUNCATE orders RESTART IDENTITY CASCADE")
    conn.commit()
    buffer = io.StringIO()
    for row in data:
        buffer.write(f"{row[0]}\t{row[1]}\t{row[2]}\n")
    buffer.seek(0)
    start = time.perf_counter()
    cur.copy_from(buffer, 'orders', columns=('customerid', 'status', 'totalamount'))
    conn.commit()
    results["Insert 3"].append(f"{size}: {round(time.perf_counter() - start, 4)} sec")

cur.close()
conn.close()

with open("insert_times_postgresql.txt", "w") as f:
    for label, lines in results.items():
        f.write(f"{label}:\n")
        for line in lines:
            f.write(line + "\n")
        f.write("\n")
