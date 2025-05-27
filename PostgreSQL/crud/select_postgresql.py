import psycopg2
import time
import csv
from pathlib import Path

conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres", host="localhost", port=5432)
cur = conn.cursor()

limits = [1000, 10000, 100000, 1000000, 3000000]
query_results = {f"Query {i}": [] for i in range(1, 6)}

for limit in limits:
    queries = [
        f"SELECT * FROM orderitems ORDER BY orderid LIMIT {limit}",
        f"SELECT * FROM orderitems WHERE quantity > 5 ORDER BY orderid LIMIT {limit}",
        f"SELECT productid, COUNT(*) FROM orderitems GROUP BY productid ORDER BY COUNT(*) DESC LIMIT {limit}",
        f"SELECT productid, AVG(unitprice) FROM orderitems WHERE quantity BETWEEN 3 AND 8 GROUP BY productid ORDER BY AVG(unitprice) DESC LIMIT {limit}",
        f"""SELECT oi.orderid, p.name, oi.quantity, oi.unitprice 
            FROM orderitems oi 
            JOIN products p ON oi.productid = p.productid 
            ORDER BY oi.orderid 
            LIMIT {limit}"""
    ]

    for i, q in enumerate(queries, 1):
        start = time.perf_counter()
        cur.execute(q)
        cur.fetchall()
        query_results[f"Query {i}"].append(f"{limit}: {round(time.perf_counter() - start, 4)} sec")

cur.close()
conn.close()

with open("select_times_postgresql.txt", "w") as f:
    for k, v in query_results.items():
        f.write(f"{k}:\n")
        for line in v:
            f.write(line + "\n")
        f.write("\n")
