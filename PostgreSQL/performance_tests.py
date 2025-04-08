import psycopg2
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Zapytanie SQL do testowania
QUERY = """
SELECT 
    e.EmployeeID,
    e.FirstName || ' ' || e.LastName AS Employee,
    COUNT(o.OrderID) AS OrderCount,
    SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) AS TotalSales
FROM Employees e
JOIN Orders o ON o.EmployeeID = e.EmployeeID
JOIN OrderDetails od ON od.OrderID = o.OrderID
GROUP BY e.EmployeeID, Employee
ORDER BY TotalSales DESC;
"""

DB_PARAMS = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432
}

def run_query():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        cur.execute(QUERY)
        cur.fetchall()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        return f"ERROR: {e}"

def sequential_run(n):
    print(f"\n▶ Sekwencyjne wykonanie {n} zapytań...")
    start = time.time()
    for _ in range(n):
        run_query()
    end = time.time()
    return end - start

def parallel_run(n):
    print(f"\n Równoległe wykonanie {n} zapytań...")
    start = time.time()
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(run_query) for _ in range(n)]
        for future in as_completed(futures):
            future.result()
    end = time.time()
    return end - start

runs = [10, 100, 1000, 10000, 100000]
results = []

for r in runs:
    t_seq = sequential_run(r)
    t_par = parallel_run(r)
    results.append((r, t_seq, t_par))

with open("query_times.txt", "a", encoding="utf-8") as f:
    f.write(f"\n--- TEST ZAPYTAŃ ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---\n")
    for r, seq, par in results:
        f.write(f"Zapytania: {r} | Sekwencyjnie: {seq:.2f}s | Równolegle: {par:.2f}s\n")
        print(f"[{r} zapytań] Sekwencyjnie: {seq:.2f}s | Równolegle: {par:.2f}s")
