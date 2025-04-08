import psycopg2
from faker import Faker
from tqdm import tqdm
import random
import time
from datetime import datetime

fake = Faker()

# Połączenie z bazą danych
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="postgres",
    host="localhost",
    port=5432
)
cur = conn.cursor()

# Parametry testu
insert_sizes = [1000, 10000, 100000]
NUM_CUSTOMERS = 999
NUM_EMPLOYEES = 50
NUM_SHIPPERS = 10

# Wyniki do zapisania
results = []

def generate_order():
    return (
        random.randint(1, NUM_CUSTOMERS),
        random.randint(1, NUM_EMPLOYEES),
        fake.date_time_this_decade(),
        fake.date_time_this_year(),
        fake.date_time_this_year(),
        random.randint(1, NUM_SHIPPERS),
        round(random.uniform(10, 500), 2),
        fake.company(),
        fake.street_address(),
        fake.city(),
        fake.state_abbr(),
        fake.postcode(),
        fake.country()
    )

for size in insert_sizes:
    print(f"\n Wstawianie {size} rekordów do tabeli Orders...")
    orders = [generate_order() for _ in range(size)]
    query = """
        INSERT INTO Orders (
            CustomerID, EmployeeID, OrderDate, RequiredDate, ShippedDate,
            ShipVia, Freight, ShipName, ShipAddress, ShipCity, ShipRegion, ShipPostalCode, ShipCountry
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    start = time.time()
    for batch_start in tqdm(range(0, size, 1000)):
        batch = orders[batch_start:batch_start + 1000]
        cur.executemany(query, batch)
        conn.commit()
    end = time.time()

    elapsed = end - start
    print(f" Czas wstawiania {size} rekordów: {elapsed:.2f} sekundy")
    results.append((size, elapsed))

cur.close()
conn.close()


with open("insert_times.txt", "a", encoding="utf-8") as f:
    f.write(f"\n--- WYNIKI TESTU ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---\n")
    for size, seconds in results:
        f.write(f"Wstawiono {size} rekordów w {seconds:.2f} sekundy\n")
    f.write("\n")
