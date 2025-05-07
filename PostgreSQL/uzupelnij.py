import psycopg2
from faker import Faker
from tqdm import tqdm
import random

fake = Faker()

# Database connection
pg_conn = psycopg2.connect(
    dbname="postgres", user="postgres", password="postgres", host="localhost", port=5432
)
pg_cur = pg_conn.cursor()

# Config
NUM_CUSTOMERS = 10000
NUM_PRODUCTS = 500
NUM_CATEGORIES = 50
NUM_EMPLOYEES = 200
NUM_SHIPPERS = 20
NUM_ORDERS = 1000000
MAX_ORDER_ITEMS = 5

# Helpers
def pg_insert(query, values):
    pg_cur.execute(query, values)

# Base tables
print("Generating base data...")

for _ in tqdm(range(NUM_CUSTOMERS), desc="Customers"):
    pg_insert("INSERT INTO Customers (FirstName, LastName, Email, Phone) VALUES (%s, %s, %s, %s)",
              (fake.first_name(), fake.last_name(), fake.unique.email(), fake.phone_number()))

for _ in tqdm(range(NUM_PRODUCTS), desc="Products"):
    pg_insert("INSERT INTO Products (Name, Description, Price, Stock) VALUES (%s, %s, %s, %s)",
              (fake.word(), fake.sentence(), round(random.uniform(10, 500), 2), random.randint(10, 1000)))

for _ in tqdm(range(NUM_CATEGORIES), desc="Categories"):
    pg_insert("INSERT INTO Categories (Name, Description) VALUES (%s, %s)",
              (fake.word(), fake.sentence()))

for product_id in range(1, NUM_PRODUCTS + 1):
    category_id = random.randint(1, NUM_CATEGORIES)
    pg_insert("INSERT INTO ProductCategories (ProductID, CategoryID) VALUES (%s, %s)",
              (product_id, category_id))

for _ in tqdm(range(NUM_EMPLOYEES), desc="Employees"):
    pg_insert("INSERT INTO Employees (FirstName, LastName, Email, Position) VALUES (%s, %s, %s, %s)",
              (fake.first_name(), fake.last_name(), fake.unique.email(), fake.job()))

for _ in tqdm(range(NUM_SHIPPERS), desc="Shippers"):
    pg_insert("INSERT INTO Shippers (CompanyName, Phone) VALUES (%s, %s)",
              (fake.company(), fake.phone_number()))

pg_conn.commit()

# Pobierz dostępne ID
pg_cur.execute("SELECT CustomerID FROM Customers")
customer_ids = [row[0] for row in pg_cur.fetchall()]

pg_cur.execute("SELECT ProductID FROM Products")
product_ids = [row[0] for row in pg_cur.fetchall()]

pg_cur.execute("SELECT ShipperID FROM Shippers")
shipper_ids = [row[0] for row in pg_cur.fetchall()]

# Orders
print("Generating orders...")

for order_id in tqdm(range(1, NUM_ORDERS + 1), desc="Orders"):
    customer_id = random.choice(customer_ids)
    status = random.choice(["pending", "shipped", "delivered", "cancelled"])
    total_amount = 0.0

    # Insert order
    pg_cur.execute("INSERT INTO Orders (CustomerID, Status, TotalAmount) VALUES (%s, %s, %s) RETURNING OrderID",
                   (customer_id, status, 0.0))
    order_id_db = pg_cur.fetchone()[0]

    num_items = random.randint(1, MAX_ORDER_ITEMS)
    for _ in range(num_items):
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 5)
        price = round(random.uniform(10, 500), 2)
        total_amount += price * quantity

        pg_cur.execute("INSERT INTO OrderItems (OrderID, ProductID, Quantity, UnitPrice) VALUES (%s, %s, %s, %s)",
                       (order_id_db, product_id, quantity, price))

    # Update total
    pg_cur.execute("UPDATE Orders SET TotalAmount = %s WHERE OrderID = %s", (round(total_amount, 2), order_id_db))

    # Shipment
    if status in ("shipped", "delivered"):
        pg_cur.execute("INSERT INTO Shipments (OrderID, ShipperID, ShippedDate, TrackingNumber) VALUES (%s, %s, %s, %s)",
                       (order_id_db, random.choice(shipper_ids), fake.date_this_year(), fake.uuid4()))

    # Payment
    pg_cur.execute("INSERT INTO Payments (OrderID, PaymentDate, Amount, Method) VALUES (%s, %s, %s, %s)",
                   (order_id_db, fake.date_this_year(), round(total_amount, 2), random.choice(["card", "paypal", "bank"])))

    # Commit co 10k
    if order_id % 10000 == 0:
        pg_conn.commit()

pg_conn.commit()
pg_cur.close()
pg_conn.close()

print("✅ Data generation complete.")
