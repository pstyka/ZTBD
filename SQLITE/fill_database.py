import sqlite3
from faker import Faker
from tqdm import tqdm
import random

# Konfiguracja
NUM_CUSTOMERS = 1000
NUM_EMPLOYEES = 50
NUM_SHIPPERS = 10
NUM_CATEGORIES = 5
NUM_SUPPLIERS = 20
NUM_PRODUCTS = 100
NUM_ORDERS = 1000000
BATCH_SIZE = 1000

fake = Faker()
conn = sqlite3.connect("sqlite.db")  
cur = conn.cursor()

def insert_customers():
    print("Customers...")
    for _ in tqdm(range(NUM_CUSTOMERS)):
        cur.execute("""
            INSERT INTO Customers (CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            fake.company(), fake.name(), fake.job(), fake.street_address(),
            fake.city(), fake.state_abbr(), fake.postcode(), fake.country(),
            fake.phone_number(), fake.phone_number()
        ))

def insert_employees():
    print("Employees...")
    for _ in tqdm(range(NUM_EMPLOYEES)):
        cur.execute("""
            INSERT INTO Employees (LastName, FirstName, Title, TitleOfCourtesy, BirthDate, HireDate,
                                   Address, City, Region, PostalCode, Country, HomePhone, Extension, Photo, Notes, ReportsTo, PhotoPath)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, ?)
        """, (
            fake.last_name(), fake.first_name(), fake.job(), "Mr.",
            fake.date_of_birth(), fake.date_this_decade(), fake.street_address(),
            fake.city(), fake.state_abbr(), fake.postcode(), fake.country(),
            fake.phone_number(), str(random.randint(100, 999)), fake.text(), fake.image_url()
        ))

def insert_shippers():
    print("Shippers...")
    for _ in tqdm(range(NUM_SHIPPERS)):
        cur.execute("INSERT INTO Shippers (CompanyName, Phone) VALUES (?, ?)", (fake.company(), fake.phone_number()))

def insert_categories():
    print("Categories...")
    for _ in tqdm(range(NUM_CATEGORIES)):
        cur.execute("INSERT INTO Categories (CategoryName, Description) VALUES (?, ?)", (fake.word(), fake.text()))

def insert_suppliers():
    print("Suppliers...")
    for _ in tqdm(range(NUM_SUPPLIERS)):
        cur.execute("""
            INSERT INTO Suppliers (CompanyName, ContactName, ContactTitle, Address, City, Region,
                                   PostalCode, Country, Phone, Fax, HomePage)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            fake.company(), fake.name(), fake.job(), fake.street_address(),
            fake.city(), fake.state_abbr(), fake.postcode(), fake.country(),
            fake.phone_number(), fake.phone_number(), fake.url()
        ))

def insert_products():
    print("Products...")
    for _ in tqdm(range(NUM_PRODUCTS)):
        cur.execute("""
            INSERT INTO Products (ProductName, SupplierID, CategoryID, QuantityPerUnit,
                                  UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            fake.word(), random.randint(1, NUM_SUPPLIERS), random.randint(1, NUM_CATEGORIES),
            f"{random.randint(1, 20)} units", round(random.uniform(10, 500), 2),
            random.randint(0, 100), random.randint(0, 50),
            random.randint(0, 20), random.choice([0, 1])
        ))

def insert_orders():
    print("Orders...")
    for batch in tqdm(range(NUM_ORDERS // BATCH_SIZE)):
        orders = []
        for _ in range(BATCH_SIZE):
            orders.append((
                random.randint(1, NUM_CUSTOMERS),
                random.randint(1, NUM_EMPLOYEES),
                fake.date_time_this_decade().isoformat(),
                fake.date_time_this_year().isoformat(),
                fake.date_time_this_year().isoformat(),
                random.randint(1, NUM_SHIPPERS),
                round(random.uniform(10, 200), 2),
                fake.company(),
                fake.street_address(), fake.city(), fake.state_abbr(), fake.postcode(), fake.country()
            ))
        cur.executemany("""
            INSERT INTO Orders (CustomerID, EmployeeID, OrderDate, RequiredDate, ShippedDate,
                                ShipVia, Freight, ShipName, ShipAddress, ShipCity, ShipRegion, ShipPostalCode, ShipCountry)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, orders)
        conn.commit()

# --- Uruchomienie wszystkich insertów ---
insert_customers()
insert_employees()
insert_shippers()
insert_categories()
insert_suppliers()
insert_products()
insert_orders()

cur.close()
conn.close()
print("✅ Gotowe!")
