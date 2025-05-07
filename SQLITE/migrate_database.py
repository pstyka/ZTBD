import decimal
import sqlite3
import psycopg2

# Konfiguracja połączenia z PostgreSQL
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'qw12qw'
}

# Ścieżka do pliku SQLite
SQLITE_DB = 'ZTB.db'

# Schemat SQLite (Twój kod wklejony jako string)
SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS Customers (
    CustomerID INTEGER PRIMARY KEY AUTOINCREMENT,
    FirstName TEXT NOT NULL,
    LastName TEXT NOT NULL,
    Email TEXT UNIQUE NOT NULL,
    Phone TEXT,
    CreatedAt TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS Products (
    ProductID INTEGER PRIMARY KEY AUTOINCREMENT,
    Name TEXT NOT NULL,
    Description TEXT,
    Price REAL NOT NULL,
    Stock INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS Categories (
    CategoryID INTEGER PRIMARY KEY AUTOINCREMENT,
    Name TEXT NOT NULL,
    Description TEXT
);

CREATE TABLE IF NOT EXISTS ProductCategories (
    ProductID INTEGER,
    CategoryID INTEGER,
    PRIMARY KEY (ProductID, CategoryID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID),
    FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID)
);

CREATE TABLE IF NOT EXISTS Orders (
    OrderID INTEGER PRIMARY KEY AUTOINCREMENT,
    CustomerID INTEGER,
    Status TEXT NOT NULL,
    TotalAmount REAL,
    CreatedAt TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);

CREATE TABLE IF NOT EXISTS OrderItems (
    OrderItemID INTEGER PRIMARY KEY AUTOINCREMENT,
    OrderID INTEGER,
    ProductID INTEGER,
    Quantity INTEGER NOT NULL,
    UnitPrice REAL NOT NULL,
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

CREATE TABLE IF NOT EXISTS Employees (
    EmployeeID INTEGER PRIMARY KEY AUTOINCREMENT,
    FirstName TEXT NOT NULL,
    LastName TEXT NOT NULL,
    Email TEXT UNIQUE,
    Position TEXT
);

CREATE TABLE IF NOT EXISTS Shippers (
    ShipperID INTEGER PRIMARY KEY AUTOINCREMENT,
    CompanyName TEXT NOT NULL,
    Phone TEXT
);

CREATE TABLE IF NOT EXISTS Shipments (
    ShipmentID INTEGER PRIMARY KEY AUTOINCREMENT,
    OrderID INTEGER,
    ShipperID INTEGER,
    ShippedDate TEXT,
    TrackingNumber TEXT,
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
    FOREIGN KEY (ShipperID) REFERENCES Shippers(ShipperID)
);

CREATE TABLE IF NOT EXISTS Payments (
    PaymentID INTEGER PRIMARY KEY AUTOINCREMENT,
    OrderID INTEGER,
    PaymentDate TEXT DEFAULT CURRENT_TIMESTAMP,
    Amount REAL,
    Method TEXT,
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID)
);
"""

# Kolejność tabel (ważna przy relacjach)
TABLES = [
    "Customers", "Products", "Categories", "ProductCategories",
    "Orders", "OrderItems", "Employees", "Shippers", "Shipments", "Payments"
]

def migrate_data():
    # Tworzenie bazy SQLite i struktury
    sqlite_conn = sqlite3.connect(SQLITE_DB)
    sqlite_cur = sqlite_conn.cursor()
    sqlite_cur.executescript(SQLITE_SCHEMA)
    sqlite_conn.commit()

    # Połączenie z PostgreSQL
    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
    pg_cur = pg_conn.cursor()

    for table in TABLES:
        pg_cur.execute(f"SELECT * FROM {table}")
        rows = pg_cur.fetchall()

        if not rows:
            print(f"⚠️ Brak danych w tabeli {table}")
            continue

        placeholders = ', '.join(['?'] * len(rows[0]))
        insert_sql = f"INSERT INTO {table} VALUES ({placeholders})"
        # Zamiana Decimal -> float w każdej wartości
        converted_rows = [
            tuple(float(col) if isinstance(col, decimal.Decimal) else col for col in row)
            for row in rows
        ]
        sqlite_cur.executemany(insert_sql, converted_rows)
        sqlite_conn.commit()
        print(f"✅ {len(rows)} rekordów skopiowano do {table}")

    # Zamknięcie połączeń
    pg_cur.close()
    pg_conn.close()
    sqlite_conn.close()
    print("🎉 Migracja zakończona pomyślnie.")

if __name__ == "__main__":
    migrate_data()
