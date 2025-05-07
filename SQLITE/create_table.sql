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