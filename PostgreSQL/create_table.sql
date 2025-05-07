
CREATE TABLE Customers (
    CustomerID INTEGER PRIMARY KEY,
    FirstName TEXT NOT NULL,
    LastName TEXT NOT NULL,
    Email TEXT UNIQUE NOT NULL,
    Phone TEXT,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Products (
    ProductID INTEGER PRIMARY KEY,
    Name TEXT NOT NULL,
    Description TEXT,
    Price NUMERIC(10,2) NOT NULL,
    Stock INTEGER NOT NULL
);

CREATE TABLE Categories (
    CategoryID INTEGER PRIMARY KEY,
    Name TEXT NOT NULL,
    Description TEXT
);

CREATE TABLE ProductCategories (
    ProductID INTEGER,
    CategoryID INTEGER,
    PRIMARY KEY (ProductID, CategoryID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID),
    FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID)
);

CREATE TABLE Orders (
    OrderID INTEGER PRIMARY KEY,
    CustomerID INTEGER,
    OrderDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    Status TEXT NOT NULL,
    TotalAmount NUMERIC(10,2),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID)
);

CREATE TABLE OrderItems (
    OrderItemID INTEGER PRIMARY KEY,
    OrderID INTEGER,
    ProductID INTEGER,
    Quantity INTEGER NOT NULL,
    UnitPrice NUMERIC(10,2) NOT NULL,
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

CREATE TABLE Employees (
    EmployeeID INTEGER PRIMARY KEY,
    FirstName TEXT NOT NULL,
    LastName TEXT NOT NULL,
    Email TEXT UNIQUE,
    Position TEXT,
    HiredAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE Shippers (
    ShipperID INTEGER PRIMARY KEY,
    CompanyName TEXT NOT NULL,
    Phone TEXT
);

CREATE TABLE Shipments (
    ShipmentID INTEGER PRIMARY KEY,
    OrderID INTEGER,
    ShipperID INTEGER,
    ShippedDate TIMESTAMP,
    TrackingNumber TEXT,
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
    FOREIGN KEY (ShipperID) REFERENCES Shippers(ShipperID)
);

CREATE TABLE Payments (
    PaymentID INTEGER PRIMARY KEY,
    OrderID INTEGER,
    PaymentDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    Amount NUMERIC(10,2),
    Method TEXT,
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID)
);
