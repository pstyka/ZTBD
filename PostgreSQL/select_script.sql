-- 1. Lista wszystkich klientów
SELECT * FROM Customers;

-- 2. Lista wszystkich pracowników
SELECT * FROM Employees;

-- 3. Lista wszystkich produktów z nazwą kategorii i dostawcy
SELECT 
    p.ProductID,
    p.ProductName,
    c.CategoryName,
    s.CompanyName AS SupplierName,
    p.UnitPrice,
    p.UnitsInStock
FROM Products p
JOIN Categories c ON p.CategoryID = c.CategoryID
JOIN Suppliers s ON p.SupplierID = s.SupplierID;

-- 4. Lista wszystkich zamówień z nazwą klienta i pracownika
SELECT 
    o.OrderID,
    c.CompanyName AS Customer,
    e.FirstName || ' ' || e.LastName AS Employee,
    o.OrderDate,
    o.Freight
FROM Orders o
JOIN Customers c ON o.CustomerID = c.CustomerID
JOIN Employees e ON o.EmployeeID = e.EmployeeID;

-- 5. Szczegóły zamówienia (OrderDetails)
SELECT 
    od.OrderID,
    p.ProductName,
    od.Quantity,
    od.UnitPrice,
    od.Discount,
    (od.UnitPrice * od.Quantity * (1 - od.Discount)) AS Total
FROM OrderDetails od
JOIN Products p ON od.ProductID = p.ProductID;

-- 6. Ile zamówień złożył każdy klient?
SELECT 
    c.CompanyName,
    COUNT(o.OrderID) AS OrderCount
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.CompanyName
ORDER BY OrderCount DESC;

-- 7. Najlepiej sprzedające się produkty
SELECT 
    p.ProductName,
    SUM(od.Quantity) AS TotalSold
FROM OrderDetails od
JOIN Products p ON od.ProductID = p.ProductID
GROUP BY p.ProductName
ORDER BY TotalSold DESC
LIMIT 10;

-- 8. Wartość sprzedaży wg pracownika
SELECT 
    e.FirstName || ' ' || e.LastName AS Employee,
    SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) AS TotalSales
FROM Orders o
JOIN Employees e ON o.EmployeeID = e.EmployeeID
JOIN OrderDetails od ON o.OrderID = od.OrderID
GROUP BY Employee
ORDER BY TotalSales DESC;

-- 9. Lista dostawców i liczba ich produktów
SELECT 
    s.CompanyName,
    COUNT(p.ProductID) AS ProductCount
FROM Suppliers s
LEFT JOIN Products p ON s.SupplierID = p.SupplierID
GROUP BY s.CompanyName;

-- 10. Średnia wartość zamówienia
SELECT 
    ROUND(AVG(total), 2) AS AverageOrderValue
FROM (
    SELECT 
        o.OrderID,
        SUM(od.UnitPrice * od.Quantity * (1 - od.Discount)) AS total
    FROM Orders o
    JOIN OrderDetails od ON o.OrderID = od.OrderID
    GROUP BY o.OrderID
) AS order_totals;
