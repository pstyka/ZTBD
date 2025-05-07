SELECT FirstName, LastName FROM Customers;

SELECT Name, Price FROM Products
WHERE Price > 100;

SELECT * FROM Orders
ORDER BY CreatedAt DESC
LIMIT 10;

SELECT o.OrderID, c.FirstName, c.LastName, o.TotalAmount
FROM Orders o
JOIN Customers c ON o.CustomerID = c.CustomerID;

SELECT o.OrderID, c.Email, o.TotalAmount
FROM Orders o
JOIN Customers c ON o.CustomerID = c.CustomerID
WHERE o.Status = 'delivered'
ORDER BY o.TotalAmount DESC;

SELECT oi.OrderID, p.Name AS ProductName, oi.Quantity, oi.UnitPrice
FROM OrderItems oi
JOIN Products p ON oi.ProductID = p.ProductID
JOIN Orders o ON oi.OrderID = o.OrderID
WHERE o.Status = 'shipped';

SELECT c.CustomerID, COUNT(o.OrderID) AS OrderCount, SUM(o.TotalAmount) AS TotalSpent
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerID
ORDER BY TotalSpent DESC;

SELECT o.OrderID, c.FirstName || ' ' || c.LastName AS Customer, p.Name AS Product, s.CompanyName AS Shipper, pay.Method
FROM Orders o
JOIN Customers c ON o.CustomerID = c.CustomerID
JOIN OrderItems oi ON o.OrderID = oi.OrderID
JOIN Products p ON oi.ProductID = p.ProductID
JOIN Shipments s ON o.OrderID = s.OrderID
JOIN Payments pay ON o.OrderID = pay.OrderID
WHERE o.Status IN ('shipped', 'delivered') AND pay.Method = 'card'
ORDER BY o.CreatedAt DESC
LIMIT 20;
