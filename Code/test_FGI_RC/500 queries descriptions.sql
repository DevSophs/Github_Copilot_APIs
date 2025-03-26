-- Basic SELECT Queries
-- 1. SELECT * FROM Employees
-- Description: Retrieves all columns and rows from the Employees table, showing complete employee information

-- 2. SELECT FirstName, LastName FROM Customers
-- Description: Retrieves only the first and last names of all customers, useful for basic customer listings

-- 3. SELECT DISTINCT Country FROM Suppliers
-- Description: Gets a unique list of countries where suppliers are located, eliminating duplicates

-- 4. SELECT COUNT(*) FROM Products
-- Description: Counts the total number of products in the Products table

-- 5. SELECT AVG(Price) FROM Products
-- Description: Calculates the average price of all products

-- Filtering Queries
-- 6. SELECT * FROM Orders WHERE OrderDate >= '2024-01-01'
-- Description: Retrieves all orders placed on or after January 1st, 2024

-- 7. SELECT ProductName FROM Products WHERE UnitPrice > 50
-- Description: Lists names of premium products priced above $50

-- 8. SELECT * FROM Employees WHERE Department = 'Sales'
-- Description: Shows all information about employees in the Sales department

-- 9. SELECT * FROM Customers WHERE Country IN ('USA', 'UK', 'Canada')
-- Description: Retrieves customer information from specific North American and European countries

-- 10. SELECT * FROM Orders WHERE OrderStatus != 'Cancelled'
-- Description: Shows all active orders by excluding cancelled orders

-- JOIN Queries
-- 11. SELECT o.OrderID, c.CustomerName 
--     FROM Orders o 
--     INNER JOIN Customers c ON o.CustomerID = c.CustomerID
-- Description: Combines order information with customer names, showing which customer placed each order

-- 12. SELECT p.ProductName, c.CategoryName 
--     FROM Products p 
--     LEFT JOIN Categories c ON p.CategoryID = c.CategoryID
-- Description: Lists all products with their categories, including products without categories

-- Aggregation Queries
-- 13. SELECT COUNT(*), Country 
--     FROM Customers 
--     GROUP BY Country
-- Description: Shows how many customers are in each country

-- 14. SELECT SUM(OrderTotal), YEAR(OrderDate) 
--     FROM Orders 
--     GROUP BY YEAR(OrderDate)
-- Description: Calculates total order value for each year, useful for annual revenue analysis