
-- chapter 4

SELECT * FROM Sales.SalesOrderDetail
ORDER BY OrderQty

sp_configure 'max degree of parallelism', 1
RECONFIGURE

SELECT * FROM Sales.SalesOrderDetail
ORDER BY OrderQty
OPTION (MAXDOP 4)

sp_configure 'max degree of parallelism', 0
RECONFIGURE

ALTER DATABASE AdventureWorks SET COMPATIBILITY_LEVEL = 130

ALTER DATABASE SCOPED CONFIGURATION
SET LEGACY_CARDINALITY_ESTIMATION = ON

SELECT * FROM Person.Address WHERE City = 'Burbank' AND PostalCode = '91502'
OPTION (QUERYTRACEON 9481)

sp_configure 'max server memory', 16384
GO
RECONFIGURE

sp_configure 'max server memory', 2147483647
GO
RECONFIGURE

sp_configure 'query governor cost limit', 20
RECONFIGURE

SELECT * FROM Sales.SalesOrderDetail s1 CROSS JOIN Sales.SalesOrderDetail s2

sp_configure 'query governor cost limit', 0
RECONFIGURE

sp_configure 'blocked process threshold (s)', 5
RECONFIGURE

CREATE EVENT SESSION blocked_process_test ON SERVER 
ADD EVENT sqlserver.blocked_process_report

sp_configure 'blocked process threshold (s)', 0
RECONFIGURE

DROP EVENT SESSION blocked_process_test ON SERVER 
GO
