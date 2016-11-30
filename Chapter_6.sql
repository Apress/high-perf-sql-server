
-- chapter 6

CREATE DATABASE Test
ON PRIMARY (NAME = Test_data,
FILENAME = 'C:\DATA\Test_data.mdf', SIZE=500MB),
FILEGROUP Test_fg CONTAINS MEMORY_OPTIMIZED_DATA
(NAME = Test_fg, FILENAME = 'C:\DATA\Test_fg')
LOG ON (NAME = Test_log, Filename='C:\DATA\Test_log.ldf', SIZE=500MB)

-- returns error 12317
CREATE TABLE dbo.SalesOrderHeader(
	SalesOrderID int IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	RevisionNumber tinyint NOT NULL,
	OrderDate datetime NOT NULL,
	DueDate datetime NOT NULL,
	ShipDate datetime NULL,
	Status tinyint NOT NULL,
	SalesOrderNumber  AS (isnull(N'SO'+CONVERT(nvarchar(23),SalesOrderID),N'*** ERROR ***')),
	CustomerID int NOT NULL,
	SalesPersonID int NULL,
	TerritoryID int NULL,
	BillToAddressID int NOT NULL,
	ShipToAddressID int NOT NULL,
	ShipMethodID int NOT NULL,
	CreditCardID int NULL,
	CreditCardApprovalCode varchar(15) NULL,
	CurrencyRateID int NULL,
	SubTotal money NOT NULL,
	TaxAmt money NOT NULL,
	Freight money NOT NULL,
	TotalDue  AS (isnull((SubTotal+TaxAmt)+Freight,(0))),
	Comment nvarchar(128) NULL,
	rowguid uniqueidentifier ROWGUIDCOL  NOT NULL,
	ModifiedDate datetime NOT NULL,
	CONSTRAINT PK_SalesOrderHeader_SalesOrderID PRIMARY KEY CLUSTERED (
	SalesOrderID ASC
	)
) WITH (MEMORY_OPTIMIZED = ON) 

CREATE TABLE dbo.SalesOrderHeader(
	SalesOrderID int PRIMARY KEY NONCLUSTERED NOT NULL,
	RevisionNumber tinyint NOT NULL,
	OrderDate datetime NOT NULL,
	DueDate datetime NOT NULL,
	ShipDate datetime NULL,
	Status tinyint NOT NULL,
	CustomerID int NOT NULL,
	SalesPersonID int NULL,
	TerritoryID int NULL,
	BillToAddressID int NOT NULL,
	ShipToAddressID int NOT NULL,
	ShipMethodID int NOT NULL,
	CreditCardID int NULL,
	CreditCardApprovalCode varchar(15) NULL,
	CurrencyRateID int NULL,
	SubTotal money NOT NULL,
	TaxAmt money NOT NULL,
	Freight money NOT NULL,
	Comment nvarchar(128) NULL,
	ModifiedDate datetime NOT NULL
) WITH (MEMORY_OPTIMIZED = ON)

DROP TABLE dbo.SalesOrderHeader

CREATE TABLE dbo.SalesOrderHeader(
	SalesOrderID int PRIMARY KEY NONCLUSTERED HASH WITH
		(BUCKET_COUNT = 10000),
	OrderDate datetime NOT NULL,
	ShipDate datetime NULL,
	Status tinyint NOT NULL,
	CustomerID int NOT NULL,
) WITH (MEMORY_OPTIMIZED = ON)

SELECT OBJECT_NAME(object_id), * FROM sys.dm_db_xtp_hash_index_stats

CREATE TABLE dbo.SalesOrderHeader(
	SalesOrderID int PRIMARY KEY NONCLUSTERED,
	OrderDate datetime NOT NULL,
	ShipDate datetime NULL,
	Status tinyint NOT NULL,
	CustomerID int NOT NULL,
) WITH (MEMORY_OPTIMIZED = ON)

CREATE PROCEDURE test
WITH NATIVE_COMPILATION, SCHEMABINDING, 
EXECUTE AS OWNER
AS
BEGIN ATOMIC WITH (
TRANSACTION ISOLATION LEVEL = SNAPSHOT,
LANGUAGE = 'us_english')
SELECT SalesOrderID, OrderDate, CustomerID
FROM dbo.SalesOrderHeader
END

EXEC test

CREATE FUNCTION test_function()
RETURNS int
WITH NATIVE_COMPILATION, SCHEMABINDING 
AS BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = 'English')  
	RETURN 1
END

SELECT dbo.test_function()

CREATE FUNCTION dbo.test_tvf()
RETURNS TABLE
WITH NATIVE_COMPILATION, SCHEMABINDING 
AS 
	RETURN SELECT SalesOrderID, SalesOrderDetailID, OrderQty FROM dbo.SalesOrderDetail

SELECT * FROM dbo.test_tvf()

CREATE TRIGGER Insert_SalesOrderHeader
ON dbo.SalesOrderHeader
WITH NATIVE_COMPILATION, SCHEMABINDING
FOR INSERT, UPDATE, DELETE
AS BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = 'us_english')
	-- trigger code
END

DROP PROCEDURE test
DROP TABLE dbo.SalesOrderHeader

CREATE TABLE dbo.SalesOrderHeader(
SalesOrderID int PRIMARY KEY NONCLUSTERED NOT NULL,
RevisionNumber tinyint NOT NULL,
OrderDate datetime NOT NULL,
DueDate datetime NOT NULL,
ShipDate datetime NULL,
Status tinyint NOT NULL,
CustomerID int NOT NULL,
SalesPersonID int NULL,
TerritoryID int NULL,
BillToAddressID int NOT NULL,
ShipToAddressID int NOT NULL,
ShipMethodID int NOT NULL,
CreditCardID int NULL,
CreditCardApprovalCode varchar(15) NULL,
CurrencyRateID int NULL,
SubTotal money NOT NULL,
TaxAmt money NOT NULL,
Freight money NOT NULL,
Comment nvarchar(128) NULL,
ModifiedDate datetime NOT NULL
) WITH (MEMORY_OPTIMIZED = ON)

ALTER TABLE dbo.SalesOrderHeader
	ADD OnlineOrderFlag bit NOT NULL,
	PurchaseOrderNumber nvarchar(25) NULL,
	AccountNumber nvarchar(25) NULL

ALTER TABLE dbo.SalesOrderHeader  
ADD INDEX IX_ModifiedDate (ModifiedDate),
	INDEX IX_CustomerID HASH (CustomerID) WITH (BUCKET_COUNT = 10000)

ALTER TABLE dbo.SalesOrderHeader   
    ALTER INDEX IX_CustomerID  
    REBUILD WITH (BUCKET_COUNT = 100000)

ALTER TABLE dbo.SalesOrderHeader
	DROP COLUMN OnlineOrderFlag, PurchaseOrderNumber, AccountNumber

CREATE PROCEDURE dbo.test
WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS owner
AS
BEGIN ATOMIC
WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = 'us_english')
SELECT SubTotal, ModifiedDate FROM dbo.SalesOrderHeader   
END

-- returns error 5074
ALTER TABLE  dbo.SalesOrderHeader
	DROP COLUMN SubTotal

ALTER TABLE dbo.SalesOrderHeader
	DROP COLUMN TaxAmt

ALTER PROCEDURE dbo.test
WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS owner
AS
BEGIN ATOMIC
WITH (TRANSACTION ISOLATION LEVEL = REPEATABLE READ, LANGUAGE = 'us_english')
SELECT SubTotal FROM dbo.SalesOrderHeader   
END

SELECT * FROM sys.dm_os_loaded_modules  
WHERE description = 'XTP Native DLL'  

CREATE TABLE #temp (
	CustomerID int,
	Name varchar(40)
)
GO
CREATE TYPE test
AS TABLE (
	CustomerID int INDEX IX_CustomerID,
	Name varchar(40)
)
WITH (MEMORY_OPTIMIZED = ON)
GO
DECLARE @mytest test

SELECT * FROM @mytest
SELECT * FROM #temp

USE AdventureWorksDW2016
GO

CREATE TABLE dbo.FactResellerSalesNew(
	ProductKey int NOT NULL,
	OrderDateKey int NOT NULL,
	DueDateKey int NOT NULL,
	ShipDateKey int NOT NULL,
	ResellerKey int NOT NULL,
	EmployeeKey int NOT NULL,
	PromotionKey int NOT NULL,
	CurrencyKey int NOT NULL,
	SalesTerritoryKey int NOT NULL,
	SalesOrderNumber nvarchar(20) NOT NULL,
	SalesOrderLineNumber tinyint NOT NULL,
	RevisionNumber tinyint NULL,
	OrderQuantity smallint NULL,
	UnitPrice money NULL,
	ExtendedAmount money NULL,
	UnitPriceDiscountPct float NULL,
	DiscountAmount float NULL,
	ProductStandardCost money NULL,
	TotalProductCost money NULL,
	SalesAmount money NULL,
	TaxAmt money NULL,
	Freight money NULL,
	CarrierTrackingNumber nvarchar(25) NULL,
	CustomerPONumber nvarchar(25) NULL,
	OrderDate datetime NULL,
	DueDate datetime NULL,
	ShipDate datetime NULL,
	CONSTRAINT PK_SalesOrderNumber_SalesOrderLineNumber PRIMARY KEY CLUSTERED (
	SalesOrderNumber ASC,
	SalesOrderLineNumber ASC)
)

CREATE NONCLUSTERED COLUMNSTORE INDEX ncsi_FactResellerSalesNew ON dbo.FactResellerSalesNew (
	SalesOrderNumber, SalesOrderLineNumber, ProductKey, OrderDateKey, DueDateKey, ShipDateKey,
	ResellerKey, EmployeeKey, PromotionKey, CurrencyKey, SalesTerritoryKey, RevisionNumber,
	OrderQuantity, UnitPrice, ExtendedAmount, UnitPriceDiscountPct, DiscountAmount, ProductStandardCost,
	TotalProductCost, SalesAmount, TaxAmt, Freight, CarrierTrackingNumber, CustomerPONumber,
	OrderDate, DueDate, ShipDate)

INSERT INTO dbo.FactResellerSalesNew SELECT * FROM dbo.FactResellerSales

SELECT ProductKey, SUM(SalesAmount) FROM dbo.FactResellerSalesNew
GROUP BY ProductKey

-- returns error 35372
CREATE CLUSTERED COLUMNSTORE INDEX csi_FactResellerSalesNew ON dbo.FactResellerSalesNew

DROP INDEX dbo.FactResellerSalesNew.ncsi_FactResellerSalesNew

DROP TABLE FactResellerSalesNew

USE AdventureWorksDW2016
GO

CREATE TABLE dbo.FactResellerSalesNew(
	ProductKey int NOT NULL,
	OrderDateKey int NOT NULL,
	DueDateKey int NOT NULL,
	ShipDateKey int NOT NULL,
	ResellerKey int NOT NULL,
	EmployeeKey int NOT NULL,
	PromotionKey int NOT NULL,
	CurrencyKey int NOT NULL,
	SalesTerritoryKey int NOT NULL,
	SalesOrderNumber nvarchar(20) NOT NULL,
	SalesOrderLineNumber tinyint NOT NULL,
	RevisionNumber tinyint NULL,
	OrderQuantity smallint NULL,
	UnitPrice money NULL,
	ExtendedAmount money NULL,
	UnitPriceDiscountPct float NULL,
	DiscountAmount float NULL,
	ProductStandardCost money NULL,
	TotalProductCost money NULL,
	SalesAmount money NULL,
	TaxAmt money NULL,
	Freight money NULL,
	CarrierTrackingNumber nvarchar(25) NULL,
	CustomerPONumber nvarchar(25) NULL,
	OrderDate datetime NULL,
	DueDate datetime NULL,
	ShipDate datetime NULL)

CREATE CLUSTERED COLUMNSTORE INDEX csi_FactResellerSalesNew ON dbo.FactResellerSalesNew

DROP TABLE dbo.FactResellerSalesNew

CREATE TABLE dbo.FactResellerSalesNew (
	ProductKey int NOT NULL,
	OrderDateKey int NOT NULL,
	DueDateKey int NOT NULL,
	ShipDateKey int NOT NULL,
	ResellerKey int NOT NULL,
	EmployeeKey int NOT NULL,
	PromotionKey int NOT NULL,
	CurrencyKey int NOT NULL,
	SalesTerritoryKey int NOT NULL,
	INDEX csi_FactResellerSalesNew CLUSTERED COLUMNSTORE)

CREATE INDEX IX_ProductKey ON dbo.FactResellerSalesNew(ProductKey)

CREATE TABLE dbo.FactResellerSalesNew (
	ProductKey int NOT NULL,
	OrderDateKey int NOT NULL,
	DueDateKey int NOT NULL,
	ShipDateKey int NOT NULL,
	ResellerKey int NOT NULL,
	EmployeeKey int NOT NULL,
	PromotionKey int NOT NULL,
	CurrencyKey int NOT NULL,
	SalesTerritoryKey int NOT NULL)

SELECT * FROM sys.indexes 
WHERE object_id = OBJECT_ID('dbo.FactResellerSalesNew')

CREATE CLUSTERED INDEX IX_ProductKey ON dbo.FactResellerSalesNew (ProductKey)  
  
CREATE CLUSTERED COLUMNSTORE INDEX IX_ProductKey ON dbo.FactResellerSalesNew  
WITH (DROP_EXISTING = ON)

DROP INDEX dbo.FactResellerSalesNew.IX_ProductKey

CREATE CLUSTERED INDEX IX_ProductKey ON dbo.FactResellerSalesNew(ProductKey)
WITH (DROP_EXISTING = ON)

SELECT OBJECT_NAME(object_id), 
	100 * (ISNULL(deleted_rows,0)) / total_rows AS fragmentation, *  
FROM sys.dm_db_column_store_row_group_physical_stats

ALTER INDEX csi_FactResellerSalesNew ON dbo.FactResellerSalesNew REORGANIZE

ALTER INDEX csi_FactResellerSalesNew ON dbo.FactResellerSalesNew REBUILD

USE AdventureWorks2016
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX SalesOrderHeader_ncci ON Sales.SalesOrderHeader (
	SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate, Status, 
	OnlineOrderFlag, PurchaseOrderNumber, AccountNumber, CustomerID, SalesPersonID, 
	TerritoryID, BillToAddressID, ShipToAddressID, ShipMethodID, CreditCardID, 
	CreditCardApprovalCode, CurrencyRateID, SubTotal, TaxAmt, Freight, Comment,
 	ModifiedDate)

SELECT MAX(TotalDue)
FROM Sales.SalesOrderHeader

SELECT MAX(TotalDue)
FROM Sales.SalesOrderHeader
OPTION (IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX)

UPDATE Sales.SalesOrderHeader
SET Status = 1
WHERE ShipDate = '2014-07-07 00:00:00.000'

CREATE NONCLUSTERED COLUMNSTORE INDEX SalesOrderHeader_ncci ON Sales.SalesOrderHeader (
	SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate, Status, 
	OnlineOrderFlag, PurchaseOrderNumber, AccountNumber, CustomerID, SalesPersonID, 
	TerritoryID, BillToAddressID, ShipToAddressID, ShipMethodID, CreditCardID, 
	CreditCardApprovalCode, CurrencyRateID, SubTotal, TaxAmt, Freight, Comment, 
	ModifiedDate)
WHERE Status = 5

CREATE INDEX IX_Status ON Sales.SalesOrderHeader(Status)

SELECT SalesPersonID, SUM(TotalDue), AVG(TotalDue)
FROM Sales.SalesOrderHeader
WHERE TerritoryID > 3
GROUP BY SalesPersonID

DROP INDEX Sales.SalesOrderHeader.IX_Status
DROP INDEX Sales.SalesOrderHeader.SalesOrderHeader_ncci

UPDATE Sales.SalesOrderHeader
SET Status = 5
WHERE ShipDate = '2014-07-07 00:00:00.000'

CREATE TABLE SalesOrderHeader (
	SalesOrderID int PRIMARY KEY NONCLUSTERED NOT NULL,
	RevisionNumber tinyint NOT NULL,
	OrderDate datetime NOT NULL,
	DueDate datetime NOT NULL,
	ShipDate datetime NULL,
	Status tinyint NOT NULL,
	CustomerID int NOT NULL,
	SalesPersonID int NULL,
	TerritoryID int NULL,
	BillToAddressID int NOT NULL,
	ShipToAddressID int NOT NULL,
	ShipMethodID int NOT NULL,
	CreditCardID int NULL,
	CreditCardApprovalCode varchar(15) NULL,
	CurrencyRateID int NULL,
	SubTotal money NOT NULL,
	TaxAmt money NOT NULL,
	Freight money NOT NULL,
	Comment nvarchar(128) NULL,
	ModifiedDate datetime NOT NULL,
	INDEX SalesOrderHeader_cci CLUSTERED COLUMNSTORE)
WITH (MEMORY_OPTIMIZED = ON)
  
SELECT  
	SalesOrderID,
	RevisionNumber,
    OrderDate,
    DueDate,
    ShipDate,
    Status,
    CustomerID,
    SalesPersonID,
    TerritoryID,
    BillToAddressID,
    ShipToAddressID,
    ShipMethodID,
    CreditCardID,
    CreditCardApprovalCode,
    CurrencyRateID,
    SubTotal,
    TaxAmt,
    Freight,
    Comment,
    ModifiedDate
INTO #temp
FROM AdventureWorks2016.Sales.SalesOrderHeader

INSERT INTO dbo.SalesOrderHeader (
	SalesOrderID,
	RevisionNumber,
    OrderDate,
    DueDate,
    ShipDate,
    Status,
    CustomerID,
    SalesPersonID,
    TerritoryID,
    BillToAddressID,
    ShipToAddressID,
    ShipMethodID,
    CreditCardID,
    CreditCardApprovalCode,
    CurrencyRateID,
    SubTotal,
    TaxAmt,
    Freight,
    Comment,
    ModifiedDate)
SELECT * FROM #temp

SELECT MAX(SubTotal)
FROM dbo.SalesOrderHeader

SELECT SalesPersonID, SUM(SubTotal), AVG(SubTotal)
FROM dbo.SalesOrderHeader
WHERE TerritoryID > 3
GROUP BY SalesPersonID

CREATE TABLE SalesOrderHeader (
	SalesOrderID int PRIMARY KEY NONCLUSTERED NOT NULL,
	RevisionNumber tinyint NOT NULL,
	OrderDate datetime NOT NULL,
	DueDate datetime NOT NULL,
	ShipDate datetime NULL,
	Status tinyint NOT NULL,
	CustomerID int NOT NULL,
	SalesPersonID int NULL,
	TerritoryID int NULL,
	BillToAddressID int NOT NULL,
	ShipToAddressID int NOT NULL,
	ShipMethodID int NOT NULL,
	CreditCardID int NULL,
	CreditCardApprovalCode varchar(15) NULL,
	CurrencyRateID int NULL,
	SubTotal money NOT NULL,
	TaxAmt money NOT NULL,
	Freight money NOT NULL,
	Comment nvarchar(128) NULL,
	ModifiedDate datetime NOT NULL)
WITH (MEMORY_OPTIMIZED = ON)

ALTER TABLE SalesOrderHeader ADD INDEX SalesOrderHeader_cci CLUSTERED COLUMNSTORE
