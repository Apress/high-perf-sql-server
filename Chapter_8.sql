
-- chapter 8

SELECT * FROM Sales.SalesOrderDetail
WHERE ProductID = 898

SELECT * FROM Sales.SalesOrderDetail
WHERE ABS(ProductID) = 898

SELECT * 
INTO dbo.SalesOrderDetail
FROM AdventureWorks2016.Sales.SalesOrderDetail
GO
SELECT * 
INTO dbo.SalesOrderHeader
FROM AdventureWorks2016.Sales.SalesOrderHeader

SELECT * FROM SalesOrderHeader
WHERE SalesOrderID BETWEEN 50000 AND 70000

SELECT * FROM SalesOrderHeader
WHERE SalesOrderID BETWEEN 50000 AND 70000
OPTION (QUERYTRACEON 8757)

CREATE CLUSTERED INDEX PK_SalesOrderHeader ON SalesOrderHeader(SalesOrderID)
GO
CREATE CLUSTERED INDEX PK_SalesOrderDetail 
ON SalesOrderDetail(SalesOrderID, SalesOrderDetailID)

SELECT * FROM SalesOrderDetail
WHERE ProductID = 898

SELECT * FROM SalesOrderDetail
WHERE ProductID = 898
OPTION (QUERYTRACEON 8757)

-- generated code
CREATE NONCLUSTERED INDEX [<Name of Missing Index, sysname,>]
ON [dbo].[SalesOrderDetail] ([ProductID])
INCLUDE ([SalesOrderID],[SalesOrderDetailID],[CarrierTrackingNumber],[OrderQty],[SpecialOfferID],
[UnitPrice],[UnitPriceDiscount],[LineTotal],[rowguid],[ModifiedDate])

CREATE NONCLUSTERED INDEX IX_ProductID
ON SalesOrderDetail (ProductID)

SELECT * FROM SalesOrderDetail
WHERE ProductID = 898

SELECT * FROM SalesOrderDetail
WHERE ProductID = 870

SELECT * FROM SalesOrderDetail
WITH (INDEX (IX_ProductID))
WHERE ProductID = 870

SELECT SalesOrderID, SalesOrderDetailID, ProductID FROM SalesOrderDetail
WHERE ProductID = 870

SELECT SalesOrderID, SalesOrderDetailID, ProductID, UnitPrice FROM SalesOrderDetail
WHERE ProductID = 870

-- generated code
CREATE NONCLUSTERED INDEX [<Name of Missing Index, sysname,>]
ON [dbo].[SalesOrderDetail] ([ProductID])
INCLUDE ([SalesOrderID],[SalesOrderDetailID],[UnitPrice])

DROP INDEX SalesOrderDetail.IX_ProductID
GO
CREATE NONCLUSTERED INDEX IX_ProductID
ON SalesOrderDetail (ProductID)
INCLUDE (UnitPrice)

DROP INDEX SalesOrderDetail.IX_ProductID

SELECT SalesOrderID, SalesOrderDetailID, ProductID FROM SalesOrderDetail
ORDER BY ProductID

CREATE NONCLUSTERED INDEX IX_ProductID
ON SalesOrderDetail (ProductID)

SELECT ProductID, COUNT(*) FROM SalesOrderDetail
GROUP BY ProductID

SELECT * FROM SalesOrderDetail d
JOIN SalesOrderHeader h
ON d.SalesOrderID = h.SalesOrderID
WHERE ProductID = 898

SELECT * FROM SalesOrderDetail d
JOIN SalesOrderHeader h
ON d.SalesOrderID = h.SalesOrderID
WHERE ProductID = 870

SELECT * FROM SalesOrderDetail
WITH (INDEX (IX_ProductID))
WHERE ProductID = 870

DROP INDEX SalesOrderDetail.IX_ProductID

SELECT name, index_depth, index_level, page_count, record_count
FROM sys.dm_db_index_physical_stats(DB_ID (),
	OBJECT_ID ('Sales.SalesOrderDetail'), null, null, null) s
JOIN sys.indexes i
ON s.index_id = i.index_id
WHERE name in ('IX_SalesOrderDetail_ProductID', 
	'PK_SalesOrderDetail_SalesOrderID_SalesOrderDetailID')

SELECT INDEXPROPERTY(OBJECT_ID('Sales.SalesOrderDetail'), 
	'IX_SalesOrderDetail_ProductID', 'IndexDepth')
SELECT INDEXPROPERTY(OBJECT_ID('Sales.SalesOrderDetail'), 
	'PK_SalesOrderDetail_SalesOrderID_SalesOrderDetailID', 'IndexDepth')

SET STATISTICS IO ON

SELECT * FROM Sales.SalesOrderDetail
WHERE SalesOrderID = 43659 AND SalesOrderDetailID = 1

SELECT ProductID FROM Sales.SalesOrderDetail
WHERE ProductID = 898

SELECT * 
INTO dbo.SalesOrderDetail
FROM AdventureWorks2016.Sales.SalesOrderDetail
GO
SELECT * 
INTO dbo.SalesOrderHeader
FROM AdventureWorks2016.Sales.SalesOrderHeader

SELECT * FROM SalesOrderDetail
WHERE ProductID = 898
OPTION (QUERYTRACEON 8757)

SELECT g.*, statement, column_id, column_name, column_usage  
FROM sys.dm_db_missing_index_details AS d  
CROSS APPLY sys.dm_db_missing_index_columns(d.index_handle)  
INNER JOIN sys.dm_db_missing_index_groups AS g 
	ON g.index_handle = d.index_handle  
WHERE d.database_id = DB_ID()
ORDER BY g.index_group_handle, g.index_handle, column_id

SELECT d.*, s.*  
FROM sys.dm_db_missing_index_group_stats AS s  
INNER JOIN sys.dm_db_missing_index_groups AS g  
    ON s.group_handle = g.index_group_handle  
INNER JOIN sys.dm_db_missing_index_details AS d  
    ON g.index_handle = d.index_handle  
WHERE d.database_id = DB_ID()

SELECT * FROM SalesOrderDetail
WHERE OrderQty = 1
OPTION (QUERYTRACEON 8757)

SELECT * 
INTO dbo.SalesOrderDetail
FROM AdventureWorks2016.Sales.SalesOrderDetail
GO
SELECT * 
INTO dbo.SalesOrderHeader
FROM AdventureWorks2016.Sales.SalesOrderHeader

SELECT * FROM SalesOrderHeader
WHERE SalesOrderID BETWEEN 50000 AND 70000
GO
SELECT * FROM SalesOrderDetail
WHERE ProductID = 898
GO
SELECT * FROM SalesOrderDetail
WHERE ProductID = 870
GO
SELECT SalesOrderID, SalesOrderDetailID, ProductID FROM SalesOrderDetail
WHERE ProductID = 870
GO
SELECT SalesOrderID, SalesOrderDetailID, ProductID, UnitPrice FROM SalesOrderDetail
WHERE ProductID = 870
GO
SELECT SalesOrderID, SalesOrderDetailID, ProductID FROM SalesOrderDetail
ORDER BY ProductID
GO
SELECT ProductID, COUNT(*) FROM SalesOrderDetail
GROUP BY ProductID
GO
SELECT * FROM SalesOrderDetail d
JOIN SalesOrderHeader h
ON d.SalesOrderID = h.SalesOrderID
WHERE ProductID = 898
GO
SELECT * FROM SalesOrderDetail d
JOIN SalesOrderHeader h
ON d.SalesOrderID = h.SalesOrderID
WHERE ProductID = 870
GO

-- generated code
CREATE STATISTICS [_dta_stat_565577053_1_5] ON [dbo].[SalesOrderDetail]([SalesOrderID], [ProductID])

CREATE NONCLUSTERED INDEX [_dta_index_SalesOrderDetail_9_565577053__K5_1_2] ON [dbo].[SalesOrderDetail]
(
	[ProductID] ASC
)
INCLUDE ([SalesOrderID],
	[SalesOrderDetailID]) WITH (SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]

SET ANSI_PADDING ON

CREATE NONCLUSTERED INDEX [_dta_index_SalesOrderDetail_9_565577053__K5_K1_2_3_4_6_7_8_9_10_11] ON [dbo].[SalesOrderDetail]
(
	[ProductID] ASC,
	[SalesOrderID] ASC
)
INCLUDE ([SalesOrderDetailID],
	[CarrierTrackingNumber],
	[OrderQty],
	[SpecialOfferID],
	[UnitPrice],
	[UnitPriceDiscount],
	[LineTotal],
	[rowguid],
	[ModifiedDate]) WITH (SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]

CREATE CLUSTERED INDEX [_dta_index_SalesOrderHeader_c_9_581577110__K1] ON [dbo].[SalesOrderHeader]
(
	[SalesOrderID] ASC
)WITH (SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF) ON [PRIMARY]

DBCC FREEPROCCACHE
GO
SELECT * FROM SalesOrderHeader
WHERE SalesOrderID BETWEEN 50000 AND 70000
GO
SELECT * FROM SalesOrderDetail
WHERE ProductID = 898
GO
SELECT * FROM SalesOrderDetail
WHERE ProductID = 870
GO
SELECT SalesOrderID, SalesOrderDetailID, ProductID FROM SalesOrderDetail
WHERE ProductID = 870
GO
SELECT SalesOrderID, SalesOrderDetailID, ProductID, UnitPrice FROM SalesOrderDetail
WHERE ProductID = 870
GO
SELECT SalesOrderID, SalesOrderDetailID, ProductID FROM SalesOrderDetail
ORDER BY ProductID
GO
SELECT ProductID, COUNT(*) FROM SalesOrderDetail
GROUP BY ProductID
GO
SELECT * FROM SalesOrderDetail d
JOIN SalesOrderHeader h
ON d.SalesOrderID = h.SalesOrderID
WHERE ProductID = 898
GO
SELECT * FROM SalesOrderDetail d
JOIN SalesOrderHeader h
ON d.SalesOrderID = h.SalesOrderID
WHERE ProductID = 870
GO

select top 1000 isnull(st.objectid,0), 
	isnull(st.dbid,0), avg(cp.execution_count), st.text  
from sys.dm_exec_query_stats cp                         
cross apply sys.dm_exec_sql_text(cp.plan_handle) st 						
where cp.creation_time < N'2016-08-08 03:47:06.273' and st.dbid in (  9  )                          
group by st.text,st.objectid,st.dbid                         
order by sum(total_elapsed_time) desc
