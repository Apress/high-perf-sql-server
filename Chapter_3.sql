
-- chapter 3

SELECT * FROM sys.database_query_store_options

ALTER DATABASE AdventureWorks2016 SET QUERY_STORE = ON

ALTER DATABASE AdventureWorks2016 SET QUERY_STORE = ON
GO
ALTER DATABASE AdventureWorks2016 SET QUERY_STORE (OPERATION_MODE = READ_WRITE, 
	CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 367), DATA_FLUSH_INTERVAL_SECONDS = 900, INTERVAL_LENGTH_MINUTES = 60, 
	MAX_STORAGE_SIZE_MB = 100, QUERY_CAPTURE_MODE = ALL, SIZE_BASED_CLEANUP_MODE = OFF)
GO

SELECT actual_state_desc, desired_state_desc, current_storage_size_mb, 
    max_storage_size_mb, readonly_reason
FROM sys.database_query_store_options

ALTER DATABASE AdventureWorks2016 SET QUERY_STORE (MAX_STORAGE_SIZE_MB = 2048)

ALTER DATABASE AdventureWorks2016 SET QUERY_STORE CLEAR ALL

ALTER DATABASE AdventureWorks2016 SET QUERY_STORE = OFF

CREATE PROCEDURE test (@pid int)
AS
SELECT * FROM Sales.SalesOrderDetail
WHERE ProductID = @pid

SET STATISTICS IO ON
GO
ALTER DATABASE AdventureWorks2016 SET QUERY_STORE CLEAR ALL

EXEC test @pid = 870
 
EXEC test @pid = 898

DBCC FREEPROCCACHE
GO
EXEC test @pid = 898
GO
EXEC test @pid = 870

SELECT rs.avg_logical_io_reads, qt.query_sql_text, 
    q.query_id, qt.query_text_id, p.plan_id, rs.runtime_stats_id, 
    rsi.start_time, rsi.end_time, rs.avg_rowcount, rs.count_executions
FROM sys.query_store_query_text AS qt 
JOIN sys.query_store_query AS q 
    ON qt.query_text_id = q.query_text_id 
JOIN sys.query_store_plan AS p 
    ON q.query_id = p.query_id 
JOIN sys.query_store_runtime_stats AS rs 
    ON p.plan_id = rs.plan_id 
JOIN sys.query_store_runtime_stats_interval AS rsi 
    ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id

EXEC sys.sp_query_store_force_plan @query_id = 1, @plan_id = 1

EXEC sys.sp_query_store_unforce_plan @query_id = 1, @plan_id = 1

SELECT TOP 25
    p.query_id query_id,
    qt.query_sql_text query_text,
    CONVERT(float, SUM(rs.avg_cpu_time * rs.count_executions)) total_cpu_time,
    SUM(rs.count_executions) count_executions,
    COUNT(DISTINCT p.plan_id) num_plans
FROM sys.query_store_runtime_stats rs
    JOIN sys.query_store_plan p ON p.plan_id = rs.plan_id
    JOIN sys.query_store_query q ON q.query_id = p.query_id
    JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
GROUP BY p.query_id, qt.query_sql_text
ORDER BY total_cpu_time DESC

ALTER DATABASE AdventureWorks2016 SET QUERY_STORE CLEAR ALL

-- cancel after a few seconds
SELECT * FROM Sales.SalesOrderDetail s1 CROSS JOIN Sales.SalesOrderDetail s2

-- returns error 8115
SELECT COUNT(*) FROM Sales.SalesOrderDetail s1 CROSS JOIN Sales.SalesOrderDetail s2

SELECT COUNT_BIG(*) FROM Sales.SalesOrderDetail s1 CROSS JOIN Sales.SalesOrderDetail s2

SELECT rs.avg_logical_io_reads, qt.query_sql_text, 
    q.query_id, execution_type_desc, qt.query_text_id, p.plan_id, rs.runtime_stats_id, 
    rsi.start_time, rsi.end_time, rs.avg_rowcount, rs.count_executions
FROM sys.query_store_query_text AS qt 
JOIN sys.query_store_query AS q 
    ON qt.query_text_id = q.query_text_id 
JOIN sys.query_store_plan AS p 
    ON q.query_id = p.query_id 
JOIN sys.query_store_runtime_stats AS rs 
    ON p.plan_id = rs.plan_id 
JOIN sys.query_store_runtime_stats_interval AS rsi 
    ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id

-- returns error 208
SELECT * FROM authors

-- .NET code
using System;
using System.Data.SqlClient;

public class Test {
    
   public static void Main() {
      string connectionString = "Data Source=(local);Initial Catalog=AdventureWorks2016;Integrated Security=SSPI";

      string queryString = "SELECT * FROM Sales.SalesOrderDetail s1 CROSS JOIN Sales.SalesOrderDetail s2";
      using (SqlConnection connection = new SqlConnection(connectionString)) {
         connection.Open();
         SqlCommand command = new SqlCommand(queryString, connection);

         try {
            command.ExecuteNonQuery();
         }
         catch (SqlException e) {
            Console.WriteLine("Got expected SqlException due to command timeout ");
            Console.WriteLine(e);
         }
      }
   }
}

USE Test
CREATE TABLE Orders (SalesOrderID int)
INSERT INTO Orders VALUES (43659)

USE AdventureWorks2016
GO
ALTER DATABASE AdventureWorks2016 SET QUERY_STORE CLEAR ALL

ALTER INDEX IX_SalesOrderDetail_ProductID ON Sales.SalesOrderDetail REBUILD
GO
DBCC CHECKDB
GO
SELECT * FROM Sales.SalesOrderDetail
WHERE ProductID = 898
OPTION (RECOMPILE)
GO
BACKUP DATABASE AdventureWorks2016 TO  DISK = 'c:\data\delete_me.bak'
GO
SELECT * FROM Sales.SalesOrderDetail a JOIN Test.dbo.Orders b
ON a.SalesOrderID = b.SalesOrderID

ALTER DATABASE AdventureWorks2016 SET QUERY_STORE CLEAR ALL
GO
DBCC FREEPROCCACHE

EXEC test @pid = 898

SELECT plan_id, q.query_id, query_sql_text FROM sys.query_store_plan p 
JOIN sys.query_store_query AS q 
	ON p.query_id = q.query_id
JOIN sys.query_store_query_text qt 
	ON q.query_text_id = qt.query_text_id
WHERE query_sql_text LIKE '%SELECT * FROM Sales.SalesOrderDetail%'

EXEC sys.sp_query_store_force_plan @query_id = 1, @plan_id = 1

ALTER INDEX IX_SalesOrderDetail_ProductID ON Sales.SalesOrderDetail DISABLE

SELECT * FROM sys.query_store_plan
WHERE plan_id = 1

ALTER INDEX IX_SalesOrderDetail_ProductID ON Sales.SalesOrderDetail REBUILD

SELECT * FROM Sales.SalesOrderDetail s1 CROSS JOIN Sales.SalesOrderDetail s2

SET STATISTICS PROFILE ON

SELECT * FROM Sales.SalesOrderDetail s1 CROSS JOIN Sales.SalesOrderDetail s2

SELECT * FROM sys.dm_exec_query_profiles 
WHERE session_id = 55
