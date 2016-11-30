
-- chapter 1

SELECT max_workers_count FROM sys.dm_os_sys_info
SELECT SUM(active_workers_count) FROM sys.dm_os_schedulers

EXEC sp_configure 'lightweight pooling', 1
RECONFIGURE

EXEC sp_configure 'lightweight pooling', 0
RECONFIGURE

SELECT timestamp, CONVERT(xml, record) AS record
FROM sys.dm_os_ring_buffers
WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR'

SELECT ProductID, COUNT(*) 
FROM Sales.SalesOrderHeader so JOIN Sales.SalesOrderDetail sod
ON so.SalesOrderID = sod.SalesOrderID
WHERE SalesPersonID = 275
GROUP BY ProductID

SELECT TOP(20) *
FROM Sales.SalesOrderDetail

SELECT * FROM sys.dm_exec_query_memory_grants WHERE grant_time IS NULL

SELECT * FROM sys.dm_os_wait_stats
WHERE wait_type = 'RESOURCE_SEMAPHORE'

SELECT * FROM sys.dm_os_wait_stats
WHERE wait_type LIKE '%LATCH_%'
