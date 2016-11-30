
-- chapter 7

CREATE TABLE t1 (id int IDENTITY(1,1), name varchar(20))

BEGIN
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
END
GO 10000

DROP TABLE t1

CREATE DATABASE test

CREATE TABLE t1 (id int IDENTITY(1,1), name char(8000))

BEGIN
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
INSERT INTO t1 VALUES ('Hello')
END
GO 30000

SELECT TOP 20 qstats.query_hash,     
	SUM(qstats.total_worker_time) / SUM(qstats.execution_count) AS avg_total_worker_time,     
	MIN(qstats.statement_text) AS statement_text 
FROM (SELECT qs.*, SUBSTRING(st.text, (qs.statement_start_offset/2) + 1, 
	((CASE statement_end_offset         
	WHEN -1 THEN DATALENGTH(ST.text)         
	ELSE qs.statement_end_offset END             
	- qs.statement_start_offset)/2) + 1) AS statement_text      
FROM sys.dm_exec_query_stats qs      
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st) AS qstats 
GROUP BY qstats.query_hash 
ORDER BY avg_total_worker_time DESC 

SELECT TOP 20 SUBSTRING(t.text, (r.statement_start_offset/2) + 1,     
	((CASE statement_end_offset 
	WHEN -1 THEN DATALENGTH(t.text) 
	ELSE r.statement_end_offset END 
	- r.statement_start_offset)/2) + 1) AS statement_text, * 
FROM sys.dm_exec_requests r 
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t 
ORDER BY cpu_time DESC

SELECT * FROM sys.dm_io_virtual_file_stats(DB_ID('AdventureWorks2016'), NULL)

SELECT DB_NAME(mf.database_id), physical_name, fs.* 
FROM sys.dm_io_virtual_file_stats(NULL, NULL) fs
JOIN sys.master_files mf 
ON mf.database_id = fs.database_id 
AND mf.file_id = fs.file_id

SELECT * FROM sys.dm_os_volume_stats (DB_ID('AdventureWorks2016'), 1)

SELECT OBJECT_NAME(object_id), * FROM sys.dm_db_index_usage_stats
WHERE database_id = DB_ID()
ORDER BY object_id, index_id

SELECT OBJECT_NAME(s.object_id) AS object_name, i.name, s.* 
FROM sys.dm_db_index_usage_stats s JOIN sys.indexes i     
ON s.object_id = i.object_id AND s.index_id = i.index_id 
WHERE database_id = DB_ID()
ORDER BY object_id, index_id

SELECT *
FROM sys.dm_db_index_physical_stats (DB_ID(), NULL, NULL , NULL, 'LIMITED')  
WHERE avg_fragmentation_in_percent > 10

-- partial code
IF avg_fragmentation_in_percent < 30.0  
SET @command = 'ALTER INDEX ' + ... + ' REORGANIZE'
IF avg_fragmentation_in_percent >= 30.0  
SET @command = 'ALTER INDEX ' + ... + ' REBUILD'  
EXEC (@command)

SELECT name, description 
FROM sys.dm_xe_objects 
WHERE object_type = 'event' 
ORDER BY name

SELECT * FROM Sales.SalesOrderDetail
