
-- chapter 5

DBCC TRACEON(3604) 
DBCC PAGE('tempdb', 1, 1, 0)

SELECT * from sys.dm_os_waiting_tasks
WHERE wait_type LIKE 'PAGE%LATCH%'
AND resource_description LIKE '2:%'

SELECT * FROM sys.dm_exec_requests
WHERE wait_type LIKE 'PAGE%LATCH%' 
AND wait_resource LIKE '2:%'

SELECT * FROM sysprocesses
WHERE lastwaittype LIKE 'PAGE%LATCH%' 
AND waitresource LIKE '2:%'

DBCC TRACEON (1106, -1)

DECLARE @ts_now bigint;
SELECT @ts_now=cpu_ticks/(cpu_ticks/ms_ticks)
FROM sys.dm_os_sys_info;
SELECT record_id,
DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) as event_time,
CASE
WHEN event = 0 THEN 'Allocation Cache Init'
WHEN event = 1 THEN 'Allocation Cache Add Entry'
WHEN event = 2 THEN 'Allocation Cache RMV Entry'
WHEN event = 3 THEN 'Allocation Cache Reinit'
WHEN event = 4 THEN 'Allocation Cache Free'
WHEN event = 5 THEN 'Truncate Allocation Unit'
WHEN event = 10 THEN 'PFS Alloc Page'
WHEN event = 11 THEN 'PFS Dealloc Page'
WHEN event = 20 THEN 'IAM Set Bit'
WHEN event = 21 THEN 'IAM Clear Bit'
WHEN event = 22 THEN 'GAM Set Bit'
WHEN event = 23 THEN 'GAM Clear Bit'
WHEN event = 24 THEN 'SGAM Set Bit'
WHEN event = 25 THEN 'SGAM Clear Bit'
WHEN event = 26 THEN 'SGAM Set Bit NX'
WHEN event = 27 THEN 'SGAM Clear Bit NX'
WHEN event = 28 THEN 'GAM Zap Extent'
WHEN event = 40 THEN 'Format IAM Page'
WHEN event = 41 THEN 'Format Page'
WHEN event = 42 THEN 'Reassign IAM Page'
WHEN event = 50 THEN 'Worktable Cache Add IAM'
WHEN event = 51 THEN 'Worktable Cache Add Page'
WHEN event = 52 THEN 'Worktable Cache RMV IAM'
WHEN event = 53 THEN 'Worktable Cache RMV Page'
WHEN event = 61 THEN 'IAM Cache Destroy'
WHEN event = 62 THEN 'IAM Cache Add Page'
WHEN event = 63 THEN 'IAM Cache Refresh Requested'
ELSE 'Unknown Event'
END as event_name,
session_id as s_id,
page_id,
allocation_unit_id as au_id
FROM
(
SELECT
xml_record.value('(./Record/@id)[1]', 'int') AS record_id,
xml_record.value('(./Record/SpaceMgr/Event)[1]', 'int') AS event,
xml_record.value('(./Record/SpaceMgr/SpId)[1]', 'int') AS session_id,
xml_record.value('(./Record/SpaceMgr/PageId)[1]', 'varchar(100)') AS page_id,
xml_record.value('(./Record/SpaceMgr/AuId)[1]', 'varchar(100)') AS allocation_unit_id,
timestamp
FROM
(
SELECT timestamp, convert(xml, record) as xml_record
FROM sys.dm_os_ring_buffers
WHERE ring_buffer_type = N'RING_BUFFER_SPACEMGR_TRACE'
) AS the_record
) AS ring_buffer_record
-- WHERE session_id = 52
ORDER BY record_id

CREATE TABLE #temp (c1 int, c2 varchar(20), c3 datetime)
INSERT INTO #temp VALUES (1, 'test', GETDATE())
DROP TABLE #temp

DBCC TRACEOFF (1106, -1)

DECLARE @ts_now bigint;
SELECT @ts_now=cpu_ticks/(cpu_ticks/ms_ticks)
FROM sys.dm_os_sys_info;
SELECT record_id,
DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) as event_time,
CASE
WHEN event = 0 THEN 'Allocation Cache Init'
WHEN event = 1 THEN 'Allocation Cache Add Entry'
WHEN event = 2 THEN 'Allocation Cache RMV Entry'
WHEN event = 3 THEN 'Allocation Cache Reinit'
WHEN event = 4 THEN 'Allocation Cache Free'
WHEN event = 5 THEN 'Truncate Allocation Unit'
WHEN event = 10 THEN 'PFS Alloc Page'
WHEN event = 11 THEN 'PFS Dealloc Page'
WHEN event = 20 THEN 'IAM Set Bit'
WHEN event = 21 THEN 'IAM Clear Bit'
WHEN event = 22 THEN 'GAM Set Bit'
WHEN event = 23 THEN 'GAM Clear Bit'
WHEN event = 24 THEN 'SGAM Set Bit'
WHEN event = 25 THEN 'SGAM Clear Bit'
WHEN event = 26 THEN 'SGAM Set Bit NX'
WHEN event = 27 THEN 'SGAM Clear Bit NX'
WHEN event = 28 THEN 'GAM Zap Extent'
WHEN event = 40 THEN 'Format IAM Page'
WHEN event = 41 THEN 'Format Page'
WHEN event = 42 THEN 'Reassign IAM Page'
WHEN event = 50 THEN 'Worktable Cache Add IAM'
WHEN event = 51 THEN 'Worktable Cache Add Page'
WHEN event = 52 THEN 'Worktable Cache RMV IAM'
WHEN event = 53 THEN 'Worktable Cache RMV Page'
WHEN event = 61 THEN 'IAM Cache Destroy'
WHEN event = 62 THEN 'IAM Cache Add Page'
WHEN event = 63 THEN 'IAM Cache Refresh Requested'
ELSE 'Unknown Event'
END as event_name,
session_id as s_id,
page_id,
allocation_unit_id as au_id
FROM
(
SELECT
xml_record.value('(./Record/@id)[1]', 'int') AS record_id,
xml_record.value('(./Record/ALLOC/Event)[1]', 'int') AS event,
xml_record.value('(./Record/ALLOC/SpId)[1]', 'int') AS session_id,
xml_record.value('(./Record/ALLOC/PageId)[1]', 'varchar(100)') AS page_id,
xml_record.value('(./Record/ALLOC/AuId)[1]', 'varchar(100)') AS allocation_unit_id,
timestamp
FROM
(
SELECT timestamp, convert(xml, record) as xml_record
FROM sys.dm_os_ring_buffers
WHERE ring_buffer_type = N'RING_BUFFER_ALLOC_TRACE'
) AS the_record
) AS ring_buffer_record
-- WHERE session_id = 52
ORDER BY record_id

CREATE EVENT SESSION [test] ON SERVER 
ADD EVENT sqlserver.latch_suspend_begin (
    WHERE (database_id = 2 AND (page_id = 1 OR page_id =3))
),
ADD EVENT sqlserver.latch_suspend_end (
    WHERE (database_id = 2 AND (page_id = 1 OR page_id =3))
),
ADD EVENT sqlserver.latch_suspend_warning (
    WHERE (database_id = 2 AND (page_id = 1 OR page_id =3))
)
GO

ALTER EVENT SESSION [test] ON SERVER 
STATE = START
GO

ALTER EVENT SESSION [test] ON SERVER 
STATE = STOP
GO
DROP EVENT SESSION [test] ON SERVER 
GO

USE tempdb
GO
SELECT
SUM (user_object_reserved_page_count) * 1.0 / 128 as user_object_mb,
SUM (internal_object_reserved_page_count) * 1.0 / 128 as internal_objects_mb,
SUM (version_store_reserved_page_count) * 1.0 / 128  as version_store_mb,
SUM (unallocated_extent_page_count) * 1.0 / 128 as free_space_mb,
SUM (mixed_extent_page_count) * 1.0 / 128 as mixed_extents_mb
FROM sys.dm_db_file_space_usage

SELECT t1.session_id, t1.request_id, t1.task_alloc,
  t1.task_dealloc, t2.sql_handle, t2.statement_start_offset, 
  t2.statement_end_offset, t2.plan_handle
FROM (SELECT session_id, request_id,
    SUM(internal_objects_alloc_page_count) AS task_alloc,
    SUM (internal_objects_dealloc_page_count) AS task_dealloc
  FROM sys.dm_db_task_space_usage 
  GROUP BY session_id, request_id) AS t1, 
  sys.dm_exec_requests AS t2
WHERE t1.session_id = t2.session_id
  AND (t1.request_id = t2.request_id)
ORDER BY t1.task_alloc DESC
