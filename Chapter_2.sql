
-- chapter 2

BEGIN TRANSACTION
UPDATE Sales.SalesOrderDetail
SET OrderQty = 5
WHERE SalesOrderDetailID = 121317

SELECT * FROM Sales.SalesOrderDetail
WHERE SalesOrderDetailID = 121317

SELECT * FROM Sales.SalesOrderDetail
ORDER BY OrderQty

SELECT * FROM sys.dm_exec_requests r JOIN sys.dm_exec_sessions s 
ON r.session_id = s.session_id
WHERE s.is_user_process = 1

ROLLBACK TRANSACTION

SELECT * FROM sys.dm_exec_requests
WHERE wait_type = 'REQUEST_FOR_DEADLOCK_SEARCH'

SELECT * FROM sys.dm_os_wait_stats
WHERE wait_type = 'REQUEST_FOR_DEADLOCK_SEARCH'

SELECT * FROM sys.dm_os_wait_stats

SELECT * FROM sys.dm_xe_map_values WHERE name = 'wait_types'

SELECT
	wait_type,
    wait_time_ms,
    wait_time_ms * 100.0 / SUM(wait_time_ms) OVER() AS percentage,
    signal_wait_time_ms * 100.0 / wait_time_ms as signal_pct
FROM sys.dm_os_wait_stats
WHERE wait_time_ms > 0
AND wait_type NOT IN (
	'BROKER_DISPATCHER', 'BROKER_EVENTHANDLER', 
	'BROKER_RECEIVE_WAITFOR', 'BROKER_TASK_STOP',
	'BROKER_TO_FLUSH', 'BROKER_TRANSMITTER',
	'CHECKPOINT_QUEUE', 'CHKPT',
	'CLR_AUTO_EVENT', 'CLR_MANUAL_EVENT',
	'CLR_SEMAPHORE', 'DBMIRROR_DBM_EVENT',
	'DBMIRROR_DBM_MUTEX', 'DBMIRROR_EVENTS_QUEUE',
    'DBMIRROR_WORKER_QUEUE', 'DBMIRRORING_CMD',
	'DIRTY_PAGE_POLL', 'DISPATCHER_QUEUE_SEMAPHORE',
    'EXECSYNC', 'FSAGENT',
    'FT_IFTS_SCHEDULER_IDLE_WAIT', 'FT_IFTSHC_MUTEX',
    'HADR_CLUSAPI_CALL', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
    'HADR_LOGCAPTURE_WAIT', 'HADR_NOTIFICATION_DEQUEUE',
	'HADR_TIMER_TASK', 'HADR_WORK_QUEUE',
	'KSOURCE_WAKEUP', 'LAZYWRITER_SLEEP',
	'LOGMGR_QUEUE', 'ONDEMAND_TASK_QUEUE',
	'PWAIT_ALL_COMPONENTS_INITIALIZED', 'QDS_ASYNC_QUEUE',
	'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', 'QDS_SHUTDOWN_QUEUE',
	'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', 'REQUEST_FOR_DEADLOCK_SEARCH',
	'RESOURCE_QUEUE', 'SERVER_IDLE_CHECK',
	'SLEEP_BPOOL_FLUSH', 'SLEEP_BUFFERPOOL_HELPLW',
	'SLEEP_DBSTARTUP', 'SLEEP_DCOMSTARTUP',
	'SLEEP_MASTERDBREADY', 'SLEEP_MASTERMDREADY',
	'SLEEP_MASTERUPGRADED', 'SLEEP_MSDBSTARTUP',
	'SLEEP_SYSTEMTASK', 'SLEEP_TASK',
	'SLEEP_TEMPDBSTARTUP', 'SLEEP_WORKSPACE_ALLOCATEPAGE',
	'SNI_HTTP_ACCEPT', 'SP_SERVER_DIAGNOSTICS_SLEEP',
	'SQLTRACE_BUFFER_FLUSH', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
	'SQLTRACE_WAIT_ENTRIES', 'WAIT_FOR_RESULTS',
	'WAITFOR', 'WAITFOR_TASKSHUTDOWN',
	'WAIT_XTP_HOST_WAIT', 'WAIT_XTP_OFFLINE_CKPT_NEW_LOG',
	'WAIT_XTP_CKPT_CLOSE', 'XE_DISPATCHER_JOIN',
	'XE_DISPATCHER_WAIT', 'XE_TIMER_EVENT')
ORDER BY percentage DESC

DBCC SQLPERF('sys.dm_os_wait_stats', CLEAR)

SELECT * FROM sys.dm_os_performance_counters
WHERE object_name = 'SQLServer:Wait Statistics'

SELECT * FROM sys.dm_os_performance_counters
WHERE object_name like '%Wait Statistics%'
 
SELECT * FROM sys.dm_exec_session_wait_stats
WHERE session_id = 52

CREATE EVENT SESSION waits ON SERVER
ADD EVENT sqlos.wait_info (
	WHERE (sqlserver.session_id = 58))
ADD TARGET package0.event_file
    (SET FILENAME = 'C:\data\waits.xel')

ALTER EVENT SESSION waits ON SERVER STATE = START
GO
USE AdventureWorks2016
GO
SELECT * FROM Sales.SalesOrderDetail
ORDER BY OrderQty
GO
ALTER EVENT SESSION waits ON SERVER STATE = STOP

SELECT *, CAST(event_data AS XML) AS 'event_data'
FROM sys.fn_xe_file_target_read_file('C:\data\waits*.xel', NULL, NULL, NULL)

CREATE TABLE #waits (
    event_data XML)
GO
INSERT INTO #waits (event_data)
SELECT CAST (event_data AS XML) AS event_data
FROM sys.fn_xe_file_target_read_file (
    'C:\data\waits*.xel', NULL, NULL, NULL)

SELECT
    waits.wait_type,
    COUNT (*) AS wait_count,
    SUM (waits.duration) AS total_wait_time_ms,
    SUM (waits.duration) - SUM (waits.signal_duration) AS total_resource_wait_time_ms,
    SUM (waits.signal_duration) AS total_signal_wait_time_ms
FROM 
    (SELECT
        event_data.value ('(/event/@timestamp)[1]', 'DATETIME') AS datetime,
        event_data.value ('(/event/data[@name=''wait_type'']/text)[1]', 'VARCHAR(100)') AS wait_type,
        event_data.value ('(/event/data[@name=''opcode'']/text)[1]', 'VARCHAR(100)') AS opcode,
        event_data.value ('(/event/data[@name=''duration'']/value)[1]', 'BIGINT') AS duration,
        event_data.value ('(/event/data[@name=''signal_duration'']/value)[1]', 'BIGINT') AS signal_duration
     FROM #waits
    ) AS waits
WHERE waits.opcode = 'End'
GROUP BY waits.wait_type
ORDER BY total_wait_time_ms DESC

DROP EVENT SESSION waits ON SERVER 

DROP TABLE #waits

-- partial code
ADD EVENT sqlos.wait_info(
    ACTION(package0.callstack,sqlserver.session_id,sqlserver.sql_text)
    WHERE ([duration]>(15000) AND ([wait_type]>=N'LATCH_NL' AND ([wait_type]>=N'PAGELATCH_NL' AND 
		[wait_type]<=N'PAGELATCH_DT' OR [wait_type]<=N'LATCH_DT' OR [wait_type]>=N'PAGEIOLATCH_NL' AND 
		[wait_type]<=N'PAGEIOLATCH_DT' OR [wait_type]>=N'IO_COMPLETION' AND [wait_type]<=N'NETWORK_IO' OR 
		[wait_type]=N'RESOURCE_SEMAPHORE' OR [wait_type]=N'SOS_WORKER' OR [wait_type]>=N'FCB_REPLICA_WRITE' AND 
		[wait_type]<=N'WRITELOG' OR [wait_type]=N'CMEMTHREAD' OR [wait_type]=N'TRACEWRITE' OR 
		[wait_type]=N'RESOURCE_SEMAPHORE_MUTEX') OR [duration]>(30000) AND [wait_type]<=N'LCK_M_RX_X'))),
ADD EVENT sqlos.wait_info_external(
    ACTION(package0.callstack,sqlserver.session_id,sqlserver.sql_text)
    WHERE ([duration]>(5000) AND ([wait_type]>=N'PREEMPTIVE_OS_GENERICOPS' AND [wait_type]<=N'PREEMPTIVE_OS_ENCRYPTMESSAGE' OR 
	[wait_type]>=N'PREEMPTIVE_OS_INITIALIZESECURITYCONTEXT' AND [wait_type]<=N'PREEMPTIVE_OS_QUERYSECURITYCONTEXTTOKEN' OR 
	[wait_type]>=N'PREEMPTIVE_OS_AUTHZGETINFORMATIONFROMCONTEXT' AND [wait_type]<=N'PREEMPTIVE_OS_REVERTTOSELF' OR 
	[wait_type]>=N'PREEMPTIVE_OS_CRYPTACQUIRECONTEXT' AND [wait_type]<=N'PREEMPTIVE_OS_DEVICEOPS' OR 
	[wait_type]>=N'PREEMPTIVE_OS_NETGROUPGETUSERS' AND [wait_type]<=N'PREEMPTIVE_OS_NETUSERMODALSGET' OR 
	[wait_type]>=N'PREEMPTIVE_OS_NETVALIDATEPASSWORDPOLICYFREE' AND [wait_type]<=N'PREEMPTIVE_OS_DOMAINSERVICESOPS' OR 
	[wait_type]=N'PREEMPTIVE_OS_VERIFYSIGNATURE' OR [duration]>(45000) AND ([wait_type]>=N'PREEMPTIVE_OS_SETNAMEDSECURITYINFO' AND 
	[wait_type]<=N'PREEMPTIVE_CLUSAPI_CLUSTERRESOURCECONTROL' OR [wait_type]>=N'PREEMPTIVE_OS_RSFXDEVICEOPS' AND 
	[wait_type]<=N'PREEMPTIVE_OS_DSGETDCNAME' OR [wait_type]>=N'PREEMPTIVE_OS_DTCOPS' AND [wait_type]<=N'PREEMPTIVE_DTC_ABORT' OR 
	[wait_type]>=N'PREEMPTIVE_OS_CLOSEHANDLE' AND [wait_type]<=N'PREEMPTIVE_OS_FINDFILE' OR 
	[wait_type]>=N'PREEMPTIVE_OS_GETCOMPRESSEDFILESIZE' AND [wait_type]<=N'PREEMPTIVE_ODBCOPS' OR 
	[wait_type]>=N'PREEMPTIVE_OS_DISCONNECTNAMEDPIPE' AND [wait_type]<=N'PREEMPTIVE_CLOSEBACKUPMEDIA' OR 
	[wait_type]=N'PREEMPTIVE_OS_AUTHENTICATIONOPS' OR [wait_type]=N'PREEMPTIVE_OS_FREECREDENTIALSHANDLE' OR 
	[wait_type]=N'PREEMPTIVE_OS_AUTHORIZATIONOPS' OR [wait_type]=N'PREEMPTIVE_COM_COCREATEINSTANCE' OR 
	[wait_type]=N'PREEMPTIVE_OS_NETVALIDATEPASSWORDPOLICY' OR [wait_type]=N'PREEMPTIVE_VSS_CREATESNAPSHOT'))))

-- partial code
ADD EVENT sqlserver.sp_server_diagnostics_component_result(SET collect_data=(1)
    WHERE ([sqlserver].[is_system]=(1) AND [component]<>(4)))

-- partial code
ADD TARGET package0.event_file(SET filename=N'system_health.xel',max_file_size=(5),max_rollover_files=(4)),
ADD TARGET package0.ring_buffer(SET max_events_limit=(5000),max_memory=(4096))

SELECT *, CAST(event_data AS XML)
FROM sys.fn_xe_file_target_read_file('system_health*.xel', NULL, NULL, NULL)

SELECT CAST(t.target_data AS xml) 
FROM sys.dm_xe_session_targets t
JOIN sys.dm_xe_sessions s
ON s.address = t.event_session_address
WHERE s.name = 'system_health'

SELECT * FROM Sales.SalesOrderDetail
ORDER BY OrderQty

BEGIN TRANSACTION
UPDATE Sales.SalesOrderDetail
SET OrderQty = 5
WHERE SalesOrderDetailID = 121317

SELECT * FROM sys.dm_exec_requests WHERE session_id = 52

SELECT * FROM sys.dm_os_waiting_tasks WHERE session_id = 52

ROLLBACK TRANSACTION

DBCC INPUTBUFFER(55)

SELECT * FROM sys.dm_exec_input_buffer(55, NULL)

SELECT * FROM sys.dm_os_spinlock_stats
ORDER BY spins DESC

SELECT map_value, map_key, name FROM sys.dm_xe_map_values
WHERE name = 'spinlock_types'

DBCC SQLPERF('sys.dm_os_latch_stats', CLEAR)

DBCC SQLPERF('sys.dm_os_spinlock_stats', CLEAR)

SELECT session_id, command, wait_type, wait_time FROM sys.dm_exec_requests
WHERE wait_type IN ('BAD_PAGE_PROCESS', 
'BROKER_EVENTHANDLER',
'BROKER_TRANSMITTER', 
'CHECKPOINT_QUEUE',
'DBMIRROR_EVENTS_QUEUE',
'DBMIRRORING_CMD',
'KSOURCE_WAKEUP',
'LAZYWRITER_SLEEP',
'LOGMGR_QUEUE',
'ONDEMAND_TASK_QUEUE',
'REQUEST_FOR_DEADLOCK_SEARCH',
'SQLTRACE_BUFFER_FLUSH',
'WAITFOR')

SELECT session_id, command, wait_type, wait_time, status FROM sys.dm_exec_requests
WHERE status = 'background'

sp_configure 'blocked process threshold', 5
RECONFIGURE

sp_configure 'blocked process threshold', 0
RECONFIGURE
