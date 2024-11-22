// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sqlserverreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/sqlserverreceiver"

import (
	"fmt"
	"strings"
)

// Direct access to queries is not recommended: The receiver allows filtering based on
// instance name, which means the query will change based on configuration.
// Please use getSQLServerDatabaseIOQuery
const sqlServerDatabaseIOQuery = `
SET DEADLOCK_PRIORITY -10;
IF SERVERPROPERTY('EngineEdition') NOT IN (2,3,4) BEGIN /*NOT IN Standard,Enterprise,Express*/
	DECLARE @ErrorMessage AS nvarchar(500) = 'Connection string Server:'+ @@ServerName + ',Database:' + DB_NAME() +' is not a SQL Server Standard,Enterprise or Express. This query is only supported on these editions.';
	RAISERROR (@ErrorMessage,11,1)
	RETURN
END

DECLARE
	 @SqlStatement AS nvarchar(max)
	,@MajorMinorVersion AS int = CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar),4) AS int) * 100 + CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar),3) AS int)
	,@Columns AS nvarchar(max) = ''
	,@Tables AS nvarchar(max) = ''
IF @MajorMinorVersion > 1100 BEGIN
	SET @Columns += N'
	,vfs.[io_stall_queued_read_ms] AS [rg_read_stall_ms]
	,vfs.[io_stall_queued_write_ms] AS [rg_write_stall_ms]'
END

SET @SqlStatement = N'
SELECT
	''sqlserver_database_io'' AS [measurement]
	,REPLACE(@@SERVERNAME,''\'','':'') AS [sql_instance]
	,HOST_NAME() AS [computer_name]
	,DB_NAME(vfs.[database_id]) AS [database_name]
	,COALESCE(mf.[physical_name],''RBPEX'') AS [physical_filename]	--RPBEX = Resilient Buffer Pool Extension
	,COALESCE(mf.[name],''RBPEX'') AS [logical_filename]	--RPBEX = Resilient Buffer Pool Extension
	,mf.[type_desc] AS [file_type]
	,vfs.[io_stall_read_ms] AS [read_latency_ms]
	,vfs.[num_of_reads] AS [reads]
	,vfs.[num_of_bytes_read] AS [read_bytes]
	,vfs.[io_stall_write_ms] AS [write_latency_ms]
	,vfs.[num_of_writes] AS [writes]
	,vfs.[num_of_bytes_written] AS [write_bytes]'
	+ @Columns + N'
FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS vfs
INNER JOIN sys.master_files AS mf WITH (NOLOCK)
	ON vfs.[database_id] = mf.[database_id] AND vfs.[file_id] = mf.[file_id]
%s'
+ @Tables;

EXEC sp_executesql @SqlStatement
`

func getSQLServerDatabaseIOQuery(instanceName string) string {
	if instanceName != "" {
		whereClause := fmt.Sprintf("WHERE @@SERVERNAME = ''%s''", instanceName)
		return fmt.Sprintf(sqlServerDatabaseIOQuery, whereClause)
	}

	return fmt.Sprintf(sqlServerDatabaseIOQuery, "")
}

const sqlServerPerformanceCountersQuery string = `
SET DEADLOCK_PRIORITY -10;
IF SERVERPROPERTY('EngineEdition') NOT IN (2,3,4) BEGIN /*NOT IN Standard,Enterprise,Express*/
	DECLARE @ErrorMessage AS nvarchar(500) = 'Connection string Server:'+ @@ServerName + ',Database:' + DB_NAME() +' is not a SQL Server Standard, Enterprise or Express. This query is only supported on these editions.';
	RAISERROR (@ErrorMessage,11,1)
	RETURN
END

DECLARE
	 @SqlStatement AS nvarchar(max)
	,@MajorMinorVersion AS int = CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar),4) AS int)*100 + CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar),3) AS int)

DECLARE @PCounters TABLE
(
	 [object_name] nvarchar(128)
	,[counter_name] nvarchar(128)
	,[instance_name] nvarchar(128)
	,[cntr_value] bigint
	,[cntr_type] int
	PRIMARY KEY([object_name], [counter_name], [instance_name])
);

WITH PerfCounters AS (
SELECT DISTINCT
	 RTRIM(spi.[object_name]) [object_name]
	,RTRIM(spi.[counter_name]) [counter_name]
	,RTRIM(spi.[instance_name]) AS [instance_name]
	,CAST(spi.[cntr_value] AS bigint) AS [cntr_value]
	,spi.[cntr_type]
	FROM sys.dm_os_performance_counters AS spi
	WHERE
		counter_name IN (
			 'SQL Compilations/sec'
			,'SQL Re-Compilations/sec'
			,'User Connections'
			,'Batch Requests/sec'
			,'Logouts/sec'
			,'Logins/sec'
			,'Processes blocked'
			,'Latch Waits/sec'
			,'Average Latch Wait Time (ms)'
			,'Full Scans/sec'
			,'Index Searches/sec'
			,'Page Splits/sec'
			,'Page lookups/sec'
			,'Page reads/sec'
			,'Page writes/sec'
			,'Readahead pages/sec'
			,'Lazy writes/sec'
			,'Checkpoint pages/sec'
			,'Table Lock Escalations/sec'
			,'Page life expectancy'
			,'Log File(s) Size (KB)'
			,'Log File(s) Used Size (KB)'
			,'Data File(s) Size (KB)'
			,'Transactions/sec'
			,'Write Transactions/sec'
			,'Active Transactions'
			,'Log Growths'
			,'Active Temp Tables'
			,'Logical Connections'
			,'Temp Tables Creation Rate'
			,'Temp Tables For Destruction'
			,'Free Space in tempdb (KB)'
			,'Version Store Size (KB)'
			,'Memory Grants Pending'
			,'Memory Grants Outstanding'
			,'Free list stalls/sec'
			,'Buffer cache hit ratio'
			,'Buffer cache hit ratio base'
			,'Database Pages'
			,'Backup/Restore Throughput/sec'
			,'Total Server Memory (KB)'
			,'Target Server Memory (KB)'
			,'Log Flushes/sec'
			,'Log Flush Wait Time'
			,'Memory broker clerk size'
			,'Log Bytes Flushed/sec'
			,'Bytes Sent to Replica/sec'
			,'Log Send Queue'
			,'Bytes Sent to Transport/sec'
			,'Sends to Replica/sec'
			,'Bytes Sent to Transport/sec'
			,'Sends to Transport/sec'
			,'Bytes Received from Replica/sec'
			,'Receives from Replica/sec'
			,'Flow Control Time (ms/sec)'
			,'Flow Control/sec'
			,'Resent Messages/sec'
			,'Redone Bytes/sec'
			,'XTP Memory Used (KB)'
			,'Transaction Delay'
			,'Log Bytes Received/sec'
			,'Log Apply Pending Queue'
			,'Redone Bytes/sec'
			,'Recovery Queue'
			,'Log Apply Ready Queue'
			,'CPU usage %'
			,'CPU usage % base'
			,'Queued requests'
			,'Requests completed/sec'
			,'Blocked tasks'
			,'Active memory grant amount (KB)'
			,'Disk Read Bytes/sec'
			,'Disk Read IO Throttled/sec'
			,'Disk Read IO/sec'
			,'Disk Write Bytes/sec'
			,'Disk Write IO Throttled/sec'
			,'Disk Write IO/sec'
			,'Used memory (KB)'
			,'Forwarded Records/sec'
			,'Background Writer pages/sec'
			,'Percent Log Used'
			,'Log Send Queue KB'
			,'Redo Queue KB'
			,'Mirrored Write Transactions/sec'
			,'Group Commit Time'
			,'Group Commits/Sec'
			,'Workfiles Created/sec'
			,'Worktables Created/sec'
			,'Distributed Query'
			,'DTC calls'
			,'Query Store CPU usage'
			,'Query Store physical reads'
			,'Query Store logical reads'
			,'Query Store logical writes'
		) OR (
			spi.[object_name] LIKE '%User Settable%'
			OR spi.[object_name] LIKE '%SQL Errors%'
			OR spi.[object_name] LIKE '%Batch Resp Statistics%'
		) OR (
			spi.[instance_name] IN ('_Total')
			AND spi.[counter_name] IN (
				 'Lock Timeouts/sec'
				,'Lock Timeouts (timeout > 0)/sec'
				,'Number of Deadlocks/sec'
				,'Lock Waits/sec'
				,'Latch Waits/sec'
			)
		)
)

INSERT INTO @PCounters SELECT * FROM PerfCounters;

SELECT
	 'sqlserver_performance' AS [measurement]
	,REPLACE(@@SERVERNAME,'\',':') AS [sql_instance]
	,HOST_NAME() AS [computer_name]
	,pc.[object_name] AS [object]
	,pc.[counter_name] AS [counter]
	,CASE pc.[instance_name] WHEN '_Total' THEN 'Total' ELSE ISNULL(pc.[instance_name],'') END AS [instance]
	,CAST(CASE WHEN pc.[cntr_type] = 537003264 AND pc1.[cntr_value] > 0 THEN (pc.[cntr_value] * 1.0) / (pc1.[cntr_value] * 1.0) * 100 ELSE pc.[cntr_value] END AS float(10)) AS [value]
	,CAST(pc.[cntr_type] AS varchar(25)) AS [counter_type]
FROM @PCounters AS pc
LEFT OUTER JOIN @PCounters AS pc1
	ON (
		pc.[counter_name] = REPLACE(pc1.[counter_name],' base','')
		OR pc.[counter_name] = REPLACE(pc1.[counter_name],' base',' (ms)')
	)
	AND pc.[object_name] = pc1.[object_name]
	AND pc.[instance_name] = pc1.[instance_name]
	AND pc1.[counter_name] LIKE '%base'
WHERE
	pc.[counter_name] NOT LIKE '% base'
{filter_instance_name}
OPTION(RECOMPILE)
`

func getSQLServerPerformanceCounterQuery(instanceName string) string {
	if instanceName != "" {
		whereClause := fmt.Sprintf("\tAND @@SERVERNAME = '%s'", instanceName)
		r := strings.NewReplacer("{filter_instance_name}", whereClause)
		return r.Replace(sqlServerPerformanceCountersQuery)
	}

	r := strings.NewReplacer("{filter_instance_name}", "")
	return r.Replace(sqlServerPerformanceCountersQuery)
}

const sqlServerProperties = `
SET DEADLOCK_PRIORITY -10;
IF SERVERPROPERTY('EngineEdition') NOT IN (2,3,4) BEGIN /*NOT IN Standard, Enterprise, Express*/
	DECLARE @ErrorMessage AS nvarchar(500) = 'Connection string Server:'+ @@ServerName + ',Database:' + DB_NAME() +' is not a SQL Server Standard, Enterprise or Express. This query is only supported on these editions.';
	RAISERROR (@ErrorMessage,11,1)
	RETURN
END

DECLARE
	 @SqlStatement AS nvarchar(max) = ''
	,@MajorMinorVersion AS int = CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar),4) AS int)*100 + CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar),3) AS int)
	,@Columns AS nvarchar(MAX) = ''

IF CAST(SERVERPROPERTY('ProductVersion') AS varchar(50)) >= '10.50.2500.0'
	SET @Columns = N'
	,CASE [virtual_machine_type_desc]
		WHEN ''NONE'' THEN ''PHYSICAL Machine''
		ELSE [virtual_machine_type_desc]
	END AS [hardware_type]'

SET @SqlStatement = '
DECLARE @ForceEncryption INT
DECLARE @DynamicportNo NVARCHAR(50);
DECLARE @StaticportNo NVARCHAR(50);

EXEC [xp_instance_regread]
	 @rootkey = ''HKEY_LOCAL_MACHINE''
	,@key = ''SOFTWARE\Microsoft\Microsoft SQL Server\MSSQLServer\SuperSocketNetLib''
	,@value_name = ''ForceEncryption''
	,@value = @ForceEncryption OUTPUT;

EXEC [xp_instance_regread]
	 @rootkey = ''HKEY_LOCAL_MACHINE''
	,@key = ''Software\Microsoft\Microsoft SQL Server\MSSQLServer\SuperSocketNetLib\Tcp\IpAll''
	,@value_name = ''TcpDynamicPorts''
	,@value = @DynamicportNo OUTPUT

EXEC [xp_instance_regread]
	  @rootkey = ''HKEY_LOCAL_MACHINE''
     ,@key = ''Software\Microsoft\Microsoft SQL Server\MSSQLServer\SuperSocketNetLib\Tcp\IpAll''
     ,@value_name = ''TcpPort''
     ,@value = @StaticportNo OUTPUT

SELECT
	 ''sqlserver_server_properties'' AS [measurement]
	,REPLACE(@@SERVERNAME,''\'','':'') AS [sql_instance]
	,HOST_NAME() AS [computer_name]
	,@@SERVICENAME AS [service_name]
	,si.[cpu_count]
	,(SELECT [total_physical_memory_kb] FROM sys.[dm_os_sys_memory]) AS [server_memory]
	,(SELECT [available_physical_memory_kb] FROM sys.[dm_os_sys_memory]) AS [available_server_memory]
	,SERVERPROPERTY(''Edition'') AS [sku]
	,CAST(SERVERPROPERTY(''EngineEdition'') AS int) AS [engine_edition]
	,DATEDIFF(MINUTE,si.[sqlserver_start_time],GETDATE()) AS [uptime]
	,SERVERPROPERTY(''ProductVersion'') AS [sql_version]
	,SERVERPROPERTY(''IsClustered'') AS [instance_type]
	,SERVERPROPERTY(''IsHadrEnabled'') AS [is_hadr_enabled]
	,LEFT(@@VERSION,CHARINDEX('' - '',@@VERSION)) AS [sql_version_desc]
	,@ForceEncryption AS [ForceEncryption]
	,COALESCE(@DynamicportNo,@StaticportNo) AS [Port]
	,IIF(@DynamicportNo IS NULL, ''Static'', ''Dynamic'') AS [PortType]
	,dbs.[db_online]
	,dbs.[db_restoring]
	,dbs.[db_recovering]
	,dbs.[db_recoveryPending]
	,dbs.[db_suspect]
	,dbs.[db_offline]'
	+ @Columns + N'
	FROM sys.[dm_os_sys_info] AS si
	CROSS APPLY (
		SELECT
			 SUM(CASE WHEN [state] = 0 THEN 1 ELSE 0 END) AS [db_online]
			,SUM(CASE WHEN [state] = 1 THEN 1 ELSE 0 END) AS [db_restoring]
			,SUM(CASE WHEN [state] = 2 THEN 1 ELSE 0 END) AS [db_recovering]
			,SUM(CASE WHEN [state] = 3 THEN 1 ELSE 0 END) AS [db_recoveryPending]
			,SUM(CASE WHEN [state] = 4 THEN 1 ELSE 0 END) AS [db_suspect]
			,SUM(CASE WHEN [state] IN (6,10) THEN 1 ELSE 0 END) AS [db_offline]
		FROM sys.databases
	) AS dbs
%s'

EXEC sp_executesql @SqlStatement
`

func getSQLServerPropertiesQuery(instanceName string) string {
	if instanceName != "" {
		whereClause := fmt.Sprintf("WHERE @@SERVERNAME = ''%s''", instanceName)
		return fmt.Sprintf(sqlServerProperties, whereClause)
	}

	return fmt.Sprintf(sqlServerProperties, "")
}

const sql2 = `
SELECT 
    CONVERT(NVARCHAR, TODATETIMEOFFSET(CURRENT_TIMESTAMP, DATEPART(TZOFFSET, SYSDATETIMEOFFSET())), 126) AS now,
    CONVERT(NVARCHAR, TODATETIMEOFFSET(req.start_time, DATEPART(TZOFFSET, SYSDATETIMEOFFSET())), 126) AS query_start,
    sess.login_name AS user_name,
    sess.last_request_start_time AS last_request_start_time,
    sess.session_id AS id,
    DB_NAME(sess.database_id) AS database_name,
    sess.status AS session_status,
    req.status AS request_status,
    SUBSTRING(qt.text, (req.statement_start_offset / 2) + 1, 
              ((CASE req.statement_end_offset 
                   WHEN -1 THEN DATALENGTH(qt.text) 
                   ELSE req.statement_end_offset 
               END - req.statement_start_offset) / 2) + 1) AS statement_text,
    SUBSTRING(qt.text, 1, 500) AS text,
    c.client_tcp_port AS client_port,
    c.client_net_address AS client_address,
    sess.host_name AS host_name,
    sess.program_name AS program_name,
    sess.is_user_process AS is_user_process,
    req.command,
    req.blocking_session_id,
    req.wait_type,
    req.wait_time,
    req.last_wait_type,
    req.wait_resource,
    req.open_transaction_count,
    req.transaction_id,
    req.percent_complete,
    req.estimated_completion_time,
    req.cpu_time,
    req.total_elapsed_time,
    req.reads,
    req.writes,
    req.logical_reads,
    req.transaction_isolation_level,
    req.lock_timeout,
    req.deadlock_priority,
    req.row_count,
    req.query_hash,
    req.query_plan_hash,
    req.context_info
FROM 
    sys.dm_exec_sessions sess
INNER JOIN 
    sys.dm_exec_connections c ON sess.session_id = c.session_id
INNER JOIN 
    sys.dm_exec_requests req ON c.connection_id = req.connection_id
CROSS APPLY 
    sys.dm_exec_sql_text(req.sql_handle) qt
WHERE 
--     sess.session_id != @@SPID
--     AND
    sess.status != 'sleeping';
`

const qQueryPlan = `
with qstats as (
    select
        qs.query_hash,
        qs.query_plan_hash,
        qs.last_execution_time,
        qs.last_elapsed_time,
        CONCAT(
                CONVERT(VARCHAR(64), CONVERT(binary(64), qs.plan_handle), 1),
                CONVERT(VARCHAR(10), CONVERT(varbinary(4), qs.statement_start_offset), 1),
                CONVERT(VARCHAR(10), CONVERT(varbinary(4), qs.statement_end_offset), 1)) as plan_handle_and_offsets,
        (select value from sys.dm_exec_plan_attributes(qs.plan_handle) where attribute = 'dbid') as dbid,
        eps.object_id as sproc_object_id,
        qs.execution_count as execution_count, qs.total_worker_time as total_worker_time, qs.total_physical_reads as total_physical_reads, qs.total_logical_writes as total_logical_writes, qs.total_logical_reads as total_logical_reads, qs.total_clr_time as total_clr_time, qs.total_elapsed_time as total_elapsed_time, qs.total_rows as total_rows, qs.total_dop as total_dop, qs.total_grant_kb as total_grant_kb, qs.total_used_grant_kb as total_used_grant_kb, qs.total_ideal_grant_kb as total_ideal_grant_kb, qs.total_reserved_threads as total_reserved_threads, qs.total_used_threads as total_used_threads, qs.total_columnstore_segment_reads as total_columnstore_segment_reads, qs.total_columnstore_segment_skips as total_columnstore_segment_skips, qs.total_spills as total_spills
    from sys.dm_exec_query_stats qs
             left join sys.dm_exec_procedure_stats eps ON eps.plan_handle = qs.plan_handle
),
     qstats_aggr as (
         select
             query_hash,
             query_plan_hash,
             CAST(qs.dbid as int) as dbid,
             D.name as database_name,
             max(plan_handle_and_offsets) as plan_handle_and_offsets,
             max(last_execution_time) as last_execution_time,
             max(last_elapsed_time) as last_elapsed_time,
             sproc_object_id,
             sum(qs.execution_count) as execution_count, sum(qs.total_worker_time) as total_worker_time, sum(qs.total_physical_reads) as total_physical_reads, sum(qs.total_logical_writes) as total_logical_writes, sum(qs.total_logical_reads) as total_logical_reads, sum(qs.total_clr_time) as total_clr_time, sum(qs.total_elapsed_time) as total_elapsed_time, sum(qs.total_rows) as total_rows, sum(qs.total_dop) as total_dop, sum(qs.total_grant_kb) as total_grant_kb, sum(qs.total_used_grant_kb) as total_used_grant_kb, sum(qs.total_ideal_grant_kb) as total_ideal_grant_kb, sum(qs.total_reserved_threads) as total_reserved_threads, sum(qs.total_used_threads) as total_used_threads, sum(qs.total_columnstore_segment_reads) as total_columnstore_segment_reads, sum(qs.total_columnstore_segment_skips) as total_columnstore_segment_skips, sum(qs.total_spills) as total_spills
         from qstats qs
                  left join sys.databases D on qs.dbid = D.database_id
         group by query_hash, query_plan_hash, qs.dbid, D.name, sproc_object_id
     ),
     qstats_aggr_split as (
         select TOP 10000
             convert(varbinary(64), convert(binary(64), substring(plan_handle_and_offsets, 1, 64), 1)) as plan_handle,
             convert(int, convert(varbinary(10), substring(plan_handle_and_offsets, 64+1, 10), 1)) as statement_start_offset,
             convert(int, convert(varbinary(10), substring(plan_handle_and_offsets, 64+11, 10), 1)) as statement_end_offset,
             *
         from qstats_aggr
         where DATEADD(ms, last_elapsed_time / 1000, last_execution_time) > dateadd(second, -120, getdate())
     )
select
    SUBSTRING(text, (statement_start_offset / 2) + 1,
              ((CASE statement_end_offset
                    WHEN -1 THEN DATALENGTH(text)
                    ELSE statement_end_offset END
                  - statement_start_offset) / 2) + 1) AS statement_text,
    SUBSTRING(qt.text, 1, 500) as text,
    qt.encrypted as is_encrypted,
    qp.query_plan as query_plan,
    s.* from qstats_aggr_split s
                 cross apply sys.dm_exec_sql_text(s.plan_handle) qt
                 cross apply sys.dm_exec_query_plan(s.plan_handle) qp
`

func getSQLQuery(instanceName string) string {
	return sql2
}

func getQQueryPlan() string {
	return qQueryPlan
}

func getQueryRow() string {
	return `
	SELECT 
		CONVERT(NVARCHAR, TODATETIMEOFFSET(qs.last_execution_time, DATEPART(TZOFFSET, SYSDATETIMEOFFSET())), 126) as last_execution_time,
		qs.query_hash,
		qt.text,
		qs.execution_count
	FROM 
		sys.dm_exec_query_stats AS qs
	CROSS APPLY 
		sys.dm_exec_sql_text(qs.sql_handle) AS qt
	WHERE 
		qt.text LIKE '/*%'
	`
}
