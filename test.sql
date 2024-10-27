SELECT TOP 10 deqs.query_hash, deqs.query_plan_hash, deqp.query_plan, dest.text FROM sys.dm_exec_query_stats AS deqs CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle) AS deqp CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS dest;


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

-- one line ver, getting query sample
SELECT CONVERT(NVARCHAR, TODATETIMEOFFSET(CURRENT_TIMESTAMP, DATEPART(TZOFFSET, SYSDATETIMEOFFSET())), 126) AS now, CONVERT(NVARCHAR, TODATETIMEOFFSET(req.start_time, DATEPART(TZOFFSET, SYSDATETIMEOFFSET())), 126) AS query_start, sess.login_name AS user_name, sess.last_request_start_time AS last_request_start_time, sess.session_id AS id, DB_NAME(sess.database_id) AS database_name, sess.status AS session_status, req.status AS request_status, SUBSTRING(qt.text, (req.statement_start_offset / 2) + 1, ((CASE req.statement_end_offset WHEN -1 THEN DATALENGTH(qt.text) ELSE req.statement_end_offset END - req.statement_start_offset) / 2) + 1) AS statement_text, SUBSTRING(qt.text, 1, 500) AS text, c.client_tcp_port AS client_port, c.client_net_address AS client_address, sess.host_name AS host_name, sess.program_name AS program_name, sess.is_user_process AS is_user_process, req.command, req.blocking_session_id, req.wait_type, req.wait_time, req.last_wait_type, req.wait_resource, req.open_transaction_count, req.transaction_id, req.percent_complete, req.estimated_completion_time, req.cpu_time, req.total_elapsed_time, req.reads, req.writes, req.logical_reads, req.transaction_isolation_level, req.lock_timeout, req.deadlock_priority, req.row_count, req.query_hash, req.query_plan_hash, req.context_info FROM sys.dm_exec_sessions sess INNER JOIN sys.dm_exec_connections c ON sess.session_id = c.session_id INNER JOIN sys.dm_exec_requests req ON c.connection_id = req.connection_id CROSS APPLY sys.dm_exec_sql_text(req.sql_handle) qt WHERE sess.status != 'sleeping';

-- one line ver, getting query stats.
with qstats as (select qs.query_hash, qs.query_plan_hash, qs.last_execution_time, qs.last_elapsed_time, CONCAT(CONVERT(VARCHAR(64), CONVERT(binary(64), qs.plan_handle), 1), CONVERT(VARCHAR(10), CONVERT(varbinary(4), qs.statement_start_offset), 1), CONVERT(VARCHAR(10), CONVERT(varbinary(4), qs.statement_end_offset), 1)) as plan_handle_and_offsets, (select value from sys.dm_exec_plan_attributes(qs.plan_handle) where attribute = 'dbid') as dbid, eps.object_id as sproc_object_id, qs.execution_count as execution_count, qs.total_worker_time as total_worker_time, qs.total_physical_reads as total_physical_reads, qs.total_logical_writes as total_logical_writes, qs.total_logical_reads as total_logical_reads, qs.total_clr_time as total_clr_time, qs.total_elapsed_time as total_elapsed_time, qs.total_rows as total_rows, qs.total_dop as total_dop, qs.total_grant_kb as total_grant_kb, qs.total_used_grant_kb as total_used_grant_kb, qs.total_ideal_grant_kb as total_ideal_grant_kb, qs.total_reserved_threads as total_reserved_threads, qs.total_used_threads as total_used_threads, qs.total_columnstore_segment_reads as total_columnstore_segment_reads, qs.total_columnstore_segment_skips as total_columnstore_segment_skips, qs.total_spills as total_spills from sys.dm_exec_query_stats qs left join sys.dm_exec_procedure_stats eps ON eps.plan_handle = qs.plan_handle), qstats_aggr as (select query_hash, query_plan_hash, CAST(qs.dbid as int) as dbid, D.name as database_name, max(plan_handle_and_offsets) as plan_handle_and_offsets, max(last_execution_time) as last_execution_time, max(last_elapsed_time) as last_elapsed_time, sproc_object_id, sum(qs.execution_count) as execution_count, sum(qs.total_worker_time) as total_worker_time, sum(qs.total_physical_reads) as total_physical_reads, sum(qs.total_logical_writes) as total_logical_writes, sum(qs.total_logical_reads) as total_logical_reads, sum(qs.total_clr_time) as total_clr_time, sum(qs.total_elapsed_time) as total_elapsed_time, sum(qs.total_rows) as total_rows, sum(qs.total_dop) as total_dop, sum(qs.total_grant_kb) as total_grant_kb, sum(qs.total_used_grant_kb) as total_used_grant_kb, sum(qs.total_ideal_grant_kb) as total_ideal_grant_kb, sum(qs.total_reserved_threads) as total_reserved_threads, sum(qs.total_used_threads) as total_used_threads, sum(qs.total_columnstore_segment_reads) as total_columnstore_segment_reads, sum(qs.total_columnstore_segment_skips) as total_columnstore_segment_skips, sum(qs.total_spills) as total_spills from qstats qs left join sys.databases D on qs.dbid = D.database_id group by query_hash, query_plan_hash, qs.dbid, D.name, sproc_object_id), qstats_aggr_split as (select TOP 10000 convert(varbinary(64), convert(binary(64), substring(plan_handle_and_offsets, 1, 64), 1)) as plan_handle, convert(int, convert(varbinary(10), substring(plan_handle_and_offsets, 64+1, 10), 1)) as statement_start_offset, convert(int, convert(varbinary(10), substring(plan_handle_and_offsets, 64+11, 10), 1)) as statement_end_offset, * from qstats_aggr where DATEADD(ms, last_elapsed_time / 1000, last_execution_time) > dateadd(second, -120, getdate())) select SUBSTRING(text, (statement_start_offset / 2) + 1, ((CASE statement_end_offset WHEN -1 THEN DATALENGTH(text) ELSE statement_end_offset END - statement_start_offset) / 2) + 1) AS statement_text, SUBSTRING(qt.text, 1, 500) as text, encrypted as is_encrypted, s.* from qstats_aggr_split s cross apply sys.dm_exec_sql_text(s.plan_handle) qt;

-- get a query sample, and to get a query stat from the samples' query hash or query plan hash.
-- get the query handler from this query, haven't identified how does datadog did this yet.
SELECT TOP 1 qs.plan_handle FROM sys.dm_exec_query_stats qs  WHERE qs.query_plan_hash = 0x858C642F1FF32B20
-- get the full query plan with the below query
select cast(query_plan as nvarchar(max)) as query_plan, encrypted as is_encrypted from sys.dm_exec_query_plan(CONVERT(varbinary(max), 0x06000100bfe38d1690dc465614000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000, 1))
select cast(query_plan as nvarchar(max)) as query_plan, encrypted as is_encrypted from sys.dm_exec_query_plan(CONVERT(varbinary(max), 0xd102361b67e88896, 1))

