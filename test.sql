SET DEADLOCK_PRIORITY -10;
IF SERVERPROPERTY('EngineEdition') NOT IN (2,3,4,5,8) BEGIN /*NOT IN Standard,Enterprise,Express,Azure SQL Database, Azure SQL Managed Instance*/
    DECLARE @ErrorMessage AS nvarchar(500) = 'Connection string Server:'+ @@ServerName + ',Database:' + DB_NAME() +' is not a SQL Server Standard, Enterprise, Express, Azure SQL Database or Azure SQL Managed Instance. This query is only supported on these editions.';
    RAISERROR (@ErrorMessage,11,1)
    RETURN
END

DECLARE
    @SqlStatement AS nvarchar(max) = ''
    ,@EngineEdition INT = CAST(SERVERPROPERTY('EngineEdition') AS INT)
    ,@MajorMinorVersion AS int = CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar),4) AS int)*100 + CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar),3) AS int)
    ,@Columns AS nvarchar(MAX) = ''
    -- variables that are unavailable on EngineEdition 5 or 8
    ,@ServiceName NVARCHAR(MAX) = ''
    ,@ServerMemory BIGINT = 0
    ,@AvailableServerMemory BIGINT = 0
    ,@ForceEncryption INT = 0
    ,@DynamicportNo NVARCHAR(50) = NULL
    ,@StaticportNo NVARCHAR(50) = NULL

IF CAST(SERVERPROPERTY('ProductVersion') AS varchar(50)) >= '10.50.2500.0'
    SET @Columns = N'
	,CASE [virtual_machine_type_desc]
		WHEN ''NONE'' THEN ''PHYSICAL Machine''
		ELSE [virtual_machine_type_desc]
	END AS [hardware_type]'

IF @EngineEdition IN (2, 3, 4, 8)
    BEGIN
        -- Populate physical memory details using sys.dm_os_sys_memory (only available on 2,3,4,8)
        SELECT
            @ServerMemory = total_physical_memory_kb
             ,@AvailableServerMemory = available_physical_memory_kb
        FROM sys.dm_os_sys_memory

        SET @ServiceName = @@SERVICENAME

        IF @EngineEdition IN (2, 3, 4)
            BEGIN
--                 EXEC [xp_instance_regread]
--                      @rootkey = ''HKEY_LOCAL_MACHINE''
-- 	,@key = ''SOFTWARE\Microsoft\Microsoft SQL Server\MSSQLServer\SuperSocketNetLib''
-- 	,@value_name = ''ForceEncryption''
-- 	,@value = @ForceEncryption OUTPUT;

--                 EXEC [xp_instance_regread]
--                      @rootkey = ''HKEY_LOCAL_MACHINE''
-- 	,@key = ''Software\Microsoft\Microsoft SQL Server\MSSQLServer\SuperSocketNetLib\Tcp\IpAll''
-- 	,@value_name = ''TcpDynamicPorts''
-- 	,@value = @DynamicportNo OUTPUT

--                 EXEC [xp_instance_regread]
--                      @rootkey = ''HKEY_LOCAL_MACHINE''
--      ,@key = ''Software\Microsoft\Microsoft SQL Server\MSSQLServer\SuperSocketNetLib\Tcp\IpAll''
--      ,@value_name = ''TcpPort''
--      ,@value = @StaticportNo OUTPUT
            END
    END

SET @SqlStatement = '
SELECT
	 ''sqlserver_server_properties'' AS [measurement]
	,REPLACE(@@SERVERNAME,''\'','':'') AS [sql_instance]
	,HOST_NAME() AS [computer_name]
	,@ServiceName AS [service_name] -- Will be empty on Azure SQL DB (5)
	,si.[cpu_count]
	,@ServerMemory AS [server_memory] -- Will be 0 on Azure SQL DB (5)
	,@AvailableServerMemory AS [available_server_memory] -- Will be 0 on Azure SQL DB (5)
	,SERVERPROPERTY(''Edition'') AS [sku]
	,CAST(SERVERPROPERTY(''EngineEdition'') AS int) AS [engine_edition]
	,DATEDIFF(MINUTE,si.[sqlserver_start_time],GETDATE()) AS [uptime]
	,SERVERPROPERTY(''ProductVersion'') AS [sql_version]
	,SERVERPROPERTY(''IsClustered'') AS [instance_type]
	,SERVERPROPERTY(''IsHadrEnabled'') AS [is_hadr_enabled]
	,LEFT(@@VERSION,CHARINDEX('' - '',@@VERSION)) AS [sql_version_desc]
	,@ForceEncryption AS [ForceEncryption] -- Will be 0 on Azure SQL DB/MI (5,8)
	,COALESCE(@DynamicportNo,@StaticportNo, '') AS [Port] -- Will be empty on Azure SQL DB/MI (5,8)
	,IIF(@DynamicportNo IS NOT NULL, ''Dynamic'', IIF(@StaticportNo IS NOT NULL, ''Static'', '') AS [PortType] -- Will be ''Dynamic'' or ''Static'' on 2,3,4, empty on 5,8 if ports are null
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

EXEC sp_executesql
     @SqlStatement
    ,N'@ServiceName NVARCHAR(MAX), @ServerMemory BIGINT, @AvailableServerMemory BIGINT, @ForceEncryption INT, @DynamicportNo NVARCHAR(50), @StaticportNo NVARCHAR(50)'
    ,@ServiceName = @ServiceName
    ,@ServerMemory = @ServerMemory
    ,@AvailableServerMemory = @AvailableServerMemory
    ,@ForceEncryption = @ForceEncryption
    ,@DynamicportNo = @DynamicportNo
    ,@StaticportNo = @StaticportNo
