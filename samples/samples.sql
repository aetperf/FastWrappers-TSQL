-- Samples for FastWrappers-TSQL 1.2.0 (validated with FastTransfer 0.18.2 and FastBCP 1.6.7)
-- Run them one at a time.

-- use SELECT [dbo].[EncryptString]('<YourPassWordToEncrypt>') to get the encrypted password

-- =====================================================================
-- FastTransfer
-- =====================================================================

-- Copy the dbo.orders table between 2 mssql instances using xp_RunFastTransfer_secure
EXEC dbo.xp_RunFastTransfer_secure
	@fastTransferDir = 'D:\FastTransfer\latest',
    @sourceConnectionType = 'mssql',
	@sourceServer = 'localhost',
	@sourceUser = 'FastUser',
	@sourcePasswordSecure = 'wi1/VHz9s+fp45186iLYYQ==',
	@sourceDatabase = 'tpch_test',
	@sourceSchema = 'dbo',
	@sourceTable = 'orders',
	@targetConnectionType = 'msbulk',
	@targetServer = 'localhost\SS2025',
	@targetUser = 'FastUser',
	@targetPasswordSecure = 'wi1/VHz9s+fp45186iLYYQ==',
	@targetDatabase = 'tpch_test',
	@targetSchema = 'dbo',
	@targetTable = 'orders_3',
	@loadmode = 'Truncate',
	@batchSize = 130000,
	@method = 'RangeId',
	@distributeKeyColumn = 'o_orderkey',
	@degree = 12,
	@mapmethod = 'Name',
	@runId = 'test_MSSQL_to_MSSQL_P12_RangeId',
	@license = 'D:\FastTransfer\TRIAL-FastTransfer-20261107174316-2c1d76.lic'
GO

-- Import a csv file with DuckDB streaming (no source database, no source credentials)
EXEC dbo.xp_RunFastTransfer_secure
	@fastTransferDir = 'D:\FastTransfer\latest',
	@sourceConnectionType = 'duckdbstream',
	@sourceServer = ':memory:',
	@query = 'SELECT * FROM read_csv(''D:\temp\orders.csv'')',
	@targetConnectionType = 'msbulk',
	@targetServer = 'localhost\SS2025',
	@isTargetTrusted = 1,
	@targetDatabase = 'tpch_test',
	@targetSchema = 'dbo',
	@targetTable = 'orders_3',
	@loadmode = 'Truncate',
	@runId = 'test_CSV_to_MSSQL',
	@license = 'D:\FastTransfer\TRIAL-FastTransfer-20261107174316-2c1d76.lic'
GO

-- =====================================================================
-- FastBCP
-- =====================================================================

-- Export the dbo.orders table to one csv file
EXEC dbo.xp_RunFastBCP_secure
	@fastBCPDir = 'D:\FastBCP\latest',
	@connectionType = 'mssql',
	@sourceserver = 'localhost\SS2025',
	@isTrusted = 1,
	@sourcedatabase = 'tpch_test',
	@sourceschema = 'dbo',
	@sourcetable = 'orders',
	@outputFile = 'orders.csv',
	@outputDirectory = 'D:\temp\fastbcpoutput\{sourcedatabase}\{sourcetable}',
	@dateformat = 'yyyy-MM-dd HH:mm:ss',
	@runid = 'test_FastBCP_MSSQL_to_CSV',
	@license = 'D:\FastBCP\latest\FastBCP_AetP.lic'
GO

-- Export the dbo.orders table to several parquet files, using 8 threads
EXEC dbo.xp_RunFastBCP_secure
	@fastBCPDir = 'D:\FastBCP\latest',
	@connectionType = 'mssql',
	@sourceserver = 'localhost\SS2025',
	@isTrusted = 1,
	@sourcedatabase = 'tpch_test',
	@sourceschema = 'dbo',
	@sourcetable = 'orders',
	@outputFile = 'orders.parquet',
	@outputDirectory = 'D:\temp\fastbcpoutput\{sourcedatabase}\{sourcetable}\parallel',
	@method = 'RangeId',
	@distributeKeyColumn = 'o_orderkey',
	@degree = 8,
	@runid = 'test_FastBCP_MSSQL_to_Parquet_P8_RangeId',
	@license = 'D:\FastBCP\latest\FastBCP_AetP.lic'
GO

-- Export a table straight from a SQL Server backup, without restoring it (parquet only, method None or Physical)
-- @sourceserver = directory of the backups, @sourcedatabase = names or patterns of the files
EXEC dbo.xp_RunFastBCP_secure
	@fastBCPDir = 'D:\FastBCP\latest',
	@connectionType = 'backup_mssql',
	@sourceserver = 'D:\Backups\MSSQL',
	@sourcedatabase = 'tpch_full.bak',
	@sourceschema = 'dbo',
	@sourcetable = 'orders',
	@outputFile = 'orders.parquet',
	@outputDirectory = 'D:\temp\fastbcpoutput\backup\orders',
	@method = 'Physical',
	@degree = 8,
	@runid = 'test_FastBCP_Backup_to_Parquet',
	@license = 'D:\FastBCP\latest\FastBCP_AetP.lic'
GO

-- Export a table straight from an Oracle RMAN backup set (parquet only, method None or Physical)
EXEC dbo.xp_RunFastBCP_secure
	@fastBCPDir = 'D:\FastBCP\latest',
	@connectionType = 'backup_rman',
	@sourceserver = 'D:\Backups\RMAN',
	@sourcedatabase = 'full_*',
	@sourceschema = 'SALES',
	@sourcetable = 'ORDERS',
	@outputFile = 'orders.parquet',
	@outputDirectory = 'D:\temp\fastbcpoutput\rman\orders',
	@method = 'Physical',
	@degree = 8,
	@runid = 'test_FastBCP_RMAN_to_Parquet',
	@license = 'D:\FastBCP\latest\FastBCP_AetP.lic'
GO
