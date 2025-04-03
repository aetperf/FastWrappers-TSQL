

-- Sample for the dbo.orders table between 2 mssql instances using xp_RunFastTransfer

EXEC dbo.xp_RunFastTransfer
	@fastTransferDir = 'C:\FastTransfer\win-x64\latest',
    @sourceConnectionType = 'mssql', -- other options are clickhouse, duckdbstream (use for duckdb, parquet, csv, json files), hana, mssql, mysql, nzsql, odbc, oledb, oraodp, pgcopy, pgsql, teradata
	@sourceServer = 'localhost',
	@sourceUser = 'FastUser',
	@sourcePassword = 'FastPassword',
	@sourceDatabase = 'tpch_test',
	@sourceSchema = 'dbo',
	@sourceTable = 'orders',
	@targetConnectionType = 'msbulk', -- other options are "clickhousebulk", "duckdb", "hanabulk", "msbulk", "mysqlbulk", "nzbulk", "orabulk", "oradirect", "pgcopy", "teradata"
	@targetServer = 'localhost\SS2025',
	@targetUser = 'FastUser',
	@targetPassword = 'FastPassword',
	@targetDatabase = 'tpch_test',
	@targetSchema = 'dbo',
	@targetTable = 'orders_3',
	@loadmode = 'Truncate', -- other mode is Append
	@batchSize = 130000,
	@method = 'RangeId',  -- other methods are Ntile, Random, Ctid (pgsql and pgcopy only), RowId (oraodp only)
	@distributeKeyColumn = 'o_orderkey',
	@degree = 12,
	@mapmethod = 'Name', --other method is Position
	@runId = 'test_MSSQL_to_MSSQL_P12_RangeId'
;


-- Sample for the dbo.orders table between 2 mssql instances using xp_RunFastTransfer_secure

-- use SELECT [dbo].[EncryptString]('FastPassword') to get the encrypted password

EXEC dbo.xp_RunFastTransfer_secure
	@fastTransferDir = 'C:\FastTransfer\win-x64\latest',
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
	@runId = 'test_MSSQL_to_MSSQL_P12_RangeId'
	
