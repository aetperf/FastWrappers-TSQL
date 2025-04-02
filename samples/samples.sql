

-- Sample for the dbo.orders table between 2 mssql instances using xp_RunFastTransfer

EXEC dbo.xp_RunFastTransfer
	@fastTransferDir = 'C:\FastTransfer\win-x64\latest',
    @sourceConnectionType = 'mssql',
	@sourceServer = 'localhost',
	@sourceUser = 'FastUser',
	@sourcePassword = 'FastPassword',
	@sourceDatabase = 'tpch_test',
	@sourceSchema = 'dbo',
	@sourceTable = 'orders',
	@targetConnectionType = 'msbulk',
	@targetServer = 'localhost\SS2025',
	@targetUser = 'FastUser',
	@targetPassword = 'FastPassword',
	@targetDatabase = 'tpch_test',
	@targetSchema = 'dbo',
	@targetTable = 'orders_3',
	@loadmode = 'Truncate',
	@batchSize = 130000,
	@method = 'RangeId',
	@distributeKeyColumn = 'o_orderkey',
	@degree = 12,
	@mapmethod = 'Name',
	@runId = 'test_MSSQL_to_MSSQL_P12_Ntile'
;


-- Sample for the dbo.orders table between 2 mssql instances using xp_RunFastTransfer_secure

-- use SELECT [dbo].[EncryptString]('FastPassword') to get the encrypted password

EXEC dbo.xp_RunFastTransfer_secure
	@fastTransferDir = 'C:\FastTransfer\win-x64\latest',
    @sourceConnectionType = 'mssql',
	@sourceServer = 'localhost',
	@sourceUser = 'FastUser',
	@sourcePassword = 'PSLseoMgArtO+IdmprKkHQ==',
	@sourceDatabase = 'tpch_test',
	@sourceSchema = 'dbo',
	@sourceTable = 'orders',
	@targetConnectionType = 'msbulk',
	@targetServer = 'localhost\SS2025',
	@targetUser = 'FastUser',
	@targetPassword = 'PSLseoMgArtO+IdmprKkHQ==',
	@targetDatabase = 'tpch_test',
	@targetSchema = 'dbo',
	@targetTable = 'orders_3',
	@loadmode = 'Truncate',
	@batchSize = 130000,
	@method = 'RangeId',
	@distributeKeyColumn = 'o_orderkey',
	@degree = 12,
	@mapmethod = 'Name',
	@runId = 'test_MSSQL_to_MSSQL_P12_Ntile'
	
