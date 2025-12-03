# FastWrappers-TSQL

A project to wrap FastTransfer and FastBCP in a CLR Assembly and allow to call them from T-SQL using extended store procedure.
As a reminder :
- **FastTransfer** is a CLI that allow import from file or transfer data between databases using streaming and parallel mecanism for high performance
- **FastBCP** is a CLI that allow to export data from databases to files (csv, parquet, json,bson and excel) using streaming and parallel mecanism for high performance

Samples usage :

### Copy one table using 12 threads between two MSSQL instances 
```TSQL
-- use SELECT [dbo].[EncryptString]('<YourPassWordToEncrypt>') to get the encrypted password
EXEC dbo.xp_RunFastTransfer_secure
     @fastTransferDir='C:\FastTransfert\win-x64\latest\',
     @sourceConnectionType = N'mssql',
     @sourceServer = N'localhost',
     @sourceUser = N'FastUser',
     @sourcePasswordSecure = 'wi1/VHz9s+fp45186iLYYQ==',
     @sourceDatabase = N'tpch_test',
     @sourceSchema = N'dbo',
     @sourceTable = N'orders',
     @targetConnectionType = N'msbulk',
     @targetServer = N'localhost\SS2025',
     @targetUser = N'FastUser',
     @targetPasswordSecure = 'wi1/VHz9s+fp45186iLYYQ==',
     @targetDatabase = N'tpch_test',
     @targetSchema = N'dbo',
     @targetTable = N'orders_3',
     @loadMode = N'Truncate',
     @batchSize = 130000,
     @method = N'RangeId',
     @distributeKeyColumn = N'o_orderkey',
     @degree = 12,
     @mapmethod = 'Name',
     @runId = N'CLRWrap_Run_MS2MS_20250328'
```

### Copy one table using 12 threads between an Oracle database and SQL instance 
```TSQL
-- use SELECT [dbo].[EncryptString]('<YourPassWordToEncrypt>') to get the encrypted password

EXEC dbo.xp_RunFastTransfer_secure
	@fastTransferDir = 'C:\FastTransfer\win-x64\latest\',
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
     @mapmethod = 'Name',
     @runId = N'CLRWrap_Run_ORA2MS_20250328'
```

### Export one table to a csv file mono thread 
```TSQL
-- use SELECT [dbo].[EncryptString]('<YourPassWordToEncrypt>') to get the encrypted password

EXEC dbo.xp_RunFastBCP_secure
   @fastBCPDir = 'D:\FastBCP\latest',
   @connectionType = 'mssql',
   @sourceserver = 'localhost',
   @sourceuser = 'FastUser',
   @sourceschema = 'dbo',
   @sourcetable = 'orders',
   @sourcepasswordSecure = 'wi1/VHz9s+fp45186iLYYQ==',
   @sourcedatabase = 'tpch',
   @query = 'SELECT top 1000 * FROM orders',
   @outputFile = 'orders_output.csv',
   @outputDirectory = 'D:\temp\fastbcpoutput\{sourcedatabase}\{sourceschema}\{sourcetable}',
   @delimiter = '|',
   @usequotes = 1,
   @dateformat = 'yyyy-MM-dd HH24:mm:ss',
   @encoding = 'utf-8',
   @method = 'None',
   @runid = 'test_FastBCP_export_orders'
```

### Export one table to 8 parquet files using 8 threads 
```TSQL
-- use SELECT [dbo].[EncryptString]('<YourPassWordToEncrypt>') to get the encrypted password
EXEC dbo.xp_RunFastBCP_secure
   @fastBCPDir = 'D:\FastBCP\latest',
   @connectionType = 'mssql',
   @sourceserver = 'localhost',
   @sourceuser = 'FastUser',
   @sourceschema = 'dbo',
   @sourcetable = 'orders_15M',
   @sourcepasswordSecure = 'wi1/VHz9s+fp45186iLYYQ==',
   @sourcedatabase = 'tpch10_collation_bin2',
   @outputFile = 'orders_output.parquet',
   @outputDirectory = 'D:\temp\fastbcpoutput\{sourcedatabase}\{sourceschema}\{sourcetable}',
   @method = 'Ntile',
   @distributeKeyColumn = 'o_orderkey',    
   @degree = 8,
   @mergeDistributedFile = 0
   ```


### Export one table several csv files (one file by month) using 8 threads 
```TSQL
-- use SELECT [dbo].[EncryptString]('<YourPassWordToEncrypt>') to get the encrypted password
EXEC dbo.xp_RunFastBCP_secure
   @fastBCPDir = 'D:\FastBCP\latest',
   @connectionType = 'mssql',
   @sourceserver = 'localhost',
   @sourceuser = 'FastUser',
   @sourceschema = 'dbo',
   @sourcetable = 'orders_15M',
   @sourcepasswordSecure = 'wi1/VHz9s+fp45186iLYYQ==',
   @sourcedatabase = 'tpch10_collation_bin2',
   @query = 'SELECT * FROM (SELECT *, year(o_orderdate)*100+month(o_orderdate) o_ordermonth from orders_15M) src',
   @outputFile = 'orders_output.csv',
   @outputDirectory = 'D:\temp\fastbcpoutput\{sourcedatabase}\{sourceschema}\{sourcetable}',
   @method = 'DataDriven',
   @distributeKeyColumn = 'o_ordermonth',    
   @degree = 8,
   @mergeDistributedFile = 0
   ```


## Nota :
You must have a valid trial or a valid FastTransfer.exe (or FastTransfer binary for linux) into the directory you specified with @fastTransferDir. The sql server service user must have read/execute provilege on the directory and FastTransfer(.exe) file
