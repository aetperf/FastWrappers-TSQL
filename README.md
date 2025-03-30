# FastWrappers-TSQL

A project to wrap FastTransfer and FastBCP in a CLR Assembly and allow to call them from T-SQL using extended store procedure.
As a reminder :
- **FastTransfer** is a CLI that allow import from file or transfer data between databases using streaming and parallel mecanism for high performance
- **FastBCP** is a CLI that allow to export data from databases to files (csv, parquet, json,bson and excel) using streaming and parallel mecanism for high performance

Samples usage :

### Copy one table using 12 threads between two MSSQL instances 
```TSQL
EXEC dbo.xp_RunFastTransfer
     @fastTransferDir='C:\FastTransfert\win-x64\latest',
     @sourceConnectionType = N'mssql',
     @sourceServer = N'localhost',
     @sourceUser = N'FastUser',
     @sourcePassword = N'FastPassword',
     @sourceDatabase = N'tpch_test',
     @sourceSchema = N'dbo',
     @sourceTable = N'orders',
     @targetConnectionType = N'msbulk',
     @targetServer = N'localhost\SS2025',
     @targetUser = N'FastUser',
     @targetPassword = N'FastPassword',
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
EXEC dbo.xp_RunFastTransfer
     @fastTransferDir='C:\FastTransfer\win-x64\latest',
     @sourceConnectionType = N'oraodp',
     @sourceServer = N'localhost:1521/ORCLPDB',
     @sourceUser = N'TPCH_IN',
     @sourcePassword = N'TPCH_IN',
     @sourceDatabase = N'ORCLPDB',
     @sourceSchema = N'TPCH_IN',
     @sourceTable = N'ORDERS_FLAT',
     @targetConnectionType = N'msbulk',
     @targetServer = N'localhost\SS2025',
     @targetUser = N'FastUser',
     @targetPassword = N'FastPassword',
     @targetDatabase = N'tpch_test',
     @targetSchema = N'dbo',
     @targetTable = N'orders_3',
     @loadMode = N'Truncate',
     @batchSize = 130000,
     @method = N'Rowid',
     @degree = 12,
     @mapmethod = 'Name',
     @runId = N'CLRWrap_Run_ORA2MS_20250328'
```
