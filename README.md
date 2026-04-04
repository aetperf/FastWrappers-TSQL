# FastWrappers-TSQL

A project to wrap FastTransfer and FastBCP in a CLR Assembly and allow to call them from T-SQL using extended store procedure.
As a reminder :
- **FastTransfer** is a CLI that allow import from file or transfer data between databases using streaming and parallel mecanism for high performance
- **FastBCP** is a CLI that allow to export data from databases to files (csv, parquet, json,bson and excel) using streaming and parallel mecanism for high performance

## Installation

Download the latest release from the [Releases page](https://github.com/aetperf/FastWrappers-TSQL/releases). Each release provides 2 installation options:

### Installation Methods

#### 1. **FastWrappers-TSQL.bak** (Recommended) 
SQL Server Backup file - **Requires SQL Server 2019 or higher**

✅ **Fastest installation method**  
✅ **Pre-configured database with all objects**

⚠️ **Post-installation configuration required:**
- Enable CLR integration (`sp_configure 'clr enabled', 1`)
- Trust the assembly using `sp_add_trusted_assembly` (see [Post-Installation Configuration](#post-installation-configuration))

**Installation:**
```sql
-- Restore the database
RESTORE DATABASE [FastWrappers-TSQL] 
FROM DISK = 'C:\path\to\FastWrappers-TSQL.bak' 
WITH MOVE 'FastWrappers-TSQL' TO 'C:\path\to\FastWrappers-TSQL.mdf',
     MOVE 'FastWrappers-TSQL_log' TO 'C:\path\to\FastWrappers-TSQL_log.ldf';
GO
```

#### 2. **FastWrappers-TSQL.sql** (Alternative for older SQL Server versions)
Self-contained SQL script - **Compatible with SQL Server 2016 or higher**

✅ **Complete installation in a single script**  
✅ **Includes database creation, CLR activation, trusted assembly configuration, and all objects**  
✅ **No post-installation configuration needed**

**Installation:**
```sql
-- Simply execute the script in SSMS or using sqlcmd
sqlcmd -S YourServer -i FastWrappers-TSQL.sql
```

**What's included:**
- Database creation with proper settings
- CLR integration enabled
- Assembly registration with `sp_add_trusted_assembly` (secure method)
- All stored procedures and functions
- Security roles (FastTransfer_Executor, FastBCP_Executor)

### Post-Installation Configuration

After restoring the database (especially from .bak), you **must** configure CLR integration. Two options are available depending on your environment:

#### Option 1: Using sp_add_trusted_assembly (Recommended for Production) 

This approach is **more secure** as it keeps TRUSTWORTHY OFF and doesn't require changing the database owner:

```sql
-- Enable CLR integration
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
GO

EXEC sp_configure 'clr enabled', 1;
RECONFIGURE;
GO

-- Extract the assembly hash and add it to trusted assemblies
DECLARE @hash VARBINARY(64);

SELECT @hash = HASHBYTES('SHA2_512', content)
FROM sys.assembly_files
WHERE assembly_id = (
    SELECT assembly_id 
    FROM sys.assemblies 
    WHERE name = 'FastWrappers_TSQL'
);

EXEC sys.sp_add_trusted_assembly 
    @hash = @hash,
    @description = N'FastWrappers_TSQL Assembly v0.7.0';
GO
```

**Advantages:**
- ✅ TRUSTWORTHY remains OFF (more secure)
- ✅ No need to change database owner
- ✅ Only this specific assembly is trusted

**Note:** The assembly hash changes with each version. When upgrading, you must remove the old hash and add the new one:
```sql
-- Remove old version
EXEC sys.sp_drop_trusted_assembly @hash = <old_hash>;
-- Then run the sp_add_trusted_assembly script above
```

#### Option 2: Using TRUSTWORTHY ON (Quick Setup for Dev/Test)

This approach is simpler but **less secure**. Use only for development/testing environments:

```sql
-- Enable TRUSTWORTHY for signed UNSAFE assemblies
ALTER DATABASE [FastWrappers-TSQL] SET TRUSTWORTHY ON;
GO

-- Enable CLR integration
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;
GO

EXEC sp_configure 'clr enabled', 1;
RECONFIGURE;
GO

-- Set database owner to 'sa' (required for signed UNSAFE assemblies)
EXEC sp_changedbowner 'sa';
GO
```

**Important:** With this method, the `sp_changedbowner 'sa'` command is **critical**. Without it, you will encounter error 0x80FC80F1 when trying to execute the stored procedures.

## Security Roles

The FastWrappers-TSQL database includes two predefined database roles to manage access to the stored procedures:

### 1. **FastTransfer_Executor**

This role grants `EXECUTE` permission on the **xp_RunFastTransfer_secure** stored procedure.

**Usage:**
```sql
-- Add a user to the FastTransfer_Executor role
ALTER ROLE [FastTransfer_Executor] ADD MEMBER [YourUserName];
GO
```

**Purpose:** Allows users to perform data transfers between databases without granting them broader permissions.

### 2. **FastBCP_Executor**

This role grants `EXECUTE` permission on the **xp_RunFastBCP_secure** stored procedure.

**Usage:**
```sql
-- Add a user to the FastBCP_Executor role
ALTER ROLE [FastBCP_Executor] ADD MEMBER [YourUserName];
GO
```

**Purpose:** Allows users to export data to files without granting them broader permissions.

### Combined Access

To grant a user access to both FastTransfer and FastBCP:

```sql
-- Add user to both roles
ALTER ROLE [FastTransfer_Executor] ADD MEMBER [YourUserName];
ALTER ROLE [FastBCP_Executor] ADD MEMBER [YourUserName];
GO
```

**Note:** Users also need access to the **dbo.EncryptString** function to generate encrypted passwords. Consider granting `EXECUTE` permission explicitly if needed:

```sql
GRANT EXECUTE ON dbo.EncryptString TO [YourUserName];
GO
```

## Available Stored Procedures

Once installed and configured, the FastWrappers-TSQL assembly provides the following stored procedures:

### 1. **dbo.EncryptString** - Password Encryption Function
```sql
SELECT dbo.EncryptString('YourPassword')
```
Encrypts passwords using AES-256 encryption. Use this function to generate encrypted passwords for the `@sourcePasswordSecure` and `@targetPasswordSecure` parameters.

**Returns:** Base64-encoded encrypted string

### 2. **dbo.xp_RunFastTransfer_secure** - Data Transfer Wrapper
```sql
EXEC dbo.xp_RunFastTransfer_secure @fastTransferDir = '...', ...
```
Wraps the **FastTransfer** CLI to transfer data between databases with streaming and parallel processing for high performance.

**Key Features:**
- Supports 13 source connection types (ClickHouse, DuckDB, HANA, SQL Server, MySQL, Netezza, Oracle, PostgreSQL, Teradata, ODBC, OLEDB)
- Supports 10 target connection types with bulk loading (clickhousebulk, duckdb, hanabulk, msbulk, mysqlbulk, nzbulk, orabulk, oradirect, pgcopy, teradata)
- Parallel methods: None, Random, DataDriven, RangeId, Ntile, Ctid (PostgreSQL), Physloc (SQL Server), Rowid (Oracle), NZDataSlice (Netezza)
- Automatic column mapping by position or name
- Encrypted connection strings and passwords using AES-256
- Configurable batch sizes and parallelism degree
- Load modes: Append, Truncate
- Work tables support for staging data
- Custom data-driven distribution queries

### 3. **dbo.xp_RunFastBCP_secure** - Data Export Wrapper
```sql
EXEC dbo.xp_RunFastBCP_secure @fastBCPDir = '...', ...
```
Wraps the **FastBCP** CLI to export data from databases to files with streaming and parallel processing for high performance.

**Key Features:**
- Supports 11 connection types (ClickHouse, HANA, SQL Server, MySQL, Netezza, ODBC, OLEDB, Oracle, PostgreSQL, Teradata)
- Multiple output formats: CSV, TSV, JSON, Parquet, BSON, Binary (PostgreSQL COPY), XLSX (Excel)
- Parquet compression codecs: Zstd (default), Snappy, Gzip, Lzo, Lz4, None
- Parallel methods: None, Random, DataDriven, RangeId, Ntile, Timepartition, Ctid (PostgreSQL), Physloc (SQL Server), Rowid (Oracle)
- Cloud storage support: AWS S3, Azure Blob Storage, Azure Data Lake Gen2, Google Cloud Storage, S3-Compatible, OneLake
- Configurable CSV/TSV formatting (delimiter, quotes, date format, decimal separator, boolean format, encoding)
- Encrypted connection strings and passwords using AES-256
- File merge option for parallel exports
- Timestamped output files
- YAML configuration file support

## Logging and Output

### FastTransfer Output

By default, **xp_RunFastTransfer_secure** returns a structured result set with transfer metrics:

| Column | Type | Description |
|--------|------|-------------|
| targetdatabase | nvarchar(128) | Target database name |
| targetSchema | nvarchar(128) | Target schema name |
| targetTable | nvarchar(128) | Target table name |
| TotalRows | bigint | Number of rows transferred |
| TotalColumns | int | Number of columns transferred |
| TotalCells | bigint | Total cells transferred (rows × columns) |
| TotalTimeMs | bigint | Total execution time in milliseconds |
| Status | int | Exit code (0 = success, non-zero = error) |
| StdErr | nvarchar(max) | Error message if Status ≠ 0 |

**Example output:**
```
targetdatabase  targetSchema  targetTable  TotalRows  TotalColumns  TotalCells   TotalTimeMs  Status  StdErr
postgres        public        orders       15000000   9             135000000    27502        0       
```

#### Debug Mode (@debug = 1)

When you set `@debug = 1`, the stored procedure will also output:

1. **The complete command line** being executed (in the Messages tab):
   ```
   FastTransfer Command .\FastTransfer.exe --sourceconnectiontype "mssql" --sourceserver "localhost" ...
   ```
   *(Passwords and connection strings are automatically masked with `<hidden>` for security)*

2. **The full console output (stdout)** from FastTransfer (in the Messages tab):
   - Real-time progress updates
   - Detailed execution logs
   - Performance metrics
   - Any warnings or informational messages

**Example with debug:**
```sql
EXEC dbo.xp_RunFastTransfer_secure
    @fastTransferDir = 'C:\FastTransfer\latest',
    @sourceConnectionType = 'mssql',
    -- ... other parameters ...
    @debug = 1  -- Enable verbose logging
```

## Usage Examples

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
