
-- Sample usage of the xp_RunFastBCP_secure stored procedure
-- EXEC dbo.xp_RunFastBCP_secure
--     @fastBCPDir = 'D:\FastBCP\latest',
--     @connectionType = 'mssql',
--     @server = 'localhost',
--     @user = 'FastUser',
--     @passwordSecure = 'wi1/VHz9s+fp45186iLYYQ==',
--     @database = 'tpch_test',
--     @sourcetable = 'nation',
--     @outputFile = 'nation.csv',
--     @outputDirectory = 'D:\temp\fastbcpoutput\nation',
--     @delimiter = '|',
--     @usequotes = 1,
--     @dateformat = 'yyyy-MM-dd HH24:mm:ss',
--     @encoding = 'utf-8',
--     @method = 'None',
--     @runid = 'test_FastBCP_export_nation'


-- EXEC dbo.xp_RunFastBCP_secure
--     @fastBCPDir = 'D:\FastBCP\latest',
--     @connectionType = 'mssql',
--     @server = 'localhost',
--     @user = 'FastUser',
--     @passwordSecure = 'wi1/VHz9s+fp45186iLYYQ==',
--     @database = 'tpch_test',
--     @query = 'SELECT top 1000 * FROM orders',
--     @outputFile = 'orders_output.csv',
--     @outputDirectory = 'D:\temp\fastbcpoutput\',
--     @delimiter = '|',
--     @usequotes = 1,
--     @dateformat = 'yyyy-MM-dd HH24:mm:ss',
--     @encoding = 'utf-8',
--     @method = 'None',
--     @runid = 'test_FastBCP_export_orders'

-- EXEC dbo.xp_RunFastBCP_secure
--     @fastBCPDir = 'D:\FastBCP\latest',
--     @connectionType = 'mssql',
--     @server = 'localhost',
--     @user = 'FastUser',
--     @passwordSecure = 'wi1/VHz9s+fp45186iLYYQ==',
--     @database = 'tpch_test',
--     @query = 'SELECT top 1000 * FROM orders',
--     @outputFile = 'orders_output.csv',
--     @outputDirectory = 'D:\temp\fastbcpoutput\orders\physloc',
--     @delimiter = '|',
--     @usequotes = 1,
--     @dateformat = 'yyyy-MM-dd HH24:mm:ss',
--     @encoding = 'utf-8',
--     @method = 'Physloc',
--	   @degree = 8,
--     @runid = 'test_FastBCP_export_orders_physloc'

-- EXEC dbo.xp_RunFastBCP_secure
--     @fastBCPDir = 'D:\FastBCP\latest',
--     @connectionType = 'mssql',
--     @server = 'localhost',
--     @user = 'FastUser',
--     @passwordSecure = 'wi1/VHz9s+fp45186iLYYQ==',
--     @database = 'tpch_test',
--     @query = 'select * from (SELECT *, year(o_orderdate)*100+month(o_orderdate) as o_ordermonth FROM orders where o_orderdate >= ''19980101'') src',
--     @outputFile = 'orders_output.parquet',
--     @outputDirectory = 'D:\temp\fastbcpoutput\orders',
--     @delimiter = '|',
--     @usequotes = 1,
--     @dateformat = 'yyyy-MM-dd HH24:mm:ss',
--     @encoding = 'utf-8',
--     @method = 'DataDriven',
--     @distributeKeyColumn = 'o_ordermonth',
--     @runid = 'test_FastBCP_export_orders',
--     @debug=1




CREATE PROCEDURE [dbo].[xp_RunFastBCP_secure]
@fastBCPDir [nvarchar](1000),
@connectionType [nvarchar](30),
@connectStringSecure [nvarchar](4000) = NULL,
@dsn [nvarchar](255) = NULL,
@provider [nvarchar](1000) = NULL,
@server [nvarchar](255),
@user [nvarchar](1000) = NULL,
@passwordSecure [nvarchar](255) = NULL,
@isTrusted [bit] = 0,
@database [nvarchar](1000),
@inputFile [nvarchar](1000) = NULL,
@query [nvarchar](4000) = NULL,
@sourceschema [nvarchar](255) = NULL,
@sourcetable [nvarchar](255) = NULL,
@outputFile [nvarchar](1000) = NULL,
@outputDirectory [nvarchar](2000) = NULL,
@delimiter [nvarchar](10) = NULL,
@usequotes [bit] = 0,
@dateformat [nvarchar](25) = NULL,
@encoding [nvarchar](50) = NULL,
@decimalSeparator [nvarchar](1) = NULL,
@degree [int] = -2,
@method [nvarchar](50) = 'None',
@distributeKeyColumn [nvarchar](1000) = NULL,
@datadrivenquery [nvarchar](4000) = NULL,
@mergeDistributedFile [bit] = 0,
@timestamped [bit] = 0,
@noheader [bit] = 0,
@boolformat [nvarchar](10) = NULL,
@runid [nvarchar](255) = NULL,
@settingsfile [nvarchar](4000) = NULL,
@cloudprofile [nvarchar](2000) = NULL,
@license nvarchar(4000) = NULL,
@loglevel nvarchar(50) = 'Information',
@debug [bit] = 0
WITH EXECUTE AS CALLER
AS
EXTERNAL NAME [FastWrappers_TSQL].[FastWrapper.FastBCPCLR].[RunFastBCP_Secure]
GO