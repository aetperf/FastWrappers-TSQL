﻿CREATE PROCEDURE [dbo].[xp_RunFastTransfer]
	@fastTransferDir [nvarchar](max),
	@sourceConnectionType [nvarchar](30),
	@sourceConnectString [nvarchar](4000) = NULL,
	@sourceServer [nvarchar](255),
	@sourceDSN [nvarchar](255) = NULL,
	@sourceProvider [nvarchar](1000) = NULL,
	@isSourceTrusted [bit] = 0,
	@sourceUser [nvarchar](1000) = NULL,
	@sourcePassword [nvarchar](255) = NULL,
	@sourceDatabase [nvarchar](1000),
	@fileInput [nvarchar](4000) = NULL,
	@query [nvarchar](4000) = NULL,
	@sourceSchema [nvarchar](255) = NULL,
	@sourceTable [nvarchar](255) = NULL,
	@targetConnectionType [nvarchar](30),
	@targetConnectString [nvarchar](4000) = NULL,
	@targetServer [nvarchar](255),
	@isTargetTrusted [bit] = 0,
	@targetUser [nvarchar](1000) = NULL,
	@targetPassword [nvarchar](255) = NULL,
	@targetDatabase [nvarchar](255),
	@targetSchema [nvarchar](255),
	@targetTable [nvarchar](255),
	@loadMode [nvarchar](50),
	@batchSize [int] = 1048576,
	@method [nvarchar](50) = 'None',
	@distributeKeyColumn [nvarchar](255) = NULL,
	@degree [int] = 4,
	@mapmethod [nvarchar](50) = 'Position',
	@runId [nvarchar](255) = NULL,
	@settingsFile [nvarchar](4000) = NULL,
	@debug [bit] = 0
WITH EXECUTE AS CALLER
AS
EXTERNAL NAME [FastWrappers_TSQL].[FastWrapper.FastTransferCLR].[RunFastTransfer]
GO
