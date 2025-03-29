CREATE ROLE [FastTransfer_Executor]
GO

-- Grant permissions to the role : execute ont the stored procedure xp_RunFastTransfer
GRANT EXECUTE ON xp_RunFastTransfer TO [FastTransfer_Executor];
