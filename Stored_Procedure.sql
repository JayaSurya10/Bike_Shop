USE Diamond_DB
GO

CREATE OR ALTER PROC Sliver_ServerlessSQL @ViewName nvarchar(100)
AS
BEGIN

    DECLARE @statement NVARCHAR(MAX)

    SET @statement = N'CREATE OR ALTER VIEW' + QUOTENAME(@ViewName) + 
    'AS
    SELECT * FROM
    OPENROWSET(
        BULK ''https://bikeshopsynapseaz.dfs.core.windows.net/sliver/production/'+@ViewName+'/'+@ViewName+'/'',
        FORMAT = ''DELTA''
    )  as [result]'

    EXEC sp_executesql @statement
END
Go

