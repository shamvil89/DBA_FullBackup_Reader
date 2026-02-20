-- Create a database with at least 50 MB of table data for allocation-unit tests.
-- Table: dbo.LargeTable with ~7 KB per row; we insert enough rows to exceed 50 MB.

USE master;
GO

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'BackupFormatTestLarge')
BEGIN
    CREATE DATABASE BackupFormatTestLarge;
END
GO

USE BackupFormatTestLarge;
GO

IF OBJECT_ID(N'dbo.LargeTable', N'U') IS NOT NULL
    DROP TABLE dbo.LargeTable;
GO

-- Row size ~7000 bytes so we get roughly one data page per row (8 KB pages).
CREATE TABLE dbo.LargeTable (
    id      BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    payload CHAR(7000) NOT NULL DEFAULT REPLICATE('x', 7000),
    created DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- Insert enough rows for ~50+ MB: 50*1024*1024 / 7000 ~= 7500 rows.
DECLARE @i INT = 0;
WHILE @i < 7500
BEGIN
    INSERT INTO dbo.LargeTable (payload) VALUES (REPLICATE('x', 7000));
    SET @i = @i + 1;
END
GO

-- Report size
EXEC sp_spaceused N'dbo.LargeTable';
GO
