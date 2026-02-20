-- Create test database and table for backup format reverse-engineering.
-- Idempotent: safe to run multiple times.

USE master;
GO

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'BackupFormatTest')
BEGIN
    CREATE DATABASE BackupFormatTest;
END
GO

USE BackupFormatTest;
GO

IF OBJECT_ID(N'dbo.T1', N'U') IS NOT NULL
    DROP TABLE dbo.T1;
GO

CREATE TABLE dbo.T1 (
    id    INT          NOT NULL PRIMARY KEY,
    name  NVARCHAR(64) NOT NULL,
    value INT          NULL,
    created DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

INSERT INTO dbo.T1 (id, name, value) VALUES
    (1, N'first',  100),
    (2, N'second', 200),
    (3, N'third',  300);
GO
