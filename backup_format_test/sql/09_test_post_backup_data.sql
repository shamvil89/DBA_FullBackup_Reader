-- Test scenario: Data added AFTER a backup was taken.
-- This demonstrates what allocation hints from the CURRENT database
-- can recover from an OLDER backup.

USE BackupFormatTestLarge;
GO

-- Step 1: Record current row count BEFORE adding new data
PRINT '=== BEFORE adding new rows ===';
SELECT 'Row count' AS metric, COUNT(*) AS value FROM dbo.LargeTable;
GO

-- Step 2: Add 500 new rows using a loop (each row ~7KB = ~500 new pages)
PRINT '=== Adding 500 new rows ===';
DECLARE @i INT = 1;

WHILE @i <= 500
BEGIN
    INSERT INTO dbo.LargeTable (payload, created)
    VALUES (REPLICATE('y', 7000), SYSUTCDATETIME());
    SET @i = @i + 1;
END
GO

-- Step 3: Show new row count
PRINT '=== AFTER adding new rows ===';
SELECT 'Row count' AS metric, COUNT(*) AS value FROM dbo.LargeTable;
GO

-- Step 4: Show space usage
EXEC sp_spaceused 'dbo.LargeTable';
GO
