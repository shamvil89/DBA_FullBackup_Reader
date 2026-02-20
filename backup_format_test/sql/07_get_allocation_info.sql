-- Output allocation unit / page list for dbo.LargeTable.
-- Run this against the LIVE database (or after restore) to get (file_id, page_id).
-- Use the output as a reference for testing "feed allocation unit" with a .bak file.
-- Requires SQL Server 2012+ (sys.dm_db_database_page_allocations).

USE BackupFormatTestLarge;
GO

-- Use LIMITED mode (faster) - returns all allocated pages for the table.
-- Note: page_type is NULL in LIMITED mode, so we filter by is_allocated only.
SELECT
    allocated_page_file_id AS file_id,
    allocated_page_page_id AS page_id
FROM sys.dm_db_database_page_allocations(
    DB_ID(),
    OBJECT_ID(N'dbo.LargeTable'),
    NULL,
    NULL,
    N'LIMITED'
)
WHERE is_allocated = 1
  AND allocated_page_page_id IS NOT NULL
ORDER BY allocated_page_file_id, allocated_page_page_id;
GO
