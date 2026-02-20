-- Get allocation info AFTER new rows were inserted.
-- Compare this with the allocation from before to see which pages are new.

USE BackupFormatTestLarge;
GO

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
