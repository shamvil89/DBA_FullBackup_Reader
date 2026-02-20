-- Backup the large database AFTER new rows were added.
-- This creates backup_large_v2.bak with the additional 500 rows.

BACKUP DATABASE [BackupFormatTestLarge]
TO DISK = N'$(BackupPath)'
WITH FORMAT, INIT, NAME = N'BackupFormatTestLarge-Full-V2',
     SKIP, NOREWIND, NOUNLOAD, STATS = 10;
GO
