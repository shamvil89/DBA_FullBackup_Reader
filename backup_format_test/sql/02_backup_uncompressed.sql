-- Backup BackupFormatTest without compression.
-- Backup path is passed by the runner as sqlcmd variable BackupPath.

USE master;
GO

BACKUP DATABASE BackupFormatTest
TO DISK = '$(BackupPath)'
WITH INIT, NO_COMPRESSION;
GO
