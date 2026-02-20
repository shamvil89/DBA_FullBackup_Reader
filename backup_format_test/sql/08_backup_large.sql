-- Backup the large database (50+ MB) for allocation-identification tests.

USE master;
GO

BACKUP DATABASE BackupFormatTestLarge
TO DISK = '$(BackupPath)'
WITH INIT, NO_COMPRESSION;
GO
