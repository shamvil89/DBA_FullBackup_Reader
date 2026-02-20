-- Backup BackupFormatTest with compression.

USE master;
GO

BACKUP DATABASE BackupFormatTest
TO DISK = '$(BackupPath)'
WITH INIT, COMPRESSION;
GO
