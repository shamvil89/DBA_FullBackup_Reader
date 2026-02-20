-- Small update then backup (layout change test).

USE BackupFormatTest;
GO

UPDATE dbo.T1 SET value = 111 WHERE id = 1;
GO

USE master;
GO

BACKUP DATABASE BackupFormatTest
TO DISK = '$(BackupPath)'
WITH INIT, NO_COMPRESSION;
GO
