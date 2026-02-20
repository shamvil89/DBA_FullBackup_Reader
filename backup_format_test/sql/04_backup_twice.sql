-- Same DB backed up twice to two files (determinism test).

USE master;
GO

BACKUP DATABASE BackupFormatTest
TO DISK = '$(BackupPath1)'
WITH INIT, NO_COMPRESSION;
GO

BACKUP DATABASE BackupFormatTest
TO DISK = '$(BackupPath2)'
WITH INIT, NO_COMPRESSION;
GO
