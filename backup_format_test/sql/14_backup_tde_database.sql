-- Backup the TDE-encrypted database.

BACKUP DATABASE [TDETestDB]
TO DISK = N'$(BackupPath)'
WITH FORMAT, INIT, NAME = N'TDETestDB-Full',
     SKIP, NOREWIND, NOUNLOAD, STATS = 10;
GO

PRINT 'TDE database backup created.';
GO
