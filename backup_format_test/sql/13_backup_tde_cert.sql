-- Backup the TDE certificate and private key.
-- These files are required to restore/read the TDE backup on another server.

USE master;
GO

BACKUP CERTIFICATE TDE_Test_Cert
TO FILE = '$(CertPath)'
WITH PRIVATE KEY (
    FILE = '$(KeyPath)',
    ENCRYPTION BY PASSWORD = '$(KeyPassword)'
);
GO

PRINT 'Certificate and private key exported successfully.';
GO
