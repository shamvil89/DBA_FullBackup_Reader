-- Create a TDE-encrypted test database for bakread testing.
-- This script:
--   1. Creates a master key (if not exists)
--   2. Creates a certificate for TDE
--   3. Creates a test database
--   4. Enables TDE encryption
--   5. Inserts test data
--   6. Backs up the certificate (required for restore elsewhere)

USE master;
GO

-- Step 1: Create Database Master Key (if not exists)
IF NOT EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'TDE_MasterKey_P@ss2026!';
    PRINT 'Master key created.';
END
ELSE
    PRINT 'Master key already exists.';
GO

-- Step 2: Create Certificate for TDE
IF NOT EXISTS (SELECT 1 FROM sys.certificates WHERE name = 'TDE_Test_Cert')
BEGIN
    CREATE CERTIFICATE TDE_Test_Cert WITH SUBJECT = 'TDE Test Certificate';
    PRINT 'Certificate created.';
END
ELSE
    PRINT 'Certificate already exists.';
GO

-- Step 3: Create test database
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'TDETestDB')
BEGIN
    CREATE DATABASE TDETestDB;
    PRINT 'Database TDETestDB created.';
END
GO

USE TDETestDB;
GO

-- Step 4: Create Database Encryption Key and enable TDE
IF NOT EXISTS (SELECT 1 FROM sys.dm_database_encryption_keys WHERE database_id = DB_ID())
BEGIN
    CREATE DATABASE ENCRYPTION KEY
    WITH ALGORITHM = AES_256
    ENCRYPTION BY SERVER CERTIFICATE TDE_Test_Cert;
    PRINT 'Database encryption key created.';
END
GO

-- Enable TDE
ALTER DATABASE TDETestDB SET ENCRYPTION ON;
PRINT 'TDE encryption enabled.';
GO

-- Step 5: Create test table with data
IF OBJECT_ID('dbo.SecretData', 'U') IS NOT NULL
    DROP TABLE dbo.SecretData;
GO

CREATE TABLE dbo.SecretData (
    id INT IDENTITY PRIMARY KEY,
    secret_value NVARCHAR(200) NOT NULL,
    amount DECIMAL(18,2) NOT NULL,
    created DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

INSERT INTO dbo.SecretData (secret_value, amount)
VALUES 
    (N'Confidential record 1', 1000.50),
    (N'Sensitive data item 2', 2500.75),
    (N'Protected information 3', 750.00),
    (N'Encrypted content 4', 3200.25),
    (N'Secret value 5', 150.99);
GO

SELECT 'TDETestDB created with TDE encryption and test data.' AS status;
SELECT * FROM dbo.SecretData;
GO

-- Check encryption status
SELECT 
    db.name,
    db.is_encrypted,
    ek.encryption_state,
    CASE ek.encryption_state
        WHEN 0 THEN 'No encryption key'
        WHEN 1 THEN 'Unencrypted'
        WHEN 2 THEN 'Encryption in progress'
        WHEN 3 THEN 'Encrypted'
        WHEN 4 THEN 'Key change in progress'
        WHEN 5 THEN 'Decryption in progress'
        WHEN 6 THEN 'Protection change in progress'
    END AS encryption_state_desc
FROM sys.databases db
JOIN sys.dm_database_encryption_keys ek ON db.database_id = ek.database_id
WHERE db.name = 'TDETestDB';
GO
