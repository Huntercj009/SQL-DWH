/*
=============================================================
Database Initialization & Schema Layering Script
=============================================================

Script Name:
    Create DataWarehouse Database with Bronze, Silver, Gold Schemas

Script Purpose:
    This script performs a clean initialization of a SQL Server
    data warehouse environment by:

    1. Checking whether a database named 'DataWarehouse' already exists
    2. If it exists:
        - Forcing all active connections to close
        - Dropping the database entirely
    3. Creating a fresh 'DataWarehouse' database
    4. Creating three logical schemas inside the database:
        - bronze : Raw / ingested data
        - silver : Cleansed & transformed data
        - gold   : Business-ready / aggregated data

Intended Usage:
    - Initial environment setup
    - Non-production refresh (DEV / QA / UAT)
    - Controlled production rebuilds with full backup assurance

WARNING ⚠️:
    - This script is DESTRUCTIVE.
    - If 'DataWarehouse' exists, ALL data will be permanently deleted.
    - Ensure:
        ✔ Proper approvals
        ✔ Full backups are taken
        ✔ No unintended users are connected

Execution Platform:
    Microsoft SQL Server

=============================================================
*/

-- Switch context to the system database
-- Required because CREATE/DROP DATABASE commands
-- cannot be executed inside a user database
USE master;
GO

-------------------------------------------------------------
-- Step 1: Check if the DataWarehouse database already exists
-------------------------------------------------------------
IF EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE name = 'DataWarehouse'
)
BEGIN
    -- Force the database into SINGLE_USER mode
    -- This immediately disconnects all active sessions
    -- ROLLBACK IMMEDIATE cancels open transactions
    ALTER DATABASE DataWarehouse
    SET SINGLE_USER
    WITH ROLLBACK IMMEDIATE;

    -- Drop the database completely
    DROP DATABASE DataWarehouse;
END;
GO

-------------------------------------------------------------
-- Step 2: Create a fresh DataWarehouse database
-------------------------------------------------------------
CREATE DATABASE DataWarehouse;
GO

-------------------------------------------------------------
-- Step 3: Switch context to the newly created database
-------------------------------------------------------------
USE DataWarehouse;
GO

-------------------------------------------------------------
-- Step 4: Create logical schemas for medallion architecture
-------------------------------------------------------------

-- Bronze schema:
-- Stores raw, unvalidated, source-aligned data
-- Typically mirrors source systems with minimal transformation
CREATE SCHEMA bronze;
GO

-- Silver schema:
-- Stores cleansed, standardized, and validated data
-- Business rules, deduplication, and type casting applied here
CREATE SCHEMA silver;
GO

-- Gold schema:
-- Stores analytics-ready, aggregated, and KPI-driven datasets
-- Used directly by BI tools, dashboards, and reporting layers
CREATE SCHEMA gold;
GO
