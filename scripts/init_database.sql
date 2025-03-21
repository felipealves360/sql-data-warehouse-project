/*
================================================================================================
Create Database and schemas for the Datawarehouse
================================================================================================
Script Purpose:
    This script creates the database ´Datawarehouse´ and the schemas ´bronze´, ´silver´ and ´gold´.
    The ´bronze´ schema is used to store the raw data, the ´silver´ schema is used to store the cleaned data and the ´gold´ schema is used to store the aggregated data.

Warnings:
    - This script will drop the database ´Datawarehouse´ if it already exists.
    - This script will drop the schemas ´bronze´, ´silver´ and ´gold´ if they already exist.
    - This script will delete all data in the database ´Datawarehouse´.
    - This script will delete all data in the schemas ´bronze´, ´silver´ and ´gold´.
*/

USE master;
GO

-- Drop and recreate the ´Datawarehouse´ database
IF EXISTS (SELECT 1
FROM sys.databases
WHERE name = 'Datawarehouse')
BEGIN
    ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE Datawarehouse;
END;
GO

-- Create Database ´Datawarehouse´
CREATE DATABASE Datawarehouse;
GO

USE Datawarehouse;
GO

-- Create schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
