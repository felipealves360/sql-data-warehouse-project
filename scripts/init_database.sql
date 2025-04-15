/*
 ================================================================================================
 Create Database and schemas for the datawarehouse_v2
 ================================================================================================
 Script Purpose:
 This script creates the database ´datawarehouse_v2´ and the schemas ´bronze´, ´silver´ and ´gold´.
 The ´bronze´ schema is used to store the raw data;
 the ´silver´ schema is used to store the cleaned data;
 the ´gold´ schema is used to store the aggregated data.
 */
 
-- Create Database 'datawarehouse_v2'
CREATE DATABASE datawarehouse_v2;
-- Create schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
