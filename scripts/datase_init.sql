/*
Create database and schemas

Script purpose:
  This script creates a new database named `DATAWAREHOUSE`. The script then creates
  the three schemas necessary for the Medallion Architecture; `BRONZE`, `SILVER`, `GOLD`.

*/

-- Use the super role
use role accountadmin;

-- Create the `DATAWAREHOUSE` database
create database DATAWAREHOUSE;

-- Use DATAWAREHOUSE
use DATAWAREHOUSE;

-- Create the schemas following the Medallion Architecture
create schema BRONZE;

create schema SILVER;

create schema GOLD;
