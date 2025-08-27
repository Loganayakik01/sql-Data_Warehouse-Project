/*

Create the database and schemas

Script purpose :
	This script creates a new database name 'Datawarehouse' after checking if it already exsists.
	If the database exists, it is dropped and recreated additionally , the scropt sets up 3 schemas with in the database: 'Bronze','Silver', and 'Gold'

Warning:
	Running this script will drop the entire 'Datawarehouse' database it it exists.
	All data in the database will be permenently deleted.Proceed with caution.
	and ensure you have proper backup before running this script

*/

--Create Database 'DataWareHouse'

Use master; --System dataBase in SQL server where we can create other databases
Go

--Drop and recreate 'Datawarehouse' database 

IF exists(Select 1 from  sys.databases where name = 'Datawarehouse')
BEGIN 
	ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE Datawarehouse;
END;
GO

--Create the 'Datawarehouse' database

Create Database Datawarehouse;
Go

Use Datawarehouse;
GO

--Creating Schemas

Create Schema Bronze;
Go

Create Schema Silver;
Go

Create Schema Gold;
Go --Its like a sepertor in SQL server
