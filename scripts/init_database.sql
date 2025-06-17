/*
 create  the  data warehouse  and  different  schemas bronze, silver, and  gold  for  three  datalayers  for a zone
*/

use master;
 IF  EXISTS(select  1  from  databases where name='Datawarehouse')
   BEGIN
   ALTER  database DataWarehouse  set  SINGLE_USER  WITH  ROLLBACK IMMEDIATE;
   DROP DATABASE Datawarehouse;

   END
   
create  database DataWarehouse;
USE DataWarehouse;
create schema  bronze;
GO
create schema  silver;
GO
create schema  gold;
