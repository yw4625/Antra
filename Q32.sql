/*
32.	Remember the discussion about those two databases from the class, also remember, 
those data models are not perfect. You can always add new columns (but not alter or drop columns) 
to any tables. Suggesting adding Ingested DateTime and Surrogate Key columns. Study the Wide World Importers DW. 
Think the integration schema is the ODS. Come up with a TSQL Stored Procedure driven solution to move 
the data from WWI database to ODS, and then from the ODS to the fact tables and dimension tables. 
By the way, WWI DW is a galaxy schema db. Requirements:
a.	Luckily, we only start with 1 fact: Order. Other facts can be ignored for now.
b.	Add a new dimension: Country of Manufacture. It should be given on top of Stock Items.
c.	Write script(s) and stored procedure(s) for the entire ETL from WWI db to DW.
*/
--Insert into city Staging Table
Insert Into [WideWorldImportersDW].[Integration].[City_Staging]
( [WWI City ID],[City],[State Province],[Country],[Continent],[Sales Territory],[Region]
,[Subregion],[Location],[Latest Recorded Population], [Valid From],[Valid To])

select c.CityID,c.CityName,sp.StateProvinceName,co.CountryName,co.Continent, sp.SalesTerritory,
co.Region, co.Subregion, c.[Location], ISNULL(c.LatestRecordedPopulation,0), CURRENT_TIMESTAMP Validfrom,
DateADD (Day, 7, CURRENT_TIMESTAMP) Validto
from WideWorldImporters.[Application].Cities c
join WideWorldImporters.[Application].StateProvinces sp on c.StateProvinceID = sp.StateProvinceID
join WideWorldImporters.[Application].Countries co on sp.CountryID = co.CountryID

--insert into customer staging-- Declare table variable utilizing the newly created type - MemoryType
DROP TYPE [dbo].[MemoryType]
CREATE TYPE [dbo].[MemoryType]  
    AS TABLE  
    (  
	[Customer Staging Key] [int] PRIMARY KEY NONCLUSTERED,
	[WWI Customer ID] [int] NOT NULL,
	[Customer] [nvarchar](100) COLLATE Latin1_General_100_CI_AS NOT NULL,
	[Bill To Customer] [nvarchar](100) COLLATE Latin1_General_100_CI_AS NOT NULL,
	[Category] [nvarchar](50) COLLATE Latin1_General_100_CI_AS NOT NULL,
	[Buying Group] [nvarchar](50) COLLATE Latin1_General_100_CI_AS NOT NULL,
	[Primary Contact] [nvarchar](50) COLLATE Latin1_General_100_CI_AS NOT NULL,
	[Postal Code] [nvarchar](10) COLLATE Latin1_General_100_CI_AS NOT NULL,
	[Valid From] [datetime2](7) NOT NULL,
	[Valid To] [datetime2](7) NOT NULL
    )  
    WITH  
        (MEMORY_OPTIMIZED = ON);  
GO

DECLARE @InMem dbo.MemoryType;
-- Populate table variable
INSERT into @InMem select cus.CustomerID, cus.CustomerID, cus.CustomerName, cus2.CustomerName BilltoCustomer, cat.CustomerCategoryName, bg.BuyingGroupName,p.FullName, cus.PostalPostalCode,CURRENT_TIMESTAMP Validfrom, DateADD (Day, 7, CURRENT_TIMESTAMP) Validtofrom [WideWorldImporters].[Sales].Customers cusjoin [WideWorldImporters].[Sales].CustomerCategories cat on cus.CustomerCategoryID = cat.CustomerCategoryIDjoin [WideWorldImporters].[Application].People p on p.PersonID = cus.PrimaryContactPersonIDjoin [WideWorldImporters].[Sales].BuyingGroups bg on cus.BuyingGroupID = bg.BuyingGroupIDjoin [WideWorldImporters].[Sales].Customers cus2 on cus2.CustomerID =cus.BillToCustomerID;

-- Populate the destination memory-optimized table
INSERT into [WideWorldImportersDW].[Integration].[Customer_Staging](	[WWI Customer ID],[Customer],[Bill To Customer],[Category],[Buying Group],[Primary Contact],[Postal Code],[Valid From],[Valid To]) 		SELECT [WWI Customer ID],[Customer],[Bill To Customer],[Category],[Buying Group],[Primary Contact],	[Postal Code],[Valid From],[Valid To] FROM @InMem;
GO --Employee StagingDROP TYPE [dbo].[MemoryType]CREATE TYPE [dbo].[MemoryType]  
    AS TABLE  
    (  
	[Employee Staging Key] [int] PRIMARY KEY NONCLUSTERED,
	[WWI Employee ID] [int] NOT NULL,
	[Employee] [nvarchar](50) COLLATE Latin1_General_100_CI_AS NOT NULL,
	[Preferred Name] [nvarchar](50) COLLATE Latin1_General_100_CI_AS NOT NULL,
	[Is Salesperson] [bit] NOT NULL,
	[Photo] [varbinary](max) NULL,
	[Valid From] [datetime2](7) NOT NULL,
	[Valid To] [datetime2](7) NOT NULL
    )  
    WITH  
        (MEMORY_OPTIMIZED = ON);  
GO

DECLARE @InMem dbo.MemoryType;INSERT into @InMem select p.PersonID, p.PersonID,p.FullName,p.PreferredName,p.IsSalesperson,NULL,CURRENT_TIMESTAMP Validfrom,
DateADD (Day, 7, CURRENT_TIMESTAMP) Validto from WideWorldImporters.[Application].People pInsert Into [WideWorldImportersDW].[Integration].[Employee_Staging]
([WWI Employee ID],[Employee],[Preferred Name],[Is Salesperson],[Valid From],[Valid To])
SELECT [WWI Employee ID],[Employee],[Preferred Name],[Is Salesperson],[Valid From],[Valid To] FROM @InMem
-----------------------------------------------------------------------------------------------------------------------------------
--stockitem staging
DROP TYPE [dbo].[MemoryType]CREATE TYPE [dbo].[MemoryType]  
    AS TABLE  
    (  
	[Stock Item Staging Key] [int] PRIMARY KEY NONCLUSTERED,
	[WWI Stock Item ID] [int] NOT NULL,
	[Stock Item] [nvarchar](100) COLLATE Latin1_General_100_CI_AS NOT NULL,
	[Color] [nvarchar](20) COLLATE Latin1_General_100_CI_AS NOT NULL,
	[Selling Package] [nvarchar](50) COLLATE Latin1_General_100_CI_AS NOT NULL,
	[Buying Package] [nvarchar](50) COLLATE Latin1_General_100_CI_AS NOT NULL,
	[Brand] [nvarchar](50) COLLATE Latin1_General_100_CI_AS NOT NULL,
	[Size] [nvarchar](20) COLLATE Latin1_General_100_CI_AS NOT NULL,
	[Lead Time Days] [int] NOT NULL,
	[Quantity Per Outer] [int] NOT NULL,
	[Is Chiller Stock] [bit] NOT NULL,
	[Barcode] [nvarchar](50) COLLATE Latin1_General_100_CI_AS NULL,
	[Tax Rate] [decimal](18, 3) NOT NULL,
	[Unit Price] [decimal](18, 2) NOT NULL,
	[Recommended Retail Price] [decimal](18, 2) NULL,
	[Typical Weight Per Unit] [decimal](18, 3) NOT NULL,
	[Photo] [varbinary](max) NULL,
	[Valid From] [datetime2](7) NOT NULL,
	[Valid To] [datetime2](7) NOT NULL
    )  
    WITH  
        (MEMORY_OPTIMIZED = ON);  
GO

DECLARE @InMem dbo.MemoryType;
INSERT into @InMem select a.StockItemID, a.StockItemID, a.StockItemName, d.ColorName, b.PackageTypeName, c.PackageTypeName, isnull (a.Brand,''), isnull (a.Size,''),a.LeadTimeDays,a.QuantityPerOuter, a.IsChillerStock, a.Barcode, a.TaxRate, a.UnitPrice,a.RecommendedRetailPrice, a.TypicalWeightPerUnit,a.Photo, CURRENT_TIMESTAMP Validfrom,
DateADD (Day, 7, CURRENT_TIMESTAMP) Validtofrom [WideWorldImporters].[Warehouse].StockItems a inner join [WideWorldImporters].[Warehouse].PackageTypes b on a.UnitPackageID = b.PackageTypeIDinner join [WideWorldImporters].[Warehouse].PackageTypes c on a.OuterPackageID = c.PackageTypeIDinner join [WideWorldImporters].[Warehouse].Colors d on a.ColorID = d.ColorID

Insert Into [WideWorldImportersDW].[Integration].[StockItem_Staging]([WWI Stock Item ID],[Stock Item],[Color],[Selling Package],[Buying Package],[Brand],[Size],[Lead Time Days] ,[Quantity Per Outer],[Is Chiller Stock],[Barcode],[Tax Rate],[Unit Price],[Recommended Retail Price],[Typical Weight Per Unit],[Photo],[Valid From],[Valid To])select [WWI Stock Item ID],[Stock Item],[Color],[Selling Package],[Buying Package],[Brand],[Size],[Lead Time Days] ,[Quantity Per Outer],[Is Chiller Stock],[Barcode],[Tax Rate],[Unit Price],[Recommended Retail Price],[Typical Weight Per Unit],[Photo],[Valid From],[Valid To] FROM @InMem

--order staging
DROP TYPE [dbo].[MemoryType]
CREATE TYPE [dbo].[MemoryType]  
    AS TABLE  
    (  
	[Order Staging Key] [bigint] PRIMARY KEY NONCLUSTERED,
	[City Key] [int] NULL,
	[Customer Key] [int] NULL,
	[Stock Item Key] [int] NULL,
	[Order Date Key] [date] NULL,
	[Picked Date Key] [date] NULL,
	[Salesperson Key] [int] NULL,
	[Picker Key] [int] NULL,
	[WWI Order ID] [int] NULL,
	[WWI Backorder ID] [int] NULL,
	[Description] [nvarchar](100) COLLATE Latin1_General_100_CI_AS NULL,
	[Package] [nvarchar](50) COLLATE Latin1_General_100_CI_AS NULL,
	[Quantity] [int] NULL,
	[Unit Price] [decimal](18, 2) NULL,
	[Tax Rate] [decimal](18, 3) NULL,
	[Total Excluding Tax] [decimal](18, 2) NULL,
	[Tax Amount] [decimal](18, 2) NULL,
	[Total Including Tax] [decimal](18, 2) NULL,
	[Lineage Key] [int] NULL,
	[WWI City ID] [int] NULL,
	[WWI Customer ID] [int] NULL,
	[WWI Stock Item ID] [int] NULL,
	[WWI Salesperson ID] [int] NULL,
	[WWI Picker ID] [int] NULL,
	[Last Modified When] [datetime2](7) NULL
    )  
    WITH  
        (MEMORY_OPTIMIZED = ON);  
GO

DECLARE @InMem dbo.MemoryType;
INSERT into @InMem ([Order Staging Key],[WWI City ID],[WWI Customer ID],[WWI Stock Item ID],[Order Date Key],[Picked Date Key]
,[WWI Salesperson ID],[WWI Picker ID],[WWI Order ID],[Description]
,[Package],[Quantity],[Unit Price],[Tax Rate],[Total Excluding Tax]
,[Tax Amount],[Total Including Tax])

select ROW_NUMBER() OVER(ORDER BY o.OrderID ASC), ci.CityID, o.CustomerID, si.StockItemID,  o.OrderDate, CONVERT(date, ol.PickingCompletedWhen) PickupDate, 
o.SalespersonPersonID, o.PickedByPersonID,
o.OrderID, ol.[Description], 'Each' Package, ol.Quantity,ol.UnitPrice,ol.TaxRate,
(ol.Quantity*ol.UnitPrice) TotalExcludingTax, (ol.Quantity*ol.UnitPrice)*(ol.TaxRate/100) TaxAmount, 
(ol.Quantity*ol.UnitPrice)*(1+ol.TaxRate/100)TotalIncludeTax
from WideWorldImporters.Sales.Orders o
join WideWorldImporters.Sales.OrderLines ol on  o.OrderID = ol.OrderID
join WideWorldImporters.Warehouse.StockItems si on si.StockItemID = ol.StockItemID
JOIN WideWorldImporters.Sales.Customers cus ON cus.CustomerID = o.CustomerID
JOIN WideWorldImporters.[Application].Cities ci ON ci.CityID = cus.DeliveryCityID

Insert Into [WideWorldImportersDW].[Integration].Order_Staging 
([WWI City ID],[WWI Customer ID],[WWI Stock Item ID],[Order Date Key],[Picked Date Key]
,[WWI Salesperson ID],[WWI Picker ID],[WWI Order ID],[Description]
,[Package],[Quantity],[Unit Price],[Tax Rate],[Total Excluding Tax]
,[Tax Amount],[Total Including Tax])
select [WWI City ID],[WWI Customer ID],[WWI Stock Item ID],[Order Date Key],[Picked Date Key]
,[WWI Salesperson ID],[WWI Picker ID],[WWI Order ID],[Description]
,[Package],[Quantity],[Unit Price],[Tax Rate],[Total Excluding Tax]
,[Tax Amount],[Total Including Tax] from @InMem

GO;
------------------------END of Intergration Now all in DW table-----------------------------------
--update intergation to match the key
--match city key
UPDATE [WideWorldImportersDW].[Integration].Order_Staging
SET [WideWorldImportersDW].[Integration].Order_Staging.[City Key] = c.[City Key]
FROM [WideWorldImportersDW].[Integration].Order_Staging os
INNER JOIN [WideWorldImportersDW].Dimension.City C
ON c.[WWI City ID] = os.[WWI City ID];

--match stock item key
UPDATE [WideWorldImportersDW].[Integration].Order_Staging
SET [WideWorldImportersDW].[Integration].Order_Staging.[Stock Item Key] = si.[Stock Item Key]
FROM [WideWorldImportersDW].[Integration].Order_Staging os
INNER JOIN [WideWorldImportersDW].Dimension.[Stock Item] si
ON si.[WWI Stock Item ID] = os.[WWI Stock Item ID];

--match sales person
UPDATE [WideWorldImportersDW].[Integration].Order_Staging
SET [WideWorldImportersDW].[Integration].Order_Staging.[Salesperson Key] = e.[Employee Key]
FROM [WideWorldImportersDW].[Integration].Order_Staging os
INNER JOIN [WideWorldImportersDW].Dimension.Employee e
ON e.[WWI Employee ID] = os.[WWI Salesperson ID];

--match picker 
UPDATE [WideWorldImportersDW].[Integration].Order_Staging
SET [WideWorldImportersDW].[Integration].Order_Staging.[Picker Key] = e.[Employee Key]
FROM [WideWorldImportersDW].[Integration].Order_Staging os
INNER JOIN [WideWorldImportersDW].Dimension.Employee e
ON e.[WWI Employee ID] = os.[WWI Picker ID];

--
Insert Into WideWorldImportersDW.Fact.[Order]([City Key],[Customer Key],[Stock Item Key]
,[Order Date Key],[Picked Date Key],[Salesperson Key],[Picker Key],[WWI Order ID],[WWI Backorder ID]
,[Description],[Package],[Quantity],[Unit Price],[Tax Rate],[Total Excluding Tax],[Tax Amount],[Total Including Tax],[Lineage Key])

select [City Key],0 [Customer Key],[Stock Item Key],[Order Date Key],[Picked Date Key],
[Salesperson Key],[Picker Key],[WWI Order ID],[WWI Backorder ID],[Description],[Package],[Quantity],[Unit Price],[Tax Rate],
[Total Excluding Tax],[Tax Amount],[Total Including Tax], 0 [Lineage Key]
from Integration.Order_Staging 

--clear table
delete from Integration.City_Staging
delete from Integration.Customer_Staging
delete from Integration.Employee_Staging
delete from Integration.StockItem_Staging
delete from Integration.Order_Staging