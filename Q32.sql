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

------------------------Start Intergration----------------------------
--order staging
DROP TYPE [dbo].[MemoryType]

Delete from [Integration].Order_Staging
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

--select *from [WideWorldImportersDW].[Integration].Order_Staging 
--delete from [WideWorldImportersDW].[Integration].Order_Staging 
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
GO

--Insert into fact table
Insert Into WideWorldImportersDW.Fact.[Order]([City Key],[Customer Key],[Stock Item Key]
,[Order Date Key],[Picked Date Key],[Salesperson Key],[Picker Key],[WWI Order ID],[WWI Backorder ID]
,[Description],[Package],[Quantity],[Unit Price],[Tax Rate],[Total Excluding Tax],[Tax Amount],[Total Including Tax],[Lineage Key])

select [City Key],0 [Customer Key],[Stock Item Key],[Order Date Key],[Picked Date Key],
[Salesperson Key],[Picker Key],[WWI Order ID],[WWI Backorder ID],[Description],[Package],[Quantity],[Unit Price],[Tax Rate],
[Total Excluding Tax],[Tax Amount],[Total Including Tax], 0 [Lineage Key]
from Integration.Order_Staging 

--clear table
delete from [Integration].Order_Staging
