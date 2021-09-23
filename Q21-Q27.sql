/* 21.	Create a new table called ods.Orders. Create a stored procedure, with proper error handling and transactions, 
that input is a date; when executed, it would find orders of that day, calculate order total, and save the information 
(order id, order date, order total, customer id) into the new table. 
If a given date is already existing in the new table, throw an error and roll back. 
Execute the stored procedure 5 times using different dates.  */

CREATE TABLE ods.orders (
OrderID int,
OrderDate date,
OrderTotal decimal(18, 2)	,
CustomerID int,
);

Create PROCEDURE inputdate (@inputdate DATE) 
--Alter PROCEDURE inputdate (@inputdate DATE) 
AS
BEGIN TRY
	BEGIN TRANSACTION;  
		IF EXISTS (SELECT * from ods.orders where OrderDate= @inputdate)
			RAISERROR('Date Already Exists', 16, 1)
		ELSE 
			with Orderinfo(OrderID, OrderTotal)
			AS(SELECT ol.OrderID,  sum(ol.Quantity*ol.UnitPrice*(1+ol.TaxRate/100)) OrderTotal from Sales.OrderLines ol group by ol.OrderID)

			INSERT into ods.orders (OrderID,OrderDate,OrderTotal,CustomerID )select oi.OrderID,o.OrderDate,oi.OrderTotal,o.CustomerID 
			from Orderinfo oi join sales.Orders o on oi.OrderID = o.OrderID 
			where o.OrderDate= @inputdate
			COMMIT
END TRY

BEGIN CATCH
	IF @@TRANCOUNT > 0
		ROLLBACK
	DECLARE @ErrorMessage NVARCHAR(4000), @ErrorSeverity INT;
	SELECT @ErrorMessage = ERROR_MESSAGE(),@ErrorSeverity = ERROR_SEVERITY();
	RAISERROR(@ErrorMessage, @ErrorSeverity, 1);
END CATCH;

EXEC inputdate '2013-01-10';EXEC inputdate '2013-01-11';EXEC inputdate '2013-01-12';
EXEC inputdate '2013-01-13';EXEC inputdate '2013-01-14';

--For Test
select * from ods.orders
Delete from ods.orders
----------------------------------------------------------------------------------


/* 
22.	Create a new table called ods.StockItem. It has following columns: 
[StockItemID], [StockItemName] ,[SupplierID] ,[ColorID] ,[UnitPackageID] ,[OuterPackageID] ,[Brand] ,[Size] ,
[LeadTimeDays] ,[QuantityPerOuter] ,[IsChillerStock] ,[Barcode] ,[TaxRate]  ,[UnitPrice],[RecommendedRetailPrice] ,
[TypicalWeightPerUnit] ,[MarketingComments]  ,
[InternalComments], [CountryOfManufacture], [Range], [Shelflife]. Migrate all the data in the original stock item table.
 */
 
 CREATE TABLE ods.StockItem (
    StockItemID int,
    StockItemName nvarchar(100),
	SupplierID int,ColorID int,
	UnitPackageID int,OuterPackageID int,
	Brand nvarchar(50),Size nvarchar(20),	
	LeadTimeDays int,QuantityPerOuter int,
	IsChillerStock bit,
	Barcode nvarchar(50),
	TaxRate decimal(18, 3),UnitPrice decimal(18, 2),
	RecommendedRetailPrice decimal(18, 2),TypicalWeightPerUnit decimal(18, 3),
	MarketingComments nvarchar(MAX), InternalComments nvarchar(MAX),
	CountryOfManufacture nvarchar(50),[Range] nvarchar(50),Shelflife nvarchar(50)
);

INSERT Into ods.StockItem([StockItemID], [StockItemName] ,[SupplierID] ,[ColorID] ,[UnitPackageID] ,[OuterPackageID] ,[Brand] ,[Size] ,
[LeadTimeDays] ,[QuantityPerOuter] ,[IsChillerStock] ,[Barcode] ,[TaxRate]  ,[UnitPrice],[RecommendedRetailPrice] ,
[TypicalWeightPerUnit] ,[MarketingComments],[InternalComments], [CountryOfManufacture], [Range], [Shelflife])

select l1.[StockItemID], l1.[StockItemName] ,l1.[SupplierID] ,l1.[ColorID] ,l1.[UnitPackageID] ,l1.[OuterPackageID],l1.[Brand] ,l1.[Size] ,
l1.[LeadTimeDays] ,l1.[QuantityPerOuter] ,l1.[IsChillerStock] ,l1.[Barcode] ,l1.[TaxRate] ,l1.[UnitPrice],l1.[RecommendedRetailPrice] ,
l1.[TypicalWeightPerUnit] ,l1.[MarketingComments],l1.[InternalComments],l1.[origin], l1.[Range], l1.[Shelflife] from
(SELECT *, JSON_VALUE(si.CustomFields,'$.CountryOfManufacture') AS origin, 
JSON_VALUE(si.CustomFields,'$.Range') AS [Range],
JSON_VALUE(si.CustomFields,'$.ShelfLife') AS ShelfLife
FROM Warehouse.StockItems si) l1

select *from ods.StockItem

/* 
23.	Rewrite your stored procedure in (21). Now with a given date, it should wipe out all the 
order data prior to the input date and load the order data that was placed in the next 7 days following the input date.
*/
select * from ods.orders

Create PROCEDURE inputdate_load7days (@inputdate DATE) 
--Alter PROCEDURE inputdate_load7days (@inputdate DATE) 
AS

DELETE FROM ods.orders WHERE OrderDate<@inputdate;

with Orderinfo(OrderID, OrderTotal)
AS(SELECT ol.OrderID,  sum(ol.Quantity*ol.UnitPrice*(1+ol.TaxRate/100)) OrderTotal from Sales.OrderLines ol group by ol.OrderID)

INSERT into ods.orders (OrderID,OrderDate,OrderTotal,CustomerID )select oi.OrderID,o.OrderDate,oi.OrderTotal,o.CustomerID 
from Orderinfo oi join sales.Orders o on oi.OrderID = o.OrderID 
where o.OrderDate between @inputdate AND DATEADD(day, 7,@inputdate)

EXEC inputdate_load7days '2013-01-01';
select * from ods.orders
--24
/*Looks like that it is our missed purchase orders. 
Migrate these data into Stock Item, Purchase Order and Purchase Order Lines tables. Of course, save the script.*/

declare @json nvarchar(max) = 
'
{
   "PurchaseOrders":[
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"7",
         "UnitPackageId":"1",
         "OuterPackageId":[
            6,
            7
         ],
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-01",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"WWI2308"
      },
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"5",
         "UnitPackageId":"1",
         "OuterPackageId":"7",
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-25",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"269622390"
      }
   ]
}'


Insert into Warehouse.Stockitems (
StockItemName, SupplierID, UnitPackageID, OuterPackageId, Brand, LeadTimeDays,
QuantityPerOuter, TaxRate,UnitPrice,RecommendedRetailPrice,TypicalWeightPerUnit, CustomFields, 
IsChillerStock,LastEditedBy
)

select
Concat (StockItemName, ' By supplier ',CAST(SupplierID AS varchar), ' Package ',CAST(OuterPackageId AS varchar) ), 
SupplierID, UnitPackageID, OuterPackageId, Brand, LeadTimeDays,
QuantityPerOuter, TaxRate,UnitPrice,RecommendedRetailPrice,TypicalWeightPerUnit,
(select CountryOrigin, [Range] FOR JSON PATH) CustomFields, 0, 1

from
(
SELECT *
FROM OpenJson(@json)
WITH (   
              StockItemName nvarchar(100)				'$.PurchaseOrders[0].StockItemName',  
              SupplierID    INT							'$.PurchaseOrders[0].Supplier',  
              UnitPackageID INT							'$.PurchaseOrders[0].UnitPackageId',  
              OuterPackageId INT						'$.PurchaseOrders[0].OuterPackageId[0]',  
			  Brand nvarchar(50)						'$.PurchaseOrders[0].Brand', 
			  LeadTimeDays INT							'$.PurchaseOrders[0].LeadTimeDays', 
			  QuantityPerOuter INT						'$.PurchaseOrders[0].QuantityPerOuter', 
			  TaxRate 		decimal(18, 3)				'$.PurchaseOrders[0].TaxRate', 
			  UnitPrice 	decimal(18, 2)				'$.PurchaseOrders[0].UnitPrice', 
			  RecommendedRetailPrice decimal(18, 2)		'$.PurchaseOrders[0].RecommendedRetailPrice', 
			  TypicalWeightPerUnit decimal(18, 3)		'$.PurchaseOrders[0].TypicalWeightPerUnit',
			  CountryOrigin nvarchar(50)				'$.PurchaseOrders[0].CountryOfManufacture',
			  [Range]		nvarchar(50)				'$.PurchaseOrders[0].Range'
 )
UNION ALL
SELECT * FROM OpenJson(@json)
WITH (   
              StockItemName nvarchar(100)				'$.PurchaseOrders[0].StockItemName',  
              SupplierID    INT							'$.PurchaseOrders[0].Supplier',  
              UnitPackageID INT							'$.PurchaseOrders[0].UnitPackageId',  
              OuterPackageId INT						'$.PurchaseOrders[0].OuterPackageId[1]',  
			  Brand nvarchar(50)						'$.PurchaseOrders[0].Brand', 
			  LeadTimeDays INT							'$.PurchaseOrders[0].LeadTimeDays', 
			  QuantityPerOuter INT						'$.PurchaseOrders[0].QuantityPerOuter', 
			  TaxRate 		decimal(18, 3)				'$.PurchaseOrders[0].TaxRate', 
			  UnitPrice 	decimal(18, 2)				'$.PurchaseOrders[0].UnitPrice', 
			  RecommendedRetailPrice decimal(18, 2)		'$.PurchaseOrders[0].RecommendedRetailPrice', 
			  TypicalWeightPerUnit decimal(18, 3)		'$.PurchaseOrders[0].TypicalWeightPerUnit',
			  CountryOrigin nvarchar(50)				'$.PurchaseOrders[0].CountryOfManufacture',
			  [Range]		nvarchar(50)				'$.PurchaseOrders[0].Range'
 )

UNION ALL
SELECT * FROM OpenJson(@json)
WITH (   
              StockItemName nvarchar(100)				'$.PurchaseOrders[1].StockItemName',  
              SupplierID    INT							'$.PurchaseOrders[1].Supplier',  
              UnitPackageID INT							'$.PurchaseOrders[1].UnitPackageId',  
              OuterPackageId INT						'$.PurchaseOrders[1].OuterPackageId',  
			  Brand nvarchar(50)						'$.PurchaseOrders[1].Brand', 
			  LeadTimeDays INT							'$.PurchaseOrders[1].LeadTimeDays', 
			  QuantityPerOuter INT						'$.PurchaseOrders[1].QuantityPerOuter', 
			  TaxRate 		decimal(18, 3)				'$.PurchaseOrders[1].TaxRate', 
			  UnitPrice 	decimal(18, 2)				'$.PurchaseOrders[1].UnitPrice', 
			  RecommendedRetailPrice decimal(18, 2)		'$.PurchaseOrders[1].RecommendedRetailPrice', 
			  TypicalWeightPerUnit decimal(18, 3)		'$.PurchaseOrders[1].TypicalWeightPerUnit',
			  CountryOrigin nvarchar(50)				'$.PurchaseOrders[1].CountryOfManufacture',
			  [Range]		nvarchar(50)				'$.PurchaseOrders[1].Range'
 )
 )l1

--------------------------------------------------------------------------------------------

declare @json nvarchar(max) = 
'
{
   "PurchaseOrders":[
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"7",
         "UnitPackageId":"1",
         "OuterPackageId":[
            6,
            7
         ],
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-01",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"WWI2308"
      },
      {
         "StockItemName":"Panzer Video Game",
         "Supplier":"5",
         "UnitPackageId":"1",
         "OuterPackageId":"7",
         "Brand":"EA Sports",
         "LeadTimeDays":"5",
         "QuantityPerOuter":"1",
         "TaxRate":"6",
         "UnitPrice":"59.99",
         "RecommendedRetailPrice":"69.99",
         "TypicalWeightPerUnit":"0.5",
         "CountryOfManufacture":"Canada",
         "Range":"Adult",
         "OrderDate":"2018-01-25",
         "DeliveryMethod":"Post",
         "ExpectedDeliveryDate":"2018-02-02",
         "SupplierReference":"269622390"
      }
   ]
}'

Insert into Purchasing.PurchaseOrders (SupplierID,OrderDate,DeliveryMethodID, ContactPersonID,IsOrderFinalized, LastEditedBy)

select SupplierID, OrderDate, 1, 1, 1, 1
from(SELECT *FROM OpenJson(@json)
WITH (   
              SupplierID    INT							'$.PurchaseOrders[0].Supplier',  
			  OrderDate		DATE						'$.PurchaseOrders[0].OrderDate',
			  DeliveryMethod nvarchar(50)				'$.PurchaseOrders[0].DeliveryMethod',
			  ExpectedDeliveryDate		DATE			'$.PurchaseOrders[0].ExpectedDeliveryDate'
 )
UNION ALL
SELECT * FROM OpenJson(@json)
WITH (   
              SupplierID    INT							'$.PurchaseOrders[0].Supplier',  
			  OrderDate		DATE						'$.PurchaseOrders[0].OrderDate',
			  DeliveryMethod nvarchar(50)				'$.PurchaseOrders[0].DeliveryMethod',
			  ExpectedDeliveryDate		DATE			'$.PurchaseOrders[0].ExpectedDeliveryDate'
 )

UNION ALL
SELECT * FROM OpenJson(@json)
WITH (   
              SupplierID    INT							'$.PurchaseOrders[1].Supplier',  
			  OrderDate		DATE						'$.PurchaseOrders[1].OrderDate',
			  DeliveryMethod nvarchar(50)				'$.PurchaseOrders[1].DeliveryMethod',
			  ExpectedDeliveryDate		DATE			'$.PurchaseOrders[1].ExpectedDeliveryDate'
 )
 )l1


--25 Revisit your answer in (19). Convert the result in JSON string and save it to the server using TSQL FOR JSON PATH.
create view stockbygroup as

select * from(
SELECT l1.StockGroupName,L1.stockyear,ABS(SUM(L1.Quantity)) Quant FROM
(
SELECT sg.StockGroupName, year(st.TransactionOccurredWhen) as stockyear,st.Quantity FROM Warehouse.StockGroups sg
join Warehouse.StockItemStockGroups sisg on sg.StockGroupID = sisg.StockGroupID
join Warehouse.StockItems si on si.StockItemID = sisg.StockItemID
join Warehouse.StockItemTransactions st on st.StockItemID = si.StockItemID and ST.Quantity<0
) L1 GROUP BY L1.stockyear, L1.StockGroupName) l2

pivot 
(
sum([Quant])
for StockGroupName in([Novelty Items],[Clothing],[Mugs],[T-Shirts],[Computing Novelties],[USB Novelties],
[Furry Footwear],[Toys],[Packaging Materials]) 
)As pvi

--25
select * from stockbygroup order by stockyear FOR JSON Path 
--26.	Revisit your answer in (19). Convert the result into 
--an XML string and save it to the server using TSQL FOR XML PATH.
select * from stockbygroup order by stockyear FOR XML AUTO 

/*
27.	Create a new table called ods.ConfirmedDeviveryJson with 3 columns (id, date, value) . 
Create a stored procedure, input is a date. The logic would load invoice information (all columns) as well as 
invoice line information (all columns) and forge them into a JSON string and then insert into the new table just created. 
Then write a query to run the stored procedure for each DATE that customer id 1 got something delivered to him.
*/
-- cross apply
CREATE TABLE ods.ConfirmedDeviveryJson (
    DeviveryID int IDENTITY(1,1) PRIMARY KEY,
    DeviveryDate Date,
	Deviveryvalue nvarchar(MAX)
);

--DELETE FROM ods.ConfirmedDeviveryJson;

Create PROCEDURE loadinvoice (@inputdate DATE) 
--ALTER PROCEDURE loadinvoice (@inputdate DATE) 
AS
INSERT INTO ods.ConfirmedDeviveryJson (DeviveryDate, Deviveryvalue) 
select l1.InvoiceDate, (select l1.[InvoiceID],l1.[CustomerID],l1.[BillToCustomerID],l1.[OrderID],l1.[DeliveryMethodID],
l1.[ContactPersonID],l1.[AccountsPersonID],l1.[SalespersonPersonID],l1.[PackedByPersonID],
l1.[InvoiceDate],l1.[CustomerPurchaseOrderNumber],l1.[IsCreditNote],l1.[CreditNoteReason],l1.[Comments],l1.[DeliveryInstructions],l1.[InternalComments]
,l1.[TotalDryItems],l1.[TotalChillerItems],l1.[DeliveryRun],l1.[RunPosition]
,l1.[ReturnedDeliveryData],l1.[ConfirmedDeliveryTime],l1.[ConfirmedReceivedBy]
,l1.[InvoiceLineID],l1.[StockItemID],l1.[Description],l1.[PackageTypeID],l1.[Quantity]
,l1.[UnitPrice],l1.[TaxRate],l1.[TaxAmount],l1.[LineProfit],l1.[ExtendedPrice] FOR JSON PATH) Deviveryvalue

from 
(select i.[InvoiceID] ,i.[CustomerID],i.[BillToCustomerID],i.[OrderID],i.[DeliveryMethodID],
i.[ContactPersonID],i.[AccountsPersonID],i.[SalespersonPersonID],i.[PackedByPersonID],
i.[InvoiceDate],i.[CustomerPurchaseOrderNumber],i.[IsCreditNote],i.[CreditNoteReason],i.[Comments],i.[DeliveryInstructions],i.[InternalComments]
,i.[TotalDryItems],i.[TotalChillerItems],i.[DeliveryRun],i.[RunPosition]
,i.[ReturnedDeliveryData],i.[ConfirmedDeliveryTime],i.[ConfirmedReceivedBy]
,il.[InvoiceLineID],il.[StockItemID],il.[Description],il.[PackageTypeID],il.[Quantity]
,il.[UnitPrice],il.[TaxRate],il.[TaxAmount],il.[LineProfit],il.[ExtendedPrice] from Sales.Invoices i inner join
sales.InvoiceLines il on i.InvoiceID = il.InvoiceID AND i.InvoiceDate = @inputdate AND i.[CustomerID]=1) l1
----------------------------------------------------------------------------------------------------------------------

--get number of rows
DECLARE @numberofdate AS INT
select @numberofdate=count(distinct i.InvoiceDate) from Sales.Invoices i join
sales.InvoiceLines il on i.InvoiceID = il.InvoiceID AND i.CustomerID =1

DECLARE @cnt INT = 0;
WHILE @cnt < @numberofdate
BEGIN
	DECLARE @dateinput AS DATE

	select distinct @dateinput = i.InvoiceDate from Sales.Invoices i join
	sales.InvoiceLines il on i.InvoiceID = il.InvoiceID AND i.CustomerID =1
	order by i.InvoiceDate
	OFFSET @cnt ROWS 
	FETCH NEXT 1 ROWS ONLY

	EXEC loadinvoice @dateinput
	SET @cnt = @cnt + 1;
END;
select *from ods.ConfirmedDeviveryJson