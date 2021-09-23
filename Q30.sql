/*
30.	Write a short essay talking about a scenario: Good news everyone! We (Wide World Importers) 
just brought out a small company called ¡°Adventure works¡±! Now that bike shop is our sub-company. 
The first thing of all works pending would be to merge the user logon information, person information 
(including emails, phone numbers) and products (of course, add category, colors) to WWI database. 
Include screenshot, mapping and query.
*/

Use AdventureWorks2019

--add supplier category
INSERT INTO WideWorldImporters.Purchasing.SupplierCategories (SupplierCategoryName, LastEditedBy)
values('Bike Supplier',1)

--add vendor to supplier
INSERT INTO WideWorldImporters.Purchasing.Suppliers (SupplierName, SupplierCategoryID, PrimaryContactPersonID, 
AlternateContactPersonID, DeliveryCityID, PostalCityID, PaymentDays, PhoneNumber, FaxNumber,WebsiteURL, [DeliveryAddressLine1],
[DeliveryPostalCode],[PostalAddressLine1],[PostalPostalCode],[LastEditedBy])
select [Name], 11, 21, 22,38171,38171,14,'','','','','','','',1
FROM AdventureWorks2019.Purchasing.Vendor 
where [Name] COLLATE Latin1_General_100_CI_AI Not IN (select s.SupplierName from WideWorldImporters.Purchasing.Suppliers s)

--combine product category    Warehouse.StockGroups
Insert into WideWorldImporters.Warehouse.StockGroups (StockGroupName, LastEditedBy)
select [Name],1 from Production.ProductCategory
where [Name] COLLATE Latin1_General_100_CI_AI Not IN (select sg.StockGroupName from WideWorldImporters.Warehouse.StockGroups sg)

--insert item    Warehouse.StockItems
Insert into WideWorldImporters.Warehouse.StockItems ([StockItemName], [SupplierID],[ColorID],[UnitPackageID],
[OuterPackageID],[Size],[LeadTimeDays],[QuantityPerOuter],[IsChillerStock],[TaxRate],[UnitPrice],[RecommendedRetailPrice],
[TypicalWeightPerUnit],[LastEditedBy]
)

select concat (pro.[Name] COLLATE Latin1_General_100_CI_AI, ' (', s.SupplierName , ')'),
s.SupplierID,
ColorID,6 [UnitPackageID],7 [OuterPackageID], pro.Size,pv.AverageLeadTime, 
1 [QuantityPerOuter], 0 [IsChillerStock],0 [TaxRate],
pro.StandardCost, pro.ListPrice, 0, 1 from Production.Product pro
left join WideWorldImporters.Warehouse.Colors color on color.ColorName = pro.Color COLLATE Latin1_General_100_CI_AI
join Purchasing.ProductVendor pv on pv.ProductID = pro.ProductID
join Purchasing.Vendor v on pv.BusinessEntityID = v.BusinessEntityID
join WideWorldImporters.Purchasing.Suppliers s on s.SupplierName = v.[Name] COLLATE Latin1_General_100_CI_AI
where pro.[Name] COLLATE Latin1_General_100_CI_AI Not IN (select si.StockItemName from WideWorldImporters.Warehouse.StockItems si)


--assign product to category Warehouse.StockItemStockGroups
Insert into WideWorldImporters.Warehouse.StockItemStockGroups ([StockItemID],[StockGroupID],[LastEditedBy])
select s.StockItemID,sg.StockGroupID,1
from Production.Product pro
join Production.ProductModel pmodel on pmodel.ProductModelID = pro.ProductModelID
join Purchasing.ProductVendor pv on pv.ProductID = pro.ProductID
join Purchasing.Vendor v on pv.BusinessEntityID = v.BusinessEntityID
join Production.ProductSubcategory pscat on pscat.ProductSubcategoryID = pro.ProductSubcategoryID
join Production.ProductCategory pcat on pcat.ProductCategoryID = pscat.ProductCategoryID
join WideWorldImporters.Warehouse.StockItems s on s.StockItemName = concat (pro.[Name] COLLATE Latin1_General_100_CI_AI, ' (', v.[Name], ')')
join WideWorldImporters.Warehouse.StockGroups sg on sg.StockGroupName = pcat.[Name] COLLATE Latin1_General_100_CI_AI

---for people table
--merge to application.people
Insert into WideWorldImporters.[Application].People([FullName],[PreferredName],
[IsPermittedToLogon],[IsExternalLogonProvider],[HashedPassword],[IsSystemUser], 
[IsEmployee],[IsSalesperson],[PhoneNumber],[EmailAddress],[CustomFields],[LastEditedBy])

select CONCAT(p.FirstName, ' ', MiddleName,' ',p.LastName ) FullName,  
p.FirstName,0, 0, convert(varbinary(max),pwd.PasswordHash), 0,
case when p.PersonType='EM' THEN 1 ELSE 0 END as IsEmployee,
case when p.PersonType= 'SP' THEN 1 ELSE 0 END as IsSalesperson,
phone.PhoneNumber,e.EmailAddress,
(select emp.JobTitle, emp.HireDate FOR JSON PATH)  CustomFields,1
from Person.EmailAddress e
join Person.[Password] pwd on e.BusinessEntityID = pwd.BusinessEntityID
join Person.PersonPhone phone on e.BusinessEntityID = phone.BusinessEntityID
join Person.Person p on e.BusinessEntityID = p.BusinessEntityID
join HumanResources.Employee emp on emp.BusinessEntityID = p.BusinessEntityID