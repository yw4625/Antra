/* 1.List of Persons’ full name, all their fax and phone numbers, as well as the phone number 
and fax of the company they are working for (if any). */

SELECT c.CustomerName, c.PhoneNumber,c.FaxNumber,bg.BuyingGroupName Company, p.PhoneNumber, p.FaxNumber FROM Sales.Customers c
join [Application].People p on c.PrimaryContactPersonID = p.PersonID
left join sales.BuyingGroups bg on bg.BuyingGroupID  = c.BuyingGroupID
-------------------------------------------------------------

/*
2.	If the customer's primary contact person has the 
same phone number as the customer’s phone number, list the customer companies. 
*/
SELECT A.CustomerName, p.FullName As Primarycontactname ,p.PhoneNumber from Sales.Customers A
join [Application].People p on a.PrimaryContactPersonID = p.PersonID where a.PhoneNumber = p.PhoneNumber
-------------------------------------------------------------------

--3.List of customers to whom we made a sale prior to 2016 but no sale since 2016-01-01.
select q1.CustomerID,prior2016,since2016 from
(select c.CustomerID,count(c.CustomerID) As prior2016 from sales.Customers c
join sales.Orders o on (o.CustomerID = c.CustomerID AND o.OrderDate< '2016-01-01') 
group by c.CustomerID) As q1 join
(select c.CustomerID,count(c.CustomerID) As since2016 from sales.Customers c
join sales.Orders o on o.CustomerID = c.CustomerID AND o.OrderDate>= '2016-01-01' 
group by c.CustomerID) As q2 on q1.CustomerID =q2.CustomerID
where prior2016>0 AND since2016 = 0
-------------------------------------------------------------------

--4.List of Stock Items and total quantity for each stock item in Purchase Orders in Year 2013.
select si.StockItemName,  SUM(st.Quantity) OVER(PARTITION BY si.StockItemID) AS totalquantityinPO from Warehouse.StockItems si
join Warehouse.StockItemTransactions st on si.StockItemID = st.StockItemID AND st.PurchaseOrderID IS NOT NULL
join Purchasing.PurchaseOrders po on po.PurchaseOrderID = st.PurchaseOrderID AND year(po.OrderDate) = 2013

--------------------------------------------------------------------------
--5.List of stock items that have at least 10 characters in description.
SELECT StockItemName FROM WideWorldImporters.Warehouse.StockItems WHERE LEN(SearchDetails) >= 10
--------------------------------------------------------------------------
--6.List of stock items that are not sold to the state of Alabama and Georgia in 2014.
SELECT DISTINCT si.StockItemName FROM Warehouse.StockItems SI
JOIN Sales.OrderLines ol ON OL.StockItemID = SI.StockItemID
JOIN Sales.Orders o on o.OrderID = ol.OrderID
JOIN Sales.Customers cus ON cus.CustomerID = o.CustomerID
JOIN [Application].Cities ci ON ci.CityID = cus.DeliveryCityID
JOIN [Application].StateProvinces S ON ci.StateProvinceID = S.StateProvinceID
WHERE s.StateProvinceCode NOT IN ('AL', 'GA') AND year(o.OrderDate) = 2014
--------------------------------------------------------------------------
--7.List of States and Avg dates for processing (confirmed delivery date – order date)
SELECT L1.StateProvinceCode, avg(DATEDIFF(day,L1.OrderDate, L1.ExpectedDeliveryDate)) as Processing FROM 
(SELECT s.StateProvinceID,S.StateProvinceCode, PO.OrderDate, PO.ExpectedDeliveryDate from Purchasing.PurchaseOrders po
JOIN Purchasing.Suppliers sup ON Sup.SupplierID = po.SupplierID
JOIN [Application].Cities ci ON ci.CityID = sup.DeliveryCityID
JOIN [Application].StateProvinces S ON ci.StateProvinceID = S.StateProvinceID) l1
GROUP BY L1.StateProvinceCode
---------------------------------------------------------------------------------
--8.List of States and Avg dates for processing (confirmed delivery date – order date) by month.
SELECT L1.StateProvinceCode, avg(DATEDIFF(day,L1.OrderDate, L1.ExpectedDeliveryDate)) as Processing,L1.ordermonth FROM 
(SELECT s.StateProvinceID,S.StateProvinceCode, PO.OrderDate, PO.ExpectedDeliveryDate, DATEPART(month,PO.OrderDate) As ordermonth 
from Purchasing.PurchaseOrders po
JOIN Purchasing.Suppliers sup ON Sup.SupplierID = po.SupplierID
JOIN [Application].Cities ci ON ci.CityID = sup.DeliveryCityID
JOIN [Application].StateProvinces S ON ci.StateProvinceID = S.StateProvinceID) l1
GROUP BY L1.StateProvinceCode, L1.ordermonth
ORDER BY L1.StateProvinceCode, L1.ordermonth
------------------------------------------------------------------------

--9 List of StockItems that the company purchased more than sold in the year of 2015.
select si.StockItemName,q1.quantchange from Warehouse.StockItems si join
(select st.StockItemID, sum(st.Quantity) quantchange from Warehouse.StockItemTransactions st 
group by st.StockItemID) As q1 on q1.StockItemID = si.StockItemID
where q1.quantchange>0
------------------------------------------------------------------------

/*10.	List of Customers and their phone number, 
together with the primary contact person’s name, 
to whom we did not sell more than 10  mugs (search by name) in the year 2016.
*/
select c.CustomerID,c.CustomerName,c.PhoneNumber, p.FullName As Primarycontact, l1.mugnumber from sales.Customers c
join 
(select c.CustomerID, count(c.CustomerID) Mugnumber from Warehouse.StockItems si 
join Sales.OrderLines ol on si.StockItemID = ol.StockItemID
join sales.Orders o on ol.OrderID = o.OrderID
join sales.Customers c on o.CustomerID = c.CustomerID
where si.SearchDetails like '%mug%' AND year(o.OrderDate) = 2016
group by c.CustomerID) l1 on l1.CustomerID = c.CustomerID
join [Application].People p on p.PersonID = c.PrimaryContactPersonID
where l1.mugnumber<10
------------------------------------------------------------------------