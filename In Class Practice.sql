-- List of Customers in the state of mississippi and total numbers of orders in 2015
SELECT CUS.CustomerID, Count(o.OrderID) AS TotalOrders FROM Sales.Orders o
JOIN Sales.Customers cus ON cus.CustomerID = o.CustomerID
JOIN [Application].Cities ci ON ci.CityID = cus.DeliveryCityID
JOIN [Application].StateProvinces S ON ci.StateProvinceID = S.StateProvinceID
WHERE s.StateProvinceCode = 'MS' AND year(o.OrderDate) = 2015
GROUP BY CUS.CustomerID
--------------------------------------------------------------------

-- List of all US Postal Codes and the number of customers in each Postal Code
SELECT PostalPostalCode, Count(CustomerID) AS totalnumber from Sales.Customers cus GROUP BY PostalPostalCode
--------------------------------------------------------------------

-- List of stockgroup and the customer who bought most stockitems of that stockgroup
SELECT L3.StockGroupID,L3.CustomerID FROM
(SELECT *, RANK() OVER (PARTITION BY L2.StockGroupID ORDER BY L2.Nooforder DESC) rank1 FROM 
(SELECT L1.CustomerID, L1.StockGroupID, Count(L1.StockGroupID) NoOforder FROM 
(SELECT sg.StockGroupID,sg.StockGroupName, cust.CustomerID,cust.CustomerName FROM Warehouse.StockGroups sg
JOIN Warehouse.StockItemStockGroups sisg ON sg.StockGroupID = sisg.StockGroupID
JOIN Warehouse.StockItems si ON si.StockItemID = sisg.StockItemID
JOIN Sales.OrderLines ol on ol.StockItemID = si.StockItemID
JOIN Sales.Orders o on o.OrderID = ol.OrderID
JOIN Sales.Customers cust on cust.CustomerID = o.CustomerID) L1 
GROUP BY L1.StockGroupID, L1.CustomerID) L2) L3
WHERE L3.rank1=1
--------------------------------------------------------------------

-- List of stockitems that belongs to more than 1 stockgroup
SELECT SISG.StockItemID, SISG.NoofGroups from
(SELECT Distinct StockItemID, count(StockGroupID) over (partition by StockItemID) AS NoofGroups from Warehouse.StockItemStockGroups) SISG
WHERE SISG.NoofGroups>1
--------------------------------------------------------------------

-- Numbers of customer by State as of the date of '2015-01-01', Numbers of customer by State as of the date of today, and the different between two values 
select distinct q1.StateProvinceName,q1.noofcustomer2015,q2.noofcustometoday,  q2.noofcustometoday-q1.noofcustomer2015 As increase from 
(select distinct st.StateProvinceName, count(c.CustomerID) over (partition by st.StateProvinceName) AS noofcustomer2015 from Sales.Customers 
For System_time as of '2015-01-01'c 
jOIN [Application].Cities ci ON ci.CityID = c.DeliveryCityID
join [Application].StateProvinces st on st.StateProvinceID = ci.StateProvinceID) As q1 JOIN 
(select distinct st.StateProvinceName, count(c.CustomerID) over (partition by st.StateProvinceName) AS noofcustometoday from Sales.Customers c 
jOIN [Application].Cities ci ON ci.CityID = c.DeliveryCityID
join [Application].StateProvinces st on st.StateProvinceID = ci.StateProvinceID) As q2 ON q1.StateProvinceName = q2.StateProvinceName
--------------------------------------------------------------------

-- List of manufacture contries and count of kinds of stockitems
select l1.origin, count(l1.origin) as numberofkinds from
(SELECT JSON_VALUE(si.CustomFields,'$.CountryOfManufacture') AS origin FROM Warehouse.StockItems si) L1
group by l1.origin
--------------------------------------------------------------------

-- List of manufacture contries and count of kinds of stockitems, but using contries as columns
select * from(
select l1.origin, count(l1.origin) as numberofkinds from
(SELECT JSON_VALUE(si.CustomFields,'$.CountryOfManufacture') AS origin FROM Warehouse.StockItems si) L1
group by l1.origin) orig
pivot 
(
sum([numberofkinds])
for origin in([China],[Japan],[USA]) 
)As pvi
--------------------------------------------------------------------

-- (using invoice, orders) list of distinct customers whose order has more than one delivery attempt
select distinct l1.CustomerID, l1.Comment from(
select i.OrderID, i.CustomerID, JSON_VALUE (i.ReturnedDeliveryData,'$.Events[1].Event') AS deliveryattempt, 
JSON_VALUE (i.ReturnedDeliveryData,'$.Events[1].Status') AS deliverystatus,
JSON_VALUE (i.ReturnedDeliveryData,'$.Events[1].Comment') AS Comment
from sales.Invoices i )l1 where l1.deliveryattempt is Not NULL AND l1.deliverystatus is NULL