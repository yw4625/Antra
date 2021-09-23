--11. List all the cities that were updated after 2015-01-01.
select CityName from [Application].Cities WHERE ValidFrom BETWEEN '2015-01-01' AND GETDATE()
------------------------------------------------------------------------

--12 List all the Order Detail (Stock Item name, delivery address, delivery state, city, country, customer name, 
--customer contact person name, customer phone, quantity) for the date of 2014-07-01. Info should be relevant to that date.
select ol.OrderID,si.StockItemName,ol.Quantity, cus.DeliveryAddressLine1, cus.DeliveryAddressLine2,
ci.CityName,s.StateProvinceCode, c.CountryName,cus.CustomerName,p.FullName as contactperson, cus.PhoneNumber
from Sales.OrderLines ol
join Warehouse.StockItems si on si.StockItemID = ol.StockItemID
join Sales.Orders o on o.OrderID = ol.OrderID
join [Application].People p on o.ContactPersonID = p.PersonID
join sales.Customers cus on o.CustomerID = cus.CustomerID
join [Application].Cities ci ON ci.CityID = cus.DeliveryCityID
JOIN [Application].StateProvinces S ON ci.StateProvinceID = S.StateProvinceID
join [Application].Countries c on c.CountryID = s.CountryID
where o.OrderDate ='2014-07-01'

--13.	List of stock item groups and total quantity purchased, total quantity sold, 
--and the remaining stock quantity (quantity purchased – quantity sold)
select q1.StockGroupName,q1.totalpurchase,q2.totalsell,q1.totalpurchase-q2.totalsell As remainstock from 
(SELECT sg.StockGroupName, sum(st.Quantity) totalpurchase FROM Warehouse.StockGroups sg
join Warehouse.StockItemStockGroups sisg on sg.StockGroupID = sisg.StockGroupID
join Warehouse.StockItems si on si.StockItemID = sisg.StockItemID
join Warehouse.StockItemTransactions st on st.StockItemID = si.StockItemID
where st.Quantity>0
group by sg.StockGroupName) q1 join

(SELECT sg.StockGroupName, abs(sum(st.Quantity)) totalsell FROM Warehouse.StockGroups sg
join Warehouse.StockItemStockGroups sisg on sg.StockGroupID = sisg.StockGroupID
join Warehouse.StockItems si on si.StockItemID = sisg.StockItemID
join Warehouse.StockItemTransactions st on st.StockItemID = si.StockItemID
where st.Quantity<0
group by sg.StockGroupName) q2 on q1.StockGroupName = q2.StockGroupName

--14.	List of Cities in the US and the stock item that the city got the most deliveries in 2016. 
--If the city did not purchase any stock items in 2016, print “No Sales”.
with delivery_CTE(CityID, StockItemName)
AS
(
select l3.CityID, min(l3.StockItemName) As itemname from
(select *, RANK() OVER (PARTITION BY l2.CityID ORDER BY l2.numberdelivery DESC) rank from
(select CityID,l1.StockItemName,sum(l1.Quantity) as numberdelivery from
(select si.StockItemName,ol.Quantity,ci.CityName,ci.CityID from Sales.OrderLines ol
join Warehouse.StockItems si on si.StockItemID = ol.StockItemID
join Sales.Orders o on o.OrderID = ol.OrderID AND year(o.OrderDate) = 2016
join [Application].People p on o.ContactPersonID = p.PersonID
join sales.Customers cus on o.CustomerID = cus.CustomerID
join [Application].Cities ci ON ci.CityID = cus.DeliveryCityID)l1
group by l1.CityID, l1.StockItemName)l2)l3
where l3.rank =1
group by l3.CityID
)

select c.CityID, isnull(d.StockItemName, 'No Sales')as mostsale from delivery_CTE d
right join [Application].Cities c on d.CityID = c.CityID
--------------------------------------------------------------------------------------------

--15.	List any orders that had more than one delivery attempt (located in invoice table).
select l1.OrderID,l1.Comment from(
select i.OrderID, JSON_VALUE (i.ReturnedDeliveryData,'$.Events[1].Event') AS deliveryattempt, 
JSON_VALUE (i.ReturnedDeliveryData,'$.Events[1].Status') AS deliverystatus,
JSON_VALUE (i.ReturnedDeliveryData,'$.Events[1].Comment') AS Comment
from sales.Invoices i )l1 where l1.deliveryattempt is Not NULL AND l1.deliverystatus is NULL
--------------------------------------------------------------------------------------------

--16.	List all stock items that are manufactured in China. (Country of Manufacture)
select l1.StockItemName, l1.Origin from
(SELECT *, JSON_VALUE(si.CustomFields,'$.CountryOfManufacture') AS Origin FROM Warehouse.StockItems si) l1
where l1.origin = 'China'
--------------------------------------------------------------------------------------------

--17.	Total quantity of stock items sold in 2015, group by country of manufacturing.
select l1.origin, count(l1.origin) numbersold from Warehouse.StockItemTransactions st join 
(SELECT *, JSON_VALUE(si.CustomFields,'$.CountryOfManufacture') AS origin FROM Warehouse.StockItems si
) l1 on l1.StockItemID = st.StockItemID AND year(st.TransactionOccurredWhen) = 2015
Group By l1.origin
--------------------------------------------------------------------------------------------

--18 Create a view that shows the total quantity of stock items of each stock group sold (in orders) by year 2013-2017. 
--[Stock Group Name, 2013, 2014, 2015, 2016, 2017]
create view stockbyyear as

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
for stockyear in([2013],[2014],[2015],[2016]) 
)As pvi

select * from stockbyyear

-- 19.	Create a view that shows the total quantity of stock items of each stock group sold (in orders) 
--by year 2013-2017. [Year, Stock Group Name1, Stock Group Name2, Stock Group Name3, … , Stock Group Name10] 
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

select *from stockbygroup order by stockyear
----------------------------------------------------------------------------------

--20.	Create a function, input: order id; return: total of that order. 
--List invoices and use that function to attach the order total to the other fields of invoices. 

CREATE FUNCTION dbo.totaloforder (@IDinput int)  
RETURNS float As
Begin
	DECLARE @OrderTotal float; 
	SELECT @OrderTotal=sum(ol.Quantity*ol.UnitPrice*(1+ol.TaxRate/100)) from Sales.OrderLines ol
	where ol.OrderID= @IDinput group by ol.OrderID 

	IF (@OrderTotal IS NULL)   
		SET @OrderTotal = 0

	RETURN @OrderTotal
End;

select i.OrderID, dbo.totaloforder(i.OrderID) OrderTotal from sales.Invoices i;
----------------------------------------------------------------------------------