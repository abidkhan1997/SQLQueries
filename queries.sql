USE AdventureWorks2017;
GO
SELECT CompanyName,AddressType,AddressLine1
FROM Customer JOIN CustomerAddress
ON (Customer.CustomerID=CustomerAddress.CustomerID)
JOIN Address
ON (CustomerAddress.AddressID=Address.AddressID)
WHERE CompanyName='Modular Cycle Systems'
--------------------------------------------------
SELECT OrderQty,Name,ListPrice
FROM SalesOrderHeader JOIN SalesOrderDetail
ON SalesOrderDetail.SalesOrderID = SalesOrderHeader.SalesOrderID
JOIN Product
ON SalesOrderDetail.ProductID=Product.ProductID
WHERE CustomerID=635
---------------------------------------------------------

USE AdventureWorksDW2017;
GO

SELECT *
FROM dbo.DimEmployee;
--------------------------------------
SELECT e.EmployeeKey, e.FirstName, e.LastName,
 fr.SalesAmount
FROM dbo.DimEmployee AS e
 INNER JOIN dbo.FactResellerSales AS fr
  ON e.EmployeeKey = fr.EmployeeKey;
--------------------------------------------------
SELECT e.EmployeeKey, e.FirstName, e.LastName,
 r.ResellerKey, r.ResellerName,
 d.DateKey, d.CalendarYear, d.CalendarQuarter,
 p.ProductKey, p.EnglishProductName,
 ps.EnglishProductSubcategoryName,
 pc.EnglishProductCategoryName,
 fr.OrderQuantity, fr.SalesAmount
FROM dbo.DimEmployee AS e
 INNER JOIN dbo.FactResellerSales AS fr
  ON e.EmployeeKey = fr.EmployeeKey
 INNER JOIN dbo.DimReseller AS r
  ON r.ResellerKey = fr.ResellerKey
 INNER JOIN dbo.DimDate AS d
  ON fr.OrderDateKey = d.DateKey
 INNER JOIN dbo.DimProduct AS p
  ON fr.ProductKey = p.ProductKey
 INNER JOIN dbo.DimProductSubcategory AS ps
  ON p.ProductSubcategoryKey = ps.ProductSubcategoryKey
 INNER JOIN dbo.DimProductCategory AS pc
  ON ps.ProductCategoryKey = pc.ProductCategoryKey;
-------------------------------------------------------------------------------------
SELECT e.EmployeeKey, e.FirstName, e.LastName,
 fr.SalesAmount
FROM dbo.DimEmployee AS e
 LEFT OUTER JOIN dbo.FactResellerSales AS fr
  ON e.EmployeeKey = fr.EmployeeKey;
  
---------------------------------------------------------------------------------------
SELECT e.EmployeeKey, e.FirstName, e.LastName,
 r.ResellerKey, r.ResellerName,
 d.DateKey, d.CalendarYear, d.CalendarQuarter,
 p.ProductKey, p.EnglishProductName,
 ps.EnglishProductSubcategoryName,
 pc.EnglishProductCategoryName,
 fr.OrderQuantity, fr.SalesAmount
FROM (dbo.FactResellerSales AS fr
 INNER JOIN dbo.DimReseller AS r
  ON r.ResellerKey = fr.ResellerKey
 INNER JOIN dbo.DimDate AS d
  ON fr.OrderDateKey = d.DateKey
 INNER JOIN dbo.DimProduct AS p
  ON fr.ProductKey = p.ProductKey
 INNER JOIN dbo.DimProductSubcategory AS ps
  ON p.ProductSubcategoryKey = ps.ProductSubcategoryKey
 INNER JOIN dbo.DimProductCategory AS pc
  ON ps.ProductCategoryKey = pc.ProductCategoryKey)
 RIGHT OUTER JOIN dbo.DimEmployee AS e
  ON e.EmployeeKey = fr.EmployeeKey;
------------------------------------------------------------------------------------------
SELECT e.EmployeeKey,
 MIN(e.LastName) AS LastName,
 SUM(fr.OrderQuantity)AS EmpTotalQuantity,
 SUM(fr.SalesAmount) AS EmpTotalAmount
FROM dbo.DimEmployee AS e
 INNER JOIN dbo.FactResellerSales AS fr
  ON e.EmployeeKey = fr.EmployeeKey
GROUP BY e.EmployeeKey;
-----------------------------------------------------------------------------------------

SELECT e.EmployeeKey,
 MIN(e.LastName) AS LastName,
 SUM(fr.OrderQuantity)AS EmpTotalQuantity,
 SUM(fr.SalesAmount) AS EmpTotalAmount
FROM dbo.DimEmployee AS e
 INNER JOIN dbo.FactResellerSales AS fr
  ON e.EmployeeKey = fr.EmployeeKey
GROUP BY e.EmployeeKey
HAVING SUM(fr.OrderQuantity) > 10000
ORDER BY EmpTotalQuantity DESC;

----------------------------------------------------------------------------------------------
USE northwind2020;
GO

select OrderID, 
    format(sum(UnitPrice * Quantity * (1 - Discount)), 2) as Subtotal
from order_details
group by OrderID
order by OrderID;
-------------------------------------------------------------------------------------------------
select distinct date(a.ShippedDate) as ShippedDate, 
    a.OrderID, 
    b.Subtotal, 
    year(a.ShippedDate) as Year
from Orders a 
inner join
(
    -- Get subtotal for each order
    select distinct OrderID, 
        format(sum(UnitPrice * Quantity * (1 - Discount)), 2) as Subtotal
    from order_details
    group by OrderID    
) b on a.OrderID = b.OrderID
where a.ShippedDate is not null
    and a.ShippedDate between date('1996-12-24') and date('1997-09-30')
order by a.ShippedDate;
-----------------------------------------------------------------------------------------------------

select distinct b.*, a.CategoryName
from Categories a 
inner join Products b on a.CategoryID = b.CategoryID
where b.Discontinued = 'N'
order by b.ProductName;
------------------------------------------------------------------------------------------------------
select distinct b.*, a.Category_Name
from Categories a 
inner join Products b on a.Category_ID = b.Category_ID
where b.Discontinued = 'N'
order by b.Product_Name;
---------------------------------------------------------------------------------------------

USE WideWorldImporters;
GO
SET NOCOUNT ON
GO

-- The following query can be used to sum the stock yet to be picked for all stock items.

DECLARE @StartingTime datetime2(7) = SYSDATETIME();

SELECT ol.StockItemID, [Description], SUM(Quantity - PickedQuantity) AS AllocatedQuantity
FROM Sales.OrderLines AS ol WITH (NOLOCK)
GROUP BY ol.StockItemID, [Description];

PRINT 'Using nonclustered columnstore index: ' + CAST(DATEDIFF(millisecond, @StartingTime, SYSDATETIME()) AS varchar(20)) + ' ms';

SET @StartingTime = SYSDATETIME();

SELECT ol.StockItemID, [Description], SUM(Quantity - PickedQuantity) AS AllocatedQuantity
FROM Sales.OrderLines AS ol WITH (NOLOCK)
GROUP BY ol.StockItemID, [Description]
OPTION (IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX);

PRINT 'Without nonclustered columnstore index: ' + CAST(DATEDIFF(millisecond, @StartingTime, SYSDATETIME()) AS varchar(20)) + ' ms';
GO
------------------------------------------------------------------------------------------------------------------
USE WideWorldImporters;
GO


EXEC [Application].Configuration_ApplyPolybase;
GO

SELECT CityID, StateProvinceCode, CityName, YearNumber, LatestRecordedPopulation FROM dbo.CityPopulationStatistics;
GO
--------------------------------------------------------------------------------------------------------------------

WITH PotentialCities
AS
(
	SELECT cps.CityName,
	       cps.StateProvinceCode,
		   MAX(cps.LatestRecordedPopulation) AS PopulationIn2016,
		   (MAX(cps.LatestRecordedPopulation) - MIN(cps.LatestRecordedPopulation)) * 100.0
		       / MIN(cps.LatestRecordedPopulation) AS GrowthRate
	FROM dbo.CityPopulationStatistics AS cps
	WHERE cps.LatestRecordedPopulation IS NOT NULL
	AND cps.LatestRecordedPopulation <> 0
	GROUP BY cps.CityName, cps.StateProvinceCode
)
SELECT CityName, StateProvinceCode, PopulationIn2016, GrowthRate
FROM PotentialCities
WHERE GrowthRate > 2.0;
GO
------------------------------------------------------------------------------------------------------------
WITH PotentialCities
AS
(
	SELECT cps.CityName,
	       cps.StateProvinceCode,
		   MAX(cps.LatestRecordedPopulation) AS PopulationIn2016,
		   (MAX(cps.LatestRecordedPopulation) - MIN(cps.LatestRecordedPopulation)) * 100.0
		       / MIN(cps.LatestRecordedPopulation) AS GrowthRate
	FROM dbo.CityPopulationStatistics AS cps
	WHERE cps.LatestRecordedPopulation IS NOT NULL
	AND cps.LatestRecordedPopulation <> 0
	GROUP BY cps.CityName, cps.StateProvinceCode
),
InterestingCities
AS
(
	SELECT DISTINCT pc.CityName,
					pc.StateProvinceCode,
				    pc.PopulationIn2016,
					FLOOR(pc.GrowthRate) AS GrowthRate
	FROM PotentialCities AS pc
	INNER JOIN Dimension.City AS c
	ON pc.CityName = c.City
	WHERE GrowthRate > 2.0
	AND NOT EXISTS (SELECT 1 FROM Fact.Sale AS s WHERE s.[City Key] = c.[City Key])
)
SELECT TOP(100) CityName, StateProvinceCode, PopulationIn2016, GrowthRate
FROM InterestingCities
ORDER BY PopulationIn2016 DESC;
GO
------------------------------------------------------------------------------
DROP EXTERNAL TABLE dbo.CityPopulationStatistics;
GO
DROP EXTERNAL FILE FORMAT CommaDelimitedTextFileFormat;
GO
DROP EXTERNAL DATA SOURCE AzureStorage;
GO
-------------------------------------------------------------------------------------







