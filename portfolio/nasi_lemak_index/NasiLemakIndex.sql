--Create PriceCatcher table if not exist
CREATE TABLE PortfolioProject.dbo.PriceTracker (
	date DATE,
	state VARCHAR (100) NOT NULL,
	item_group VARCHAR(100) NOT NULL, 
	avg_price_per_unit FLOAT NOT NULL, 
	per_unit_metric VARCHAR(50) NOT NULL
);

-- Data cleaning and transformation procedures
-- 1. Create temptable that contains the number lists to be used for cleaning
IF OBJECT_ID(N'tempdb..#NumberList') IS NOT NULL
BEGIN
DROP TABLE #NumberList
END;

WITH CTE
AS (
SELECT 1 AS n
UNION ALL 
SELECT n + 1
FROM CTE WHERE n <= 3999
)

SELECT n INTO #NumberList
FROM CTE OPTION (MAXRECURSION 3999);

-- 2. Join table and select items related to Nasi Lemak and insert into temporary table
IF OBJECT_ID(N'tempdb..#ItemPrice2') IS NOT NULL
BEGIN
DROP TABLE #ItemPrice2
END;

SELECT p.date, j.state, i.item, p.price, i.unit
INTO #ItemPrice2
FROM PortfolioProject..PriceCatcher2022_12 AS p
LEFT JOIN PortfolioProject..LookupItem AS i
ON p.item_code = i.item_code
LEFT JOIN PortfolioProject..LookupPremises As j
ON p.premise_code = j.premise_code
WHERE i.item LIKE'%SANTAN KELAPA%'
OR i.item LIKE '%BERAS CAP%'
OR i.item LIKE '%CILI KERING%'
OR i.item LIKE '%TELUR AYAM GRED%'
OR i.item LIKE '%IKAN BILIS%'
OR i.item LIKE '%HALIA%'
OR i.item LIKE '%TIMUN%'
OR i.item LIKE '%KACANG TANAH%'
OR i.item LIKE '%BAWANG BESAR%'
OR i.item LIKE '%BAWANG KECIL MERAH%'
OR i.item LIKE '%BAWANG PUTIH%'
OR i.item LIKE '%BELACAN%'
OR i.item LIKE '%MINYAK MASAK TULEN%'
OR i.item LIKE '%GARAM%'
OR i.item LIKE '%GULA PUTIH BERTAPIS%'
OR i.item LIKE '%ASAM JAWA%'

-- 2.1 check #ItemPrice2
SELECT date, state, item, price, unit
FROM #ItemPrice2
WHERE  state = 'Johor'
AND item LIKE '%BAWANG%'
ORDER BY 1


--3. Clean and transform the data and insert into temp table #ItemPrice3
IF OBJECT_ID(N'tempdb..#ItemPrice3') IS NOT NULL
BEGIN
DROP TABLE #ItemPrice3
END;

;WITH CTE AS (SELECT date, state,
	CASE
		WHEN item LIKE '%SANTAN KELAPA%' THEN 'Coconut Milk'
		WHEN item LIKE '%BERAS CAP%' THEN 'Rice'
		WHEN item LIKE '%CILI KERING%' THEN 'Dried Chillies'
		WHEN item LIKE '%TELUR AYAM GRED%' THEN 'Egg'
		WHEN item LIKE '%IKAN BILIS%' THEN 'Anchovies'
		WHEN item LIKE '%HALIA%' THEN 'Ginger'
		WHEN item LIKE '%TIMUN%' THEN 'Cucumber'
		WHEN item LIKE '%KACANG TANAH%' THEN 'Peanut'
		WHEN item LIKE '%BAWANG BESAR%' THEN 'Onion'
		WHEN item LIKE '%BAWANG KECIL MERAH%' THEN 'Shallot'
		WHEN item LIKE '%BAWANG PUTIH%' THEN 'Garlic'
		WHEN item LIKE '%BELACAN%' THEN 'Roasted Belacan'
		WHEN item LIKE '%MINYAK MASAK TULEN%' THEN 'Cooking Oil'
		WHEN item LIKE '%GARAM%' THEN 'Salt'
		WHEN item LIKE '%GULA%' THEN 'Sugar'
		WHEN item LIKE '%ASAM JAWA%' THEN 'Tamarind Paste'
	END AS item_group,
	(
		SELECT SUBSTRING(unit, n, 1) FROM
		#NumberList
		WHERE SUBSTRING(unit, n, 1) LIKE '[0-9]' 
		FOR xml PATH ('') 
	) AS numbers,
	(
		SELECT SUBSTRING(unit, n, 1) FROM
		#NumberList 
		WHERE SUBSTRING(unit, n, 1) LIKE '[A-Z]' 
		FOR xml PATH ('') 
	)AS letters,
	price, unit
	FROM #ItemPrice2
	WHERE state <> ''
)
SELECT i.date , i.state, i.item_group, i.price_per_unit, i.per_unit_metric,
	AVG(i.price_per_unit) OVER (PARTITION BY MONTH(i.date), i.state, i.item_group) AS avg_price_per_unit,
	ROW_NUMBER() OVER (PARTITION BY MONTH(i.date), i.state, i.item_group ORDER BY i.date DESC, i.state, i.item_group) AS row_no
INTO #ItemPrice3
FROM
(
	SELECT date, state, item_group, 
	CASE
		WHEN letters LIKE '%kg%' THEN price/(CAST(numbers AS float) * 1000)
		WHEN letters LIKE '%g%' THEN price/CAST(numbers AS float)
		WHEN letters LIKE '%biji%' THEN price/CAST(numbers AS float)
		WHEN letters LIKE '%liter%' THEN price/(CAST(numbers AS float) * 1000)
		WHEN letters LIKE '%ml%' THEN price/CAST(numbers AS float)
	END AS price_per_unit,
	CASE
		WHEN letters LIKE '%kg%' THEN 'MYR/g'
		WHEN letters LIKE '%biji%' THEN 'MYR/unit'
		WHEN letters LIKE '%ml%' THEN 'MYR/ml'
		WHEN letters LIKE '%g%' THEN 'MYR/g'
		WHEN letters LIKE '%liter%' THEN 'MYR/ml'
	END AS per_unit_metric,
	price, unit
	FROM CTE
) AS i
ORDER BY 1, 2

--4. Calculate average price per unit and moving average using windows function
SELECT date, state, item_group, avg_price_per_unit, 
per_unit_metric
FROM #ItemPrice3
WHERE row_no = 1
ORDER BY state

--4)Insert into PriceTracker Table
INSERT INTO PortfolioProject..PriceTracker
(date, state, item_group, avg_price_per_unit, per_unit_metric)
SELECT date, state, item_group, avg_price_per_unit, 
per_unit_metric
FROM #ItemPrice3
WHERE row_no = 1
AND item_group LIKE '%ONION%'

--5. Check PriceTrackerTable
SELECT *
FROM PortfolioProject..PriceTracker
ORDER BY state

--DELETE FROM PortfolioProject..PriceTracker

--6. Join PriceTrackerTable with IngredientsTable to get Average price of nasi lemak
SELECT t.*,
	SUM(t.price_per_item) OVER (PARTITION BY MONTH(t.date), t.state)
FROM
(
	SELECT 
		p.* , 
		i.normalize_measurement as measurement, 
		(p.avg_price_per_unit * i.normalize_measurement)/5 AS price_per_item
	FROM PortfolioProject..PriceTracker AS p
	LEFT JOIN PortfolioProject..Ingredients AS i
	ON p.item_group = i.ingredients
	WHERE p.item_group NOT LIKE '%GINGER%'
) As t
