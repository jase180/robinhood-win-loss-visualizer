-- Drop and Overwrite existing temptable 
DROP TABLE IF EXISTS #temptable;
-- Select necessary columns and perform calculations
SELECT [Activity_Date]
      ,[Instrument]
      ,[Description]
      ,[Trans_Code]
	,SUM(CAST([Quantity] AS FLOAT)) AS TotalQuantity
    ,AVG(CAST(Price AS FLOAT)) AS AvgPrice
    ,SUM(CAST(Amount AS FLOAT)) AS Amount
    -- Calculate the total quantity, average price, and total amount

-- Store the result in a temporary table
INTO #TempTable
FROM [Robinhood_data].[dbo].[robinhood_data]

WHERE [Trans_Code] IN ('STC', 'BTC', 'BTO', 'STO')
    AND Description LIKE '%Put%'
GROUP BY
    -- Group by these columns
    SUBSTRING(Description, 0, PATINDEX('% Put%', Description)),
    SUBSTRING(Description, PATINDEX('% %', Description) + 1, PATINDEX('%Put%', Description) - PATINDEX('% %', Description) - 1),
    CAST(RIGHT(Description, LEN(Description) - PATINDEX('%$%', Description)) AS FLOAT),
    [Activity_Date],
    [Process_Date],
    [Settle_Date],
    Instrument,
    Description,
    [Trans_Code];

SELECT * FROM #TempTable