-- Drop and Overwrite existing temptable 
DROP TABLE IF EXISTS #temptable;

-- Select necessary columns and perform calculations
SELECT 
    [Activity_Date],
    [Instrument],
    [Description],
    [Trans_Code],
    SUM(CAST([Quantity] AS FLOAT)) AS TotalQuantity,
    AVG(CAST(Price AS FLOAT)) AS AvgPrice,
    SUM(CAST(Amount AS FLOAT)) AS Amount
-- Store the result in a temporary table
INTO #TempTable
FROM [Robinhood_data].[dbo].[newer data]
GROUP BY
    [Activity_Date],  -- Include Activity_Date in the GROUP BY clause
    [Instrument],
    [Description],
    [Trans_Code];

-- Display the result from the temporary table
SELECT DISTINCT Trans_CODE FROM #TempTable;
