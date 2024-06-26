-- Drop and Overwrite existing temptable 
DROP TABLE IF EXISTS #temptable;

-- Select necessary columns and perform calculations
SELECT 
    Activity_Date,
    [Process_Date],
    [Settle_Date],
    Instrument,
    Description,
    [Trans_Code],
    -- Calculate the total quantity, average price, and total amount
    SUM(CAST(Quantity AS FLOAT)) AS Quantity,
    AVG(CAST(Price AS FLOAT)) AS AvgPrice,
    SUM(CAST(Amount AS FLOAT)) AS Amount, 
    -- Extract information from the Description column
    -- NewDescription: Extracting the part of Description before " Put" string
    SUBSTRING(Description, 0, PATINDEX('% Put%', Description)) AS NewDescription,
    -- StrikeDate: Extracting the date from Description between spaces
    SUBSTRING(Description, PATINDEX('% %', Description) + 1, PATINDEX('%Put%', Description) - PATINDEX('% %', Description) - 1) AS StrikeDate,
    -- StrikePrice: Extracting the price from Description after "$" sign
    CAST(RIGHT(Description, LEN(Description) - PATINDEX('%$%', Description)) AS FLOAT) AS StrikePrice
-- Store the result in a temporary table
INTO #TempTable
FROM dbo.[newest robinhood data] 
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

-- Select data from the temporary table and perform joins
SELECT 
    -- Selecting columns from the BTO (Buy to Open) table
    BTO.[Activity_Date],
    BTO.[Process_Date],
    BTO.[Settle_Date],
    BTO.StrikeDate AS 'Strike Date',
    BTO.Instrument,
    BTO.Description AS 'BTO Description',
    BTO.Quantity AS 'BTO Quantity',
    BTO.AvgPrice  AS 'BTO Avg. Price',
    BTO.Amount AS 'BTO Amount',
    BTO.NewDescription,
    BTO.StrikePrice AS 'BTO Price',
    -- Selecting columns from the STO (Sell to Open) table
    STO.Description AS 'STO Description',
    STO.Quantity AS 'STO Quantity',
    STO.AvgPrice AS 'STO Avg. Price',
    STO.Amount AS 'STO Amount',
    STO.StrikePrice  AS 'STO Price',
    -- Selecting columns from the subquery (for BTC and STC)
    C.*
FROM 
    #TempTable BTO
-- Perform left join with the STO table
LEFT JOIN 
    #TempTable STO ON STO.NewDescription = BTO.NewDescription
                   AND STO.[Activity_Date] = BTO.[Activity_Date]
-- Left join with the subquery to get BTC and STC data
LEFT JOIN 
    -- Subquery to fetch BTC and STC data
    (SELECT 
         BTC.[Activity_Date],
         BTC.[Process_Date],
         BTC.[Settle_Date],
         BTC.StrikeDate AS 'Strike Date',
         BTC.Instrument,
         BTC.Description AS 'BTC Description',
         BTC.Quantity AS 'BTC Quantity',
         BTC.AvgPrice  AS 'BTC Avg. Price',
         BTC.Amount AS 'BTC Amount',
         BTC.NewDescription,
         BTC.StrikePrice AS 'BTC Price',
         STC.Description AS 'STC Description',
         STC.Quantity AS 'STC Quantity',
         STC.AvgPrice AS 'STC Avg. Price',
         STC.Amount AS 'STC Amount',
         STC.StrikePrice  AS 'STC Price'
     FROM 
         #TempTable BTC
     LEFT JOIN 
         #TempTable STC ON STC.NewDescription = BTC.NewDescription
                       AND STC.[Activity_Date] = BTC.[Activity_Date]
     WHERE 
         BTC.[Trans_Code] = 'BTC' AND STC.[Trans_Code] = 'STC')  C ON C.NewDescription = BTO.NewDescription
-- Filter only BTO and STO transactions
WHERE 
    BTO.[Trans_Code] = 'BTO' AND STO.[Trans_Code] = 'STO';
