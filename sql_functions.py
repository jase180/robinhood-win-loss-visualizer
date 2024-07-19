import sqlite3
import csv
from datetime import datetime

def create_table(cursor):
    print("Executing create table statement...")  
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS csv_data (
        "Activity Date" TEXT,
        "Process Date" TEXT,
        "Settle Date" TEXT,
        "Instrument" TEXT,
        "Description" TEXT,
        "Trans Code" TEXT,
        "Quantity" TEXT,
        "Price" TEXT,
        "Amount" TEXT
    )
''')

def format_date(date_str):
    return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')

def format_amount(amount):
    ls = []
    for character in amount:
        if character.isdigit() or character == '.' or character == '-':
            ls.append(character)
    return ''.join(ls)

def import_data_from_csv(cursor, filepath): #also formats dates to acommodate SQLite
    with open(filepath, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)  # Skip header row
        print("CSV header:", header)

        for row in reader:
            row[0] = format_date(row[0])  # Format Activity Date
            row[1] = format_date(row[1])  # Format Process Date
            row[2] = format_date(row[2])  # Format Settle Date
            row[8] = format_amount(row[8])  # Format Amount to take away comma and $ signs

            cursor.execute('INSERT INTO csv_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', row[:9])


def query_data(cursor):
    print("Dropping TempTable if exists...")  
    cursor.execute('DROP TABLE IF EXISTS TempTable')

    print("Creating TempTable...")  
    cursor.execute('''
        CREATE TEMP TABLE TempTable AS
        SELECT 
            DATE("Activity Date") AS "Activity Date",
            DATE("Process Date") AS "Process Date",
            DATE("Settle Date") AS "Settle Date",
            Instrument,
            Description,
            "Trans Code",
            Quantity,
            Price,
            Amount, 
            SUBSTR(Description, 0, INSTR(Description, ' Put')) AS NewDescription,
            SUBSTR(Description, INSTR(Description, ' ') + 1, INSTR(Description, 'Put') - INSTR(Description, ' ') - 1) AS StrikeDate,
            CAST(SUBSTR(Description, INSTR(Description, '$') + 1) AS FLOAT) AS StrikePrice
        FROM 
            csv_data 
        WHERE 
            "Trans Code" IN ('STC', 'BTC', 'BTO', 'STO') 
            AND Description LIKE '%Put%'
        GROUP BY
            SUBSTR(Description, 0, INSTR(Description, ' Put')),
            SUBSTR(Description, INSTR(Description, ' ') + 1, INSTR(Description, 'Put') - INSTR(Description, ' ') - 1),
            CAST(SUBSTR(Description, INSTR(Description, '$') + 1) AS FLOAT),
            "Activity Date",
            "Process Date",
            "Settle Date",
            Instrument,
            Description,
            "Trans Code"
    ''')
    cursor.execute('SELECT * FROM TempTable')
    rows = cursor.fetchall()
    print("TempTable rows after creation:", rows)

    print("CREATE PUTS MATCHED TABLE OPENS")  
    cursor.execute('''
        CREATE TABLE MatchedTableOpens AS
        SELECT 
            BTO."Activity Date",
            BTO."Process Date",
            BTO."Settle Date",
            BTO.StrikeDate AS "Strike Date",
            BTO.Instrument,
            BTO.Description AS "BTO Description",
            BTO.Quantity AS "BTO Quantity",
            BTO.Price AS "BTO Avg. Price",
            BTO.Amount AS "BTO Amount",
            BTO.NewDescription,
            BTO.StrikePrice AS "BTO Price",
            STO.Description AS "STO Description",
            STO.Quantity AS "STO Quantity",
            STO.Price AS "STO Avg. Price",
            STO.Amount AS "STO Amount",
            STO.StrikePrice AS "STO Price"

        FROM 
            TempTable BTO
        LEFT JOIN 
            TempTable STO ON STO.NewDescription = BTO.NewDescription
                          AND STO."Activity Date" = BTO."Activity Date"
        WHERE 
            BTO."Trans Code" = 'BTO' AND STO."Trans Code" = 'STO'
        ORDER BY DATE(BTO."Activity Date") DESC
    ''')
    cursor.execute('SELECT * FROM MatchedTableOpens')
    rows = cursor.fetchall()
    print("MatchedTableOpens rows after creation:", rows)

    # Create MatchedTableCloses to identify matched BTC and STC transactions
    print("CREATE PUTS MATCHED TABLE CLOSES")  
    cursor.execute('''
        CREATE TABLE MatchedTableCloses AS
        SELECT 
            BTC."Activity Date",
            BTC."Process Date",
            BTC."Settle Date",
            BTC.StrikeDate AS "Strike Date",
            BTC.Instrument,
            BTC.Description AS "BTC Description",
            BTC.Quantity AS "BTC Quantity",
            BTC.Price AS "BTC Avg. Price",
            BTC.Amount AS "BTC Amount",
            BTC.NewDescription,
            BTC.StrikePrice AS "BTC Price",
            STC.Description AS "STC Description",
            STC.Quantity AS "STC Quantity",
            STC.Price AS "STC Avg. Price",
            STC.Amount AS "STC Amount",
            STC.StrikePrice AS "STC Price"
        FROM 
            TempTable BTC
        LEFT JOIN 
            TempTable STC ON STC.NewDescription = BTC.NewDescription
                          AND STC."Activity Date" = BTC."Activity Date"
        WHERE 
            BTC."Trans Code" = 'BTC' AND STC."Trans Code" = 'STC'
        ORDER BY DATE(BTC."Activity Date") DESC
    ''')
    cursor.execute('SELECT * FROM MatchedTableCloses')
    rows = cursor.fetchall()
    print("MatchedTableCloses rows after creation:", rows)

    # Create CombinedTable to match MatchedTableOpens and MatchedTableCloses based on Description
    print("CREATE COMBINED TABLE")  
    cursor.execute('''
        CREATE TABLE CombinedTable AS
        SELECT
            Opens."Activity Date" AS "Open Activity Date",
            Opens.Instrument AS "Open Instrument",
            Opens."BTO Quantity" AS "Open Buy Quantity",
            Opens."BTO Avg. Price" AS "Open Buy Avg. Price",
            Opens."BTO Amount" AS "Open Buy Amount",
            Opens.NewDescription AS "Open New Description",
            Opens."BTO Price" AS "Open Buy Price",
            Opens."STO Quantity" AS "Open Sell Quantity",
            Opens."STO Avg. Price" AS "Open Sell Avg. Price",
            Opens."STO Amount" AS "Open Sell Amount",
            Opens."STO Price" AS "Open Sell Price",
            Closes."Activity Date" AS "Close Activity Date",
            Closes."BTC Quantity" AS "Close Buy Quantity",
            Closes."BTC Avg. Price" AS "Close Buy Avg. Price",
            Closes."BTC Amount" AS "Close Buy Amount",
            Closes.NewDescription AS "Close New Description",
            Closes."BTC Price" AS "Close Buy Price",
            Closes."STC Quantity" AS "Close Sell Quantity",
            Closes."STC Avg. Price" AS "Close Sell Avg. Price",
            Closes."STC Amount" AS "Close Sell Amount",
            Closes."STC Price" AS "Close Sell Price",
            (Opens."BTO Amount" + Opens."STO Amount") AS "Entry Credit",
            (Opens."BTO Amount" + Opens."STO Amount") + (Closes."BTC Amount" + Closes."STC Amount") AS "Return",
            CASE 
                WHEN (Opens."BTO Amount" + Opens."STO Amount") + (Closes."BTC Amount" + Closes."STC Amount") > 0 THEN 'Win'
                ELSE 'Loss'
            END AS "Win/Loss"                   
        FROM
            MatchedTableOpens Opens
        LEFT JOIN
            MatchedTableCloses Closes ON Opens.NewDescription = Closes.NewDescription
    ''')
    cursor.execute('SELECT * FROM CombinedTable')
    rows = cursor.fetchall()
    print("CombinedTable rows after creation:", rows)
    return rows




    # cursor.execute('''
    #     CREATE TABLE CombinedTable AS
    #     SELECT
    #         "Activity Date",
    #         Instrument,
    #         "Strike Date",
    #         "BTO Description" AS "Buy Description",
    #         "STO Description" AS "Sell Description",
    #         "BTO Quantity" AS "Quantity",
    #         ROUND(CAST(REPLACE(REPLACE("BTO Amount", '$', ''), ',', '') AS FLOAT) +
    #             CAST(REPLACE(REPLACE("STO Amount", '$', ''), ',', '') AS FLOAT), 2) AS "Credit Received"
    #     FROM 
    #         MatchedTable
    # ''')
    # rows = cursor.fetchall()
    # print("Final Select:", rows)
    # return rows