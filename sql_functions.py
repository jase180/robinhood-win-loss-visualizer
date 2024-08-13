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


#WORKING# OEXP will need to lok at 1 or 1S
# def format_special_transcode_transcode(trans):
#       if trans not in ['OASGN','OEXCS','OEXP']:
#         return trans
#     if trans == 'OASGN':
#         print('special transcode', next_row)
#         return next_row[8]
#     if trans == 'OEXCS':
#         print('special transcode', next_row)
#         return next_row[8]
#     if trans == 'OEXP':
#         return '$0.00'  

def format_special_transcode_amount(trans,amount,next_row):
    if trans not in ['OASGN','OEXCS','OEXP']:
        return amount
    if trans == 'OASGN':
        print('special transcode', next_row)
        return next_row[8]
    if trans == 'OEXCS':
        print('special transcode', next_row)
        return next_row[8]
    if trans == 'OEXP':
        return '$0.00'

def import_data_from_csv(cursor, filepath): #also formats dates to acommodate SQLite
    with open(filepath, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)  # Skip header row
        print("CSV header:", header)

        rows = list(reader)

        for i,row in enumerate(rows):
            next_row = rows[i+1] if i + 1< len(rows) else row # next_row variable for special transcodes.  If else statement to handle edge case if last row

            row[0] = format_date(row[0])  # Format Activity Date
            row[1] = format_date(row[1])  # Format Process Date
            row[2] = format_date(row[2])  # Format Settle Date
            row[8] = format_special_transcode_amount(row[5],row[8],next_row)
            # print(row)
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
            "Trans Code" IN ('STC', 'BTC', 'BTO', 'STO', 'OEXCS', 'OASGN', 'OEXP') 
            AND Description LIKE '%Put%'
        /* TEST NO GROUP BY */
        --GROUP BY
            -- SUBSTR(Description, 0, INSTR(Description, ' Put')),
            -- SUBSTR(Description, INSTR(Description, ' ') + 1, INSTR(Description, 'Put') - INSTR(Description, ' ') - 1),
            -- CAST(SUBSTR(Description, INSTR(Description, '$') + 1) AS FLOAT),
            -- "Activity Date",
            -- "Process Date",
            -- "Settle Date",
            -- Instrument,
            -- Description,
            -- "Trans Code"
    ''')
    cursor.execute('SELECT * FROM TempTable')
    rows = cursor.fetchall()
    print("TempTable rows after creation:", rows)
    
    # return rows

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
    
        # Create a OEXCS OASGN table then Union into the puts close table
    # Hint, use UNION ALL

    # Create CombinedTable to match MatchedTableOpens and MatchedTableCloses based on Description

    #ORRRRR

    #Replace all OASGN and OEXCS with BTC and STC so it'll capture correctly

    
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

