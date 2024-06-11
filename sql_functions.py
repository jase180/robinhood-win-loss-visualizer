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

def import_data_from_csv(cursor, filepath): #also formats dates to acommodate SQLite
    with open(filepath, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)  # Skip header row
        print("CSV header:", header)

        for row in reader:
            row[0] = format_date(row[0])  # Format Activity Date
            row[1] = format_date(row[1])  # Format Process Date
            row[2] = format_date(row[2])  # Format Settle Date

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

    print("CREATE PUTS MATCHED TABLE")  
    cursor.execute('''
        --CREATE TABLE MatchedTable AS
        SELECT 
            BTO."Activity Date",
            BTO."Process Date",
            BTO."Settle Date",
            BTO.StrikeDate AS 'Strike Date',
            BTO.Instrument,
            BTO.Description AS 'BTO Description',
            BTO.Quantity AS 'BTO Quantity',
            BTO.Price AS 'BTO Avg. Price',
            BTO.Amount AS 'BTO Amount',
            BTO.NewDescription,
            BTO.StrikePrice AS 'BTO Price',
            STO.Description AS 'STO Description',
            STO.Quantity AS 'STO Quantity',
            STO.Price AS 'STO Avg. Price',
            STO.Amount AS 'STO Amount',
            STO.StrikePrice AS 'STO Price'
        FROM 
            TempTable BTO
        LEFT JOIN 
            TempTable STO ON STO.NewDescription = BTO.NewDescription
                          AND STO."Activity Date" = BTO."Activity Date"
        WHERE 
            BTO."Trans Code" = 'BTO' AND STO."Trans Code" = 'STO'
        ORDER BY DATE(BTO."Activity Date") DESC
    ''')

    rows = cursor.fetchall()
    return rows
