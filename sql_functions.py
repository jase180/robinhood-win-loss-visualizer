import sqlite3
import csv

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

def import_data_from_csv(cursor, filepath):
    print("Opening CSV file...")  
    with open(filepath, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row

        print("Inserting data from CSV into the database...")  
        for row in reader:
            cursor.execute('INSERT INTO csv_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', row[:9])

def query_data(cursor):
    print("Dropping TempTable if exists...")  
    cursor.execute('DROP TABLE IF EXISTS TempTable')

    print("Creating TempTable...")  
    cursor.execute('''
        CREATE TEMP TABLE TempTable AS
        SELECT 
            "Activity Date",
            "Process Date",
            "Settle Date",
            Instrument,
            Description,
            "Trans Code",
            CAST(SUM(CAST(Quantity AS FLOAT)) AS TEXT) AS Quantity,
            CAST(AVG(CAST(Price AS FLOAT)) AS TEXT) AS AvgPrice,
            CAST(SUM(CAST(Amount AS FLOAT)) AS TEXT) AS Amount,
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

    print("Querying data from TempTable...")  
    cursor.execute('''
        SELECT 
            BTO."Activity Date",
            BTO."Process Date",
            BTO."Settle Date",
            BTO.StrikeDate AS 'Strike Date',
            BTO.Instrument,
            BTO.Description AS 'BTO Description',
            BTO.Quantity AS 'BTO Quantity',
            BTO.AvgPrice AS 'BTO Avg. Price',
            BTO.Amount AS 'BTO Amount',
            BTO.NewDescription,
            BTO.StrikePrice AS 'BTO Price',
            STO.Description AS 'STO Description',
            STO.Quantity AS 'STO Quantity',
            STO.AvgPrice AS 'STO Avg. Price',
            STO.Amount AS 'STO Amount',
            STO.StrikePrice AS 'STO Price'
        FROM 
            TempTable BTO
        LEFT JOIN 
            TempTable STO ON STO.NewDescription = BTO.NewDescription
                          AND STO."Activity Date" = BTO."Activity Date"
        WHERE 
            BTO."Trans Code" = 'BTO' AND STO."Trans Code" = 'STO'
        ORDER BY DATE (BTO."Activity Date") DESC
    ''')

    rows = cursor.fetchall()
    print(rows)
    return rows
