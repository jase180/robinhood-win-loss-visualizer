# for running SQLite query tests to make sure SQL is working

import sqlite3
from sql_functions_local import *
import pandas as pd

def grab_clean_data(filepath):
    print("Connecting to database...")  
    connection = sqlite3.connect('database.db')
    cursor = connection.cursor()

    print("Creating table...") 
    create_table(cursor)

    print("Importing data from CSV...")  
    csv_file_path = filepath
    import_data_from_csv(cursor, csv_file_path)

    print("Querying data...")  
    data = query_data(cursor)

    connection.close()

    # print("Data fetched from DB)")  
    # print(data)
    return data


data = grab_clean_data('data/robinhood_data.csv')
print('data grab complete')

