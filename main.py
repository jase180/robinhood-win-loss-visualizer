import sqlite3
import csv
from sql_functions import *

def grab_clean_data(filepath):
    connection = sqlite3.connect('database.db')
    cursor = connection.cursor()

    create_table(cursor)

    csv_file_path = filepath
    import_data_from_csv(cursor,csv_file_path)

    query_data(cursor)
    
    connection.close()

def main():
    grab_clean_data('data/robinhood_data.csv')



main()