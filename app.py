import sqlite3
from sql_functions import *
from flask import Flask, render_template

app = Flask(__name__)

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

    print("Data fetched from DB)")  
    return data

@app.route('/')
def index():
    print("Rendering index page...")  
    data = grab_clean_data('data/robinhood_data.csv')
    return render_template('index.html', data=data)

if __name__ == '__main__':
    app.run(debug=True)
