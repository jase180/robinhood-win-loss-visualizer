from flask import Flask, render_template, request, jsonify
import sqlite3
import csv
import io
from sql_functions import *

app = Flask(__name__)


# query_clean_data is now unnecessary since processing data on-demand rather than from a local file
# def query_clean_data():  
#     print("Connecting to database...")  
#     connection = sqlite3.connect('database.db')
#     cursor = connection.cursor()

#     print("Querying data...")  
#     data = query_data(cursor)  # This should be defined in sql_functions.py

#     connection.close()
#     return data

@app.route('/')
def index():
    print("Rendering index page...")
    return render_template('index.html')  # Load page without querying data

# Route to handle CSV data uploaded via the modal
@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    try:
        data = request.get_json()
        csv_data = data['csv']

        # Convert the CSV string into a list of rows
        csv_reader = csv.reader(io.StringIO(csv_data))
        
        # Open an in-memory database connection
        connection = sqlite3.connect(':memory:')
        cursor = connection.cursor()
        
        # Create the table
        create_table(cursor)

        # Import CSV data into the database
        import_data_from_csv(cursor, csv_reader)

        # Query and process the data
        processed_data = query_data(cursor)

        connection.close()

        # Return the processed data as JSON to update the table
        return jsonify({'status': 'success', 'data': processed_data}), 200
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
