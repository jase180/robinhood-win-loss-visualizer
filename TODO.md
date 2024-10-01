Add Pandas column for better access

Something like this:

cursor.execute('SELECT * FROM CombinedTable')
rows = cursor.fetchall()

# Fetch the column names from the cursor description
column_names = [description[0] for description in cursor.description]

# Create the DataFrame using the rows and column names
df = pd.DataFrame(rows, columns=column_names)



Plot seaborn and graph
