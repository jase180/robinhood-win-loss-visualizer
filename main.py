import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from IPython.display import display

def getData(csv):
    df = pd.read_csv(csv)
    return df

def processData(df):
    # Convert date columns to datetime objects
    date_columns = ['Activity Date', 'Process Date', 'Settle Date']
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # Remove commas and dollar signs from Amount column and convert to numeric
    df['Amount'] = df['Amount'].replace({'[$]': '', ',': ''}, regex=True).astype(float)

    return df

def findSpreads(df):

    # Filter for only buying or selling rows
    df = df[df['Trans Code'].isin(['BTC', 'STC', 'BTO', 'STO'])]

    #Filter for only PUTs
    df = df[df['Description'].isin('Put')]

    display(df)
    return df

    # Initialize an empty list to store spreads
    spreads = []

    # Iterate over the rows
    for index, row in df.iloc[:-1].iterrows():
        try:
            current_row_description = row['Description'].split()
            next_row_description = df.iloc[index + 1]['Description'].split()

            # Check if the descriptions (excluding the last part) match
            if current_row_description[:-1] == next_row_description[:-1]:
                # Append the current row and the next row as a spread
                spreads.append(pd.concat([row, df.iloc[index + 1]], axis=1))
        except (IndexError, AttributeError):
            # Handle any errors related to index out of range or invalid data types
            print(f"Error processing row {index}: {row}")
            print(current_row_description,next_row_description)
            print('end of error message for this')
            continue

    # Create a DataFrame from the list of spreads
    spreads_df = pd.concat(spreads, axis=1).T

    return spreads_df

    # filter all opens
    # filter only puts
    # combine opens if they match
    #filter all closes
    #combine closes if they match

def main():

    #Parse CSV file from specific path into pandas dataframe
    csv_file_path = 'data/robinhood_data.csv'
    df = getData(csv_file_path)

    df = processData(df)


    #Finds and compiles all spreads and Win/Loss
    df_out = findSpreads(df)

    #display graphics - table and graphs
    display(df_out)
    
    # printTable
    # displayGraphs
  
main()