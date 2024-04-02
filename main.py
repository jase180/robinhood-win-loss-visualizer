import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def getData(csv):
    df = pd.read_csv(csv)
    return df

def findSpreads(df):

    #Filter df for closes only
    closes_df = df[df['Trans Code'].isin(['STC','BTC'])]

    #Filter df for opens only
    opens_df = df[df['Trans Code'].isin(['STO','BTO'])]
    
    #Match and merge based on 'Description', which contains Ticker, Date, and Strike Price
    merged_df = pd.merge(closes_df,opens_df,on='Description',suffixes=('_closes','_opens'))

    #Convert prices and amounts to numeric (also removing $ and , sign) and make errors become 'NaN'

    merged_df['Price_closes'] = pd.to_numeric(merged_df['Price_closes'].str.replace('$', '').str.replace(',',''), errors='coerce')
    merged_df['Price_opens'] = pd.to_numeric(merged_df['Price_opens'].str.replace('$', '').str.replace(',',''), errors='coerce')

    merged_df['Amount_closes'] = pd.to_numeric(merged_df['Amount_closes'].str.replace('$', '').str.replace(',',''), errors='coerce')
    merged_df['Amount_opens'] = pd.to_numeric(merged_df['Amount_opens'].str.replace('$', '').str.replace(',',''), errors='coerce')

    #Calculate price and amount differences and add column

    merged_df['Price Difference'] = merged_df['Price_closes'] - merged_df['Price_opens']
    merged_df['Amount Difference'] = merged_df['Amount_closes'] - merged_df['Amount_opens']

    #Create Out DF with wanted columns
    out_columns = ['Settle Date_opens', 'Settle Date_closes', 'Description', 'Quantity_closes', 'Trans Code_closes', 'Price_opens', 'Price_closes', 'Price Difference', 'Amount_opens', 'Amount_closes', 'Amount Difference']
    out_df = merged_df[out_columns]
  
    # #Add Win/Loss column based on pos/neg price diff
    # if out_df['Amount Difference'] > 0:
    #   out_df['Win/Loss'] = 'W'
    # else:
    #   out_df['Win/Loss'] = 'L'
        
    return out_df

def main():

    #Parse CSV file from specific path into pandas dataframe
    csv_file_path = 'data/robinhood_data.csv'
    df = getData(csv_file_path)

    #Finds and compiles all spreads and Win/Loss
    df_out = findSpreads(df)

    #display graphics - table and graphs
    print(df_out)
    
    # printTable
    # displayGraphs
  
main()