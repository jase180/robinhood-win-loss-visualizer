import pandas as pd

# Read data from the testdata CSV file (replace 'testdata.csv' with your file path)
df = pd.read_csv('testdata.csv')

# Filter rows where Trans Code is in ('STC', 'BTC', 'BTO', 'STO') and Description contains 'Put'
df_filtered = df[df['Trans Code'].isin(['STC', 'BTC', 'BTO', 'STO']) & df['Description'].str.contains('Put')]

# Perform data cleaning and transformation
df_filtered['Quantity'] = df_filtered['Quantity'].astype(float)
df_filtered['Price'] = df_filtered['Price'].astype(float)
df_filtered['Amount'] = df_filtered['Amount'].astype(float)
df_filtered['NewDescription'] = df_filtered['Description'].apply(lambda x: x.split(' Put')[0])
df_filtered['StrikeDate'] = df_filtered['Description'].apply(lambda x: x.split()[-2])
df_filtered['StrikePrice'] = df_filtered['Description'].apply(lambda x: float(x.split('$')[-1]))

# Group by relevant columns and aggregate functions
grouped = df_filtered.groupby(['NewDescription', 'StrikeDate', 'StrikePrice', 'Activity Date', 
                                'Process Date', 'Settle Date', 'Instrument', 'Description', 'Trans Code'])
result = grouped.agg({'Quantity': 'sum', 'Price': 'mean', 'Amount': 'sum'}).reset_index()

# Select specific columns for BTO transactions
bto_transactions = result[result['Trans Code'] == 'BTO'].rename(columns={'Description': 'BTO Description',
                                                                       'Quantity': 'BTO Quantity',
                                                                       'Price': 'BTO Avg. Price',
                                                                       'Amount': 'BTO Amount'})

# Select specific columns for STO transactions
sto_transactions = result[result['Trans Code'] == 'STO'].rename(columns={'Description': 'STO Description',
                                                                       'Quantity': 'STO Quantity',
                                                                       'Price': 'STO Avg. Price',
                                                                       'Amount': 'STO Amount'})

# Perform left join between BTO and STO transactions
merged_transactions = pd.merge(bto_transactions, sto_transactions, on=['NewDescription', 'Activity Date'], how='left', suffixes=('_BTO', '_STO'))

# Select specific columns for BTC and STC transactions
btc_transactions = result[result['Trans Code'] == 'BTC'].rename(columns={'Description': 'BTC Description',
                                                                       'Quantity': 'BTC Quantity',
                                                                       'Price': 'BTC Avg. Price',
                                                                       'Amount': 'BTC Amount'})
stc_transactions = result[result['Trans Code'] == 'STC'].rename(columns={'Description': 'STC Description',
                                                                       'Quantity': 'STC Quantity',
                                                                       'Price': 'STC Avg. Price',
                                                                       'Amount': 'STC Amount'})

# Perform left join between BTC and STC transactions
merged_btc_stc = pd.merge(btc_transactions, stc_transactions, on=['NewDescription', 'Activity Date'], how='left', suffixes=('_BTC', '_STC'))

# Perform left join between BTO-STO transactions and BTC-STC transactions
final_result = pd.merge(merged_transactions, merged_btc_stc, on=['NewDescription', 'Activity Date'], how='left')

# Filter out BTO and STO transactions
final_result = final_result[(final_result['Trans Code_BTO'] == 'BTO') & (final_result['Trans Code_STO'] == 'STO')]

print(final_result)
